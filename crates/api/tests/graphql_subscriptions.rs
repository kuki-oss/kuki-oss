mod common;

use std::time::{Duration, Instant};

use chrono::Utc;
use futures::{FutureExt, SinkExt, StreamExt};
use kuki_outbox::{EventFeed, LiveEvent};
use kuki_store::recommendations::{NewRecommendation, upsert_pending};
use kuki_store::session::{OrgSession, with_org_tx};
use kuki_types as kt;
use serde_json::{Value, json};
use sqlx::PgPool;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use uuid::Uuid;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;

const CREATED: &str = "recommendation.created";
const CHANGED: &str = "recommendation.status_changed";

async fn spawn_server(app_pool: PgPool, feed: Option<EventFeed>) -> String {
    let mut app = common::app(app_pool);
    if let Some(f) = feed {
        app = app.layer(axum::Extension(f));
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    format!("ws://{addr}/v1/graphql")
}

async fn connect(url: &str) -> Socket {
    let mut req = url.into_client_request().unwrap();
    req.headers_mut().insert(
        "Sec-WebSocket-Protocol",
        "graphql-transport-ws".parse().unwrap(),
    );
    connect_async(req).await.unwrap().0
}

async fn send(ws: &mut Socket, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}

/// The next JSON text frame, or None when the server closed the socket.
async fn next_json(ws: &mut Socket) -> Option<Value> {
    while let Some(m) = ws.next().await {
        match m {
            Ok(Message::Text(t)) => return Some(serde_json::from_str(t.as_str()).unwrap()),
            Ok(Message::Close(_)) | Err(_) => return None,
            Ok(_) => {}
        }
    }
    None
}

async fn recv(ws: &mut Socket) -> Option<Value> {
    tokio::time::timeout(Duration::from_secs(20), next_json(ws))
        .await
        .expect("timed out waiting for the server")
}

async fn init(ws: &mut Socket, token: &str) -> Option<Value> {
    send(
        ws,
        json!({ "type": "connection_init", "payload": { "Authorization": format!("Bearer {token}") } }),
    )
    .await;
    recv(ws).await
}

async fn open(url: &str, token: &str) -> Socket {
    let mut ws = connect(url).await;
    let ack = init(&mut ws, token)
        .await
        .expect("socket closed during init");
    assert_eq!(ack["type"], "connection_ack", "{ack}");
    ws
}

async fn subscribe(ws: &mut Socket, query: &str) {
    send(
        ws,
        json!({ "id": "1", "type": "subscribe", "payload": { "query": query } }),
    )
    .await;
}

fn is_failure(m: &Value) -> bool {
    m["type"] == "error" || (m["type"] == "next" && m["payload"]["errors"].is_array())
}

fn event(
    org: Uuid,
    rec: Uuid,
    kind: &str,
    decision_type: &str,
    previous: Option<&str>,
) -> LiveEvent {
    LiveEvent {
        organisation_id: org,
        event_id: Uuid::new_v4().to_string(),
        event_type: kind.into(),
        payload: json!({
            "type": kind, "id": rec.to_string(), "version": 1, "status": "accepted",
            "previousStatus": previous, "decisionType": decision_type,
            "orgUnitId": null, "occurredAt": "2026-10-17T00:00:00Z",
        }),
    }
}

/// The server registers a subscription asynchronously and the feed does not replay, so publish
/// until the first delivery arrives. Events are processed in order: a wrongly delivered event
/// in front of the wanted one would be the first message.
async fn first_delivery(ws: &mut Socket, feed: &EventFeed, events: Vec<LiveEvent>) -> Value {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        for e in &events {
            feed.publish(e.clone());
        }
        if let Ok(m) = tokio::time::timeout(Duration::from_millis(400), next_json(ws)).await {
            let m = m.expect("socket closed");
            assert_eq!(m["type"], "next", "{m}");
            return m;
        }
        assert!(Instant::now() < deadline, "nothing was delivered");
    }
}

async fn seed_rec(app: &PgPool, org: Uuid, unit: Option<Uuid>, decision_type: &str) -> Uuid {
    let provenance = kt::Provenance {
        generator_id: "g".into(),
        generator_version: "1".into(),
        tier: kt::Tier::T0,
        pack: "p@1".into(),
        ontology_version: 1,
        config_hash: "h".into(),
        as_of: Utc::now(),
        input_digest: "d".into(),
        model_artefact: None,
    };
    let action = kt::Action {
        proposed: true,
        action_type: None,
        payload_schema_ref: "x".into(),
        payload: json!({}),
    };
    let rationale = kt::Rationale {
        summary: "s".into(),
        factors: vec![],
        confidence: 0.9,
        confidence_kind: kt::ConfidenceKind::DataQuality,
        counterfactual: None,
        narrative: None,
    };
    let id = Uuid::new_v4();
    let new = NewRecommendation {
        id,
        org_unit_id: unit,
        decision_type: decision_type.into(),
        dedup_key: format!("k-{id}"),
        urgency: 0.5,
        confidence: 0.9,
        rationale_summary: "s".into(),
        body: json!({ "action": action, "rationale": rationale, "impact": null }),
        provenance: serde_json::to_value(&provenance).unwrap(),
        expires_at: None,
    };
    with_org_tx::<_, sqlx::Error, _>(
        app,
        &OrgSession {
            org_id: org,
            scope_path: None,
        },
        move |c| async move { upsert_pending(c, org, &new, Utc::now()).await.map(|_| ()) }.boxed(),
    )
    .await
    .unwrap();
    id
}

fn id_of(m: &Value, field: &str) -> String {
    m["payload"]["data"][field]["id"]
        .as_str()
        .unwrap_or_default()
        .to_owned()
}

#[sqlx::test(migrations = "../../migrations")]
async fn connection_init_rejects_a_bad_key(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let url = spawn_server(app, Some(EventFeed::local(8))).await;
    let mut ws = connect(&url).await;
    let reply = init(&mut ws, "nope").await;
    assert!(
        reply.as_ref().is_none_or(|m| m["type"] != "connection_ack"),
        "a bad key must not be acknowledged: {reply:?}"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_key_without_decisions_read_cannot_subscribe(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let org = common::seed_org(&admin, "a").await;
    let token = common::seed_key(&admin, org, &["ingest:read"]).await;
    let url = spawn_server(app, Some(EventFeed::local(8))).await;

    let mut ws = open(&url, &token).await;
    subscribe(&mut ws, "subscription { recommendationCreated { id } }").await;
    let m = recv(&mut ws).await.expect("socket closed");
    assert!(is_failure(&m), "{m}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn subscriptions_are_unavailable_without_a_feed(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let org = common::seed_org(&admin, "a").await;
    let token = common::seed_key(&admin, org, &["decisions:read"]).await;
    let url = spawn_server(app, None).await;

    let mut ws = open(&url, &token).await;
    subscribe(&mut ws, "subscription { recommendationCreated { id } }").await;
    let m = recv(&mut ws).await.expect("socket closed");
    assert!(is_failure(&m), "{m}");
}

#[sqlx::test(migrations = "../../migrations")]
async fn status_changes_reach_a_subscriber_with_the_previous_status(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let org = common::seed_org(&admin, "a").await;
    let token = common::seed_key(&admin, org, &["decisions:read"]).await;
    let rec = seed_rec(&app, org, None, "inventory.replenish").await;
    let feed = EventFeed::local(64);
    let url = spawn_server(app, Some(feed.clone())).await;

    let mut ws = open(&url, &token).await;
    subscribe(
        &mut ws,
        "subscription { recommendationStatusChanged { previousStatus recommendation { id version decisionType } } }",
    )
    .await;
    let m = first_delivery(
        &mut ws,
        &feed,
        vec![event(
            org,
            rec,
            CHANGED,
            "inventory.replenish",
            Some("pending"),
        )],
    )
    .await;

    let change = &m["payload"]["data"]["recommendationStatusChanged"];
    assert_eq!(change["previousStatus"], "PENDING");
    assert_eq!(change["recommendation"]["id"], rec.to_string());
    assert_eq!(
        change["recommendation"]["decisionType"],
        "inventory.replenish"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn filters_and_tenancy_decide_what_is_delivered(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let (org, other) = (
        common::seed_org(&admin, "a").await,
        common::seed_org(&admin, "b").await,
    );
    let token = common::seed_key(&admin, org, &["decisions:read"]).await;
    let wanted = seed_rec(&app, org, None, "inventory.replenish").await;
    let wrong_type = seed_rec(&app, org, None, "scheduling.conflict").await;
    let foreign = seed_rec(&app, other, None, "inventory.replenish").await;
    let feed = EventFeed::local(64);
    let url = spawn_server(app, Some(feed.clone())).await;

    let mut ws = open(&url, &token).await;
    subscribe(
        &mut ws,
        r#"subscription { recommendationCreated(filter: { decisionTypes: ["inventory.replenish"] }) { id } }"#,
    )
    .await;
    let m = first_delivery(
        &mut ws,
        &feed,
        vec![
            // another organisation's event
            event(other, foreign, CREATED, "inventory.replenish", None),
            // our organisation's label on another organisation's recommendation: row-level
            // security must hide it even though the event passes the cheap checks
            event(org, foreign, CREATED, "inventory.replenish", None),
            // a decision type the subscription did not ask for
            event(org, wrong_type, CREATED, "scheduling.conflict", None),
            event(org, wanted, CREATED, "inventory.replenish", None),
        ],
    )
    .await;
    assert_eq!(id_of(&m, "recommendationCreated"), wanted.to_string());
}

#[sqlx::test(migrations = "../../migrations")]
async fn unit_scope_and_org_unit_path_apply(admin: PgPool) {
    let app = common::app_pool(&admin).await;
    let org = common::seed_org(&admin, "a").await;
    let east_unit = common::unit(&admin, org, "hq.east").await;
    let west_unit = common::unit(&admin, org, "hq.west").await;
    let east = seed_rec(&app, org, Some(east_unit), "inventory.replenish").await;
    let west = seed_rec(&app, org, Some(west_unit), "inventory.replenish").await;
    let feed = EventFeed::local(64);
    let url = spawn_server(app, Some(feed.clone())).await;
    let both = vec![
        event(org, west, CREATED, "inventory.replenish", None),
        event(org, east, CREATED, "inventory.replenish", None),
    ];

    // Organisation-wide key, narrowed by the subscription's own filter.
    let wide = common::seed_key(&admin, org, &["decisions:read"]).await;
    let mut ws = open(&url, &wide).await;
    subscribe(
        &mut ws,
        r#"subscription { recommendationCreated(filter: { orgUnitPath: "hq.east" }) { id } }"#,
    )
    .await;
    let m = first_delivery(&mut ws, &feed, both.clone()).await;
    assert_eq!(id_of(&m, "recommendationCreated"), east.to_string());

    // A key scoped to hq.east never sees hq.west, with no filter at all.
    let scoped = common::scoped_key(&admin, org, &["decisions:read"], "hq.east").await;
    let mut ws = open(&url, &scoped).await;
    subscribe(&mut ws, "subscription { recommendationCreated { id } }").await;
    let m = first_delivery(&mut ws, &feed, both).await;
    assert_eq!(id_of(&m, "recommendationCreated"), east.to_string());
}
