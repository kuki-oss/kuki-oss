use chrono::{DateTime, Utc};
use kuki_store::outbox::TOPIC_RECOMMENDATIONS;
use kuki_store::recommendations::{
    self as recs, NewRecommendation, Transition, TransitionError, Transitioned, Upserted,
};
use kuki_types::RecommendationStatus as S;
use serde_json::{Value, json};
use sqlx::{PgConnection, PgPool};
use uuid::Uuid;

fn t(s: &str) -> DateTime<Utc> {
    DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
}

fn rec(key: &str) -> NewRecommendation {
    NewRecommendation {
        id: Uuid::new_v4(),
        org_unit_id: None,
        decision_type: "work.escalate".into(),
        dedup_key: key.into(),
        urgency: 0.7,
        confidence: 0.8,
        rationale_summary: "Case 41 is three days overdue.".into(),
        body: json!({ "action": { "proposed": true } }),
        provenance: json!({ "as_of": "2026-10-11T00:00:00Z" }),
        expires_at: None,
    }
}

async fn events(c: &mut PgConnection) -> Vec<(String, Value)> {
    sqlx::query_as("SELECT key, payload FROM outbox ORDER BY id")
        .fetch_all(c)
        .await
        .unwrap()
}

fn move_to(id: Uuid, to: S) -> Transition<'static> {
    Transition {
        id,
        to,
        expected_version: None,
        actor: "user:amina",
        resolution: None,
    }
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_new_recommendation_is_pending_and_announced(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let now = t("2026-10-11T12:00:00Z");
    let new = rec("k1");

    let out = recs::upsert_pending(&mut c, org, &new, now).await.unwrap();
    assert_eq!(
        out,
        Upserted::Created {
            id: new.id,
            version: 1
        }
    );

    let row = recs::get(&mut c, org, new.id).await.unwrap().unwrap();
    assert_eq!(row.status, "pending");
    assert_eq!(row.urgency, 0.7);
    assert_eq!(row.confidence, 0.8);
    assert_eq!(row.created_at, now);
    assert_eq!(row.last_confirmed_at, now);
    assert_eq!(row.version, 1);

    let ev = events(&mut c).await;
    assert_eq!(ev.len(), 1);
    assert_eq!(ev[0].0, new.id.to_string(), "keyed by recommendation id");
    assert_eq!(ev[0].1["type"], "recommendation.created");
    assert_eq!(ev[0].1["id"], new.id.to_string());
    assert_eq!(ev[0].1["version"], 1);
    assert_eq!(ev[0].1["status"], "pending");
    assert_eq!(ev[0].1["decisionType"], "work.escalate");
    assert!(ev[0].1["previousStatus"].is_null());
    assert_eq!(ev[0].1["occurredAt"], "2026-10-11T12:00:00.000000Z");

    let topic: String = sqlx::query_scalar("SELECT topic FROM outbox")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(topic, TOPIC_RECOMMENDATIONS);
}

#[sqlx::test(migrations = "../../migrations")]
async fn an_identical_candidate_only_confirms(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let first = rec("k1");
    recs::upsert_pending(&mut c, org, &first, t("2026-10-11T12:00:00Z"))
        .await
        .unwrap();

    let mut again = rec("k1");
    again.provenance = json!({ "as_of": "2026-10-11T13:00:00Z" });
    again.urgency = 0.705; // within tolerance
    again.expires_at = Some(t("2026-10-20T00:00:00Z"));
    let later = t("2026-10-11T13:00:00Z");
    assert_eq!(
        recs::upsert_pending(&mut c, org, &again, later)
            .await
            .unwrap(),
        Upserted::Unchanged { id: first.id }
    );

    let row = recs::get(&mut c, org, first.id).await.unwrap().unwrap();
    assert_eq!(row.version, 1, "a confirmation is not a new version");
    assert_eq!(row.last_confirmed_at, later);
    assert_eq!(row.expires_at, Some(t("2026-10-20T00:00:00Z")));
    assert_eq!(
        row.provenance,
        json!({ "as_of": "2026-10-11T00:00:00Z" }),
        "provenance moves only on a revision"
    );
    assert_eq!(row.urgency, 0.7);
    assert_eq!(
        row.updated_at, row.created_at,
        "updated_at is untouched too"
    );
    assert_eq!(events(&mut c).await.len(), 1, "no event for a confirmation");
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_material_change_revises_in_place_and_keeps_history(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let first = rec("k1");
    recs::upsert_pending(&mut c, org, &first, t("2026-10-11T12:00:00Z"))
        .await
        .unwrap();

    let mut changed = rec("k1");
    changed.body = json!({ "action": { "proposed": false } });
    changed.rationale_summary = "Case 41 is now four days overdue.".into();
    let out = recs::upsert_pending(&mut c, org, &changed, t("2026-10-12T12:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        out,
        Upserted::Revised {
            id: first.id,
            version: 2
        }
    );

    let row = recs::get(&mut c, org, first.id).await.unwrap().unwrap();
    assert_eq!(row.body, changed.body);
    assert_eq!(row.rationale_summary, "Case 41 is now four days overdue.");
    assert_eq!(row.revision_history.as_array().unwrap().len(), 1);
    let old = &row.revision_history[0];
    assert_eq!(old["version"], 1);
    assert_eq!(old["body"], first.body);
    assert_eq!(old["rationaleSummary"], "Case 41 is three days overdue.");

    let ev = events(&mut c).await;
    assert_eq!(ev.len(), 2);
    assert_eq!(ev[1].1["type"], "recommendation.updated");
    assert_eq!(ev[1].1["version"], 2);

    // A urgency change beyond the tolerance is material too.
    let mut hotter = changed.clone();
    hotter.urgency = 0.95;
    let out = recs::upsert_pending(&mut c, org, &hotter, t("2026-10-13T12:00:00Z"))
        .await
        .unwrap();
    assert_eq!(
        out,
        Upserted::Revised {
            id: first.id,
            version: 3
        }
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn history_keeps_the_last_twenty_versions(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let mut id = None;
    for i in 0..=25 {
        let mut r = rec("k1");
        r.body = json!({ "n": i });
        let now = t("2026-10-11T00:00:00Z") + chrono::Duration::minutes(i);
        match recs::upsert_pending(&mut c, org, &r, now).await.unwrap() {
            Upserted::Created { id: new, .. } => id = Some(new),
            Upserted::Revised { .. } => {}
            other => panic!("{other:?}"),
        }
    }
    let row = recs::get(&mut c, org, id.unwrap()).await.unwrap().unwrap();
    assert_eq!(row.version, 26);
    let history = row.revision_history.as_array().unwrap();
    assert_eq!(history.len(), 20);
    assert_eq!(history[0]["version"], 6, "the oldest were dropped");
    assert_eq!(history[19]["version"], 25);
}

#[sqlx::test(migrations = "../../migrations")]
async fn a_resolved_recommendation_never_reopens(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let now = t("2026-10-11T12:00:00Z");
    let first = rec("k1");
    recs::upsert_pending(&mut c, org, &first, now)
        .await
        .unwrap();
    recs::transition(&mut c, org, &move_to(first.id, S::Rejected), now)
        .await
        .unwrap();

    let second = rec("k1");
    let out = recs::upsert_pending(&mut c, org, &second, now)
        .await
        .unwrap();
    assert_eq!(
        out,
        Upserted::Created {
            id: second.id,
            version: 1
        }
    );

    let statuses: Vec<String> = sqlx::query_scalar(
        "SELECT status FROM recommendations WHERE dedup_key = 'k1' ORDER BY created_at, status",
    )
    .fetch_all(&mut *c)
    .await
    .unwrap();
    assert_eq!(statuses, ["pending", "rejected"]);
}

#[sqlx::test(migrations = "../../migrations")]
async fn transitions_follow_the_state_machine_and_are_idempotent(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let now = t("2026-10-11T12:00:00Z");
    let r = rec("k1");
    recs::upsert_pending(&mut c, org, &r, now).await.unwrap();

    let note = json!({ "note": "ok" });
    let accept = Transition {
        id: r.id,
        to: S::Accepted,
        expected_version: None,
        actor: "user:amina",
        resolution: Some(&note),
    };
    let at = t("2026-10-11T14:00:00Z");
    assert_eq!(
        recs::transition(&mut c, org, &accept, at).await.unwrap(),
        Transitioned::Changed { version: 2 }
    );

    let row = recs::get(&mut c, org, r.id).await.unwrap().unwrap();
    assert_eq!(row.status, "accepted");
    assert_eq!(row.resolved_by.as_deref(), Some("user:amina"));
    assert_eq!(row.resolved_at, Some(at));
    assert_eq!(row.resolution, Some(note.clone()));

    // A retry of the same request writes nothing.
    assert_eq!(
        recs::transition(&mut c, org, &accept, at).await.unwrap(),
        Transitioned::Unchanged
    );
    assert_eq!(events(&mut c).await.len(), 2);

    let err = recs::transition(&mut c, org, &move_to(r.id, S::Rejected), at)
        .await
        .unwrap_err();
    assert!(
        matches!(
            err,
            TransitionError::Illegal {
                from: S::Accepted,
                to: S::Rejected
            }
        ),
        "{err:?}"
    );

    // Execution states do not overwrite who resolved it.
    let later = t("2026-10-11T15:00:00Z");
    let run = Transition {
        id: r.id,
        to: S::Executing,
        expected_version: None,
        actor: "system:executor",
        resolution: None,
    };
    assert_eq!(
        recs::transition(&mut c, org, &run, later).await.unwrap(),
        Transitioned::Changed { version: 3 }
    );
    let done = Transition {
        to: S::Executed,
        ..run
    };
    assert_eq!(
        recs::transition(&mut c, org, &done, later).await.unwrap(),
        Transitioned::Changed { version: 4 }
    );
    let row = recs::get(&mut c, org, r.id).await.unwrap().unwrap();
    assert_eq!(row.status, "executed");
    assert_eq!(row.resolved_by.as_deref(), Some("user:amina"));
    assert_eq!(row.resolved_at, Some(at));

    let ev = events(&mut c).await;
    assert_eq!(ev.len(), 4);
    assert_eq!(ev[1].1["type"], "recommendation.status_changed");
    assert_eq!(ev[1].1["status"], "accepted");
    assert_eq!(ev[1].1["previousStatus"], "pending");
    assert_eq!(ev[3].1["status"], "executed");
    assert_eq!(ev[3].1["version"], 4);
}

#[sqlx::test(migrations = "../../migrations")]
async fn stale_versions_and_unknown_ids_are_refused(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let now = t("2026-10-11T12:00:00Z");
    let r = rec("k1");
    recs::upsert_pending(&mut c, org, &r, now).await.unwrap();
    let mut changed = rec("k1");
    changed.body = json!({ "action": { "proposed": false } });
    recs::upsert_pending(&mut c, org, &changed, now)
        .await
        .unwrap(); // now at version 2

    let stale = Transition {
        id: r.id,
        to: S::Accepted,
        expected_version: Some(1),
        actor: "user:amina",
        resolution: None,
    };
    let err = recs::transition(&mut c, org, &stale, now)
        .await
        .unwrap_err();
    assert!(
        matches!(err, TransitionError::VersionConflict { current: 2 }),
        "{err:?}"
    );
    assert_eq!(
        recs::get(&mut c, org, r.id).await.unwrap().unwrap().status,
        "pending"
    );

    let fresh = Transition {
        expected_version: Some(2),
        ..stale
    };
    assert_eq!(
        recs::transition(&mut c, org, &fresh, now).await.unwrap(),
        Transitioned::Changed { version: 3 }
    );

    let err = recs::transition(&mut c, org, &move_to(Uuid::new_v4(), S::Accepted), now)
        .await
        .unwrap_err();
    assert!(matches!(err, TransitionError::NotFound));
    let err = recs::transition(&mut c, Uuid::new_v4(), &move_to(r.id, S::Rejected), now)
        .await
        .unwrap_err();
    assert!(
        matches!(err, TransitionError::NotFound),
        "another organisation's id reads as missing"
    );
}

#[sqlx::test(migrations = "../../migrations")]
async fn due_recommendations_expire_oldest_first(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let org = Uuid::new_v4();
    let now = t("2026-10-11T12:00:00Z");
    let mut ids = vec![];
    for (key, expires) in [
        ("k1", Some("2026-10-11T10:00:00Z")),
        ("k2", Some("2026-10-11T11:00:00Z")),
        ("k3", Some("2026-10-11T13:00:00Z")),
        ("k4", None),
    ] {
        let mut r = rec(key);
        r.expires_at = expires.map(t);
        recs::upsert_pending(&mut c, org, &r, now).await.unwrap();
        ids.push(r.id);
    }

    assert_eq!(
        recs::expire_due(&mut c, org, now, 1).await.unwrap(),
        [ids[0]],
        "the limit is respected"
    );
    assert_eq!(
        recs::expire_due(&mut c, org, now, 10).await.unwrap(),
        [ids[1]]
    );
    assert!(
        recs::expire_due(&mut c, org, now, 10)
            .await
            .unwrap()
            .is_empty()
    );

    let row = recs::get(&mut c, org, ids[0]).await.unwrap().unwrap();
    assert_eq!(row.status, "expired");
    assert_eq!(row.resolved_by.as_deref(), Some("system:expiry"));
    assert_eq!(row.resolved_at, Some(now));
    assert_eq!(row.resolution, Some(json!({ "reason": "expired" })));
    assert_eq!(
        recs::get(&mut c, org, ids[2])
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );
    assert_eq!(
        recs::get(&mut c, org, ids[3])
            .await
            .unwrap()
            .unwrap()
            .status,
        "pending"
    );

    let changed = events(&mut c)
        .await
        .iter()
        .filter(|e| e.1["type"] == "recommendation.status_changed")
        .count();
    assert_eq!(changed, 2);
}

#[sqlx::test(migrations = "../../migrations")]
async fn the_app_role_works_under_a_tenant_context_and_only_there(pool: PgPool) {
    let mut c = pool.acquire().await.unwrap();
    let (a, b) = (Uuid::new_v4(), Uuid::new_v4());
    let now = t("2026-10-11T12:00:00Z");
    sqlx::query("SET ROLE kuki_app")
        .execute(&mut *c)
        .await
        .unwrap();
    let context = |org: Uuid| {
        sqlx::query("SELECT set_config('kuki.org_id', $1, false)").bind(org.to_string())
    };

    context(a).execute(&mut *c).await.unwrap();
    let r = rec("k1");
    recs::upsert_pending(&mut c, a, &r, now).await.unwrap();
    assert_eq!(
        recs::transition(&mut c, a, &move_to(r.id, S::Accepted), now)
            .await
            .unwrap(),
        Transitioned::Changed { version: 2 }
    );
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM recommendations")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(visible, 1);

    context(b).execute(&mut *c).await.unwrap();
    let visible: i64 = sqlx::query_scalar("SELECT count(*) FROM recommendations")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(visible, 0, "another tenant sees nothing");
    let outbox_visible: i64 = sqlx::query_scalar("SELECT count(*) FROM outbox")
        .fetch_one(&mut *c)
        .await
        .unwrap();
    assert_eq!(outbox_visible, 0);
    assert!(recs::get(&mut c, a, r.id).await.unwrap().is_none());
    assert!(
        recs::upsert_pending(&mut c, a, &rec("k2"), now)
            .await
            .is_err(),
        "writing into another organisation is refused by row-level security"
    );
}
