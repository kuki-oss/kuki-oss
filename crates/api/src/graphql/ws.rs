//! graphql-ws (`graphql-transport-ws`) over `GET /v1/graphql`. The credential
//! travels in the `connection_init` payload (`{"Authorization": "Bearer kuki_..."}`),
//! never a cookie (spec 3.4), and goes through the same `authenticate` and rate
//! limiter as a POST.
//!
//! A key that is revoked while a socket is open stays connected until it closes, this
//! is a known issue currently.

use std::{sync::Arc, time::Instant};

use async_graphql::{
    Data,
    http::{WebSocket as GqlSocket, WebSocketProtocols, WsMessage},
};
use axum::{
    extract::{
        Request, State,
        ws::{CloseFrame, Message, WebSocket, WebSocketUpgrade},
    },
    response::Response,
};
use chrono::Utc;
use futures::{SinkExt, StreamExt};
use kuki_outbox::EventFeed;

use super::schema;
use crate::{auth::authenticate, error::ApiError, ratelimit::RateLimiter, state::AppState};

const MAX_MESSAGE: usize = 64 * 1024;

pub async fn upgrade(State(st): State<AppState>, ws: WebSocketUpgrade, req: Request) -> Response {
    let feed = req.extensions().get::<EventFeed>().cloned();
    let limiter = req.extensions().get::<Arc<RateLimiter>>().cloned();
    ws.protocols(["graphql-transport-ws"])
        .max_message_size(MAX_MESSAGE)
        .max_frame_size(MAX_MESSAGE)
        .on_upgrade(move |socket| run(socket, st, feed, limiter))
}

async fn init(
    st: AppState,
    feed: Option<EventFeed>,
    limiter: Option<Arc<RateLimiter>>,
    payload: serde_json::Value,
) -> async_graphql::Result<Data> {
    let header = payload.as_object().and_then(|o| {
        o.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .and_then(|(_, v)| v.as_str())
    });
    let principal = authenticate(
        &*st.keys,
        &*st.hasher,
        st.config.key_env.as_str(),
        header,
        Utc::now(),
    )
    .await?;
    if let Some(limiter) = limiter {
        let verdict = limiter.check(principal.key_id, Instant::now());
        if !verdict.allowed {
            return Err(ApiError::rate_limited(verdict.retry_after_secs).into());
        }
    }
    st.keys.touch(principal.key_id);

    let mut data = Data::default();
    data.insert(principal);
    data.insert(st.pool.clone());
    data.insert(st.extensions.clone());
    if let Some(feed) = feed {
        data.insert(feed);
    }
    Ok(data)
}

async fn run(
    socket: WebSocket,
    st: AppState,
    feed: Option<EventFeed>,
    limiter: Option<Arc<RateLimiter>>,
) {
    let (mut tx, rx) = socket.split();
    let input = rx
        .take_while(|m| std::future::ready(matches!(m, Ok(msg) if !matches!(msg, Message::Close(_)))))
        .filter_map(|m| {
            std::future::ready(match m {
                Ok(Message::Text(t)) => Some(t.as_str().to_owned()),
                _ => None,
            })
        });

    let gql = GqlSocket::new(schema().clone(), input, WebSocketProtocols::GraphQLWS)
        .on_connection_init(move |payload| init(st, feed, limiter, payload));
    let mut gql = Box::pin(gql);

    while let Some(out) = gql.next().await {
        let (frame, last) = match out {
            WsMessage::Text(t) => (Message::Text(t.into()), false),
            WsMessage::Close(code, reason) => {
                (Message::Close(Some(CloseFrame { code, reason: reason.into() })), true)
            }
        };
        if tx.send(frame).await.is_err() || last {
            break;
        }
    }
}
