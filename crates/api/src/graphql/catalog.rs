//! Registry reads: object, event and decision types.

use async_graphql::{Context, Json, Object, Result, SimpleObject};
use futures::FutureExt;
use kuki_store::catalog::{DecisionTypeRow, EventTypeRow, ObjectTypeRow};
use sqlx::PgPool;

use super::connection::{
    Cursor, PageInfo, decode_name_cursor, decode_name_version_cursor, encode_name_cursor,
    encode_name_version_cursor, page_size,
};
use super::guard::RequiresScope;
use crate::auth::Principal;
use crate::error::ApiError;

#[derive(Default)]
pub struct CatalogQuery;

#[derive(SimpleObject)]
#[graphql(name = "ObjectType")]
pub struct ObjectTypeNode {
    name: String,
    version: i32,
    json_schema: Json<serde_json::Value>,
}

impl From<ObjectTypeRow> for ObjectTypeNode {
    fn from(r: ObjectTypeRow) -> Self {
        Self {
            name: r.name,
            version: r.version,
            json_schema: Json(r.json_schema),
        }
    }
}

#[derive(SimpleObject)]
pub struct ObjectTypeEdge {
    node: ObjectTypeNode,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct ObjectTypeConnection {
    edges: Vec<ObjectTypeEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
#[graphql(name = "EventType")]
pub struct EventTypeNode {
    name: String,
    version: i32,
    subject_type: String,
    json_schema: Json<serde_json::Value>,
}

impl From<EventTypeRow> for EventTypeNode {
    fn from(r: EventTypeRow) -> Self {
        Self {
            name: r.name,
            version: r.version,
            subject_type: r.subject_type,
            json_schema: Json(r.json_schema),
        }
    }
}

#[derive(SimpleObject)]
pub struct EventTypeEdge {
    node: EventTypeNode,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct EventTypeConnection {
    edges: Vec<EventTypeEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
#[graphql(name = "DecisionTypeDefinition")]
pub struct DecisionTypeNode {
    name: String,
    pack: String,
    payload_schema_ref: String,
    payload_schema: Json<serde_json::Value>,
}

impl From<DecisionTypeRow> for DecisionTypeNode {
    fn from(r: DecisionTypeRow) -> Self {
        Self {
            name: r.name,
            pack: r.pack,
            payload_schema_ref: r.payload_schema_ref,
            payload_schema: Json(r.payload_schema),
        }
    }
}

#[derive(SimpleObject)]
pub struct DecisionTypeDefinitionEdge {
    node: DecisionTypeNode,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct DecisionTypeDefinitionConnection {
    edges: Vec<DecisionTypeDefinitionEdge>,
    page_info: PageInfo,
}

/// Rows were fetched with `limit + 1`; trim, compute `PageInfo` and build edges.
fn paginate<R, E>(
    mut rows: Vec<R>,
    limit: i64,
    cursor_of: impl Fn(&R) -> Cursor,
    edge: impl Fn(R, Cursor) -> E,
) -> (Vec<E>, PageInfo) {
    let has_next_page = rows.len() as i64 > limit;
    rows.truncate(limit as usize);
    let cursors: Vec<Cursor> = rows.iter().map(&cursor_of).collect();
    let page_info = PageInfo {
        has_next_page,
        has_previous_page: false, // forward-only pagination (contract)
        start_cursor: cursors.first().cloned(),
        end_cursor: cursors.last().cloned(),
    };
    let edges = rows
        .into_iter()
        .zip(cursors)
        .map(|(r, c)| edge(r, c))
        .collect();
    (edges, page_info)
}

#[Object]
impl CatalogQuery {
    /// Every version of every object type, ordered by (name, version).
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn object_types(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
    ) -> Result<ObjectTypeConnection> {
        let limit = page_size(first)?;
        let after = after.as_ref().map(decode_name_version_cursor).transpose()?;
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let after = after.clone();
            async move { kuki_store::catalog::object_types_page(c, org, after, limit + 1).await }
                .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let (edges, page_info) = paginate(
            rows,
            limit,
            |r| encode_name_version_cursor(&r.name, r.version),
            |r, cursor| ObjectTypeEdge {
                node: r.into(),
                cursor,
            },
        );
        Ok(ObjectTypeConnection { edges, page_info })
    }

    /// Every version of every event type, ordered by (name, version).
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn event_types(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
    ) -> Result<EventTypeConnection> {
        let limit = page_size(first)?;
        let after = after.as_ref().map(decode_name_version_cursor).transpose()?;
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let after = after.clone();
            async move { kuki_store::catalog::event_types_page(c, org, after, limit + 1).await }
                .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let (edges, page_info) = paginate(
            rows,
            limit,
            |r| encode_name_version_cursor(&r.name, r.version),
            |r, cursor| EventTypeEdge {
                node: r.into(),
                cursor,
            },
        );
        Ok(EventTypeConnection { edges, page_info })
    }

    /// Decision types, ordered by name.
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn decision_types(
        &self,
        ctx: &Context<'_>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
    ) -> Result<DecisionTypeDefinitionConnection> {
        let limit = page_size(first)?;
        let after = after.as_ref().map(decode_name_cursor).transpose()?;
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let after = after.clone();
            async move { kuki_store::catalog::decision_types_page(c, org, after, limit + 1).await }
                .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let (edges, page_info) = paginate(
            rows,
            limit,
            |r| encode_name_cursor(&r.name),
            |r, cursor| DecisionTypeDefinitionEdge {
                node: r.into(),
                cursor,
            },
        );
        Ok(DecisionTypeDefinitionConnection { edges, page_info })
    }
}
