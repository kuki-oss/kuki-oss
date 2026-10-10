//! Ontology reads: objects (this slice). Links, events and registries follow the same pattern.

use async_graphql::{Context, Enum, ID, Json, Object, Result, SimpleObject};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use kuki_store::ontology::{EventFilter, EventRow, LinkDir, LinkFilter, LinkRow, ObjectRow};
use sqlx::PgPool;

use super::connection::{
    Cursor, PageInfo, check_ltree, conflict, decode_event_cursor, decode_id_cursor,
    encode_event_cursor, encode_id_cursor, page_size, parse_uuid, validation,
};

use super::guard::RequiresScope;
use crate::auth::Principal;
use crate::error::ApiError;

#[derive(Default)]
pub struct OntologyQuery;

#[derive(SimpleObject)]
pub struct OntologyObject {
    id: ID,
    #[graphql(name = "type")]
    kind: String,
    type_version: i32,
    org_unit_id: Option<ID>,
    /// e.g. {"erp":"SKU-204"}
    external_ids: Json<serde_json::Value>,
    /// Fields tagged sensitive may be redacted after crypto-shredding.
    attrs: Json<serde_json::Value>,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
    recorded_at: DateTime<Utc>,
}

impl From<ObjectRow> for OntologyObject {
    fn from(r: ObjectRow) -> Self {
        OntologyObject {
            id: ID(r.id.to_string()),
            kind: r.kind,
            type_version: r.type_version,
            org_unit_id: r.org_unit_id.map(|u| ID(u.to_string())),
            external_ids: Json(r.external_ids),
            attrs: Json(r.attrs),
            valid_from: r.valid_from,
            valid_to: r.valid_to,
            recorded_at: r.recorded_at,
        }
    }
}

#[derive(SimpleObject)]
pub struct OntologyObjectEdge {
    node: OntologyObject,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct OntologyObjectConnection {
    edges: Vec<OntologyObjectEdge>,
    page_info: PageInfo,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq, Default)]
pub enum LinkDirection {
    Out,
    In,
    #[default]
    Both,
}

impl From<LinkDirection> for LinkDir {
    fn from(d: LinkDirection) -> Self {
        match d {
            LinkDirection::Out => LinkDir::Out,
            LinkDirection::In => LinkDir::In,
            LinkDirection::Both => LinkDir::Both,
        }
    }
}

#[derive(SimpleObject)]
pub struct OntologyLink {
    id: ID,
    #[graphql(name = "type")]
    kind: String,
    src_id: ID,
    dst_id: ID,
    attrs: Json<serde_json::Value>,
    valid_from: DateTime<Utc>,
    valid_to: Option<DateTime<Utc>>,
}

impl From<LinkRow> for OntologyLink {
    fn from(r: LinkRow) -> Self {
        OntologyLink {
            id: ID(r.id.to_string()),
            kind: r.kind,
            src_id: ID(r.src_id.to_string()),
            dst_id: ID(r.dst_id.to_string()),
            attrs: Json(r.attrs),
            valid_from: r.valid_from,
            valid_to: r.valid_to,
        }
    }
}

#[derive(SimpleObject)]
pub struct OntologyLinkEdge {
    node: OntologyLink,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct OntologyLinkConnection {
    edges: Vec<OntologyLinkEdge>,
    page_info: PageInfo,
}

#[derive(SimpleObject)]
pub struct OntologyEvent {
    id: ID,
    #[graphql(name = "type")]
    kind: String,
    type_version: i32,
    subject_id: ID,
    /// Event time.
    occurred_at: DateTime<Utc>,
    /// Ingest time.
    recorded_at: DateTime<Utc>,
    payload: Json<serde_json::Value>,
    /// Correction pointer (invariant I5).
    supersedes: Option<ID>,
    source: String,
}

impl From<EventRow> for OntologyEvent {
    fn from(r: EventRow) -> Self {
        OntologyEvent {
            id: ID(r.id.to_string()),
            kind: r.kind,
            type_version: r.type_version,
            subject_id: ID(r.subject_id.to_string()),
            occurred_at: r.occurred_at,
            recorded_at: r.recorded_at,
            payload: Json(r.payload),
            supersedes: r.supersedes.map(|u| ID(u.to_string())),
            source: r.source,
        }
    }
}

#[derive(SimpleObject)]
pub struct OntologyEventEdge {
    node: OntologyEvent,
    cursor: Cursor,
}

#[derive(SimpleObject)]
pub struct OntologyEventConnection {
    edges: Vec<OntologyEventEdge>,
    page_info: PageInfo,
}

#[Object]
impl OntologyQuery {
    /// The current version, or the version valid at `asOf`. Null when it does not exist
    /// or is outside the caller's scope (the two are indistinguishable by design).
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn ontology_object(
        &self,
        ctx: &Context<'_>,
        id: ID,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Option<OntologyObject>> {
        let id = parse_uuid(&id, "id")?;
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let row = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            async move { kuki_store::ontology::object_get(c, org, id, as_of).await }.boxed()
        })
        .await
        .map_err(ApiError::internal)?;
        Ok(row.map(Into::into))
    }

    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    async fn ontology_objects(
        &self,
        ctx: &Context<'_>,
        #[graphql(name = "type")] kind: String,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
        org_unit_path: Option<String>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<OntologyObjectConnection> {
        let limit = page_size(first)?;
        let after_id = after.as_ref().map(decode_id_cursor).transpose()?;
        if let Some(u) = &org_unit_path {
            check_ltree(u, "orgUnitPath")?;
        }

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let (kind, under) = (kind.clone(), org_unit_path.clone());
            async move {
                kuki_store::ontology::objects_page(
                    c,
                    org,
                    &kind,
                    under.as_deref(),
                    after_id,
                    as_of,
                    limit + 1,
                )
                .await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next_page = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let edges: Vec<OntologyObjectEdge> = rows
            .into_iter()
            .map(|r| OntologyObjectEdge {
                cursor: encode_id_cursor(r.id),
                node: r.into(),
            })
            .collect();
        Ok(OntologyObjectConnection {
            page_info: PageInfo {
                has_next_page,
                has_previous_page: false, // forward-only pagination (contract)
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor: edges.last().map(|e| e.cursor.clone()),
            },
            edges,
        })
    }

    /// Null when nothing matches; CONFLICT when the external id is ambiguous.
    #[graphql(guard = "RequiresScope::new(\"decisions:read\")")]
    async fn ontology_object_by_external_id(
        &self,
        ctx: &Context<'_>,
        system: String,
        id: String,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<Option<OntologyObject>> {
        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let (system, id) = (system.clone(), id.clone());
            async move {
                kuki_store::ontology::objects_by_external_id(c, org, &system, &id, as_of).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        if rows.len() > 1 {
            return Err(conflict("the external id matches more than one object"));
        }
        Ok(rows.pop().map(Into::into))
    }

    /// Links touching an object, ordered by link id. Both ends must be visible to the caller.
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    #[allow(clippy::too_many_arguments)]
    async fn ontology_links(
        &self,
        ctx: &Context<'_>,
        object_id: ID,
        #[graphql(name = "type")] kind: Option<String>,
        #[graphql(default_with = "Some(LinkDirection::Both)")] direction: Option<LinkDirection>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<OntologyLinkConnection> {
        let limit = page_size(first)?;
        let object = parse_uuid(&object_id, "objectId")?;
        let after_id = after.as_ref().map(decode_id_cursor).transpose()?;

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let dir: LinkDir = direction.unwrap_or_default().into();
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let kind = kind.clone();
            async move {
                let f = LinkFilter {
                    object,
                    kind: kind.as_deref(),
                    dir,
                    after: after_id,
                    as_of,
                };
                kuki_store::ontology::links_page(c, org, &f, limit + 1).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next_page = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let edges: Vec<OntologyLinkEdge> = rows
            .into_iter()
            .map(|r| OntologyLinkEdge {
                cursor: encode_id_cursor(r.id),
                node: r.into(),
            })
            .collect();
        Ok(OntologyLinkConnection {
            page_info: PageInfo {
                has_next_page,
                has_previous_page: false,
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor: edges.last().map(|e| e.cursor.clone()),
            },
            edges,
        })
    }

    /// Events newest first by occurredAt. `occurredFrom` is inclusive, `occurredTo` exclusive.
    /// `asOf` keeps events recorded at or before that instant.
    #[graphql(
        guard = "RequiresScope::new(\"decisions:read\")",
        complexity = "first.unwrap_or(25).clamp(0, 100) as usize * child_complexity"
    )]
    #[allow(clippy::too_many_arguments)]
    async fn ontology_events(
        &self,
        ctx: &Context<'_>,
        subject_id: Option<ID>,
        #[graphql(name = "type")] kind: Option<String>,
        occurred_from: Option<DateTime<Utc>>,
        occurred_to: Option<DateTime<Utc>>,
        #[graphql(default_with = "Some(25)")] first: Option<i32>,
        after: Option<Cursor>,
        as_of: Option<DateTime<Utc>>,
    ) -> Result<OntologyEventConnection> {
        let limit = page_size(first)?;
        let subject = subject_id
            .as_ref()
            .map(|i| parse_uuid(i, "subjectId"))
            .transpose()?;
        let after_pos = after.as_ref().map(decode_event_cursor).transpose()?;
        if let (Some(a), Some(b)) = (occurred_from, occurred_to)
            && a > b
        {
            return Err(validation(
                "occurredFrom must not be after occurredTo",
                None,
            ));
        }

        let p = ctx.data::<Principal>()?;
        let pool = ctx.data::<PgPool>()?;
        let org = p.session.org_id;
        let mut rows = kuki_store::session::with_org_tx(pool, &p.session, |c| {
            let kind = kind.clone();
            async move {
                let f = EventFilter {
                    subject,
                    kind: kind.as_deref(),
                    from: occurred_from,
                    to: occurred_to,
                    after: after_pos,
                    as_of,
                };
                kuki_store::ontology::events_page(c, org, &f, limit + 1).await
            }
            .boxed()
        })
        .await
        .map_err(ApiError::internal)?;

        let has_next_page = rows.len() as i64 > limit;
        rows.truncate(limit as usize);
        let edges: Vec<OntologyEventEdge> = rows
            .into_iter()
            .map(|r| OntologyEventEdge {
                cursor: encode_event_cursor(r.occurred_at, r.id),
                node: r.into(),
            })
            .collect();
        Ok(OntologyEventConnection {
            page_info: PageInfo {
                has_next_page,
                has_previous_page: false,
                start_cursor: edges.first().map(|e| e.cursor.clone()),
                end_cursor: edges.last().map(|e| e.cursor.clone()),
            },
            edges,
        })
    }
}
