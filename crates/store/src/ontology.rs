//! Ontology reads. Run inside `with_org_tx`, so RLS (org isolation and unit scope) applies.
//! Valid time only: `as_of = None` means the current version.

use chrono::{DateTime, Utc};
use sqlx::{PgConnection, Postgres, QueryBuilder};
use uuid::Uuid;

#[derive(Debug, sqlx::FromRow)]
pub struct ObjectRow {
    pub id: Uuid,
    pub org_unit_id: Option<Uuid>,
    pub kind: String,
    pub type_version: i32,
    pub external_ids: serde_json::Value,
    pub attrs: serde_json::Value,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
    pub recorded_at: DateTime<Utc>,
}

const COLS: &str = "o.id, o.org_unit_id, o.\"type\" AS kind, o.type_version, o.external_ids, \
                    o.attrs, o.valid_from, o.valid_to, o.recorded_at";

/// The version visible at `as_of`; `valid_to` is exclusive.
fn visible_at(qb: &mut QueryBuilder<'_, Postgres>, as_of: Option<DateTime<Utc>>) {
    match as_of {
        None => {
            qb.push(" AND o.valid_to IS NULL");
        }
        Some(t) => {
            qb.push(" AND o.valid_from <= ")
                .push_bind(t)
                .push(" AND (o.valid_to IS NULL OR o.valid_to > ")
                .push_bind(t)
                .push(")");
        }
    }
}

pub async fn object_get(
    c: &mut PgConnection,
    org: Uuid,
    id: Uuid,
    as_of: Option<DateTime<Utc>>,
) -> Result<Option<ObjectRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(format!(
        "SELECT {COLS} FROM objects o WHERE o.organisation_id = "
    ));
    qb.push_bind(org).push(" AND o.id = ").push_bind(id);
    visible_at(&mut qb, as_of);
    qb.push(" LIMIT 1");
    qb.build_query_as::<ObjectRow>().fetch_optional(c).await
}

/// Keyset page ordered by id. `under` must already be valid ltree text.
pub async fn objects_page(
    c: &mut PgConnection,
    org: Uuid,
    kind: &str,
    under: Option<&str>,
    after: Option<Uuid>,
    as_of: Option<DateTime<Utc>>,
    limit: i64,
) -> Result<Vec<ObjectRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(format!("SELECT {COLS} FROM objects o "));
    if under.is_some() {
        qb.push("JOIN org_units u ON u.id = o.org_unit_id ");
    }
    qb.push("WHERE o.organisation_id = ").push_bind(org);
    qb.push(" AND o.\"type\" = ").push_bind(kind);
    if let Some(p) = under {
        qb.push(" AND u.path <@ ")
            .push_bind(p)
            .push("::text::ltree");
    }
    if let Some(a) = after {
        qb.push(" AND o.id > ").push_bind(a);
    }
    visible_at(&mut qb, as_of);
    qb.push(" ORDER BY o.id LIMIT ").push_bind(limit);
    qb.build_query_as::<ObjectRow>().fetch_all(c).await
}

/// At most two rows: more than one means the external id is ambiguous.
pub async fn objects_by_external_id(
    c: &mut PgConnection,
    org: Uuid,
    system: &str,
    external_id: &str,
    as_of: Option<DateTime<Utc>>,
) -> Result<Vec<ObjectRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(format!(
        "SELECT {COLS} FROM objects o WHERE o.organisation_id = "
    ));
    qb.push_bind(org)
        .push(" AND o.external_ids @> jsonb_build_object(")
        .push_bind(system)
        .push("::text, ")
        .push_bind(external_id)
        .push("::text)");
    visible_at(&mut qb, as_of);
    qb.push(" ORDER BY o.id LIMIT 2");
    qb.build_query_as::<ObjectRow>().fetch_all(c).await
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LinkDir {
    Out,
    In,
    Both,
}

#[derive(Debug, sqlx::FromRow)]
pub struct LinkRow {
    pub id: Uuid,
    pub kind: String,
    pub src_id: Uuid,
    pub dst_id: Uuid,
    pub attrs: serde_json::Value,
    pub valid_from: DateTime<Utc>,
    pub valid_to: Option<DateTime<Utc>>,
}

pub struct LinkFilter<'a> {
    pub object: Uuid,
    pub kind: Option<&'a str>,
    pub dir: LinkDir,
    pub after: Option<Uuid>,
    pub as_of: Option<DateTime<Utc>>,
}

/// Links and events carry no unit-scope policy of their own, so a scoped key may only see
/// rows whose object is a current object it can see (objects RLS decides). Unscoped: no-op.
fn scoped(qb: &mut QueryBuilder<'_, Postgres>, alias: &str, col: &str) {
    qb.push(format!(
        " AND (kuki_scope_path() IS NULL OR EXISTS (SELECT 1 FROM objects v \
         WHERE v.organisation_id = {alias}.organisation_id AND v.id = {alias}.{col} \
         AND v.valid_to IS NULL))"
    ));
}

/// Keyset page ordered by link id. Both endpoints must be visible to the caller.
pub async fn links_page(
    c: &mut PgConnection,
    org: Uuid,
    f: &LinkFilter<'_>,
    limit: i64,
) -> Result<Vec<LinkRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(
        "SELECT l.id, l.\"type\" AS kind, l.src_id, l.dst_id, l.attrs, l.valid_from, l.valid_to \
         FROM links l WHERE l.organisation_id = ",
    );
    qb.push_bind(org);
    match f.dir {
        LinkDir::Out => {
            qb.push(" AND l.src_id = ").push_bind(f.object);
        }
        LinkDir::In => {
            qb.push(" AND l.dst_id = ").push_bind(f.object);
        }
        LinkDir::Both => {
            qb.push(" AND (l.src_id = ")
                .push_bind(f.object)
                .push(" OR l.dst_id = ")
                .push_bind(f.object)
                .push(")");
        }
    }
    if let Some(k) = f.kind {
        qb.push(" AND l.\"type\" = ").push_bind(k);
    }
    if let Some(a) = f.after {
        qb.push(" AND l.id > ").push_bind(a);
    }
    match f.as_of {
        None => {
            qb.push(" AND l.valid_to IS NULL");
        }
        Some(t) => {
            qb.push(" AND l.valid_from <= ")
                .push_bind(t)
                .push(" AND (l.valid_to IS NULL OR l.valid_to > ")
                .push_bind(t)
                .push(")");
        }
    }
    scoped(&mut qb, "l", "src_id");
    scoped(&mut qb, "l", "dst_id");
    qb.push(" ORDER BY l.id LIMIT ").push_bind(limit);
    qb.build_query_as::<LinkRow>().fetch_all(c).await
}

#[derive(Debug, sqlx::FromRow)]
pub struct EventRow {
    pub id: Uuid,
    pub kind: String,
    pub type_version: i32,
    pub subject_id: Uuid,
    pub occurred_at: DateTime<Utc>,
    pub recorded_at: DateTime<Utc>,
    pub payload: serde_json::Value,
    pub supersedes: Option<Uuid>,
    pub source: String,
}

#[derive(Default)]
pub struct EventFilter<'a> {
    pub subject: Option<Uuid>,
    pub kind: Option<&'a str>,
    /// Inclusive.
    pub from: Option<DateTime<Utc>>,
    /// Exclusive.
    pub to: Option<DateTime<Utc>>,
    /// Keyset position: the last (occurred_at, id) already returned.
    pub after: Option<(DateTime<Utc>, Uuid)>,
    /// Ledger time: only events recorded at or before this instant.
    pub as_of: Option<DateTime<Utc>>,
}

/// Newest first: (occurred_at DESC, id DESC). The partition key is in the keyset and the
/// range filters, so partitions are pruned.
pub async fn events_page(
    c: &mut PgConnection,
    org: Uuid,
    f: &EventFilter<'_>,
    limit: i64,
) -> Result<Vec<EventRow>, sqlx::Error> {
    let mut qb = QueryBuilder::new(
        "SELECT e.id, e.\"type\" AS kind, e.type_version, e.subject_id, e.occurred_at, \
         e.recorded_at, e.payload, e.supersedes, e.source FROM events e WHERE e.organisation_id = ",
    );
    qb.push_bind(org);
    if let Some(s) = f.subject {
        qb.push(" AND e.subject_id = ").push_bind(s);
    }
    if let Some(k) = f.kind {
        qb.push(" AND e.\"type\" = ").push_bind(k);
    }
    if let Some(t) = f.from {
        qb.push(" AND e.occurred_at >= ").push_bind(t);
    }
    if let Some(t) = f.to {
        qb.push(" AND e.occurred_at < ").push_bind(t);
    }
    if let Some(t) = f.as_of {
        qb.push(" AND e.recorded_at <= ").push_bind(t);
    }
    if let Some((at, id)) = f.after {
        qb.push(" AND (e.occurred_at, e.id) < (")
            .push_bind(at)
            .push(", ")
            .push_bind(id)
            .push(")");
    }
    scoped(&mut qb, "e", "subject_id");
    qb.push(" ORDER BY e.occurred_at DESC, e.id DESC LIMIT ")
        .push_bind(limit);
    qb.build_query_as::<EventRow>().fetch_all(c).await
}
