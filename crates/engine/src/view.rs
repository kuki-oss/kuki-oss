//! `OntologyView` over Postgres, pinned to one organisation and one evaluation clock.
//!
//! Objects and links are read in valid time at `as_of`; events by ledger time
//! (`recorded_at <= as_of`). Together with a fixed `as_of`, that makes a run
//! reproducible. The evaluator reads as the organisation (unscoped), through
//! `with_org_tx`, so row-level security still applies.

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use futures::FutureExt;
use kuki_extension::{EventQuery, EventRecord, GenError, LinkRecord, ObjectRecord, OntologyView};
use kuki_store::{
    engine_reads::object_as_of,
    ontology::{
        EventFilter, EventRow, LinkDir, LinkFilter, LinkRow, ObjectRow, events_page, links_page,
        objects_page,
    },
    session::{OrgSession, with_org_tx},
};
use sqlx::PgPool;
use uuid::Uuid;

pub(crate) const PAGE: i64 = 500;
/// A generator that needs more than this in one read must narrow its scope, not page forever.
const MAX_OBJECTS: usize = 50_000;
const MAX_LINKS: usize = 10_000;
const MAX_EVENTS: usize = 10_000;

pub struct PgOntologyView {
    pool: PgPool,
    session: OrgSession,
    as_of: DateTime<Utc>,
}

impl PgOntologyView {
    pub fn new(pool: PgPool, organisation_id: Uuid, as_of: DateTime<Utc>) -> Self {
        Self {
            pool,
            session: OrgSession {
                org_id: organisation_id,
                scope_path: None,
            },
            as_of,
        }
    }
}

fn db(e: sqlx::Error) -> GenError {
    GenError::Internal(format!("database: {e}"))
}

fn too_many(what: &str, max: usize) -> GenError {
    GenError::InvalidData(format!(
        "more than {max} {what} in one read; narrow the generator's scope"
    ))
}

fn object(r: ObjectRow) -> ObjectRecord {
    ObjectRecord {
        id: r.id,
        type_name: r.kind,
        type_version: r.type_version,
        org_unit_id: r.org_unit_id,
        external_ids: r.external_ids,
        attrs: r.attrs,
        valid_from: r.valid_from,
        valid_to: r.valid_to,
    }
}

fn link(r: LinkRow) -> LinkRecord {
    LinkRecord {
        id: r.id,
        type_name: r.kind,
        src_id: r.src_id,
        dst_id: r.dst_id,
        attrs: r.attrs,
        valid_from: r.valid_from,
        valid_to: r.valid_to,
    }
}

fn event(r: EventRow) -> EventRecord {
    EventRecord {
        id: r.id,
        type_name: r.kind,
        type_version: r.type_version,
        subject_id: r.subject_id,
        occured_at: r.occurred_at,
        recorded_at: r.recorded_at,
        payload: r.payload,
        supersedes: r.supersedes,
        source: r.source,
    }
}

#[async_trait]
impl OntologyView for PgOntologyView {
    async fn object(&self, id: Uuid) -> Result<Option<ObjectRecord>, GenError> {
        let (org, as_of) = (self.session.org_id, self.as_of);
        let row = with_org_tx::<_, sqlx::Error, _>(&self.pool, &self.session, move |c| {
            async move { object_as_of(c, org, id, as_of).await }.boxed()
        })
        .await
        .map_err(db)?;
        Ok(row.map(object))
    }

    async fn objects_of_type(
        &self,
        type_name: &str,
        org_unit: Option<Uuid>,
    ) -> Result<Vec<ObjectRecord>, GenError> {
        let (org, as_of) = (self.session.org_id, self.as_of);
        let kind = type_name.to_owned();
        let rows = with_org_tx::<_, sqlx::Error, _>(&self.pool, &self.session, move |c| {
            async move {
                let mut all: Vec<ObjectRow> = Vec::new();
                let mut after = None;
                loop {
                    let page = objects_page(c, org, &kind, None, after, Some(as_of), PAGE).await?;
                    let n = page.len() as i64;
                    after = page.last().map(|r| r.id);
                    all.extend(page);
                    if n < PAGE || all.len() > MAX_OBJECTS {
                        break;
                    }
                }
                Ok(all)
            }
            .boxed()
        })
        .await
        .map_err(db)?;
        if rows.len() > MAX_OBJECTS {
            return Err(too_many("objects", MAX_OBJECTS));
        }
        // The unit filter is applied here: the store's path filter takes an ltree, not a unit id.
        Ok(rows
            .into_iter()
            .filter(|r| org_unit.is_none() || r.org_unit_id == org_unit)
            .map(object)
            .collect())
    }

    async fn links(
        &self,
        object_id: Uuid,
        type_name: Option<&str>,
    ) -> Result<Vec<LinkRecord>, GenError> {
        let (org, as_of) = (self.session.org_id, self.as_of);
        let kind = type_name.map(str::to_owned);
        let rows = with_org_tx::<_, sqlx::Error, _>(&self.pool, &self.session, move |c| {
            async move {
                let mut all: Vec<LinkRow> = Vec::new();
                let mut after = None;
                loop {
                    let f = LinkFilter {
                        object: object_id,
                        kind: kind.as_deref(),
                        dir: LinkDir::Both,
                        after,
                        as_of: Some(as_of),
                    };
                    let page = links_page(c, org, &f, PAGE).await?;
                    let n = page.len() as i64;
                    after = page.last().map(|r| r.id);
                    all.extend(page);
                    if n < PAGE || all.len() > MAX_LINKS {
                        break;
                    }
                }
                Ok(all)
            }
            .boxed()
        })
        .await
        .map_err(db)?;
        if rows.len() > MAX_LINKS {
            return Err(too_many("links", MAX_LINKS));
        }
        Ok(rows.into_iter().map(link).collect())
    }

    async fn events(&self, q: &EventQuery) -> Result<Vec<EventRecord>, GenError> {
        let (org, as_of) = (self.session.org_id, self.as_of);
        let (subject, kind, from, to) = (
            q.subject_id,
            q.type_name.clone(),
            q.occurred_from,
            q.occurred_to,
        );
        let mut rows = with_org_tx::<_, sqlx::Error, _>(&self.pool, &self.session, move |c| {
            async move {
                let mut all: Vec<EventRow> = Vec::new();
                let mut after = None;
                loop {
                    let f = EventFilter {
                        subject: Some(subject),
                        kind: kind.as_deref(),
                        from,
                        to,
                        after,
                        as_of: Some(as_of),
                    };
                    let page = events_page(c, org, &f, PAGE).await?;
                    let n = page.len() as i64;
                    after = page.last().map(|r| (r.occurred_at, r.id));
                    all.extend(page);
                    if n < PAGE || all.len() > MAX_EVENTS {
                        break;
                    }
                }
                Ok(all)
            }
            .boxed()
        })
        .await
        .map_err(db)?;
        if rows.len() > MAX_EVENTS {
            return Err(too_many("events", MAX_EVENTS));
        }
        // The store pages newest-first; the view contract is oldest-first, ties by id.
        rows.sort_by(|a, b| a.occurred_at.cmp(&b.occurred_at).then(a.id.cmp(&b.id)));
        Ok(rows.into_iter().map(event).collect())
    }
}
