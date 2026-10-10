<!--
  Reconcile before publishing: commands, ports and paths below describe the TARGET layout
  (specification sections 4.3 and 16, release R0). Adjust to the repository as R0 lands.
  Do not publish claims the code does not yet satisfy. The full specification also describes
  Kuki Core and stays private; public documentation lives at https://docs.kuki.co.ke.
-->

# Kuki OSS

**Operational intelligence for organisations. Open source, self-hostable.**

Kuki connects to the operational data an organisation already has, models it as an **ontology** (objects, links, events), continuously evaluates **decision generators** against it, and produces ranked, explained, auditable **recommendations**. Where you opt in, it hands the resulting actions to your own systems through signed webhooks and governed adapters.

It is not a dashboard. The unit of output is a recommendation with a reason, evidence, a confidence, and a reproducible provenance record.

- **For:** any organisation with resources, demand, constraints and recurring decisions: companies, clinics, logistics operators, public agencies, utilities, NGOs.
- **License:** AGPL-3.0, see [`LICENSE`](./LICENSE).
- **API-first:** GraphQL for queries and subscriptions, REST/OpenAPI for ingest, signed webhooks. Build any frontend you like.
- **One binary:** a single `kuki` binary runs the whole kernel on one node, or split by role when you need to scale.
- **Status:** pre-1.0. The contract described in the docs is stabilising; expect changes before `1.0`.

## What you get

| | |
|---|---|
| **Kernel** | Public API, ingest, ontology registry, append-only event ledger, decision engine, outbox, append-only audit log |
| **Decision packs** | `inventory` (replenishment), `scheduling` (conflict detection), `anomaly` (exceptions under a review budget), `prioritisation` (explainable ranking) |
| **Method tiers** | T0 rules and T1 classical formulas (reorder point with demand and lead-time variance), T2 statistical forecasting |
| **Approvals** | Single-approver accept and reject on every recommendation |
| **Explainability** | Every recommendation stores a structured rationale, evidence references and an `input_digest` for replay |
| **Shadow mode** | Run a new generator beside the baseline, record its output without surfacing it, and measure whether it is better on your data |
| **Optional cognition** | Plug in any OpenAI-compatible model endpoint for column mapping, document extraction and plain-language explanations. Off by default. Never makes decisions |

Not in OSS (these are part of **Kuki Core**): gradient-boosted, pooled and deep-sequence ML tiers with SHAP, approval workflows with the two-person rule and guarded auto-execution, hash-chained audit, SCIM and per-organisation KMS keys, cross-organisation benchmarks, and the bundled large-model serving profile.

## Editions

Four products ship from two codebases. This repository is the open-source kernel that all of them are built on.

| Edition | Code | Run by |
|---|---|---|
| **Kuki OSS** | this repository | you, self-hosted |
| **Kuki OSS Cloud** | this repository, identical artefact | Kuki, one isolated cell per organisation (planned) |
| **Kuki Core** | this kernel plus proprietary extensions | you, self-hosted |
| **Kuki Core Cloud** | same as Core | Kuki, one isolated cell per organisation (planned) |

The Cloud editions add operations only: provisioning, backups, upgrades and billing. Anything Cloud can do, you can do self-hosted.

## Architecture at a glance

```
  Your systems                        kuki  (one binary: kuki serve --role all)
  ERP, POS, spreadsheets, DBs         ┌───────────────────────────────────────────────────────┐
        │                             │                                                       │
        │  REST ingest / CSV          │   api ──► ingest ──► store ──► engine ──► outbox      │
        └────────────────────────────►│    ▲      validate    ledger    generators   signed   │
                                      │    │      resolve     RLS       shadow mode  webhooks │
   Any client ◄───────────────────────│────┘      quarantine  digest    review budget         │
   your UI, scripts, agents           │   GraphQL, REST,                                      │
        ▲                             │   subscriptions                                       │
        │  signed webhooks            └───────────────┬───────────────────────┬───────────────┘
        └─────────────────────────────────────────────┼───────────────────────┘
                                                      │ SQL
                                                      ▼
                                        PostgreSQL: append-only event ledger,
                                        recommendations, provenance, audit

   - - optional - -►  any OpenAI-compatible model endpoint (mapping and explanation only)
```

Design rules you can rely on:

1. **Reproducible decisions.** Same ledger, ontology version, generator version and config give the identical recommendation set.
2. **No model in the decision path.** A language model may help map and explain. It never computes a quantity or ranking.
3. **PostgreSQL is the only hard dependency.** Graph and vector stores are optional, rebuildable projections.
4. **Append-only ledger.** Corrections are new events.
5. **Tenant isolation in the database.** Row-level security is on from the first migration.
6. **The public API is the only interface**, including for our own tools.

Architecture and API guides: https://docs.kuki.co.ke

## Quickstart

Requires Docker with Compose.

```bash
git clone https://github.com/kuki-org/kuki-oss.git
cd kuki-oss/deploy/docker
cp .env.example .env            # set KUKI_ADMIN_EMAIL and a database password
docker compose up -d

# create the first organisation and an API key (printed once)
docker compose exec kuki kuki admin bootstrap --org "My Organisation"
```

Load the sandbox dataset to see recommendations immediately:

```bash
docker compose exec kuki kuki sim seed --scenario inventory-basic
```

### Ingest events

```bash
curl -X POST http://localhost:8080/v1/events \
  -H "Authorization: Bearer $KUKI_API_KEY" \
  -H "Idempotency-Key: 2026-10-01-batch-001" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary @- <<'NDJSON'
{"type":"stock.delta","type_version":1,"subject":{"external_id":{"erp":"SKU-204"}},"occurred_at":"2026-10-01T08:15:00Z","payload":{"delta":-3,"reason":"sale"}}
{"type":"stock.delta","type_version":1,"subject":{"external_id":{"erp":"SKU-204"}},"occurred_at":"2026-10-01T09:40:00Z","payload":{"delta":-2,"reason":"sale"}}
NDJSON
```

The response lists `accepted` and `rejected` rows. Rejected rows are quarantined with a reason code and can be corrected and replayed. Re-sending the same `Idempotency-Key` is safe.

### Read recommendations

```bash
curl -s http://localhost:8080/graphql \
  -H "Authorization: Bearer $KUKI_API_KEY" -H "Content-Type: application/json" \
  -d '{"query":"{ recommendations(status: PENDING, first: 10) { edges { node { id decisionType urgency rationale { summary confidence factors { label value } } provenance { generatorId generatorVersion asOf } } } } }"}'
```

Subscribe to new ones over WebSocket (`graphql-ws`) with `recommendationCreated`, or receive signed webhooks. Accept or reject a recommendation with the `acceptRecommendation` and `rejectRecommendation` mutations.

## Concepts in one minute

| Concept | Meaning |
|---|---|
| **Organisation / org unit** | The tenant, and its own structure (sites, depots, wards) |
| **Object, link, event** | The ontology: things, relationships between them, and timestamped facts |
| **Decision generator** | A pure function of the ontology state that returns recommendation candidates |
| **Tier** | The method behind a generator: T0 rules, T1 classical formulas, T2 statistical forecasting |
| **Pack** | A versioned bundle of types, generators and test fixtures (for example `inventory`) |
| **Recommendation** | Proposed action plus rationale, evidence, confidence and provenance |
| **Review budget** | Cap on surfaced recommendations per unit per day, so teams are not flooded |
| **Shadow mode** | A generator that runs and records output without surfacing it |

## Configuration

Behaviour that differs between organisations lives in configuration, never in code branches.

```yaml
# kuki.yaml (excerpt)
decision:
  review_budget: { per_org_unit_per_day: 20 }
  min_confidence_for_display: 0.30
  generators:
    inventory.replenish.t0: { enabled: true, params: { velocity_window_days: 14, safety_multiplier: 1.5, coverage_buffer_days: 7 } }
    inventory.replenish.t1: { enabled: true, mode: shadow, params: { service_level_z: 1.65 } }

cognition:              # optional, off by default
  enabled: false
  providers:
    small: { base_url: "http://localhost:8001/v1", model: "local-small", data_class_max: confidential }
```

`mode: shadow` runs a generator and records its output without surfacing it, so you can measure whether a more sophisticated method actually beats the baseline on your data.

## Running in production

The same image runs as one process or as several roles:

```bash
kuki serve --role all        # api, ingest, engine and outbox in one process (default)
kuki serve --role api        # scale the front door
kuki serve --role ingest     # scale validation and normalisation
kuki serve --role engine     # decision evaluation
```

Splitting roles is a deployment change, not a code change. PostgreSQL is shared; migrations are forward-only.

## Repository layout

```
crates/            Rust workspace
  types/           ontology and recommendation structs, JSON Schema, input_digest
  extension/       extension traits and OSS defaults (see below)
  ontology/        type registry, validation, pack loader
  store/           PostgreSQL repositories, row-level security helpers
  ingest/          validation, identity resolution, quarantine, CSV connector
  engine/          orchestrator, scheduler, review budget, shadow mode
  packs/           built-in generators: inventory, scheduling, anomaly, prioritisation
  actions/         action executors and adapters
  outbox/          transactional outbox relay and signed webhooks
  api/             GraphQL, REST/OpenAPI, subscriptions, auth
  llm/             optional OpenAI-compatible client and verifier
  client/          Rust SDK generated from the contract
  cli/             the `kuki` binary and embeddable entry point
api/schema/        GraphQL SDL and OpenAPI: the contract of record
migrations/        PostgreSQL migrations, forward-only
packs/             pack manifests (YAML), schemas, known-answer fixtures
conformance/       kuki-conformance: tests any deployment can run against the contract
examples/          kuki.yaml samples, sandbox scenarios, sample CSVs
deploy/docker/     Dockerfile and Compose files
```

### Extension points

The kernel defines a small set of traits in `crates/extension`, each with an open-source default. Edition-specific code replaces an implementation without changing the kernel.

| Trait | OSS default |
|---|---|
| `DecisionGenerator` | T0, T1 and T2 generators |
| `InferenceClient` | none; the engine skips ML tiers |
| `PolicyEvaluator` | single approver, human required |
| `AuditSink` | append-only table |
| `PriorProvider` | local defaults, no network |
| `LlmProvider` | disabled |

## Development

```bash
cargo build --workspace
cargo test --workspace                      # unit, property and fixture tests
cargo test -p kuki-engine determinism       # same inputs must produce byte-identical output
cargo test -p kuki-store isolation          # tenant isolation under row-level security
kuki-conformance --target http://localhost:8080
```

Pull requests that change a generator's logic must bump its version and update its known-answer fixture. Pull requests that change the API schema must pass the schema diff gate (additive changes only without a deprecation notice).

## Contributing

**GitLab is the source of truth. This GitHub repository is a read-only mirror.** Direct pushes to mirrored branches are overwritten on the next sync, so please work through pull requests.

1. Fork this repository on GitHub and push to a branch in your fork.
2. Open a pull request. CI runs on the mirror; the DCO check must pass.
3. **Sign off every commit** (Developer Certificate of Origin): `git commit -s`. To fix a branch: `git rebase --signoff main` then `git push --force-with-lease`.
4. A maintainer reviews on GitHub, then applies your commits to the GitLab repository with authorship and sign-off preserved. The merge reaches this mirror on the next sync, and your pull request is closed with a link to the resulting commit.

Issues, bug reports and design discussion are welcome here. See [`CONTRIBUTING.md`](./CONTRIBUTING.md) for the full guide and the code of conduct.

## Security

Please report vulnerabilities privately to the security contact in [`SECURITY.md`](./SECURITY.md) rather than opening a public issue.

## Links

- Website: https://about.kuki.co.ke
- Documentation: https://docs.kuki.co.ke
- Research: https://research.kuki.co.ke
