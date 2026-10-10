//! SDL conformance and ratchet. No database needed.
//! 1. Everything served exists in api/schema/kuki.graphql with the same shape.
//! 2. Contract root fields that are not served are exactly the ones in unserved.txt.

use std::collections::{BTreeMap, BTreeSet};

use async_graphql::parser::{parse_schema, types::*};

const CONTRACT: &str = include_str!("../../../api/schema/kuki.graphql");
const UNSERVED: &str = include_str!("../../../api/schema/unserved.txt");
const ROOTS: [&str; 3] = ["Query", "Mutation", "Subscription"];

#[derive(Debug)]
struct Fld {
    ty: String,
    args: BTreeMap<String, String>,
}

#[derive(Debug)]
enum Def {
    Scalar,
    Object(BTreeMap<String, Fld>),
    Input(BTreeMap<String, String>),
    Union(BTreeSet<String>),
    Enum(BTreeSet<String>),
    Other,
}

fn model(sdl: &str) -> BTreeMap<String, Def> {
    let doc = parse_schema(sdl).expect("SDL parses");
    let mut out = BTreeMap::new();
    for d in doc.definitions {
        let TypeSystemDefinition::Type(t) = d else {
            continue;
        };
        let t = t.node;
        let def = match t.kind {
            TypeKind::Scalar => Def::Scalar,
            TypeKind::Object(o) => Def::Object(
                o.fields
                    .into_iter()
                    .map(|f| {
                        let f = f.node;
                        let args = f
                            .arguments
                            .into_iter()
                            .map(|a| (a.node.name.node.to_string(), a.node.ty.node.to_string()))
                            .collect();
                        (
                            f.name.node.to_string(),
                            Fld {
                                ty: f.ty.node.to_string(),
                                args,
                            },
                        )
                    })
                    .collect(),
            ),
            TypeKind::InputObject(i) => Def::Input(
                i.fields
                    .into_iter()
                    .map(|f| (f.node.name.node.to_string(), f.node.ty.node.to_string()))
                    .collect(),
            ),
            TypeKind::Union(u) => {
                Def::Union(u.members.into_iter().map(|m| m.node.to_string()).collect())
            }
            TypeKind::Enum(e) => Def::Enum(
                e.values
                    .into_iter()
                    .map(|v| v.node.value.node.to_string())
                    .collect(),
            ),
            TypeKind::Interface(_) => Def::Other,
        };
        out.insert(t.name.node.to_string(), def);
    }
    out
}

fn served() -> BTreeMap<String, Def> {
    model(&kuki_api::graphql::schema().sdl())
}

#[test]
fn served_schema_conforms_to_contract() {
    let contract = model(CONTRACT);
    let mut problems = vec![];
    for (name, def) in served() {
        let Some(want) = contract.get(&name) else {
            problems.push(format!("type {name} is not in the contract"));
            continue;
        };
        match (&def, want) {
            (Def::Scalar, Def::Scalar) => {}
            (Def::Object(got), Def::Object(want)) => {
                for (f, g) in got {
                    match want.get(f) {
                        None => problems.push(format!("{name}.{f} is not in the contract")),
                        Some(w) => {
                            if g.ty != w.ty {
                                problems.push(format!(
                                    "{name}.{f}: served {} vs contract {}",
                                    g.ty, w.ty
                                ));
                            }
                            if g.args != w.args {
                                problems.push(format!(
                                    "{name}.{f}: args {:?} vs contract {:?}",
                                    g.args, w.args
                                ));
                            }
                        }
                    }
                }
            }
            (Def::Input(got), Def::Input(want)) if got == want => {}
            (Def::Union(got), Def::Union(want)) if got.is_subset(want) => {}
            // Enums are compared for equality: the registries are closed or fully mirrored.
            (Def::Enum(got), Def::Enum(want)) if got == want => {}
            _ => problems.push(format!(
                "type {name}: kind or members differ from the contract"
            )),
        }
    }
    assert!(
        problems.is_empty(),
        "contract violations:\n{}",
        problems.join("\n")
    );
}

#[test]
fn unserved_ratchet_is_exact() {
    let contract = model(CONTRACT);
    let served = served();

    let mut actual = BTreeSet::new();
    for root in ROOTS {
        let Some(Def::Object(fields)) = contract.get(root) else {
            continue;
        };
        let have = match served.get(root) {
            Some(Def::Object(f)) => f.keys().cloned().collect::<BTreeSet<_>>(),
            _ => BTreeSet::new(),
        };
        for f in fields.keys().filter(|f| !have.contains(*f)) {
            actual.insert(format!("{root}.{f}"));
        }
    }

    let listed: BTreeSet<String> = UNSERVED
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_owned)
        .collect();

    let shipped: Vec<_> = listed.difference(&actual).collect();
    let newly_missing: Vec<_> = actual.difference(&listed).collect();
    assert!(
        shipped.is_empty(),
        "now served, delete from unserved.txt: {shipped:?}"
    );
    assert!(
        newly_missing.is_empty(),
        "contract fields neither served nor listed (the ratchet only shrinks): {newly_missing:?}"
    );
}

#[test]
fn error_registries_are_in_the_served_sdl() {
    let sdl = kuki_api::graphql::schema().sdl();
    for needle in ["enum ErrorCode", "enum ErrorReason"] {
        assert!(
            sdl.contains(needle),
            "{needle} is missing from the served SDL:\n{sdl}"
        );
    }
}
