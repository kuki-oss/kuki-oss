//! Closed `ErrorCode` and open `ErrorReason` enums, registered so they appear in the
//! served SDL (`graphql_contract::error_registries_are_in_the_served_sdl`).
//!
//! The engine prunes types that no field references from *live introspection*
//! (`__schema.types`), so they are not visible there; SDKs should be generated from
//! api/schema/kuki.graphql. `error.rs` tests ErrorCode/ErrorReason against the contract and
//! `graphql_contract` tests these mirrors against it, so the three stay aligned.

use async_graphql::Enum;

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "ErrorCode")]
pub enum ErrorCodeRegistry {
    Unauthenticated,
    Forbidden,
    ValidationFailed,
    RateLimited,
    Conflict,
    NotFound,
    Internal,
}

#[derive(Enum, Copy, Clone, Eq, PartialEq)]
#[graphql(name = "ErrorReason")]
pub enum ErrorReasonRegistry {
    CostLimitExceeded,
    DepthLimitExceeded,
    PageSizeExceeded,
    PersistedQueryRequired,
    StaleVersion,
    InvalidStateTransition,
}
