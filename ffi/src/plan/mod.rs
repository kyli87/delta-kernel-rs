//! FFI plumbing for the [`PlanBasedEngine`](delta_kernel::engine::plan::PlanBasedEngine).
//!
//! Exposes a [`SharedPlanExecutor`](executor::SharedPlanExecutor) handle whose underlying
//! executor delegates [`DeclarativePlanNode`](delta_kernel::plan::DeclarativePlanNode)
//! evaluation back to a caller-provided C callback
//! ([`CExecutePlan`](executor::CExecutePlan)). Plan nodes are ferried across the boundary as
//! protobuf-serialized bytes -- see [`proto`].
//!
//! The result-passing direction of the callback is intentionally a stub for now; see
//! [`executor::CExecutePlan`] for the current contract.

#[cfg(feature = "default-engine-base")]
pub mod executor;

#[cfg(feature = "default-engine-base")]
pub mod proto;
