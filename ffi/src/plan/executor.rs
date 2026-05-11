//! FFI [`PlanExecutor`] backed by a caller-provided C callback.
//!
//! [`FfiPlanExecutor`] serializes each [`DeclarativePlanNode`] to protobuf bytes (see
//! [`crate::plan::proto`]) and invokes the C callback to delegate evaluation. The result-passing
//! direction of the callback is intentionally a stub for now -- see [`CExecutePlan`].

use std::sync::Arc;

use delta_kernel::plan::{DeclarativePlanNode, PlanExecutor, PlanResult};
use delta_kernel::{DeltaResult, Error};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;

use crate::handle::Handle;
use crate::plan::proto;
use crate::{ExclusiveRustString, KernelBytesSlice, NullableCvoid, OptionalValue};

/// C callback that delegates [`DeclarativePlanNode`] execution to the engine.
///
/// `plan_proto` is a borrowed slice containing a serialized
/// [`pb::DeclarativePlanNode`](crate::plan::proto::pb::DeclarativePlanNode); it is only valid
/// for the duration of the callback. `context` is the opaque pointer originally passed to
/// [`get_ffi_plan_executor`].
///
/// Return value:
///
/// - [`OptionalValue::None`] -- the plan was executed successfully. The Rust side currently
///   surfaces this as [`PlanResult::Unit`] (see TODO below).
/// - [`OptionalValue::Some`] -- the plan failed with the given error message. The handle ownership
///   transfers back to Rust, which converts it into [`Error::generic`].
///
/// TODO: This signature does not yet carry actual [`PlanResult`] payloads (data batches or
/// byte streams). A follow-up will introduce an opaque result builder so the callback can
/// stream `EngineData`/`Bytes` back. Until then, calls that would normally produce data will
/// fail with a placeholder error from the kernel handlers downstream.
pub type CExecutePlan = extern "C" fn(
    context: NullableCvoid,
    plan_proto: KernelBytesSlice,
) -> OptionalValue<Handle<ExclusiveRustString>>;

/// A [`PlanExecutor`] that serializes plans to protobuf and forwards them to a C callback.
///
/// Construct via [`get_ffi_plan_executor`] and free via [`free_ffi_plan_executor`].
pub struct FfiPlanExecutor {
    context: NullableCvoid,
    callback: CExecutePlan,
}

// SAFETY: NullableCvoid (a raw pointer) is not Send/Sync by default. The contract for callers
// of `get_ffi_plan_executor` is documented to require a thread-safe context pointer
unsafe impl Send for FfiPlanExecutor {}
unsafe impl Sync for FfiPlanExecutor {}

impl FfiPlanExecutor {
    /// Invoke the underlying C callback with already-encoded protobuf bytes. Used by the
    /// `PlanExecutor` impl and by tests that want to bypass encoding.
    fn invoke(&self, plan_proto: &[u8]) -> DeltaResult<()> {
        // SAFETY: `plan_proto` is borrowed from the local stack (the encoded buffer below) and
        // is valid for the duration of the callback. The callback contract forbids retaining
        // the slice past the call.
        let slice = unsafe { KernelBytesSlice::new_unsafe(plan_proto) };
        match (self.callback)(self.context, slice) {
            OptionalValue::None => Ok(()),
            OptionalValue::Some(handle) => {
                // SAFETY: Convention dictates that on `Some`, the callback hands back an owned
                // `ExclusiveRustString` handle (typically allocated via `allocate_kernel_string`)
                // for us to consume. Mirrors `FfiUCCommitClient::commit`.
                let boxed = unsafe { handle.into_inner() };
                Err(Error::generic(*boxed))
            }
        }
    }
}

impl PlanExecutor for FfiPlanExecutor {
    fn execute_plan(&self, plan: DeclarativePlanNode) -> DeltaResult<PlanResult> {
        let bytes = proto::encode_plan(plan);
        self.invoke(&bytes)?;
        // TODO: return real `PlanResult::Data` / `PlanResult::ByteStream` once the callback
        //   contract is designed (see `CExecutePlan` docs).
        Ok(PlanResult::Unit)
    }
}

/// An opaque shared (Arc-like) handle to an [`FfiPlanExecutor`].
///
/// Produced by [`get_ffi_plan_executor`]; consumed by `set_builder_plan_executor` (in
/// [`crate::lib`](crate)) or freed via [`free_ffi_plan_executor`].
#[handle_descriptor(target=FfiPlanExecutor, mutable=false, sized=true)]
pub struct SharedPlanExecutor;

/// Build an [`FfiPlanExecutor`] backed by a caller-provided C callback.
///
/// The returned handle can be passed to `set_builder_plan_executor` to construct a
/// [`PlanBasedEngine`](delta_kernel::engine::plan::PlanBasedEngine), or freed via
/// [`free_ffi_plan_executor`] if no longer needed.
///
/// IMPORTANT: The pointer passed for the context MUST be thread-safe (i.e. be able to be sent
/// between threads safely) and MUST remain valid for as long as the executor is used. It is
/// valid to pass NULL as the context.
///
/// # Safety
///
/// Caller is responsible for passing a valid pointer for the callback and a valid (or NULL)
/// context pointer.
#[no_mangle]
pub unsafe extern "C" fn get_ffi_plan_executor(
    context: NullableCvoid,
    callback: CExecutePlan,
) -> Handle<SharedPlanExecutor> {
    Arc::new(FfiPlanExecutor { context, callback }).into()
}

/// Free a plan executor obtained via [`get_ffi_plan_executor`]. Normally the value returned
/// from `get_ffi_plan_executor` is consumed by `set_builder_plan_executor` and need not be
/// freed by the caller; use this only when discarding the executor without using it.
///
/// # Safety
///
/// Caller is responsible for passing a valid handle obtained via [`get_ffi_plan_executor`],
/// and not using it again afterwards.
#[no_mangle]
pub unsafe extern "C" fn free_ffi_plan_executor(executor: Handle<SharedPlanExecutor>) {
    debug!("released ffi plan executor");
    executor.drop_handle();
}

#[cfg(test)]
mod tests {
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    use delta_kernel::plan::{DeclarativePlanNode, PlanExecutor, PlanResult};

    use super::*;

    struct CallbackProbe {
        invoked: AtomicBool,
        last_plan_len: AtomicUsize,
    }

    extern "C" fn record_callback(
        context: NullableCvoid,
        plan_proto: KernelBytesSlice,
    ) -> OptionalValue<Handle<ExclusiveRustString>> {
        let probe = unsafe { &*(context.unwrap().as_ptr() as *const CallbackProbe) };
        probe.invoked.store(true, Ordering::SeqCst);
        probe.last_plan_len.store(plan_proto.len, Ordering::SeqCst);
        // Verify the bytes decode back into a DeclarativePlanNode.
        let bytes = unsafe { std::slice::from_raw_parts(plan_proto.ptr, plan_proto.len) };
        let _ = proto::decode_plan(bytes).expect("callback received valid proto");
        OptionalValue::None
    }

    #[test]
    fn ffi_plan_executor_invokes_callback_with_proto_bytes() {
        let probe = Box::new(CallbackProbe {
            invoked: AtomicBool::new(false),
            last_plan_len: AtomicUsize::new(0),
        });
        let probe_ptr = Box::into_raw(probe);
        let context = NonNull::new(probe_ptr as *mut c_void);

        let executor = Arc::new(FfiPlanExecutor {
            context,
            callback: record_callback,
        });

        let url = url::Url::parse("memory:///table/_delta_log/").unwrap();
        let result = executor
            .execute_plan(DeclarativePlanNode::FileListing { url })
            .expect("execute_plan succeeds when callback returns None");
        assert!(matches!(result, PlanResult::Unit));

        let probe = unsafe { Box::from_raw(probe_ptr) };
        assert!(probe.invoked.load(Ordering::SeqCst));
        assert!(probe.last_plan_len.load(Ordering::SeqCst) > 0);
    }
}
