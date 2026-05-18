//! FFI [`PlanExecutor`] backed by a caller-provided C callback.
//!
//! [`FfiPlanExecutor`] serializes each [`DeclarativePlanNode`] to protobuf bytes (see
//! [`crate::plan::proto`]) and invokes the C callback to delegate evaluation. The callback
//! returns a [`CPlanResult`] tagged union -- streaming variants carry opaque iterators
//! (`CEngineDataIterator` / `CBytesIterator`) that the kernel drains lazily, mirroring the
//! shape of [`PlanResult`].

use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::plan::{DeclarativePlanNode, PlanExecutor, PlanResult};
use delta_kernel::{DeltaResult, EngineData, Error};
use delta_kernel_ffi_macros::handle_descriptor;
use tracing::debug;

use crate::handle::Handle;
use crate::plan::proto;
use crate::{ExclusiveEngineData, ExclusiveRustString, KernelBytesSlice, NullableCvoid};

// ============================================================================
// C wire types
// ============================================================================

/// A foreign byte buffer transferred to kernel as part of a [`CNextBytes::Some`].
///
/// Zero-copy: kernel borrows the bytes from `ptr`/`len` for as long as the resulting
/// [`Bytes`] (and any clones) are alive. When the last clone drops, `free` (if non-NULL) is
/// called exactly once to release the engine's allocation.
///
/// Pass `free = None` if the buffer's lifetime is tied to something else -- for example, a
/// statically owned slice, or a buffer owned by the iterator's `state` and released by the
/// iterator's `free` fn.
///
/// Construct only as a [`CNextBytes::Some`] payload; the [`Drop`] impl (which invokes
/// `free`) runs whenever a value of this type goes out of scope on the Rust side.
#[repr(C)]
pub struct EngineAllocatedBytes {
    pub ptr: *const u8,
    pub len: usize,
    pub free: Option<extern "C" fn(ptr: *const u8, len: usize)>,
}

impl AsRef<[u8]> for EngineAllocatedBytes {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: caller's contract is that `ptr`/`len` describe a valid buffer that lives
        // until this `EngineAllocatedBytes` is dropped.
        unsafe { std::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl Drop for EngineAllocatedBytes {
    fn drop(&mut self) {
        if let Some(free) = self.free {
            free(self.ptr, self.len);
        }
    }
}

// SAFETY: callers' contract requires `ptr`/`free` to be thread-safe (matches the
// `FfiPlanExecutor`/`FfiUCCommitClient` precedent).
unsafe impl Send for EngineAllocatedBytes {}
unsafe impl Sync for EngineAllocatedBytes {}

/// Result of advancing a [`CEngineDataIterator`] one step.
///
/// Variants:
/// - [`Self::Some`] -- a new [`EngineData`] batch. Ownership of the handle transfers to Rust; the
///   engine MUST NOT call `free_engine_data` on it.
/// - [`Self::None`] -- iteration is complete. After returning `None` the iterator's `free` fn will
///   be called and `next` will not be called again.
/// - [`Self::Err`] -- the iterator failed. The handle is consumed by Rust and surfaced as an
///   [`Error::generic`] (typically allocated via [`crate::allocate_kernel_string`]).
#[repr(C)]
pub enum CNextEngineData {
    Some(Handle<ExclusiveEngineData>),
    None,
    Err(Handle<ExclusiveRustString>),
}

/// Result of advancing a [`CBytesIterator`] one step.
///
/// Variants:
/// - [`Self::Some`] -- a new byte buffer. Ownership transfers to Rust; see [`EngineAllocatedBytes`]
///   for how the engine releases the underlying allocation.
/// - [`Self::None`] -- iteration is complete (same semantics as [`CNextEngineData::None`]).
/// - [`Self::Err`] -- the iterator failed (same semantics as [`CNextEngineData::Err`]).
#[repr(C)]
pub enum CNextBytes {
    Some(EngineAllocatedBytes),
    None,
    Err(Handle<ExclusiveRustString>),
}

/// An engine-implemented streaming iterator of [`EngineData`] batches.
///
/// `state` is opaque to kernel; it is passed back into `next`. `next` returns
/// [`CNextEngineData`] until iteration completes. The iterator does NOT own its state --
/// state lifetime is managed at the [`CPlanResultWrapper`] level via its `free` callback,
/// which is invoked when the kernel-side adapter is dropped (which itself happens after the
/// consuming code releases the [`PlanResult::Data`] iterator).
#[repr(C)]
pub struct CEngineDataIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(state: NullableCvoid) -> CNextEngineData,
}

/// An engine-implemented streaming iterator of byte buffers. See [`CEngineDataIterator`] for
/// state semantics; cleanup is performed by [`CPlanResultWrapper::free`].
#[repr(C)]
pub struct CBytesIterator {
    pub state: NullableCvoid,
    pub next: extern "C" fn(state: NullableCvoid) -> CNextBytes,
}

/// The result of executing a [`DeclarativePlanNode`] via [`CExecutePlan`].
///
/// Mirrors [`PlanResult`]: streaming variants carry opaque iterators that kernel drains, and
/// errors transfer ownership of an allocated string handle to Rust. Always carried inside a
/// [`CPlanResultWrapper`], which provides the engine-side cleanup callback covering this
/// `CPlanResult` and any state referenced by its nested iterators.
#[repr(C)]
pub enum CPlanResult {
    /// The plan completed successfully with no output data.
    Unit,
    /// The plan produced a stream of [`EngineData`] batches.
    Data(CEngineDataIterator),
    /// The plan produced a stream of byte buffers.
    Bytes(CBytesIterator),
    /// The plan failed; the handle carries an owned error message.
    Err(Handle<ExclusiveRustString>),
}

/// Engine-allocated wrapper around a [`CPlanResult`]. The engine attaches an opaque `state`
/// pointer and a `free` callback; kernel guarantees `free(state)` is invoked exactly once
/// after the result (and any iterators it carries) is no longer needed. This lets the
/// engine maintain a single arena per plan execution that owns the result enum, any
/// iterator state, and any buffers transitively referenced.
///
/// `state` is opaque to kernel and is passed verbatim back to `free`. It is valid to pass
/// `None` for `state` if `free` requires no context (e.g. a no-op cleanup).
#[repr(C)]
pub struct CPlanResultWrapper {
    pub result: CPlanResult,
    pub state: NullableCvoid,
    pub free: extern "C" fn(state: NullableCvoid),
}

/// C callback that delegates [`DeclarativePlanNode`] execution to the engine.
///
/// `plan_proto` is a borrowed slice containing a serialized
/// [`pb::DeclarativePlanNode`](crate::plan::proto::pb::DeclarativePlanNode); it is only valid
/// for the duration of the callback. `context` is the opaque pointer originally passed to
/// [`get_ffi_plan_executor`].
///
/// The returned [`CPlanResultWrapper`] is consumed by the kernel; ownership of any handles
/// or engine-owned buffers it carries transfers to Rust per the rules documented on the
/// individual [`CPlanResult`] variants and on [`CPlanResultWrapper`].
pub type CExecutePlan =
    extern "C" fn(context: NullableCvoid, plan_proto: KernelBytesSlice) -> CPlanResultWrapper;

// ============================================================================
// Rust-side adapters
// ============================================================================

/// RAII guard that invokes [`CPlanResultWrapper::free`] exactly once on drop, releasing all
/// engine-side state associated with a plan result (including iterator state and any
/// transitively referenced buffers).
struct PlanResultCleanup {
    state: NullableCvoid,
    free: extern "C" fn(state: NullableCvoid),
}

impl Drop for PlanResultCleanup {
    fn drop(&mut self) {
        (self.free)(self.state);
    }
}

unsafe impl Send for PlanResultCleanup {}

/// Drains a [`CEngineDataIterator`] as a Rust iterator of [`EngineData`] batches. Cleanup
/// of the underlying iterator state is handled by the embedded [`PlanResultCleanup`] when
/// the iterator is dropped.
struct FfiDataIter {
    iter: CEngineDataIterator,
    _cleanup: PlanResultCleanup,
}

impl Iterator for FfiDataIter {
    type Item = DeltaResult<Box<dyn EngineData>>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next)(self.iter.state) {
            CNextEngineData::Some(h) => Some(Ok(unsafe { h.into_inner() })),
            CNextEngineData::None => None,
            CNextEngineData::Err(h) => {
                let s = unsafe { h.into_inner() };
                Some(Err(Error::generic(*s)))
            }
        }
    }
}

// SAFETY: callers' contract requires the engine to provide thread-safe state/fn pointers
// (mirrors `FfiPlanExecutor`).
unsafe impl Send for FfiDataIter {}

/// Drains a [`CBytesIterator`] as a Rust iterator of [`Bytes`] buffers. Cleanup of the
/// underlying iterator state is handled by the embedded [`PlanResultCleanup`] when the
/// iterator is dropped.
struct FfiBytesIter {
    iter: CBytesIterator,
    _cleanup: PlanResultCleanup,
}

impl Iterator for FfiBytesIter {
    type Item = DeltaResult<Bytes>;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.iter.next)(self.iter.state) {
            CNextBytes::Some(raw) => Some(Ok(Bytes::from_owner(raw))),
            CNextBytes::None => None,
            CNextBytes::Err(h) => {
                let s = unsafe { h.into_inner() };
                Some(Err(Error::generic(*s)))
            }
        }
    }
}

// ============================================================================
// FfiPlanExecutor
// ============================================================================

/// A [`PlanExecutor`] that serializes plans to protobuf and forwards them to a C callback.
///
/// Construct via [`get_ffi_plan_executor`] and free via [`free_ffi_plan_executor`].
pub struct FfiPlanExecutor {
    context: NullableCvoid,
    callback: CExecutePlan,
}

// SAFETY: NullableCvoid (a raw pointer) is not Send/Sync by default. The contract for callers
// of `get_ffi_plan_executor` is documented to require a thread-safe context pointer.
unsafe impl Send for FfiPlanExecutor {}
unsafe impl Sync for FfiPlanExecutor {}

impl PlanExecutor for FfiPlanExecutor {
    fn execute_plan(&self, plan: DeclarativePlanNode) -> DeltaResult<PlanResult> {
        let bytes = proto::encode_plan(plan);
        // SAFETY: `bytes` is owned by this stack frame and outlives the callback invocation.
        // The callback contract forbids retaining the slice past the call.
        let slice = unsafe { KernelBytesSlice::new_unsafe(&bytes) };
        let CPlanResultWrapper {
            result,
            state,
            free,
        } = (self.callback)(self.context, slice);
        let cleanup = PlanResultCleanup { state, free };
        match result {
            CPlanResult::Unit => {
                drop(cleanup);
                Ok(PlanResult::Unit)
            }
            CPlanResult::Data(it) => Ok(PlanResult::Data(Box::new(FfiDataIter {
                iter: it,
                _cleanup: cleanup,
            }))),
            CPlanResult::Bytes(it) => Ok(PlanResult::ByteStream(Box::new(FfiBytesIter {
                iter: it,
                _cleanup: cleanup,
            }))),
            CPlanResult::Err(h) => {
                let s = unsafe { h.into_inner() };
                drop(cleanup);
                Err(Error::generic(*s))
            }
        }
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
    use std::collections::VecDeque;
    use std::ffi::c_void;
    use std::ptr::NonNull;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};

    use delta_kernel::arrow::array::{ArrayRef, Int32Array, RecordBatch};
    use delta_kernel::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use delta_kernel::engine::arrow_data::ArrowEngineData;
    use delta_kernel::plan::{DeclarativePlanNode, PlanExecutor, PlanResult};

    use super::*;
    use crate::ffi_test_utils::{allocate_err, ok_or_panic};
    use crate::{allocate_kernel_string, kernel_string_slice};

    // === Common helpers ===

    fn make_int_batch(values: &[i32]) -> Box<dyn EngineData> {
        let array: ArrayRef = Arc::new(Int32Array::from(values.to_vec()));
        let schema = Arc::new(Schema::new(vec![Field::new(
            "x",
            ArrowDataType::Int32,
            false,
        )]));
        let batch = RecordBatch::try_new(schema, vec![array]).unwrap();
        Box::new(ArrowEngineData::new(batch))
    }

    fn batch_values(batch: &dyn EngineData) -> Vec<i32> {
        let arrow = batch
            .any_ref()
            .downcast_ref::<ArrowEngineData>()
            .expect("ArrowEngineData");
        let col = arrow
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        (0..col.len()).map(|i| col.value(i)).collect()
    }

    // === Common test free fns ===

    /// No-op wrapper-level free for tests that don't need to observe cleanup.
    extern "C" fn noop_free(_state: NullableCvoid) {}

    /// Wrapper-level free that interprets `state` as a strong `Arc<AtomicBool>` reference
    /// (from `Arc::into_raw`). Reconstructs the Arc (releasing the strong count) and
    /// flips the bool to `true` so the test can observe via a sibling clone that cleanup
    /// ran.
    extern "C" fn flag_free(state: NullableCvoid) {
        let raw = state.unwrap().as_ptr() as *const AtomicBool;
        // SAFETY: each call site allocates the AtomicBool via `Arc::into_raw` and arranges
        // for `flag_free` to be invoked exactly once with the corresponding raw pointer.
        let flag = unsafe { Arc::from_raw(raw) };
        flag.store(true, Ordering::SeqCst);
        drop(flag);
    }

    // === Existing test: callback receives valid proto, returns Unit ===

    struct CallbackProbe {
        invoked: AtomicBool,
        last_plan_len: AtomicUsize,
    }

    extern "C" fn record_callback(
        context: NullableCvoid,
        plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        let probe = unsafe { &*(context.unwrap().as_ptr() as *const CallbackProbe) };
        probe.invoked.store(true, Ordering::SeqCst);
        probe.last_plan_len.store(plan_proto.len, Ordering::SeqCst);
        let bytes = unsafe { std::slice::from_raw_parts(plan_proto.ptr, plan_proto.len) };
        let _ = proto::decode_plan(bytes).expect("callback received valid proto");
        CPlanResultWrapper {
            result: CPlanResult::Unit,
            state: None,
            free: noop_free,
        }
    }

    #[test]
    fn ffi_plan_executor_invokes_callback_with_proto_bytes() {
        let probe = Box::new(CallbackProbe {
            invoked: AtomicBool::new(false),
            last_plan_len: AtomicUsize::new(0),
        });
        let probe_ptr = Box::into_raw(probe);
        let context = NonNull::new(probe_ptr as *mut c_void);

        let executor = FfiPlanExecutor {
            context,
            callback: record_callback,
        };

        let url = url::Url::parse("memory:///table/_delta_log/").unwrap();
        let result = executor
            .execute_plan(DeclarativePlanNode::FileListing { url })
            .expect("execute_plan succeeds when callback returns Unit");
        assert!(matches!(result, PlanResult::Unit));

        let probe = unsafe { Box::from_raw(probe_ptr) };
        assert!(probe.invoked.load(Ordering::SeqCst));
        assert!(probe.last_plan_len.load(Ordering::SeqCst) > 0);
    }

    // === Data iterator test ===

    /// Iterator state. `free_called` is an `Arc` so the test can observe it after
    /// `data_iter_free` reconstructs and drops the owning Box.
    struct DataIterState {
        batches: Mutex<VecDeque<Box<dyn EngineData>>>,
        free_called: Arc<AtomicBool>,
    }

    extern "C" fn data_iter_next(state: NullableCvoid) -> CNextEngineData {
        let state = unsafe { &*(state.unwrap().as_ptr() as *const DataIterState) };
        let mut batches = state.batches.lock().unwrap();
        match batches.pop_front() {
            Some(batch) => CNextEngineData::Some(batch.into()),
            None => CNextEngineData::None,
        }
    }

    extern "C" fn data_iter_free(state: NullableCvoid) {
        let raw = state.unwrap().as_ptr() as *mut DataIterState;
        // SAFETY: `data_iter_free` is called exactly once via the `CPlanResultWrapper`'s
        // `free` callback (driven by `FfiDataIter`'s embedded `PlanResultCleanup` on drop),
        // after which the state pointer is dead. Reconstructing the Box releases the state.
        let state = unsafe { Box::from_raw(raw) };
        state.free_called.store(true, Ordering::SeqCst);
        drop(state);
    }

    extern "C" fn data_callback(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        // Context here _is_ the iterator state pointer; we share it with the wrapper's
        // free callback so the iterator's state is released exactly once when the kernel
        // drops the `PlanResult::Data` iterator.
        CPlanResultWrapper {
            result: CPlanResult::Data(CEngineDataIterator {
                state: context,
                next: data_iter_next,
            }),
            state: context,
            free: data_iter_free,
        }
    }

    #[test]
    fn data_iterator_drains_callback_batches() {
        let free_called = Arc::new(AtomicBool::new(false));
        let state = Box::into_raw(Box::new(DataIterState {
            batches: Mutex::new(
                [vec![1, 2, 3], vec![4, 5]]
                    .into_iter()
                    .map(|v| make_int_batch(&v))
                    .collect(),
            ),
            free_called: free_called.clone(),
        }));
        let context = NonNull::new(state as *mut c_void);

        {
            let executor = FfiPlanExecutor {
                context,
                callback: data_callback,
            };

            let url = url::Url::parse("memory:///t/").unwrap();
            let result = executor
                .execute_plan(DeclarativePlanNode::FileListing { url })
                .expect("execute_plan succeeds");
            let PlanResult::Data(iter) = result else {
                panic!("expected PlanResult::Data");
            };

            let collected: Vec<Vec<i32>> =
                iter.map(|r| batch_values(r.unwrap().as_ref())).collect();
            assert_eq!(collected, vec![vec![1, 2, 3], vec![4, 5]]);
            // Iterator dropped here -> data_iter_free runs.
        }

        assert!(
            free_called.load(Ordering::SeqCst),
            "iterator's `free` should have run exactly once"
        );
    }

    // === Bytes iterator test ===

    /// Tracks per-buffer `free` calls for `bytes_iterator_drains_callback_buffers`. Bound
    /// to that test only -- the per-buffer free fn signature has no context parameter, so
    /// we use a static counter.
    static BYTES_OUTSTANDING: AtomicUsize = AtomicUsize::new(0);

    /// Reconstructs the leaked `Vec<u8>` (capacity == len, see `bytes_iter_next`) and drops
    /// it, decrementing the outstanding-buffer counter.
    extern "C" fn bytes_buffer_free(ptr: *const u8, len: usize) {
        // SAFETY: every buffer handed out by `bytes_iter_next` was produced via
        // `Vec::shrink_to_fit` + `mem::forget`, with capacity == len.
        let _vec = unsafe { Vec::from_raw_parts(ptr as *mut u8, len, len) };
        BYTES_OUTSTANDING.fetch_sub(1, Ordering::SeqCst);
    }

    struct BytesIterState {
        buffers: Mutex<VecDeque<Vec<u8>>>,
        free_called: Arc<AtomicBool>,
    }

    extern "C" fn bytes_iter_next(state: NullableCvoid) -> CNextBytes {
        let state = unsafe { &*(state.unwrap().as_ptr() as *const BytesIterState) };
        let mut buffers = state.buffers.lock().unwrap();
        match buffers.pop_front() {
            Some(mut buf) => {
                buf.shrink_to_fit();
                let len = buf.len();
                debug_assert_eq!(buf.capacity(), len);
                let ptr = buf.as_ptr();
                std::mem::forget(buf);
                BYTES_OUTSTANDING.fetch_add(1, Ordering::SeqCst);
                CNextBytes::Some(EngineAllocatedBytes {
                    ptr,
                    len,
                    free: Some(bytes_buffer_free),
                })
            }
            None => CNextBytes::None,
        }
    }

    extern "C" fn bytes_iter_free(state: NullableCvoid) {
        let raw = state.unwrap().as_ptr() as *mut BytesIterState;
        // SAFETY: `bytes_iter_free` is called exactly once via the `CPlanResultWrapper`'s
        // `free` callback (driven by `FfiBytesIter`'s embedded `PlanResultCleanup` on drop).
        let state = unsafe { Box::from_raw(raw) };
        state.free_called.store(true, Ordering::SeqCst);
        drop(state);
    }

    extern "C" fn bytes_callback(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        CPlanResultWrapper {
            result: CPlanResult::Bytes(CBytesIterator {
                state: context,
                next: bytes_iter_next,
            }),
            state: context,
            free: bytes_iter_free,
        }
    }

    #[test]
    fn bytes_iterator_drains_callback_buffers() {
        BYTES_OUTSTANDING.store(0, Ordering::SeqCst);

        let free_called = Arc::new(AtomicBool::new(false));
        let state = Box::into_raw(Box::new(BytesIterState {
            buffers: Mutex::new(VecDeque::from(vec![
                b"hello".to_vec(),
                b"world".to_vec(),
                b"!".to_vec(),
            ])),
            free_called: free_called.clone(),
        }));
        let context = NonNull::new(state as *mut c_void);

        {
            let executor = FfiPlanExecutor {
                context,
                callback: bytes_callback,
            };

            let url = url::Url::parse("memory:///bytes/").unwrap();
            let result = executor
                .execute_plan(DeclarativePlanNode::FileListing { url })
                .expect("execute_plan succeeds");
            let PlanResult::ByteStream(iter) = result else {
                panic!("expected PlanResult::ByteStream");
            };

            let collected: Vec<Vec<u8>> = iter.map(|r| r.unwrap().to_vec()).collect();
            assert_eq!(
                collected,
                vec![b"hello".to_vec(), b"world".to_vec(), b"!".to_vec()]
            );
            // Each `Bytes` was dropped as we collected, so every per-buffer `free` should
            // have run -- the outstanding counter must be zero before the iterator itself
            // drops.
            assert_eq!(BYTES_OUTSTANDING.load(Ordering::SeqCst), 0);
            // iter dropped here -> bytes_iter_free runs.
        }

        assert!(
            free_called.load(Ordering::SeqCst),
            "iterator's `free` should have run exactly once"
        );
    }

    // === Error variant test ===

    extern "C" fn error_callback(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        let msg = "boom";
        let handle = unsafe {
            ok_or_panic(allocate_kernel_string(
                kernel_string_slice!(msg),
                allocate_err,
            ))
        };
        CPlanResultWrapper {
            result: CPlanResult::Err(handle),
            state: context,
            free: flag_free,
        }
    }

    #[test]
    fn error_variant_surfaces_to_kernel() {
        let flag = Arc::new(AtomicBool::new(false));
        let raw = Arc::into_raw(flag.clone());
        let context = NonNull::new(raw as *mut c_void);

        let executor = FfiPlanExecutor {
            context,
            callback: error_callback,
        };

        let url = url::Url::parse("memory:///e/").unwrap();
        let err = executor
            .execute_plan(DeclarativePlanNode::FileListing { url })
            .expect_err("execute_plan should fail with Err callback");
        let msg = format!("{err}");
        assert!(msg.contains("boom"), "expected 'boom' in error: {msg}");
        assert!(
            flag.load(Ordering::SeqCst),
            "wrapper-level free should have fired for the Err variant"
        );
    }

    // === Unit variant invokes wrapper free ===

    extern "C" fn unit_free_callback(
        context: NullableCvoid,
        _plan_proto: KernelBytesSlice,
    ) -> CPlanResultWrapper {
        CPlanResultWrapper {
            result: CPlanResult::Unit,
            state: context,
            free: flag_free,
        }
    }

    #[test]
    fn unit_variant_invokes_wrapper_free() {
        let flag = Arc::new(AtomicBool::new(false));
        let raw = Arc::into_raw(flag.clone());
        let context = NonNull::new(raw as *mut c_void);

        let executor = FfiPlanExecutor {
            context,
            callback: unit_free_callback,
        };
        let url = url::Url::parse("memory:///u/").unwrap();
        let result = executor
            .execute_plan(DeclarativePlanNode::FileListing { url })
            .expect("execute_plan succeeds when callback returns Unit");
        assert!(matches!(result, PlanResult::Unit));
        assert!(
            flag.load(Ordering::SeqCst),
            "wrapper-level free should have fired for the Unit variant"
        );
    }
}
