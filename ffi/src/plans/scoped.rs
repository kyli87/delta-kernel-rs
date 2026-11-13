//! Scoped plan execution keeps staged and prepared transaction rows in the connector.

use std::sync::Arc;

use delta_kernel::plans::proto::plan as proto_plan;
use delta_kernel::plans::{Plan, RelationRef, ScopedPlanExecutor};
use delta_kernel::schema::StructType;
use delta_kernel::{KernelError, KernelResult, Operation, PlanExecutor, PlanResult};
use delta_kernel_ffi_macros::handle_descriptor;
use prost::Message;

use super::executor::{CExecuteOpFn, FfiPlanExecutor};
use crate::error::EngineExecResult;
use crate::handle::Handle;
use crate::{ExclusiveRustBytes, KernelBytesSlice, KernelStringSlice, NullableCvoid};

/// A shared handle to a connector-owned execution scope.
#[handle_descriptor(target=dyn ScopedPlanExecutor, mutable=false)]
pub struct SharedScopedPlanExecutor;

/// Executes a plan and transfers an encoded `RelationSourceNode` back to Kernel.
///
/// `name` and `plan_proto` are borrowed for this callback. `out` is initialized to `Uninit`;
/// write `Success` with bytes from `allocate_kernel_bytes`, or `Failure` with a Rust-owned error
/// message. Kernel consumes the returned bytes or error exactly once.
pub type CExecuteAndRetainFn = extern "C" fn(
    context: NullableCvoid,
    name: KernelStringSlice,
    plan_proto: KernelBytesSlice,
    out: *mut EngineExecResult<Handle<ExclusiveRustBytes>>,
);

struct FfiScopedPlanExecutor {
    executor: FfiPlanExecutor,
    context: NullableCvoid,
    retain: CExecuteAndRetainFn,
}

// SAFETY: The constructor requires thread-safe callbacks and context for the handle's lifetime.
unsafe impl Send for FfiScopedPlanExecutor {}
unsafe impl Sync for FfiScopedPlanExecutor {}

impl PlanExecutor for FfiScopedPlanExecutor {
    fn execute_op(&self, op: Operation) -> KernelResult<PlanResult> {
        self.executor.execute_op(op)
    }
}

impl ScopedPlanExecutor for FfiScopedPlanExecutor {
    fn execute_and_retain(&self, name: &str, plan: Plan) -> KernelResult<RelationRef> {
        let schema = plan.schema.clone();
        let bytes = proto_plan::Plan::from(&plan).encode_to_vec();
        let mut out = EngineExecResult::Uninit;
        (self.retain)(
            self.context,
            crate::kernel_string_slice!(name),
            crate::kernel_bytes_slice!(bytes),
            &mut out,
        );
        let relation = match out {
            EngineExecResult::Success(bytes) => {
                let bytes = unsafe { bytes.into_inner() };
                decode_relation_ref(&bytes)?
            }
            EngineExecResult::Failure(error) => return Err(error.into()),
            EngineExecResult::Uninit => {
                return Err(KernelError::internal_error(
                    "FFI engine returned from execute_and_retain without writing a result",
                ));
            }
        };
        if relation.schema().as_ref() != schema.as_ref() {
            return Err(KernelError::generic(
                "Retained relation schema differs from the executed plan schema",
            ));
        }
        Ok(relation)
    }
}

/// Creates a scoped executor backed by connector callbacks.
///
/// # Safety
/// The callbacks and optional context must remain valid and thread-safe until the handle is
/// freed. Retained relations must remain readable through this scope until commit returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn get_scoped_plan_executor(
    context: NullableCvoid,
    execute: CExecuteOpFn,
    retain: CExecuteAndRetainFn,
) -> Handle<SharedScopedPlanExecutor> {
    let executor: Arc<dyn ScopedPlanExecutor> = Arc::new(FfiScopedPlanExecutor {
        executor: FfiPlanExecutor::new(context, execute),
        context,
        retain,
    });
    executor.into()
}

/// Releases a scoped executor handle without releasing connector-owned relation storage.
///
/// # Safety
/// `executor` must be a valid owned handle. It is consumed and must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_scoped_plan_executor(executor: Handle<SharedScopedPlanExecutor>) {
    unsafe { executor.drop_handle() }
}

pub(crate) fn decode_relation_ref(bytes: &[u8]) -> KernelResult<RelationRef> {
    let relation =
        proto_plan::RelationSourceNode::decode(bytes).map_err(KernelError::generic_err)?;
    let schema = relation
        .schema
        .ok_or_else(|| KernelError::generic("Relation reference has no schema"))?;
    if relation.id.is_empty() {
        return Err(KernelError::generic("Relation reference has an empty id"));
    }
    Ok(RelationRef::new(
        relation.id,
        Arc::new(StructType::try_from(schema)?),
    ))
}

pub(crate) unsafe fn decode_relation_slice(bytes: KernelBytesSlice) -> KernelResult<RelationRef> {
    if bytes.ptr.is_null() {
        return Err(KernelError::generic(
            "Relation reference bytes must not be null",
        ));
    }
    decode_relation_ref(unsafe { std::slice::from_raw_parts(bytes.ptr, bytes.len) })
}

#[cfg(test)]
mod tests {
    use std::ptr::NonNull;
    use std::sync::Mutex;

    use delta_kernel::plans::PlanBuilder;
    use delta_kernel::plans::proto::schema as proto_schema;
    use delta_kernel::schema::schema_ref;
    use rstest::rstest;

    use super::*;
    use crate::error::{EngineExecError, FFIKernelError};
    use crate::plans::result::CPlanResult;

    extern "C" fn no_execute(
        _context: NullableCvoid,
        _plan: KernelBytesSlice,
        _out: *mut EngineExecResult<CPlanResult>,
    ) {
        panic!("retain must not call execute_op");
    }

    extern "C" fn retain(
        context: NullableCvoid,
        name: KernelStringSlice,
        plan: KernelBytesSlice,
        out: *mut EngineExecResult<Handle<ExclusiveRustBytes>>,
    ) {
        let result = unsafe {
            &*context
                .unwrap()
                .as_ptr()
                .cast::<Mutex<Option<EngineExecResult<Handle<ExclusiveRustBytes>>>>>()
        };
        assert_eq!(unsafe { name.try_to_string() }.unwrap(), "prepared");
        let plan = unsafe { plan.try_as_slice() }.unwrap();
        assert!(!proto_plan::Plan::decode(plan).unwrap().nodes.is_empty());
        unsafe { out.write(result.lock().unwrap().take().unwrap()) };
    }

    #[rstest]
    #[case::valid(0, None)]
    #[case::wrong_schema(1, Some("schema differs"))]
    #[case::missing_schema(2, Some("no schema"))]
    #[case::invalid_proto(3, Some("failed to decode"))]
    #[case::empty_id(4, Some("empty id"))]
    #[case::callback_error(5, Some("retain failed"))]
    #[case::uninitialized(6, Some("without writing"))]
    fn retained_result_validates_schema_and_consumes_owned_result(
        #[case] mode: usize,
        #[case] expected_error: Option<&str>,
    ) {
        let schema = schema_ref! { nullable "id": LONG };
        let output_schema = if mode == 1 {
            schema_ref! { nullable "other": STRING }
        } else {
            schema.clone()
        };
        let relation = proto_plan::RelationSourceNode {
            id: if mode == 4 { "" } else { "retained-1" }.into(),
            schema: (mode != 2).then(|| proto_schema::StructType::from(output_schema.as_ref())),
        };
        let bytes = if mode == 3 {
            vec![0xff]
        } else {
            relation.encode_to_vec()
        };
        let result: EngineExecResult<Handle<ExclusiveRustBytes>> = match mode {
            5 => EngineExecResult::Failure(EngineExecError {
                etype: FFIKernelError::GenericError,
                message: Box::new("retain failed".to_string()).into(),
            }),
            6 => EngineExecResult::Uninit,
            _ => EngineExecResult::Success(Box::new(bytes).into()),
        };
        let result = Mutex::new(Some(result));
        let context = Some(NonNull::from_ref(&result).cast());
        let executor = unsafe { get_scoped_plan_executor(context, no_execute, retain) };
        let plan = PlanBuilder::values(schema.clone(), vec![])
            .unwrap()
            .build()
            .unwrap();
        let result = unsafe { executor.as_ref() }.execute_and_retain("prepared", plan);
        match expected_error {
            Some(expected) => assert!(result.unwrap_err().to_string().contains(expected)),
            None => {
                let relation = result.unwrap();
                assert_eq!(relation.id(), "retained-1");
                assert_eq!(relation.schema(), &schema);
            }
        }
        unsafe { free_scoped_plan_executor(executor) };
    }
}
