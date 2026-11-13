//! Transaction APIs whose file actions remain in the connector's execution scope.

use delta_kernel::plans::proto::schema as proto_schema;
use delta_kernel::schema::SchemaRef;
use delta_kernel::transaction::{CommitResult, Declarative, Transaction, UpdateTable};
use delta_kernel::{KernelError, KernelResult};
use delta_kernel_ffi_macros::handle_descriptor;
use prost::Message;

use super::committed::commit_result_to_committed_handle;
use super::{
    ExclusiveCommittedTransaction, ExclusiveUpdateTableTransactionBuilder, MutableCommitter,
};
use crate::plans::scoped::{SharedScopedPlanExecutor, decode_relation_slice};
use crate::{
    ExternResult, Handle, IntoExternResult, KernelBytesSlice, KernelOwnedBytes, KernelStringSlice,
    SharedExternEngine, TryFromStringSlice,
};

/// An owned existing-table transaction with declarative file-action staging.
#[handle_descriptor(target=Transaction<UpdateTable, Declarative>, mutable=true, sized=true)]
pub struct ExclusiveDeclarativeTransaction;

/// Builds a declarative transaction using the filesystem committer.
///
/// # Safety
/// Both handles must be valid. `builder` is consumed on success and error; `engine` is borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_build_declarative(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = builder
        .with_execution_mode::<Declarative>()
        .build(
            engine.engine().as_ref(),
            Box::new(delta_kernel::committer::FileSystemCommitter::new()),
        )
        .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Builds a declarative transaction using a connector-selected committer.
///
/// # Safety
/// `builder` and `committer` are consumed on success and error. All handles must be valid;
/// `engine` is borrowed. Committer callbacks must outlive the returned transaction.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn update_table_txn_builder_build_declarative_with_committer(
    builder: Handle<ExclusiveUpdateTableTransactionBuilder>,
    committer: Handle<MutableCommitter>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let builder = unsafe { *builder.into_inner() };
    let committer = unsafe { committer.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = builder
        .with_execution_mode::<Declarative>()
        .build(engine.engine().as_ref(), committer)
        .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Releases an uncommitted declarative transaction.
///
/// # Safety
/// `txn` must be a valid owned handle. It is consumed and must not be used again.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn free_declarative_txn(txn: Handle<ExclusiveDeclarativeTransaction>) {
    unsafe { txn.drop_handle() }
}

/// Stages metadata for newly written files.
///
/// `relation` is a protobuf `RelationSourceNode`. Kernel copies its id and schema; the
/// connector retains its rows until commit finishes.
///
/// # Safety
/// All inputs are borrowed. `txn` must point to a valid exclusive handle, and the non-null
/// relation slice must be readable for this call. `engine` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_add_files(
    txn: *mut Handle<ExclusiveDeclarativeTransaction>,
    relation: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let relation = unsafe { decode_relation_slice(relation) }?;
        let txn = unsafe { txn.as_mut() }
            .ok_or_else(|| KernelError::generic("Transaction must not be null"))?;
        unsafe { txn.as_mut() }.add_files_ref(relation);
        Ok::<_, KernelError>(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Stages scan rows for file removal.
///
/// `relation` is a protobuf `RelationSourceNode`. Kernel copies its id and schema; the
/// connector retains its rows until commit finishes.
///
/// # Safety
/// All inputs are borrowed. `txn` must point to a valid exclusive handle, and the non-null
/// relation slice must be readable for this call. `engine` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_remove_files(
    txn: *mut Handle<ExclusiveDeclarativeTransaction>,
    relation: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let relation = unsafe { decode_relation_slice(relation) }?;
        let txn = unsafe { txn.as_mut() }
            .ok_or_else(|| KernelError::generic("Transaction must not be null"))?;
        unsafe { txn.as_mut() }.remove_files_ref(relation);
        Ok::<_, KernelError>(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Stages connector-prepared Add actions.
///
/// `relation` is a protobuf `RelationSourceNode`. Kernel copies its id and schema; the
/// connector retains its rows until commit finishes.
///
/// # Safety
/// All inputs are borrowed. `txn` must point to a valid exclusive handle, and the non-null
/// relation slice must be readable for this call. `engine` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_add_actions(
    txn: *mut Handle<ExclusiveDeclarativeTransaction>,
    relation: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let relation = unsafe { decode_relation_slice(relation) }?;
        let txn = unsafe { txn.as_mut() }
            .ok_or_else(|| KernelError::generic("Transaction must not be null"))?;
        unsafe { txn.as_mut() }.add_actions_ref(relation);
        Ok::<_, KernelError>(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Stages connector-prepared Remove actions.
///
/// `relation` is a protobuf `RelationSourceNode`. Kernel copies its id and schema; the
/// connector retains its rows until commit finishes.
///
/// # Safety
/// All inputs are borrowed. `txn` must point to a valid exclusive handle, and the non-null
/// relation slice must be readable for this call. `engine` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_remove_actions(
    txn: *mut Handle<ExclusiveDeclarativeTransaction>,
    relation: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let relation = unsafe { decode_relation_slice(relation) }?;
        let txn = unsafe { txn.as_mut() }
            .ok_or_else(|| KernelError::generic("Transaction must not be null"))?;
        unsafe { txn.as_mut() }.remove_actions_ref(relation);
        Ok::<_, KernelError>(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Stages a single connector-provided commit information row.
///
/// # Safety
/// `txn` is consumed on success and error; `relation` and `engine` are borrowed and valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_with_commit_info(
    txn: Handle<ExclusiveDeclarativeTransaction>,
    relation: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe { decode_relation_slice(relation) }
        .map(|relation| Box::new(txn.with_commit_info_ref(relation)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Stages deletion-vector descriptors and the corresponding existing scan rows.
///
/// # Safety
/// All inputs are borrowed. `txn` must point to a valid exclusive handle, relation slices must
/// be non-null and readable for this call, and `engine` must be valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_update_deletion_vectors(
    txn: *mut Handle<ExclusiveDeclarativeTransaction>,
    descriptors: KernelBytesSlice,
    files: KernelBytesSlice,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<bool> {
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let descriptors = unsafe { decode_relation_slice(descriptors) }?;
        let files = unsafe { decode_relation_slice(files) }?;
        let txn = unsafe { txn.as_mut() }
            .ok_or_else(|| KernelError::generic("Transaction must not be null"))?;
        unsafe { txn.as_mut() }.update_deletion_vectors_ref(descriptors, files);
        Ok::<_, KernelError>(true)
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Returns the flat Add action schema encoded as a schema protobuf.
///
/// Release the returned buffer with `free_kernel_bytes`.
#[unsafe(no_mangle)]
pub extern "C" fn declarative_txn_add_actions_schema() -> KernelOwnedBytes {
    schema_bytes(Transaction::<UpdateTable, Declarative>::add_actions_schema())
}

/// Returns the flat Remove action schema encoded as a schema protobuf.
///
/// Release the returned buffer with `free_kernel_bytes`.
#[unsafe(no_mangle)]
pub extern "C" fn declarative_txn_remove_actions_schema() -> KernelOwnedBytes {
    schema_bytes(Transaction::<UpdateTable, Declarative>::remove_actions_schema())
}

fn schema_bytes(schema: SchemaRef) -> KernelOwnedBytes {
    proto_schema::StructType::from(schema.as_ref())
        .encode_to_vec()
        .into()
}

/// Adds or removes a domain metadata action and returns the replacement transaction.
///
/// # Safety
/// `txn` is consumed on success and error. All other inputs are borrowed for this call.
/// Strings must contain readable UTF-8. `configuration` is ignored for a removal.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_with_domain_metadata(
    txn: Handle<ExclusiveDeclarativeTransaction>,
    domain: KernelStringSlice,
    configuration: KernelStringSlice,
    removed: bool,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = (|| {
        let domain = unsafe { String::try_from_slice(&domain) }?;
        let txn = if removed {
            txn.with_domain_metadata_removed(domain)
        } else {
            let configuration = unsafe { String::try_from_slice(&configuration) }?;
            txn.with_domain_metadata(domain, configuration)
        };
        Ok::<_, KernelError>(Box::new(txn).into())
    })();
    unsafe { result.into_extern_result(&engine) }
}

/// Adds an application transaction identifier and returns the replacement transaction.
///
/// # Safety
/// `txn` is consumed on success and error. `application_id` and `engine` are borrowed and valid.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_with_transaction_id(
    txn: Handle<ExclusiveDeclarativeTransaction>,
    application_id: KernelStringSlice,
    version: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result = unsafe { String::try_from_slice(&application_id) }
        .map(|id| Box::new(txn.with_transaction_id(id, version)).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Supplies the high water mark for connector-assigned row IDs.
///
/// # Safety
/// `txn` is consumed on success and error; the valid `engine` handle is borrowed.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_with_row_tracking_high_water_mark(
    txn: Handle<ExclusiveDeclarativeTransaction>,
    high_water_mark: i64,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveDeclarativeTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let engine = unsafe { engine.as_ref() };
    let result: KernelResult<_> = txn
        .with_row_tracking_high_water_mark(high_water_mark)
        .map(|txn| Box::new(txn).into());
    unsafe { result.into_extern_result(&engine) }
}

/// Validates staged relations and atomically writes one commit through the scoped executor.
///
/// # Safety
/// `txn` is consumed on success and error. The valid executor and engine handles are borrowed.
/// Connector relations and callbacks must remain valid until this call returns.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn declarative_txn_commit(
    txn: Handle<ExclusiveDeclarativeTransaction>,
    executor: Handle<SharedScopedPlanExecutor>,
    engine: Handle<SharedExternEngine>,
) -> ExternResult<Handle<ExclusiveCommittedTransaction>> {
    let txn = unsafe { *txn.into_inner() };
    let executor = unsafe { executor.as_ref() };
    let engine = unsafe { engine.as_ref() };
    let result = txn
        .commit_declarative(executor)
        .and_then(|result| match result {
            CommitResult::Conflicted(conflicted) => Err(KernelError::FileAlreadyExists(format!(
                "Commit conflict at version {}",
                conflicted.conflict_version()
            ))
            .into()),
            result => commit_result_to_committed_handle(Ok(result)),
        });
    unsafe { result.into_extern_result(&engine) }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use delta_kernel::arrow::json::ReaderBuilder;
    use delta_kernel::committer::FileSystemCommitter;
    use delta_kernel::engine::arrow_conversion::TryIntoArrow;
    use delta_kernel::engine::sync::plan::SyncPlanExecutor;
    use delta_kernel::object_store::ObjectStoreExt as _;
    use delta_kernel::object_store::memory::InMemory;
    use delta_kernel::object_store::path::Path;
    use delta_kernel::plans::ScopedPlanExecutor;
    use delta_kernel::plans::proto::plan as proto_plan;
    use delta_kernel::schema::schema_ref;
    use delta_kernel::transaction::UpdateTableOperation;
    use delta_kernel::transaction::create_table::create_table;
    use delta_kernel_default_engine::DefaultEngineBuilder;
    use rstest::rstest;
    use serde_json::json;

    use super::*;
    use crate::error::FFIKernelError;
    use crate::ffi_test_utils::{TEST_ERROR_ALLOCATORS, ok_or_panic, recover_error};
    use crate::transaction::{committed_transaction_version, free_committed_transaction};
    use crate::{engine_to_handle, free_engine, kernel_bytes_slice};

    #[rstest]
    #[case::prepared_add(false, false)]
    #[case::invalid_add(true, false)]
    #[case::version_conflict(false, true)]
    #[tokio::test]
    async fn ffi_declarative_commit_validates_and_preserves_prepared_actions(
        #[case] invalid: bool,
        #[case] conflict: bool,
    ) {
        let store = Arc::new(InMemory::new());
        let engine = Arc::new(DefaultEngineBuilder::new(store.clone()).build());
        let snapshot = create_table(
            "memory:///",
            schema_ref! { nullable "id": LONG },
            "ffi-test",
            true,
        )
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))
        .unwrap()
        .commit(engine.as_ref())
        .unwrap()
        .unwrap_post_commit_snapshot();
        let engine = engine_to_handle(engine, TEST_ERROR_ALLOCATORS);
        let builder =
            Box::new(snapshot.update_table(UpdateTableOperation::Write, "ffi-test", true));
        let mut txn = ok_or_panic(unsafe {
            update_table_txn_builder_build_declarative(builder.into(), engine.shallow_copy())
        });
        let executor = Arc::new(SyncPlanExecutor::new(Some(store.clone())).get_scoped());
        let schema = Transaction::<UpdateTable, Declarative>::add_actions_schema();
        let arrow_schema = Arc::new(schema.as_ref().try_into_arrow().unwrap());
        let row = json!({
            "path": "prepared.parquet", "partitionValues": {}, "dataChange": true,
            "size": if invalid { -1 } else { 10 }, "modificationTime": 123,
            "stats": "{\"numRecords\":2}", "tags": {"source": "connector"}
        });
        let bytes = row.to_string();
        let batch = ReaderBuilder::new(arrow_schema)
            .build(bytes.as_bytes())
            .unwrap()
            .next()
            .unwrap()
            .unwrap();
        let relation = executor.create_ref("adds", schema, batch).unwrap();
        let encoded = proto_plan::RelationSourceNode {
            id: relation.id().into(),
            schema: Some(proto_schema::StructType::from(relation.schema().as_ref())),
        }
        .encode_to_vec();
        ok_or_panic(unsafe {
            declarative_txn_add_actions(
                &raw mut txn,
                kernel_bytes_slice!(encoded),
                engine.shallow_copy(),
            )
        });
        let commit_file = Path::from("_delta_log/00000000000000000001.json");
        if conflict {
            store
                .put(&commit_file, "existing winner\n".into())
                .await
                .unwrap();
        }
        let executor: Arc<dyn ScopedPlanExecutor> = executor;
        let executor: Handle<SharedScopedPlanExecutor> = executor.into();
        let result =
            unsafe { declarative_txn_commit(txn, executor.shallow_copy(), engine.shallow_copy()) };
        if invalid || conflict {
            let ExternResult::Err(error) = result else {
                panic!("expected commit failure")
            };
            let error = unsafe { recover_error(error) };
            if conflict {
                assert_eq!(error.etype, FFIKernelError::FileAlreadyExists);
                let bytes = store
                    .get(&commit_file)
                    .await
                    .unwrap()
                    .bytes()
                    .await
                    .unwrap();
                assert_eq!(bytes.as_ref(), b"existing winner\n");
            } else {
                assert!(store.head(&commit_file).await.is_err());
            }
        } else {
            let committed = ok_or_panic(result);
            assert_eq!(unsafe { committed_transaction_version(&committed) }, 1);
            unsafe { free_committed_transaction(committed) };
            let bytes = store
                .get(&commit_file)
                .await
                .unwrap()
                .bytes()
                .await
                .unwrap();
            let actions: Vec<serde_json::Value> = serde_json::Deserializer::from_slice(&bytes)
                .into_iter()
                .collect::<Result<_, _>>()
                .unwrap();
            let add = actions.iter().find_map(|action| action.get("add")).unwrap();
            assert_eq!(add["tags"]["source"], "connector");
            assert_eq!(add["stats"], "{\"numRecords\":2}");
        }
        unsafe {
            crate::plans::scoped::free_scoped_plan_executor(executor);
            free_engine(engine);
        }
    }
}
