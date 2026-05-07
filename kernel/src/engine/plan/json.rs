//! A [`JsonHandler`] implementation backed by a [`PlanExecutor`].
//!
//! [`PlanBasedJsonHandler`] routes [`read_json_files`](JsonHandler::read_json_files) through the
//! plan executor as a [`Scan`](crate::plan::DeclarativePlanNode::Scan) node, while delegating
//! other operations to [`DefaultJsonHandler`].

use std::sync::Arc;

use url::Url;

use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::json::DefaultJsonHandler;
use crate::object_store::DynObjectStore;
use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult, ScanFileFormat};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, FilteredEngineData,
    JsonHandler, PredicateRef,
};

/// A [`JsonHandler`] that routes file reads through a [`PlanExecutor`].
///
/// [`read_json_files`](JsonHandler::read_json_files) is translated into a
/// [`Scan`](DeclarativePlanNode::Scan) plan node and executed via the plan executor.
/// All other methods delegate to [`DefaultJsonHandler`].
pub struct PlanBasedJsonHandler<E: TaskExecutor> {
    executor: Arc<dyn PlanExecutor>,
    default: DefaultJsonHandler<E>,
}

impl<E: TaskExecutor> std::fmt::Debug for PlanBasedJsonHandler<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanBasedJsonHandler")
            .finish_non_exhaustive()
    }
}

impl<E: TaskExecutor> PlanBasedJsonHandler<E> {
    /// Create a new `PlanBasedJsonHandler`.
    ///
    /// File reads are delegated to `plan_executor`. Other operations (parsing, writing) use the
    /// default JSON handler constructed from the given `object_store` and `task_executor`.
    pub fn new(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Self {
        Self {
            executor: plan_executor,
            default: DefaultJsonHandler::new(object_store, task_executor),
        }
    }
}

impl<E: TaskExecutor> JsonHandler for PlanBasedJsonHandler<E> {
    fn parse_json(
        &self,
        json_strings: Box<dyn EngineData>,
        output_schema: SchemaRef,
    ) -> DeltaResult<Box<dyn EngineData>> {
        self.default.parse_json(json_strings, output_schema)
    }

    fn read_json_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let plan = DeclarativePlanNode::Scan {
            format: ScanFileFormat::Json,
            files: files.to_vec(),
            physical_schema,
            predicate,
        };
        let result = self.executor.execute_plan(plan)?;
        match result {
            PlanResult::Data(iter) => Ok(iter),
            other => Err(Error::generic(format!(
                "expected PlanResult::Data for Scan, got {other:?}"
            ))),
        }
    }

    fn write_json_file(
        &self,
        path: &Url,
        data: Box<dyn Iterator<Item = DeltaResult<FilteredEngineData>> + Send + '_>,
        overwrite: bool,
    ) -> DeltaResult<()> {
        self.default.write_json_file(path, data, overwrite)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::PlanBasedJsonHandler;
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::plan::NaivePlanExecutor;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::schema::{DataType as DeltaDataType, StructField, StructType};
    use crate::{FileMeta, JsonHandler};

    fn make_handler(store: Arc<InMemory>) -> PlanBasedJsonHandler<TokioBackgroundExecutor> {
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = Arc::new(NaivePlanExecutor::new(store.clone(), executor.clone()));
        PlanBasedJsonHandler::new(store, executor, plan_exec)
    }

    // ==================== read_json_files tests ====================

    #[test]
    fn read_json_files_single_file() {
        let store = Arc::new(InMemory::new());
        let rt = tokio::runtime::Runtime::new().unwrap();

        let json_content = r#"{"val": 42}"#;
        rt.block_on(store.put(
            &Path::from("data/test.json"),
            bytes::Bytes::from(json_content).into(),
        ))
        .unwrap();

        let meta = rt
            .block_on(store.head(&Path::from("data/test.json")))
            .unwrap();
        let file_meta = FileMeta {
            location: url::Url::parse("memory:///data/test.json").unwrap(),
            last_modified: meta.last_modified.timestamp_millis(),
            size: meta.size,
        };

        let schema = Arc::new(
            StructType::try_new([StructField::nullable("val", DeltaDataType::INTEGER)]).unwrap(),
        );

        let handler = make_handler(store);
        let batches: Vec<_> = handler
            .read_json_files(&[file_meta], schema, None)
            .unwrap()
            .map(|r| r.unwrap())
            .collect();

        assert!(!batches.is_empty());
        let batch =
            ArrowEngineData::try_from_engine_data(batches.into_iter().next().unwrap()).unwrap();
        let col = batch
            .record_batch()
            .column(0)
            .as_any()
            .downcast_ref::<crate::arrow::array::Int32Array>()
            .unwrap();
        assert_eq!(col.value(0), 42);
    }

    #[test]
    fn read_json_files_preserves_file_order() {
        let store = Arc::new(InMemory::new());
        let rt = tokio::runtime::Runtime::new().unwrap();

        for (name, val) in [("a.json", 1), ("b.json", 2), ("c.json", 3)] {
            rt.block_on(store.put(
                &Path::from(format!("data/{name}")),
                bytes::Bytes::from(format!(r#"{{"val": {val}}}"#)).into(),
            ))
            .unwrap();
        }

        let files: Vec<FileMeta> = ["c.json", "a.json", "b.json"]
            .iter()
            .map(|name| {
                let meta = rt
                    .block_on(store.head(&Path::from(format!("data/{name}"))))
                    .unwrap();
                FileMeta {
                    location: url::Url::parse(&format!("memory:///data/{name}")).unwrap(),
                    last_modified: meta.last_modified.timestamp_millis(),
                    size: meta.size,
                }
            })
            .collect();

        let schema = Arc::new(
            StructType::try_new([StructField::nullable("val", DeltaDataType::INTEGER)]).unwrap(),
        );

        let handler = make_handler(store);
        let values: Vec<i32> = handler
            .read_json_files(&files, schema, None)
            .unwrap()
            .flat_map(|r| {
                let batch = ArrowEngineData::try_from_engine_data(r.unwrap()).unwrap();
                let col = batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<crate::arrow::array::Int32Array>()
                    .unwrap()
                    .clone();
                col.iter().map(|v| v.unwrap()).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(values, vec![3, 1, 2], "data must follow input file order");
    }

    #[test]
    fn read_json_files_empty_returns_no_data() {
        let store = Arc::new(InMemory::new());
        let schema = Arc::new(
            StructType::try_new([StructField::nullable("val", DeltaDataType::INTEGER)]).unwrap(),
        );

        let handler = make_handler(store);
        let batches: Vec<_> = handler
            .read_json_files(&[], schema, None)
            .unwrap()
            .collect();

        assert!(batches.is_empty());
    }
}
