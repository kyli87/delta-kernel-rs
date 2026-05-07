//! A [`ParquetHandler`] implementation backed by a [`PlanExecutor`].
//!
//! [`PlanBasedParquetHandler`] routes
//! [`read_parquet_files`](ParquetHandler::read_parquet_files) through the plan executor as a
//! [`Scan`](crate::plan::DeclarativePlanNode::Scan) node, while delegating other operations to
//! [`DefaultParquetHandler`].

use std::sync::Arc;

use url::Url;

use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::parquet::DefaultParquetHandler;
use crate::object_store::DynObjectStore;
use crate::plan::{DeclarativePlanNode, PlanExecutor, PlanResult, ScanFileFormat};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileDataReadResultIterator, FileMeta, ParquetFooter,
    ParquetHandler, PredicateRef,
};

/// A [`ParquetHandler`] that routes file reads through a [`PlanExecutor`].
///
/// [`read_parquet_files`](ParquetHandler::read_parquet_files) is translated into a
/// [`Scan`](DeclarativePlanNode::Scan) plan node and executed via the plan executor.
/// All other methods delegate to [`DefaultParquetHandler`].
pub struct PlanBasedParquetHandler<E: TaskExecutor> {
    executor: Arc<dyn PlanExecutor>,
    default: DefaultParquetHandler<E>,
}

impl<E: TaskExecutor> std::fmt::Debug for PlanBasedParquetHandler<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PlanBasedParquetHandler")
            .finish_non_exhaustive()
    }
}

impl<E: TaskExecutor> PlanBasedParquetHandler<E> {
    /// Create a new `PlanBasedParquetHandler`.
    ///
    /// File reads are delegated to `plan_executor`. Other operations (writing, footer reads) use
    /// the default Parquet handler constructed from the given `object_store` and `task_executor`.
    pub fn new(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Self {
        Self {
            executor: plan_executor,
            default: DefaultParquetHandler::new(object_store, task_executor),
        }
    }
}

impl<E: TaskExecutor> ParquetHandler for PlanBasedParquetHandler<E> {
    fn read_parquet_files(
        &self,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<FileDataReadResultIterator> {
        let plan = DeclarativePlanNode::Scan {
            format: ScanFileFormat::Parquet,
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

    fn write_parquet_file(
        &self,
        location: Url,
        data: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send>,
    ) -> DeltaResult<()> {
        ParquetHandler::write_parquet_file(&self.default, location, data)
    }

    fn read_parquet_footer(&self, file: &FileMeta) -> DeltaResult<ParquetFooter> {
        ParquetHandler::read_parquet_footer(&self.default, file)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::PlanBasedParquetHandler;
    use crate::arrow::array::{Array, Int64Array};
    use crate::engine::arrow_data::ArrowEngineData;
    use crate::engine::default::executor::tokio::TokioBackgroundExecutor;
    use crate::engine::plan::NaivePlanExecutor;
    use crate::object_store::memory::InMemory;
    use crate::object_store::path::Path;
    use crate::object_store::ObjectStoreExt as _;
    use crate::schema::{DataType as DeltaDataType, StructField, StructType};
    use crate::{EngineData, FileMeta, ParquetHandler};

    fn make_handler(store: Arc<InMemory>) -> PlanBasedParquetHandler<TokioBackgroundExecutor> {
        let executor = Arc::new(TokioBackgroundExecutor::new());
        let plan_exec = Arc::new(NaivePlanExecutor::new(store.clone(), executor.clone()));
        PlanBasedParquetHandler::new(store, executor, plan_exec)
    }

    // ==================== read_parquet_files tests ====================

    #[test]
    fn read_parquet_files_single_file() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store.clone());

        let data: Box<dyn EngineData> = Box::new(ArrowEngineData::new(
            crate::arrow::array::RecordBatch::try_from_iter(vec![(
                "x",
                Arc::new(Int64Array::from(vec![10, 20, 30])) as Arc<dyn Array>,
            )])
            .unwrap(),
        ));

        let file_url = url::Url::parse("memory:///data/test.parquet").unwrap();
        handler
            .write_parquet_file(file_url.clone(), Box::new(std::iter::once(Ok(data))))
            .unwrap();

        let rt = tokio::runtime::Runtime::new().unwrap();
        let obj_meta = rt
            .block_on(store.head(&Path::from("data/test.parquet")))
            .unwrap();
        let file_meta = FileMeta {
            location: file_url,
            last_modified: obj_meta.last_modified.timestamp_millis(),
            size: obj_meta.size,
        };

        let schema = Arc::new(
            StructType::try_new([StructField::not_null("x", DeltaDataType::LONG)]).unwrap(),
        );

        let values: Vec<i64> = handler
            .read_parquet_files(&[file_meta], schema, None)
            .unwrap()
            .flat_map(|r| {
                let batch = ArrowEngineData::try_from_engine_data(r.unwrap()).unwrap();
                let col = batch
                    .record_batch()
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .clone();
                col.iter().map(|v| v.unwrap()).collect::<Vec<_>>()
            })
            .collect();

        assert_eq!(values, vec![10, 20, 30]);
    }

    #[test]
    fn read_parquet_files_empty_returns_no_data() {
        let store = Arc::new(InMemory::new());
        let handler = make_handler(store);

        let schema = Arc::new(
            StructType::try_new([StructField::not_null("x", DeltaDataType::LONG)]).unwrap(),
        );

        let batches: Vec<_> = handler
            .read_parquet_files(&[], schema, None)
            .unwrap()
            .collect();

        assert!(batches.is_empty());
    }
}
