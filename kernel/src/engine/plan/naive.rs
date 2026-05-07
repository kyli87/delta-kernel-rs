//! A naive [`PlanExecutor`] implementation backed by [`ObjectStoreStorageHandler`].
//!
//! [`NaivePlanExecutor`] interprets each [`DeclarativePlanNode`] variant by delegating to the
//! default engine's existing handler implementations and converting results into columnar
//! [`EngineData`] batches.

use std::sync::Arc;

use bytes::Bytes;

use crate::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use crate::arrow::datatypes::{
    DataType as ArrowDataType, Field as ArrowField, Schema as ArrowSchema,
};
use crate::engine::arrow_data::ArrowEngineData;
use crate::engine::default::executor::TaskExecutor;
use crate::engine::default::filesystem::ObjectStoreStorageHandler;
use crate::engine::default::json::DefaultJsonHandler;
use crate::engine::default::parquet::DefaultParquetHandler;
use crate::object_store::DynObjectStore;
use crate::plan::{
    DeclarativePlanNode, PlanExecutor, PlanResult, ScanFileFormat, FILE_META_SCHEMA,
};
use crate::schema::SchemaRef;
use crate::{
    DeltaResult, EngineData, Error, FileMeta, FileSlice, JsonHandler as _, ParquetHandler as _,
    PredicateRef, StorageHandler as _,
};

/// A naive [`PlanExecutor`] that delegates to [`ObjectStoreStorageHandler`],
/// [`DefaultJsonHandler`], and [`DefaultParquetHandler`], converting results into
/// [`ArrowEngineData`] batches.
#[derive(Debug)]
pub struct NaivePlanExecutor<E: TaskExecutor> {
    storage: ObjectStoreStorageHandler<E>,
    json: DefaultJsonHandler<E>,
    parquet: DefaultParquetHandler<E>,
}

impl<E: TaskExecutor> NaivePlanExecutor<E> {
    /// Create a new `NaivePlanExecutor` backed by the given object store and task executor.
    pub fn new(store: Arc<DynObjectStore>, task_executor: Arc<E>) -> Self {
        Self {
            storage: ObjectStoreStorageHandler::new(store.clone(), task_executor.clone()),
            json: DefaultJsonHandler::new(store.clone(), task_executor.clone()),
            parquet: DefaultParquetHandler::new(store, task_executor),
        }
    }
}

impl<E: TaskExecutor> PlanExecutor for NaivePlanExecutor<E> {
    fn execute_plan(&self, plan: DeclarativePlanNode) -> DeltaResult<PlanResult> {
        match plan {
            DeclarativePlanNode::FileListing { url } => self.execute_file_listing(&url),
            DeclarativePlanNode::ReadBytes { files } => self.execute_read_bytes(files),
            DeclarativePlanNode::WriteBytes {
                url,
                data,
                overwrite,
            } => self.execute_write_bytes(&url, data, overwrite),
            DeclarativePlanNode::HeadFile { url } => self.execute_head_file(&url),
            DeclarativePlanNode::Scan {
                format,
                files,
                physical_schema,
                predicate,
            } => self.execute_scan(format, &files, physical_schema, predicate),
        }
    }
}

impl<E: TaskExecutor> NaivePlanExecutor<E> {
    fn execute_file_listing(&self, url: &url::Url) -> DeltaResult<PlanResult> {
        let file_metas: Vec<FileMeta> = self
            .storage
            .list_from(url)?
            .collect::<DeltaResult<Vec<_>>>()?;
        let batch = file_metas_to_engine_data(&file_metas)?;
        let iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(batch)));
        Ok(PlanResult::Data(iter))
    }

    fn execute_read_bytes(&self, files: Vec<FileSlice>) -> DeltaResult<PlanResult> {
        let iter = self.storage.read_files(files)?;
        Ok(PlanResult::ByteStream(iter))
    }

    fn execute_write_bytes(
        &self,
        url: &url::Url,
        data: Bytes,
        overwrite: bool,
    ) -> DeltaResult<PlanResult> {
        self.storage.put(url, data, overwrite)?;
        Ok(PlanResult::Unit)
    }

    fn execute_head_file(&self, url: &url::Url) -> DeltaResult<PlanResult> {
        let meta = self.storage.head(url)?;
        let batch = file_metas_to_engine_data(std::slice::from_ref(&meta))?;
        let iter: Box<dyn Iterator<Item = DeltaResult<Box<dyn EngineData>>> + Send> =
            Box::new(std::iter::once(Ok(batch)));
        Ok(PlanResult::Data(iter))
    }

    fn execute_scan(
        &self,
        format: ScanFileFormat,
        files: &[FileMeta],
        physical_schema: SchemaRef,
        predicate: Option<PredicateRef>,
    ) -> DeltaResult<PlanResult> {
        let iter = match format {
            ScanFileFormat::Parquet => {
                self.parquet
                    .read_parquet_files(files, physical_schema, predicate)?
            }
            ScanFileFormat::Json => self
                .json
                .read_json_files(files, physical_schema, predicate)?,
        };
        Ok(PlanResult::Data(iter))
    }
}

/// Convert a slice of [`FileMeta`] into a single [`ArrowEngineData`] batch matching
/// [`FILE_META_SCHEMA`].
fn file_metas_to_engine_data(metas: &[FileMeta]) -> DeltaResult<Box<dyn EngineData>> {
    let paths: StringArray = metas.iter().map(|m| Some(m.location.as_str())).collect();
    let last_modified: Int64Array = metas.iter().map(|m| Some(m.last_modified)).collect();
    let sizes: Int64Array = metas
        .iter()
        .map(|m| {
            i64::try_from(m.size)
                .ok()
                .map(Some)
                .unwrap_or(Some(i64::MAX))
        })
        .collect();

    let _ = &*FILE_META_SCHEMA;

    let arrow_schema = Arc::new(ArrowSchema::new(vec![
        ArrowField::new("path", ArrowDataType::Utf8, false),
        ArrowField::new("last_modified", ArrowDataType::Int64, false),
        ArrowField::new("size", ArrowDataType::Int64, false),
    ]));

    let columns: Vec<ArrayRef> = vec![Arc::new(paths), Arc::new(last_modified), Arc::new(sizes)];

    let batch = RecordBatch::try_new(arrow_schema, columns)
        .map_err(|e| Error::generic(format!("Failed to create RecordBatch: {e}")))?;

    Ok(Box::new(ArrowEngineData::new(batch)))
}
