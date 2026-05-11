//! A plan-based [`Engine`] implementation.
//!
//! [`PlanBasedEngine`] delegates operations to a [`PlanExecutor`] when possible, otherwise
//! falling back to using DefaultEngine implementations.

use std::fmt;
use std::sync::Arc;

use super::json::PlanBasedJsonHandler;
use super::parquet::PlanBasedParquetHandler;
use super::storage::PlanBasedStorageHandler;
use crate::engine::arrow_expression::ArrowEvaluationHandler;
use crate::engine::default::executor::{self, TaskExecutor};
use crate::object_store::DynObjectStore;
use crate::plan::PlanExecutor;
use crate::{Engine, EvaluationHandler, JsonHandler, ParquetHandler, StorageHandler};

/// An [`Engine`] that routes storage and file-read operations through a [`PlanExecutor`].
///
/// Storage, JSON file reads, and Parquet file reads are converted into
/// [`DeclarativePlanNode`]s and delegated to the plan executor. Non-read operations (JSON
/// parsing/writing, Parquet writing/footer reads) and expression evaluation use the same
/// default implementations as [`DefaultEngine`].
///
/// [`DeclarativePlanNode`]: crate::plan::DeclarativePlanNode
/// [`DefaultEngine`]: crate::engine::default::DefaultEngine
pub struct PlanBasedEngine<E: TaskExecutor> {
    executor: Arc<dyn PlanExecutor>,
    storage: Arc<PlanBasedStorageHandler>,
    json: Arc<PlanBasedJsonHandler<E>>,
    parquet: Arc<PlanBasedParquetHandler<E>>,
    evaluation: Arc<ArrowEvaluationHandler>,
}

impl<E: TaskExecutor> fmt::Debug for PlanBasedEngine<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBasedEngine")
            .field("storage", &self.storage)
            .finish_non_exhaustive()
    }
}

impl<E: TaskExecutor> PlanBasedEngine<E> {
    /// Create a new `PlanBasedEngine`.
    ///
    /// Storage, JSON file reads, and Parquet file reads are delegated to `plan_executor`.
    /// Non-read operations (JSON parsing/writing, Parquet writing/footer reads) and expression
    /// evaluation are constructed from the given `object_store` and `task_executor`, identically
    /// to [`DefaultEngine`](crate::engine::default::DefaultEngine).
    pub fn new(
        object_store: Arc<DynObjectStore>,
        task_executor: Arc<E>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> Self {
        Self {
            storage: Arc::new(PlanBasedStorageHandler::new(plan_executor.clone())),
            json: Arc::new(PlanBasedJsonHandler::new(
                object_store.clone(),
                task_executor.clone(),
                plan_executor.clone(),
            )),
            parquet: Arc::new(PlanBasedParquetHandler::new(
                object_store,
                task_executor,
                plan_executor.clone(),
            )),
            executor: plan_executor,
            evaluation: Arc::new(ArrowEvaluationHandler {}),
        }
    }
}

impl PlanBasedEngine<executor::tokio::TokioBackgroundExecutor> {
    /// Create a [`PlanBasedEngineBuilder`] for constructing a [`PlanBasedEngine`].
    ///
    /// # Parameters
    ///
    /// - `object_store`: The object store used by non-read operations (JSON parsing/writing,
    ///   Parquet writing/footer reads).
    /// - `plan_executor`: The [`PlanExecutor`] that interprets storage and file-read plans.
    pub fn builder(
        object_store: Arc<DynObjectStore>,
        plan_executor: Arc<dyn PlanExecutor>,
    ) -> PlanBasedEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
        PlanBasedEngineBuilder::new(object_store, plan_executor)
    }
}

/// Builder for creating [`PlanBasedEngine`] instances.
///
/// # Example
///
/// ```no_run
/// # use std::sync::Arc;
/// # use delta_kernel::engine::plan::{NaivePlanExecutor, PlanBasedEngineBuilder};
/// # use delta_kernel::engine::default::executor::tokio::TokioBackgroundExecutor;
/// # use delta_kernel::object_store::local::LocalFileSystem;
/// let store = Arc::new(LocalFileSystem::new());
/// let task_executor = Arc::new(TokioBackgroundExecutor::new());
/// let plan_executor = Arc::new(NaivePlanExecutor::new(store.clone(), task_executor.clone()));
/// let engine = PlanBasedEngineBuilder::new(store, plan_executor).build();
/// ```
pub struct PlanBasedEngineBuilder<E: TaskExecutor> {
    object_store: Arc<DynObjectStore>,
    task_executor: Arc<E>,
    plan_executor: Arc<dyn PlanExecutor>,
}

impl<E: TaskExecutor> fmt::Debug for PlanBasedEngineBuilder<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PlanBasedEngineBuilder")
            .finish_non_exhaustive()
    }
}

impl PlanBasedEngineBuilder<executor::tokio::TokioBackgroundExecutor> {
    /// Create a new [`PlanBasedEngineBuilder`] using the default
    /// [`TokioBackgroundExecutor`](executor::tokio::TokioBackgroundExecutor) as the task executor.
    pub fn new(object_store: Arc<DynObjectStore>, plan_executor: Arc<dyn PlanExecutor>) -> Self {
        Self {
            object_store,
            task_executor: Arc::new(executor::tokio::TokioBackgroundExecutor::new()),
            plan_executor,
        }
    }
}

impl<E: TaskExecutor> PlanBasedEngineBuilder<E> {
    /// Set a custom task executor for the engine.
    ///
    /// See [`TaskExecutor`] for more details.
    pub fn with_task_executor<F: TaskExecutor>(
        self,
        task_executor: Arc<F>,
    ) -> PlanBasedEngineBuilder<F> {
        PlanBasedEngineBuilder {
            object_store: self.object_store,
            task_executor,
            plan_executor: self.plan_executor,
        }
    }

    /// Build the [`PlanBasedEngine`] instance.
    pub fn build(self) -> PlanBasedEngine<E> {
        PlanBasedEngine::new(self.object_store, self.task_executor, self.plan_executor)
    }
}

impl<E: TaskExecutor> Engine for PlanBasedEngine<E> {
    fn evaluation_handler(&self) -> Arc<dyn EvaluationHandler> {
        self.evaluation.clone()
    }

    fn storage_handler(&self) -> Arc<dyn StorageHandler> {
        self.storage.clone()
    }

    fn json_handler(&self) -> Arc<dyn JsonHandler> {
        self.json.clone()
    }

    fn parquet_handler(&self) -> Arc<dyn ParquetHandler> {
        self.parquet.clone()
    }

    fn plan_executor(&self) -> Arc<dyn PlanExecutor> {
        self.executor.clone()
    }
}
