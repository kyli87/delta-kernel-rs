//! Plan-based engine implementation.
//!
//! This module contains an implementation of the [`Engine`](crate::Engine) trait that is
//! backed by a [`PlanExecutor`](crate::plan::PlanExecutor). The engine delegates handler
//! operations (storage, JSON, parquet, evaluation) to declarative plan execution, rather than
//! implementing each handler independently.
//!
//! [`PlanBasedEngine`] routes storage, JSON file reads, and Parquet file reads through a
//! [`PlanExecutor`](crate::plan::PlanExecutor) via [`PlanBasedStorageHandler`],
//! [`PlanBasedJsonHandler`], and [`PlanBasedParquetHandler`], while using the default
//! Arrow-based handlers for expression evaluation and non-read operations (JSON parsing/writing,
//! Parquet writing/footer reads). [`NaivePlanExecutor`] provides a concrete executor backed by
//! [`ObjectStoreStorageHandler`](crate::engine::default::filesystem::ObjectStoreStorageHandler),
//! [`DefaultJsonHandler`](crate::engine::default::json::DefaultJsonHandler), and
//! [`DefaultParquetHandler`](crate::engine::default::parquet::DefaultParquetHandler).

pub mod engine;
pub mod json;
pub mod naive;
pub mod parquet;
pub mod storage;

pub use engine::PlanBasedEngine;
pub use json::PlanBasedJsonHandler;
pub use naive::NaivePlanExecutor;
pub use parquet::PlanBasedParquetHandler;
pub use storage::PlanBasedStorageHandler;
