//! Interfaces for generic, pluggable table indexes.
//!
//! This module defines the *engine-facing* extension points for table level indexes.
//!
//! # Model
//!
//! - An [`IndexConfig`] is the *per-index declaration* that is stored in the table metadata (the
//!   `delta.indexes` property). It is stable across versions and changes only via DDL.
//! - An [`IndexSpec`] is the *per-table-version index state* that is stored inline in domain
//!   metadata (`delta.index.<name>`). It carries the version the index covers plus a path to the
//!   index artifact and index-type-specific properties.
//! - An [`IndexProvider`] (obtained from [`Engine::get_index_provider`]) mints an [`IndexWriter`]
//!   (to build/extend an index during a write) or an [`IndexReader`] (to use an index to skip work
//!   during a scan).
//!
//! [`Engine::get_index_provider`]: crate::Engine::get_index_provider

use std::collections::HashMap;
use std::ops::Bound;

use crate::expressions::{ColumnName, Scalar};
use crate::{AsAny, DeltaResult, EngineData, Version};

/// Declarative configuration for a single index, as stored in the table's metadata.
///
/// This mirrors one entry of the `delta.indexes` metadata property. It describes *what* the
/// index is; it says nothing about the current contents of the index (see [`IndexSpec`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexConfig {
    /// Table-unique identifier for the index. Also the suffix of the index's state domain
    /// (`delta.index.<name>`).
    pub name: String,
    /// Index type identifier used to select the [`IndexProvider`]'s behavior.
    pub index_type: String,
    /// Logical column the index is built over.
    pub column: ColumnName,
    /// Arbitrary index-type-specific properties, serialized verbatim into table metadata.
    pub properties: HashMap<String, String>,
}

/// Per-table-version state for a single index, as stored inline in domain metadata.
///
/// This mirrors the value of the `delta.index.<name>` domain. It is produced by an
/// [`IndexWriter`] on close and consumed by an [`IndexReader`] on open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexSpec {
    /// The table version up to which this index is valid. A reader may use the index only for
    /// data files present at or before this version; files added later must be read normally.
    pub covers_version: Version,
    /// Location of the index artifact. Interpreted by the [`IndexProvider`] for the matching
    /// `index_type`.
    pub path: String,
    /// Arbitrary index-type-specific state (parameters, offsets, sub-paths, etc.).
    pub properties: HashMap<String, String>,
}

/// Mints [`IndexWriter`]s and [`IndexReader`]s for the index types an engine supports.
///
/// Obtained from [`Engine::get_index_provider`]. A provider typically dispatches on
/// [`IndexConfig::index_type`] to the concrete index implementation.
///
/// [`Engine::get_index_provider`]: crate::Engine::get_index_provider
pub trait IndexProvider: AsAny {
    /// Create a writer that builds or extends the index described by `config`.
    ///
    /// `previous` is the index state from the prior table version, if any. The writer either
    /// appends to it (incremental) or rewrites it (full rebuild); on [`IndexWriter::close`] it
    /// returns the updated [`IndexSpec`]. For a brand-new index `previous` is `None`.
    fn get_writer(
        &self,
        config: &IndexConfig,
        previous: Option<&IndexSpec>,
    ) -> DeltaResult<Box<dyn IndexWriter>>;

    /// Create a reader over the current index state `spec` (as discovered for the snapshot
    /// version being scanned).
    fn get_reader(
        &self,
        config: &IndexConfig,
        spec: &IndexSpec,
    ) -> DeltaResult<Box<dyn IndexReader>>;
}

/// Builds or extends an index during a write transaction.
///
/// Lifecycle: feed data via [`add_data`](IndexWriter::add_data), optionally
/// [`optimize`](IndexWriter::optimize), then [`close`](IndexWriter::close) to obtain the
/// updated [`IndexSpec`] to commit alongside the data.
pub trait IndexWriter: Send {
    /// Incorporate a batch of newly written table data into the index.
    ///
    /// `data` is the logical data being added in this transaction (the same data written to the
    /// data files). The writer extracts the indexed column it cares about.
    fn add_data(&mut self, data: Box<dyn EngineData>) -> DeltaResult<()>;

    /// Compact / reorganize the in-progress index (e.g. merge segments, re-balance). Optional
    /// to call; provided so writers can amortize maintenance before closing.
    fn optimize(&mut self) -> DeltaResult<()>;

    /// Finalize the index and return the updated [`IndexSpec`] to be written to domain metadata.
    ///
    /// Consumes the writer. The returned spec's `covers_version` should be set by the caller (or
    /// the writer, by convention) to the version being committed.
    fn close(self: Box<Self>) -> DeltaResult<IndexSpec>;
}

/// Uses an index to skip work during a scan.
///
/// A reader is created for a specific [`IndexSpec`] (the index state current at the snapshot
/// version) over the single column named by its [`IndexConfig`]. Both methods take a half-open
/// or closed range over that column, expressed as `start_bound` / `end_bound` [`Scalar`] bounds
/// (use [`Bound::Unbounded`] for one-sided lookups and `Included(x)..=Included(x)` for
/// equality), and return conservative candidate sets (no false negatives).
///
/// Results are carried as opaque [`EngineData`]; the concrete schema is defined by the
/// (forthcoming) index read path and is the same across index types so kernel can consume it
/// uniformly.
pub trait IndexReader: Send + Sync {
    /// File pruning: return the data files that may contain rows in `[start_bound, end_bound]`.
    fn scan_files(
        &self,
        start_bound: Bound<Scalar>,
        end_bound: Bound<Scalar>,
    ) -> DeltaResult<Box<dyn EngineData>>;

    /// Row skipping: return the physical row positions that may match `[start_bound, end_bound]`.
    fn scan_rows(
        &self,
        start_bound: Bound<Scalar>,
        end_bound: Bound<Scalar>,
    ) -> DeltaResult<Box<dyn EngineData>>;
}
