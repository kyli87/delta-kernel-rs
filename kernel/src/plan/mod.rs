//! Declarative plan algebra for data-intensive operations.
//!
//! This module defines a SQL-like plan algebra that describes *what* to do without prescribing
//! *how*. A [`PlanExecutor`] interprets the plan and performs the actual I/O and computation,
//! returning results via [`PlanResult`].
//!
//! The algebra will evolve over time to include relational operators (filter, project, join,
//! etc.). For now it contains simple 1-for-1 operation nodes that mirror [`StorageHandler`]
//! methods, plus a [`Scan`](DeclarativePlanNode::Scan) node for structured file reads.
//!
//! [`StorageHandler`]: crate::StorageHandler

mod executor;
mod result;
mod schema;

use bytes::Bytes;
pub use executor::{PlanExecutor, PlanExecutorRef};
pub use result::PlanResult;
pub use schema::FILE_META_SCHEMA;
use url::Url;

use crate::schema::SchemaRef;
use crate::{FileMeta, FileSlice, PredicateRef};

/// The file format for a [`Scan`](DeclarativePlanNode::Scan) operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanFileFormat {
    /// Apache Parquet format.
    Parquet,
    /// Newline-delimited JSON format.
    Json,
}

/// A declarative plan node representing a data-intensive operation.
///
/// Each variant describes an operation and its parameters. The output schema of the operation
/// is documented on the variant and defined as a schema constant (e.g. [`FILE_META_SCHEMA`]).
#[derive(Debug, Clone)]
pub enum DeclarativePlanNode {
    /// List files at the given URL, returning file metadata as columnar data.
    ///
    /// The result contains one row per file with the schema defined by [`FILE_META_SCHEMA`]:
    /// - `path` (String): fully qualified URL of the file
    /// - `last_modified` (Long): milliseconds since Unix epoch
    /// - `size` (Long): file size in bytes
    ///
    /// Files are returned sorted lexicographically by path. If the URL is directory-like
    /// (ends with '/'), all files in that directory are listed. Otherwise, files
    /// lexicographically greater than the given path are listed.
    FileListing {
        /// The URL to list from.
        url: Url,
    },
    /// Read raw bytes from one or more files (or byte ranges within files).
    ///
    /// Each [`FileSlice`] specifies a file URL and an optional byte range. Results are returned
    /// as [`PlanResult::ByteStream`] in the same order as the input slices.
    ReadBytes {
        /// The file slices to read.
        files: Vec<FileSlice>,
    },
    /// Write raw bytes to a file at the given URL.
    ///
    /// Returns [`PlanResult::Unit`] on success. If `overwrite` is false and the file already
    /// exists, the executor must return
    /// [`Error::FileAlreadyExists`](crate::Error::FileAlreadyExists).
    WriteBytes {
        /// The destination URL.
        url: Url,
        /// The data to write.
        data: Bytes,
        /// Whether to overwrite an existing file.
        overwrite: bool,
    },
    /// Retrieve metadata for a single file (HEAD request).
    ///
    /// Returns [`PlanResult::Data`] with a single-row batch matching [`FILE_META_SCHEMA`].
    /// If the file does not exist, the executor must return an error.
    HeadFile {
        /// The URL of the file to inspect.
        url: Url,
    },
    /// Read and parse structured data files (Parquet or JSON), returning columnar data.
    ///
    /// Returns [`PlanResult::Data`] with columns matching the provided `physical_schema`.
    /// The ordering contract is the same as [`JsonHandler::read_json_files`] and
    /// [`ParquetHandler::read_parquet_files`]: data must be emitted file-by-file in the
    /// order given, with rows in file order, and no cross-file merging.
    ///
    /// [`JsonHandler::read_json_files`]: crate::JsonHandler::read_json_files
    /// [`ParquetHandler::read_parquet_files`]: crate::ParquetHandler::read_parquet_files
    Scan {
        /// The format of the files to read.
        format: ScanFileFormat,
        /// Metadata for the files to read.
        files: Vec<FileMeta>,
        /// Select list and order of columns to read.
        physical_schema: SchemaRef,
        /// Optional push-down predicate hint (executor is free to ignore it).
        predicate: Option<PredicateRef>,
    },
}
