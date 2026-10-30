//! Shared input fields and the fixed result layout for staged-data checks.

use std::sync::{Arc, LazyLock};

use crate::schema::{ColumnNamesAndTypes, DataType, SchemaRef, StructField, lazy_schema_ref};

/// Common action fields. Plan construction replaces `add.stats` with the table's statistics schema.
pub(super) static INPUT_SCHEMA: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "add": {
        nullable "path": STRING,
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": { nullable "numRecords": LONG },
    },
    nullable "remove": {
        nullable "path": STRING,
        nullable "size": LONG,
    },
};

/// One error field per check, including checks that are disabled for this transaction.
pub(super) static ERROR_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    // CheckKind declares distinct names, so these fields cannot collide.
    Arc::new(crate::schema::StructType::new_unchecked(
        CheckKind::ALL
            .map(|kind| StructField::nullable(kind.error_name(), kind.error_schema().clone())),
    ))
});

/// One validity Boolean and error struct per check, in `CheckKind::ALL` order.
pub(super) static RESULT_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    // Each distinct check name has separate validity and error suffixes.
    Arc::new(crate::schema::StructType::new_unchecked(
        CheckKind::ALL.into_iter().flat_map(|kind| {
            [
                StructField::not_null(format!("{}.is_valid", kind.name()), DataType::BOOLEAN),
                StructField::nullable(kind.error_name(), kind.error_schema().clone()),
            ]
        }),
    ))
});

/// Static leaf metadata lets one visitor select the entire summary without borrowing local schemas.
pub(super) static RESULT_COLUMNS: LazyLock<ColumnNamesAndTypes> =
    LazyLock::new(|| RESULT_SCHEMA.leaves(None));

/// Identifies a check's slot in the fixed result schema.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::transaction::declarative) enum CheckKind {
    AddFields,
    RemoveFields,
    AppendOnly,
    NumRecords,
    ClusteringStats,
}

impl CheckKind {
    pub(super) const ALL: [Self; 5] = [
        Self::AddFields,
        Self::RemoveFields,
        Self::AppendOnly,
        Self::NumRecords,
        Self::ClusteringStats,
    ];

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::AddFields => "add.fields",
            Self::RemoveFields => "remove.fields",
            Self::AppendOnly => "append_only.no_removes",
            Self::NumRecords => "add.num_records_present",
            Self::ClusteringStats => "add.clustering_stats",
        }
    }

    pub(super) fn error_name(self) -> String {
        format!("{}.err", self.name())
    }

    pub(super) fn error_schema(self) -> &'static SchemaRef {
        match self {
            Self::AddFields => &ADD_FIELDS_ERROR,
            Self::RemoveFields => &REMOVE_FIELDS_ERROR,
            Self::AppendOnly => &APPEND_ONLY_ERROR,
            Self::NumRecords => &NUM_RECORDS_ERROR,
            Self::ClusteringStats => &CLUSTERING_STATS_ERROR,
        }
    }

    pub(super) fn result_offset(self) -> usize {
        Self::ALL
            .into_iter()
            .take_while(|kind| *kind != self)
            .map(|kind| 1 + kind.error_schema().num_fields())
            .sum()
    }
}

static ADD_FIELDS_ERROR: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
    nullable "size": LONG,
    nullable "modification_time": LONG,
};

static REMOVE_FIELDS_ERROR: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
    nullable "size": LONG,
};

static APPEND_ONLY_ERROR: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
};

static NUM_RECORDS_ERROR: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
    nullable "num_records": LONG,
};

static CLUSTERING_STATS_ERROR: LazyLock<SchemaRef> = lazy_schema_ref! {
    nullable "path": STRING,
    nullable "num_records": LONG,
    not_null "column": STRING,
    nullable "null_count": LONG,
    not_null "has_min": BOOLEAN,
    not_null "has_max": BOOLEAN,
};
