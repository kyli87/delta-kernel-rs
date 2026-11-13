use rstest::rstest;

use super::*;
use crate::Result;
use crate::actions::deletion_vector::DeletionVectorDescriptor;
use crate::committer::FileSystemCommitter;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::scan::scan_row_schema;
use crate::schema::{ToSchema, schema_ref};
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::exec_mode::StagedDataChanges;
use crate::unit_test_utils::load_test_table;

// This test only validates which checks are selected. The check logic is validated in
// precommit/tests.rs.
#[rstest]
#[case::plain(&[], false)]
#[case::v2_supported(&[
    ("delta.feature.icebergCompatV2", "supported"),
    ("delta.columnMapping.mode", "name"),
], false)]
#[case::v2_enabled(&[("delta.enableIcebergCompatV2", "true")], true)]
#[case::v3_supported(&[
    ("delta.feature.icebergCompatV3", "supported"),
    ("delta.columnMapping.mode", "name"),
    ("delta.enableRowTracking", "true"),
], false)]
#[case::v3_enabled(&[("delta.enableIcebergCompatV3", "true")], true)]
fn selects_checks_based_on_table_props(
    #[case] properties: &[(&str, &str)],
    #[case] requires_num_records: bool,
    #[values(false, true)] append_only: bool,
    #[values(false, true)] data_change: bool,
) -> Result<()> {
    let tempdir = tempfile::tempdir().map_err(KernelError::from)?;
    let txn = create_table(
        tempdir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test-engine",
        data_change,
    )
    .with_table_properties(properties.iter().copied())
    .with_table_properties([("delta.appendOnly", append_only.to_string())])
    .with_execution_mode::<Declarative>()
    .build(&SyncEngine::new(), Box::new(FileSystemCommitter::new()))?;
    let checks = txn.file_action_checks()?;
    let mut expected = vec!["add.fields", "remove.fields"];
    if append_only && data_change {
        expected.push("append_only.no_removes");
    }
    if requires_num_records {
        expected.push("add.num_records_present");
    }
    assert_eq!(
        checks.iter().map(|check| check.name()).collect::<Vec<_>>(),
        expected
    );
    Ok(())
}

// This test only validates which checks are selected. The check logic is validated in
// precommit/tests.rs.
#[rstest]
#[case::no_clustering(&[])]
#[case::one_column(&["id"])]
#[case::multiple_columns(&["id", "tag"])]
fn selects_checks_for_clustering_columns(
    #[case] columns: &[&str],
    #[values("none", "name")] column_mapping: &str,
) -> Result<()> {
    let tempdir = tempfile::tempdir().map_err(KernelError::from)?;
    let layout = if columns.is_empty() {
        DataLayout::None
    } else {
        DataLayout::clustered(columns)
    };
    let mut txn = create_table(
        tempdir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG, nullable "tag": STRING },
        "test-engine",
        true,
    )
    .with_table_properties([("delta.columnMapping.mode", column_mapping)])
    .with_data_layout(layout)
    .with_execution_mode::<Declarative>()
    .build(&SyncEngine::new(), Box::new(FileSystemCommitter::new()))?;
    let mut expected = vec!["add.fields", "remove.fields"];
    if !columns.is_empty() {
        expected.push("add.clustering_stats");
    }
    let checks = txn.file_action_checks()?;
    assert_eq!(
        checks.iter().map(|check| check.name()).collect::<Vec<_>>(),
        expected
    );
    let source_schema = schema_ref! {
        ..(txn.add_files_schema().fields().filter(|field| field.name() != "stats")),
        nullable "stats": (txn.stats_schema()?),
    };
    txn.add_files_ref(RelationRef::new("adds", source_schema));
    precommit::build_plan(
        &txn.staged_data_changes.add_files,
        &txn.staged_data_changes.remove_files,
        &checks,
        txn.stats_schema()?.as_ref(),
    )?;
    Ok(())
}

#[rstest]
fn declarative_transaction_can_stage_relations(
    #[values(false, true)] add_files: bool,
    #[values(false, true)] remove_files: bool,
    #[values(false, true)] update_dvs: bool,
) -> Result<()> {
    let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .with_execution_mode::<Declarative>()
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    if add_files {
        txn.add_files_ref(RelationRef::new("adds", txn.add_files_schema().clone()));
    }
    if remove_files {
        txn.remove_files_ref(RelationRef::new("removes", scan_row_schema()));
    }
    if update_dvs {
        txn.update_deletion_vectors_ref(
            RelationRef::new(
                "dvs",
                schema_ref! {
                    not_null "path": STRING,
                    not_null "deletionVector": (DeletionVectorDescriptor::to_schema()),
                },
            ),
            RelationRef::new("files", scan_row_schema()),
        );
    }

    assert_eq!(txn.staged_data_changes.has_adds(), add_files);
    assert_eq!(txn.staged_data_changes.has_removes(), remove_files);
    assert_eq!(txn.staged_data_changes.has_dv_inputs(), update_dvs);
    assert_eq!(txn.staged_data_changes.has_dv_updates(), update_dvs);
    Ok(())
}

#[test]
fn commit_returns_unsupported_error() -> Result<()> {
    let (engine, snapshot, _tempdir) = load_test_table("table-without-dv-small")?;
    let txn = snapshot
        .update_table(UpdateTableOperation::Write, "test-engine", true)
        .with_execution_mode::<Declarative>()
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?;
    let executor = SyncPlanExecutor::default().get_scoped();
    let error = txn.commit_declarative(&executor).unwrap_err();
    assert!(matches!(&error, KernelError::Unsupported(_)));
    assert!(error.to_string().contains("not implemented"));
    Ok(())
}
