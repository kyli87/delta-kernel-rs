use rstest::rstest;

use super::*;
use crate::actions::deletion_vector::{DeletionVectorDescriptor, DeletionVectorStorageType};
use crate::committer::FileSystemCommitter;
use crate::engine::arrow_data::EngineDataArrowExt;
use crate::engine::sync::SyncEngine;
use crate::engine::sync::plan::SyncPlanExecutor;
use crate::expressions::{MapData, StructData};
use crate::plans::PlanBuilder;
use crate::scan::scan_row_schema;
use crate::schema::{DataType, MapType, ToSchema, schema_ref};
use crate::transaction::UpdateTableOperation;
use crate::transaction::create_table::create_table;
use crate::transaction::data_layout::DataLayout;
use crate::transaction::exec_mode::StagedDataChanges;
use crate::unit_test_utils::load_test_table;
use crate::{KernelError, Result};

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
fn empty_commit_succeeds() -> Result<()> {
    let directory = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let txn = create_table(
        directory.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let committed = txn.commit_declarative(&executor)?.unwrap_committed();
    assert_eq!(committed.commit_version(), 0);
    Ok(())
}

fn staged_add(
    executor: &impl ScopedPlanExecutor,
    path: &str,
    count: i64,
) -> KernelResult<RelationRef> {
    let stats = schema_ref! { nullable "numRecords": LONG };
    let schema = schema_ref! {
        nullable "path": STRING,
        nullable "partitionValues": { STRING => nullable STRING },
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "stats": (stats.clone()),
    };
    let partitions = MapData::try_new(
        MapType::new(DataType::STRING, DataType::STRING, true),
        std::iter::empty::<(String, Option<String>)>(),
    )?;
    let rows = vec![vec![
        path.into(),
        partitions.into(),
        100i64.into(),
        10i64.into(),
        StructData::try_new(stats.fields().cloned().collect(), vec![count.into()])?.into(),
    ]];
    executor.execute_and_retain(
        "test_add",
        crate::plans::PlanBuilder::values(schema, rows)?.build()?,
    )
}

#[rstest]
fn create_append_remove_round_trip(#[values(false, true)] row_tracking: bool) -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let mut create = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_table_properties([("delta.enableRowTracking", row_tracking.to_string())])
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    create.add_files_ref(staged_add(&executor, "first.parquet", 3)?);
    create.add_files_ref(staged_add(&executor, "second.parquet", 4)?);
    let snapshot = create
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    assert_eq!(snapshot.version(), 0);
    assert_eq!(snapshot.get_file_stats_if_present().unwrap().num_files(), 2);
    if row_tracking {
        assert_eq!(
            snapshot
                .reader()
                .get_row_tracking_high_water_mark(&engine)?,
            Some(6)
        );
    }
    let mut append = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    append.add_files_ref(staged_add(&executor, "third.parquet", 5)?);
    let snapshot = append
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    assert_eq!(snapshot.version(), 1);
    assert_eq!(snapshot.get_file_stats_if_present().unwrap().num_files(), 3);
    if row_tracking {
        assert_eq!(
            snapshot
                .reader()
                .get_row_tracking_high_water_mark(&engine)?,
            Some(11)
        );
    }
    let remove_schema = schema_ref! {
        nullable "path": STRING, nullable "size": LONG, nullable "dataChange": BOOLEAN,
        nullable "deletionTimestamp": LONG, nullable "baseRowId": LONG,
        nullable "defaultRowCommitVersion": LONG,
    };
    let reference = executor.execute_and_retain(
        "remove",
        crate::plans::PlanBuilder::values(
            remove_schema,
            vec![vec![
                "first.parquet".into(),
                100i64.into(),
                true.into(),
                123i64.into(),
                if row_tracking {
                    0i64.into()
                } else {
                    crate::expressions::Scalar::Null(DataType::LONG)
                },
                if row_tracking {
                    0i64.into()
                } else {
                    crate::expressions::Scalar::Null(DataType::LONG)
                },
            ]],
        )?
        .build()?,
    )?;
    let mut remove = snapshot
        .clone()
        .update_table(UpdateTableOperation::Delete, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    remove.ack_row_tracking_preservation();
    remove.remove_actions_ref(reference);
    let snapshot = remove
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    assert_eq!(snapshot.version(), 2);
    assert_eq!(snapshot.get_file_stats_if_present().unwrap().num_files(), 2);
    let reloaded = crate::Snapshot::builder_for(snapshot.table_root().clone()).build(&engine)?;
    assert_eq!(reloaded.version(), 2);
    if row_tracking {
        assert_eq!(
            reloaded
                .reader()
                .get_row_tracking_high_water_mark(&engine)?,
            Some(11)
        );
    }
    Ok(())
}

#[rstest]
#[case::duplicate("duplicate")]
#[case::bad_size("negative")]
#[case::append_only("append_only")]
fn invalid_staging_does_not_write_a_commit(#[case] invalid: &str) -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let snapshot = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_table_properties([("delta.appendOnly", (invalid == "append_only").to_string())])
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit(&engine)?
    .unwrap_post_commit_snapshot();
    let mut txn = snapshot
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    match invalid {
        "duplicate" => {
            let reference = staged_add(&executor, "duplicate.parquet", 1)?;
            txn.add_files_ref(reference.clone());
            txn.add_files_ref(reference);
        }
        "negative" => {
            let reference = staged_add(&executor, "negative.parquet", 1)?;
            let plan =
                crate::plans::PlanBuilder::relation_source(reference).project_patch(|p| {
                    p.replace(
                        "size",
                        crate::schema::StructField::nullable("size", crate::DataType::LONG),
                        crate::expressions::lit(-1i64),
                    )
                })?;
            txn.add_files_ref(executor.execute_and_retain("invalid", plan.build()?)?);
        }
        "append_only" => {
            let schema = schema_ref! { nullable "path": STRING, nullable "size": LONG, nullable "dataChange": BOOLEAN };
            txn.remove_actions_ref(
                executor.execute_and_retain(
                    "remove",
                    crate::plans::PlanBuilder::values(
                        schema,
                        vec![vec!["file.parquet".into(), 1i64.into(), true.into()]],
                    )?
                    .build()?,
                )?,
            );
        }
        _ => unreachable!(),
    }
    assert!(txn.commit_declarative(&executor).is_err());
    assert!(
        !dir.path()
            .join("_delta_log/00000000000000000001.json")
            .exists()
    );
    Ok(())
}

#[test]
fn version_conflict_preserves_winning_commit() -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let snapshot = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit_declarative(&executor)?
    .unwrap_post_commit_snapshot();
    let build = || {
        snapshot
            .clone()
            .update_table(UpdateTableOperation::Write, "test", true)
            .with_execution_mode::<Declarative>()
            .build(&engine, Box::new(FileSystemCommitter::new()))
    };
    let mut winner = build()?;
    winner.add_files_ref(staged_add(&executor, "winner.parquet", 1)?);
    let mut loser = build()?;
    loser.add_files_ref(staged_add(&executor, "loser.parquet", 1)?);
    winner.commit_declarative(&executor)?.unwrap_committed();
    assert!(matches!(
        loser.commit_declarative(&executor)?,
        CommitResult::Conflicted(_)
    ));
    let adds = ::test_utils::read_actions_from_commit(snapshot.table_root(), 1, "add").unwrap();
    assert_eq!(adds.len(), 1);
    assert_eq!(adds[0]["path"], "winner.parquet");
    Ok(())
}

#[rstest]
fn dv_update_preserves_file_metadata_and_rejects_missing_matches(
    #[values(false, true)] unmatched: bool,
) -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let mut create = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_table_properties([
        ("delta.enableDeletionVectors", "true"),
        ("delta.enableRowTracking", "true"),
    ])
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    create.add_files_ref(staged_add(&executor, "file.parquet", 10)?);
    let snapshot = create
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    let scan = snapshot.clone().scan_builder().build()?;
    let batch = scan
        .scan_metadata(&engine)?
        .next()
        .unwrap()?
        .scan_files
        .apply_selection_vector()?;
    let reference =
        executor.create_ref("scan", scan_row_schema(), batch.try_into_record_batch()?)?;
    let descriptor =
        DeletionVectorDescriptor::try_new(DeletionVectorStorageType::Inline, "test", None, 4, 2)?;
    let schema = schema_ref! { nullable "path": STRING, nullable "deletionVector": (DeletionVectorDescriptor::to_schema()) };
    let path = if unmatched {
        "missing.parquet"
    } else {
        "file.parquet"
    };
    let dvs = executor.execute_and_retain(
        "dvs",
        crate::plans::PlanBuilder::values(schema, vec![vec![path.into(), descriptor.into()]])?
            .build()?,
    )?;
    let mut txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Delete, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.ack_row_tracking_preservation();
    txn.update_deletion_vectors_ref(dvs, reference);
    if unmatched {
        let error = txn.commit_declarative(&executor).unwrap_err();
        assert!(error.to_string().contains("match exactly one"), "{error}");
        assert!(
            !dir.path()
                .join("_delta_log/00000000000000000001.json")
                .exists()
        );
    } else {
        let committed = txn
            .commit_declarative(&executor)?
            .unwrap_post_commit_snapshot();
        assert_eq!(
            committed.get_file_stats_if_present().unwrap().num_files(),
            1
        );
        let adds = ::test_utils::read_actions_from_commit(snapshot.table_root(), 1, "add").unwrap();
        let removes =
            ::test_utils::read_actions_from_commit(snapshot.table_root(), 1, "remove").unwrap();
        assert_eq!(adds.len(), 1);
        assert_eq!(removes.len(), 1);
        assert_eq!(adds[0]["baseRowId"], 0);
        assert_eq!(adds[0]["defaultRowCommitVersion"], 0);
        assert_eq!(adds[0]["deletionVector"]["cardinality"], 2);
        let stats: serde_json::Value =
            serde_json::from_str(adds[0]["stats"].as_str().unwrap()).unwrap();
        assert_eq!(stats["numRecords"], 10);
        assert_eq!(stats["tightBounds"], false);
        assert!(removes[0]["deletionVector"].is_null());
    }
    Ok(())
}

#[test]
fn domain_transactions_and_commit_info_are_preserved() -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let snapshot = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_table_properties([
        ("delta.feature.domainMetadata", "supported"),
        ("delta.enableInCommitTimestamps", "true"),
    ])
    .with_domain_metadata("domain", "before")
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit_declarative(&executor)?
    .unwrap_post_commit_snapshot();
    let info = executor.execute_and_retain(
        "connector_info",
        PlanBuilder::values(
            schema_ref! { nullable "readVersion": LONG, nullable "isolationLevel": STRING },
            vec![vec![0i64.into(), "Serializable".into()]],
        )?
        .build()?,
    )?;
    let txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .with_domain_metadata_removed("domain".into())
        .with_domain_metadata("new-domain".into(), "after".into())
        .with_transaction_id("app".into(), 42)
        .with_commit_info_ref(info);
    let after = txn
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    assert_eq!(after.reader().get_domain_metadata("domain", &engine)?, None);
    assert_eq!(
        after.reader().get_domain_metadata("new-domain", &engine)?,
        Some("after".into())
    );
    assert_eq!(after.reader().get_app_id_version("app", &engine)?, Some(42));
    let info = ::test_utils::read_actions_from_commit(after.table_root(), 1, "commitInfo").unwrap();
    assert_eq!(info[0]["readVersion"], 0);
    assert_eq!(info[0]["isolationLevel"], "Serializable");
    assert_eq!(info[0]["operation"], "WRITE");
    assert!(
        after.reader().get_in_commit_timestamp(&engine)?
            > snapshot.reader().get_in_commit_timestamp(&engine)?
    );
    let removed =
        ::test_utils::read_actions_from_commit(after.table_root(), 1, "domainMetadata").unwrap();
    assert!(removed.iter().any(|dm| dm["domain"] == "domain"
        && dm["configuration"] == "before"
        && dm["removed"] == true));
    Ok(())
}

fn prepared_add(
    executor: &impl ScopedPlanExecutor,
    stats: &str,
    partitions: Vec<(String, Option<String>)>,
) -> KernelResult<RelationRef> {
    let schema = schema_ref! {
        nullable "path": STRING,
        nullable "partitionValues": { STRING => nullable STRING },
        nullable "size": LONG,
        nullable "modificationTime": LONG,
        nullable "dataChange": BOOLEAN,
        nullable "stats": STRING,
    };
    let partitions = MapData::try_new(
        MapType::new(DataType::STRING, DataType::STRING, true),
        partitions,
    )?;
    executor.execute_and_retain(
        "raw_add",
        PlanBuilder::values(
            schema,
            vec![vec![
                "file.parquet".into(),
                partitions.into(),
                100i64.into(),
                10i64.into(),
                true.into(),
                stats.into(),
            ]],
        )?
        .build()?,
    )
}

#[rstest]
#[case::invalid_json("broken", vec![])]
#[case::invalid_count(r#"{"numRecords":"broken"}"#, vec![])]
#[case::unknown_partition(r#"{"numRecords":1}"#, vec![("unknown".into(), None)])]
fn malformed_prepared_actions_do_not_commit(
    #[case] stats: &str,
    #[case] partitions: Vec<(String, Option<String>)>,
) -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let mut txn = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    txn.add_actions_ref(prepared_add(&executor, stats, partitions)?);
    assert!(txn.commit_declarative(&executor).is_err());
    assert!(
        !dir.path()
            .join("_delta_log/00000000000000000000.json")
            .exists()
    );
    Ok(())
}

#[test]
fn prepared_actions_preserve_stats_tags_and_row_ids() -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let snapshot = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG },
        "test",
        true,
    )
    .with_table_properties([("delta.enableRowTracking", "true")])
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?
    .commit_declarative(&executor)?
    .unwrap_post_commit_snapshot();
    let stats = r#"{ "numRecords": 2, "minValues": { "id": 7 } }"#;
    let raw = prepared_add(&executor, stats, vec![])?;
    let tags = MapData::try_new(
        MapType::new(DataType::STRING, DataType::STRING, true),
        [("source", Some("connector".to_string()))],
    )?;
    let plan = PlanBuilder::relation_source(raw).project_patch(|p| {
        p.append(
            crate::schema::StructField::nullable("baseRowId", DataType::LONG),
            crate::expressions::lit(20i64),
        )
        .append(
            crate::schema::StructField::nullable("defaultRowCommitVersion", DataType::LONG),
            crate::expressions::lit(0i64),
        )
        .append(
            crate::schema::StructField::nullable(
                "tags",
                MapType::new(DataType::STRING, DataType::STRING, true),
            ),
            crate::expressions::lit(tags),
        )
    })?;
    let mut txn = snapshot
        .clone()
        .update_table(UpdateTableOperation::Write, "test", true)
        .with_execution_mode::<Declarative>()
        .build(&engine, Box::new(FileSystemCommitter::new()))?
        .with_row_tracking_high_water_mark(21)?;
    txn.add_actions_ref(executor.execute_and_retain("prepared", plan.build()?)?);
    let after = txn
        .commit_declarative(&executor)?
        .unwrap_post_commit_snapshot();
    assert_eq!(
        after.reader().get_row_tracking_high_water_mark(&engine)?,
        Some(21)
    );
    let adds = ::test_utils::read_actions_from_commit(after.table_root(), 1, "add").unwrap();
    assert_eq!(adds[0]["stats"], stats);
    assert_eq!(adds[0]["tags"]["source"], "connector");
    assert_eq!(adds[0]["baseRowId"], 20);
    assert_eq!(adds[0]["defaultRowCommitVersion"], 0);
    Ok(())
}

#[rstest]
fn partition_validation_uses_physical_names_and_allows_null_values(
    #[values("none", "name")] mapping: &str,
    #[values(false, true)] wrong_key: bool,
) -> Result<()> {
    let dir = tempfile::tempdir().map_err(KernelError::from)?;
    let engine = SyncEngine::new();
    let executor = SyncPlanExecutor::default().get_scoped();
    let mut txn = create_table(
        dir.path().to_string_lossy(),
        schema_ref! { nullable "id": LONG, nullable "p": STRING },
        "test",
        true,
    )
    .with_data_layout(DataLayout::partitioned(["p"]))
    .with_table_properties([("delta.columnMapping.mode", mapping)])
    .with_execution_mode::<Declarative>()
    .build(&engine, Box::new(FileSystemCommitter::new()))?;
    let name = if wrong_key {
        "wrong".into()
    } else {
        txn.effective_table_config
            .physical_partition_columns()
            .next()
            .unwrap()
    };
    txn.add_actions_ref(prepared_add(
        &executor,
        r#"{"numRecords":1}"#,
        vec![(name, None)],
    )?);
    let result = txn.commit_declarative(&executor);
    if wrong_key {
        assert!(result.is_err());
        assert!(
            !dir.path()
                .join("_delta_log/00000000000000000000.json")
                .exists()
        );
    } else {
        assert_eq!(result?.unwrap_committed().commit_version(), 0);
    }
    Ok(())
}
