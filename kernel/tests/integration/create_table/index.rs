//! Integration tests for the table index write path (experimental prototype):
//! declaring index configs at table creation and attaching index specs to commits.

use std::collections::HashMap;

use delta_kernel::committer::FileSystemCommitter;
use delta_kernel::expressions::ColumnName;
use delta_kernel::index::{IndexConfig, IndexSpec};
use delta_kernel::snapshot::Snapshot;
use delta_kernel::table_features::TableFeature;
use delta_kernel::transaction::create_table::create_table;
use delta_kernel::DeltaResult;
use test_utils::{assert_result_error_with_message, begin_transaction, test_table_setup};

use super::simple_schema;

/// A sample index config over the `value` column of [`simple_schema`].
fn sample_index_config() -> IndexConfig {
    IndexConfig {
        name: "price_idx".to_string(),
        index_type: "bloom".to_string(),
        column: ColumnName::new(["value"]),
        properties: HashMap::from([("fpp".to_string(), "0.01".to_string())]),
    }
}

/// `with_index` serializes the config into the `delta.indexes` property (round-tripping via
/// the public `IndexConfig` type) and enables the `dataIndexes` + `domainMetadata` writer
/// features.
#[tokio::test]
async fn test_create_table_with_index_round_trips_config_and_features() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;
    let config = sample_index_config();

    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_index(config.clone())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;

    // The config round-trips through `metaData.configuration[delta.indexes]`.
    let raw = snapshot
        .table_configuration()
        .metadata()
        .configuration()
        .get("delta.indexes")
        .expect("delta.indexes property should be present");
    let parsed: serde_json::Value = serde_json::from_str(raw)?;
    let configs: Vec<IndexConfig> = serde_json::from_value(parsed["indexes"].clone()).unwrap();
    assert_eq!(configs, vec![config]);

    // The protocol lists both writer features.
    let protocol = snapshot.table_configuration().protocol();
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::DataIndexes)),
        "dataIndexes should be in writer features"
    );
    assert!(
        protocol
            .writer_features()
            .is_some_and(|f| f.contains(&TableFeature::DomainMetadata)),
        "domainMetadata should be in writer features"
    );

    Ok(())
}

/// `with_index_spec` attaches the spec to the commit, and the kernel stamps `covers_version`
/// with the resolved commit version (overwriting the caller-provided value). The spec is
/// readable via `Snapshot::index_spec`.
#[tokio::test]
async fn test_with_index_spec_round_trips_and_auto_fills_covers_version() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_index(sample_index_config())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;

    // Deliberately wrong covers_version: the kernel must overwrite it at commit time.
    let spec = IndexSpec {
        covers_version: 999,
        path: "indexes/price_idx/v1".to_string(),
        properties: HashMap::from([("rows".to_string(), "42".to_string())]),
    };

    let committed = begin_transaction(snapshot, engine.as_ref())?
        .with_engine_info("index write")
        .with_index_spec("price_idx", spec.clone())
        .commit(engine.as_ref())?;
    assert!(committed.is_committed());

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    assert_eq!(snapshot.version(), 1);

    let read = snapshot
        .index_spec("price_idx", engine.as_ref())?
        .expect("index spec should be present");
    assert_eq!(
        read.covers_version, 1,
        "covers_version should be auto-filled"
    );
    assert_eq!(read.path, spec.path);
    assert_eq!(read.properties, spec.properties);

    // A different index name has no spec.
    assert!(snapshot.index_spec("other", engine.as_ref())?.is_none());

    Ok(())
}

/// Attaching an index spec to a table that does not support `dataIndexes` fails at commit,
/// even when `domainMetadata` is supported.
#[tokio::test]
async fn test_with_index_spec_without_data_indexes_feature_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let _ = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_table_properties([("delta.feature.domainMetadata", "supported")])
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()))?
        .commit(engine.as_ref())?;

    let snapshot = Snapshot::builder_for(&table_path).build(engine.as_ref())?;
    let spec = IndexSpec {
        covers_version: 0,
        path: "p".to_string(),
        properties: HashMap::new(),
    };

    let res = begin_transaction(snapshot, engine.as_ref())?
        .with_engine_info("x")
        .with_index_spec("idx", spec)
        .commit(engine.as_ref());

    assert_result_error_with_message(res, "requires the 'dataIndexes' feature");

    Ok(())
}

/// Declaring two indexes with the same name fails at build time.
#[tokio::test]
async fn test_create_table_with_duplicate_index_names_fails() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let res = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_index(sample_index_config())
        .with_index(sample_index_config())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(res, "Duplicate index name");

    Ok(())
}

/// Combining `with_index` with a raw `delta.indexes` property is rejected at build time.
#[tokio::test]
async fn test_create_table_with_index_and_raw_property_conflicts() -> DeltaResult<()> {
    let (_temp_dir, table_path, engine) = test_table_setup()?;

    let res = create_table(&table_path, simple_schema()?, "Test/1.0")
        .with_table_properties([("delta.indexes", r#"{"indexes":[]}"#)])
        .with_index(sample_index_config())
        .build(engine.as_ref(), Box::new(FileSystemCommitter::new()));

    assert_result_error_with_message(res, "Cannot set the 'delta.indexes' property directly");

    Ok(())
}
