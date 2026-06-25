//! Wire format for table indexes: (de)serialization of [`IndexConfig`]s (the `delta.indexes`
//! table property) and [`IndexSpec`]s (the `delta.index.<name>` domain metadata value).
//!
//! Kernel owns this format; the transaction/builder code calls these helpers rather than
//! touching JSON directly.

use serde::{Deserialize, Serialize};

use super::{IndexConfig, IndexSpec};
use crate::DeltaResult;

/// Wire format for the `delta.indexes` table property: a JSON object `{ "indexes": [...] }`.
#[derive(Serialize)]
struct IndexConfigListRef<'a> {
    indexes: &'a [IndexConfig],
}

// Read-back of the `delta.indexes` property is currently exercised only by tests; the scan-time
// read path will consume it next.
#[cfg_attr(not(test), allow(dead_code))]
#[derive(Deserialize)]
struct IndexConfigList {
    indexes: Vec<IndexConfig>,
}

/// Serialize index configurations into the JSON string stored in the `delta.indexes` property.
pub(crate) fn serialize_index_configs(configs: &[IndexConfig]) -> DeltaResult<String> {
    Ok(serde_json::to_string(&IndexConfigListRef {
        indexes: configs,
    })?)
}

/// Parse the JSON string stored in the `delta.indexes` property back into index configurations.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn parse_index_configs(json: &str) -> DeltaResult<Vec<IndexConfig>> {
    Ok(serde_json::from_str::<IndexConfigList>(json)?.indexes)
}

/// Serialize an [`IndexSpec`] into the JSON string stored in its `delta.index.<name>` domain.
pub(crate) fn serialize_index_spec(spec: &IndexSpec) -> DeltaResult<String> {
    Ok(serde_json::to_string(spec)?)
}

/// Parse the JSON string stored in a `delta.index.<name>` domain back into an [`IndexSpec`].
pub(crate) fn parse_index_spec(json: &str) -> DeltaResult<IndexSpec> {
    Ok(serde_json::from_str(json)?)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::expressions::ColumnName;

    fn sample_config() -> IndexConfig {
        IndexConfig {
            name: "idx".to_string(),
            index_type: "bloom".to_string(),
            column: ColumnName::new(["a", "b"]),
            properties: HashMap::from([("fpp".to_string(), "0.01".to_string())]),
        }
    }

    #[test]
    fn index_configs_round_trip() {
        let configs = vec![sample_config()];
        let json = serialize_index_configs(&configs).unwrap();
        assert_eq!(parse_index_configs(&json).unwrap(), configs);
    }

    #[test]
    fn index_spec_round_trips() {
        let spec = IndexSpec {
            covers_version: 7,
            path: "indexes/idx/v7".to_string(),
            properties: HashMap::from([("rows".to_string(), "42".to_string())]),
        };
        let json = serialize_index_spec(&spec).unwrap();
        assert_eq!(parse_index_spec(&json).unwrap(), spec);
    }
}
