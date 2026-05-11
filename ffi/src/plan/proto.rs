//! Protobuf wire format for [`DeclarativePlanNode`].
//!
//! The generated bindings live in `OUT_DIR/delta_kernel_ffi.plan.rs` (produced by `prost-build`
//! in [`ffi/build.rs`](../../../build.rs) from
//! [`ffi/proto/declarative_plan_node.proto`](../../../proto/declarative_plan_node.proto)).
//! Generated module aliases:
//!
//! - [`pb::DeclarativePlanNode`] mirrors [`DeclarativePlanNode`]
//! - [`pb::ScanFileFormat`]      mirrors [`ScanFileFormat`]
//!
//! The Rust enum is converted into the protobuf message via [`encode_plan`] and
//! [`From<DeclarativePlanNode>`] / [`TryFrom<pb::DeclarativePlanNode>`].

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use delta_kernel::plan::{DeclarativePlanNode, ScanFileFormat};
use delta_kernel::schema::StructType;
use delta_kernel::{DeltaResult, Error, FileMeta, FileSlice, PredicateRef};
use prost::Message;
use url::Url;

/// Generated protobuf bindings for the declarative plan algebra.
///
/// `prost-build` produces a Rust file named `<package>.rs`, where `<package>` matches the
/// `package` declaration in the `.proto` file (here: `delta_kernel_ffi.plan`).
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/delta_kernel_ffi.plan.rs"));
}

/// Encode a [`DeclarativePlanNode`] to its protobuf wire format.
pub fn encode_plan(node: DeclarativePlanNode) -> Vec<u8> {
    let proto: pb::DeclarativePlanNode = node.into();
    proto.encode_to_vec()
}

/// Decode a [`DeclarativePlanNode`] from its protobuf wire format.
pub fn decode_plan(bytes: &[u8]) -> DeltaResult<DeclarativePlanNode> {
    let proto = pb::DeclarativePlanNode::decode(bytes)
        .map_err(|e| Error::generic(format!("failed to decode DeclarativePlanNode proto: {e}")))?;
    DeclarativePlanNode::try_from(proto)
}

// === DeclarativePlanNode <-> pb::DeclarativePlanNode ===

impl From<DeclarativePlanNode> for pb::DeclarativePlanNode {
    fn from(node: DeclarativePlanNode) -> Self {
        let op = match node {
            DeclarativePlanNode::FileListing { url } => {
                pb::declarative_plan_node::Op::FileListing(pb::FileListing { url: url.into() })
            }
            DeclarativePlanNode::ReadBytes { files } => {
                pb::declarative_plan_node::Op::ReadBytes(pb::ReadBytes {
                    files: files.into_iter().map(file_slice_to_pb).collect(),
                })
            }
            DeclarativePlanNode::WriteBytes {
                url,
                data,
                overwrite,
            } => pb::declarative_plan_node::Op::WriteBytes(pb::WriteBytes {
                url: url.into(),
                data: data.to_vec(),
                overwrite,
            }),
            DeclarativePlanNode::HeadFile { url } => {
                pb::declarative_plan_node::Op::HeadFile(pb::HeadFile { url: url.into() })
            }
            DeclarativePlanNode::Scan {
                format,
                files,
                physical_schema,
                predicate,
            } => pb::declarative_plan_node::Op::Scan(pb::Scan {
                format: scan_file_format_to_pb(format) as i32,
                files: files.iter().map(file_meta_to_pb).collect(),
                physical_schema_json: serde_json::to_string(&*physical_schema).unwrap_or_default(),
                // TODO: structured predicate. For now, JSON-serialize on a best-effort basis;
                //   opaque predicates fail to serialize and are therefore dropped.
                predicate: predicate
                    .as_deref()
                    .and_then(|p| serde_json::to_vec(p).ok())
                    .unwrap_or_default(),
            }),
        };
        pb::DeclarativePlanNode { op: Some(op) }
    }
}

impl TryFrom<pb::DeclarativePlanNode> for DeclarativePlanNode {
    type Error = Error;

    fn try_from(value: pb::DeclarativePlanNode) -> DeltaResult<Self> {
        let op = value
            .op
            .ok_or_else(|| Error::generic("DeclarativePlanNode proto missing `op` field"))?;
        Ok(match op {
            pb::declarative_plan_node::Op::FileListing(pb::FileListing { url }) => {
                DeclarativePlanNode::FileListing {
                    url: parse_url(&url)?,
                }
            }
            pb::declarative_plan_node::Op::ReadBytes(pb::ReadBytes { files }) => {
                let files: DeltaResult<Vec<FileSlice>> =
                    files.into_iter().map(file_slice_from_pb).collect();
                DeclarativePlanNode::ReadBytes { files: files? }
            }
            pb::declarative_plan_node::Op::WriteBytes(pb::WriteBytes {
                url,
                data,
                overwrite,
            }) => DeclarativePlanNode::WriteBytes {
                url: parse_url(&url)?,
                data: Bytes::from(data),
                overwrite,
            },
            pb::declarative_plan_node::Op::HeadFile(pb::HeadFile { url }) => {
                DeclarativePlanNode::HeadFile {
                    url: parse_url(&url)?,
                }
            }
            pb::declarative_plan_node::Op::Scan(pb::Scan {
                format,
                files,
                physical_schema_json,
                predicate,
            }) => {
                let format = scan_file_format_from_pb(format)?;
                let files: DeltaResult<Vec<FileMeta>> =
                    files.into_iter().map(file_meta_from_pb).collect();
                let schema: StructType =
                    serde_json::from_str(&physical_schema_json).map_err(|e| {
                        Error::generic(format!(
                            "failed to deserialize Scan.physical_schema_json: {e}"
                        ))
                    })?;
                let predicate: Option<PredicateRef> = if predicate.is_empty() {
                    None
                } else {
                    let p: delta_kernel::expressions::Predicate =
                        serde_json::from_slice(&predicate).map_err(|e| {
                            Error::generic(format!("failed to deserialize Scan.predicate: {e}"))
                        })?;
                    Some(Arc::new(p))
                };
                DeclarativePlanNode::Scan {
                    format,
                    files: files?,
                    physical_schema: Arc::new(schema),
                    predicate,
                }
            }
        })
    }
}

// === ScanFileFormat <-> pb::ScanFileFormat ===

fn scan_file_format_to_pb(format: ScanFileFormat) -> pb::ScanFileFormat {
    match format {
        ScanFileFormat::Parquet => pb::ScanFileFormat::Parquet,
        ScanFileFormat::Json => pb::ScanFileFormat::Json,
    }
}

fn scan_file_format_from_pb(format: i32) -> DeltaResult<ScanFileFormat> {
    match pb::ScanFileFormat::try_from(format) {
        Ok(pb::ScanFileFormat::Parquet) => Ok(ScanFileFormat::Parquet),
        Ok(pb::ScanFileFormat::Json) => Ok(ScanFileFormat::Json),
        Ok(pb::ScanFileFormat::Unspecified) | Err(_) => Err(Error::generic(format!(
            "Scan.format has unrecognized value {format}"
        ))),
    }
}

// === FileMeta / FileSlice helpers ===

fn file_meta_to_pb(meta: &FileMeta) -> pb::FileMeta {
    pb::FileMeta {
        location: meta.location.as_str().to_string(),
        last_modified: meta.last_modified,
        size: meta.size,
    }
}

fn file_meta_from_pb(meta: pb::FileMeta) -> DeltaResult<FileMeta> {
    Ok(FileMeta {
        location: parse_url(&meta.location)?,
        last_modified: meta.last_modified,
        size: meta.size,
    })
}

fn file_slice_to_pb(slice: FileSlice) -> pb::FileSlice {
    let (url, range) = slice;
    pb::FileSlice {
        url: url.into(),
        range: range.map(|Range { start, end }| pb::ByteRange { start, end }),
    }
}

fn file_slice_from_pb(slice: pb::FileSlice) -> DeltaResult<FileSlice> {
    let url = parse_url(&slice.url)?;
    let range = slice.range.map(|r| r.start..r.end);
    Ok((url, range))
}

fn parse_url(s: &str) -> DeltaResult<Url> {
    Url::parse(s).map_err(|e| Error::generic(format!("invalid url {s:?}: {e}")))
}

#[cfg(test)]
mod tests {
    use delta_kernel::schema::{DataType, StructField};

    use super::*;

    fn make_url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    #[test]
    fn file_listing_round_trip() {
        let node = DeclarativePlanNode::FileListing {
            url: make_url("memory:///table/_delta_log/"),
        };
        let bytes = encode_plan(node.clone());
        let decoded = decode_plan(&bytes).unwrap();
        assert!(matches!(
            decoded,
            DeclarativePlanNode::FileListing { url } if url == make_url("memory:///table/_delta_log/")
        ));
    }

    #[test]
    fn read_bytes_round_trip() {
        let node = DeclarativePlanNode::ReadBytes {
            files: vec![
                (make_url("memory:///a.parquet"), Some(0..1024)),
                (make_url("memory:///b.parquet"), None),
            ],
        };
        let bytes = encode_plan(node);
        let decoded = decode_plan(&bytes).unwrap();
        let DeclarativePlanNode::ReadBytes { files } = decoded else {
            panic!("expected ReadBytes");
        };
        assert_eq!(files.len(), 2);
        assert_eq!(files[0].0.as_str(), "memory:///a.parquet");
        assert_eq!(files[0].1, Some(0..1024));
        assert_eq!(files[1].1, None);
    }

    #[test]
    fn write_bytes_round_trip() {
        let node = DeclarativePlanNode::WriteBytes {
            url: make_url("memory:///out"),
            data: Bytes::from_static(b"hello"),
            overwrite: true,
        };
        let bytes = encode_plan(node);
        let decoded = decode_plan(&bytes).unwrap();
        let DeclarativePlanNode::WriteBytes {
            url,
            data,
            overwrite,
        } = decoded
        else {
            panic!("expected WriteBytes");
        };
        assert_eq!(url.as_str(), "memory:///out");
        assert_eq!(&*data, b"hello".as_slice());
        assert!(overwrite);
    }

    #[test]
    fn head_file_round_trip() {
        let node = DeclarativePlanNode::HeadFile {
            url: make_url("memory:///x"),
        };
        let bytes = encode_plan(node);
        let decoded = decode_plan(&bytes).unwrap();
        assert!(
            matches!(decoded, DeclarativePlanNode::HeadFile { url } if url.as_str() == "memory:///x")
        );
    }

    #[test]
    fn scan_round_trip() {
        let schema = Arc::new(
            StructType::try_new(vec![
                StructField::nullable("a", DataType::INTEGER),
                StructField::nullable("b", DataType::STRING),
            ])
            .unwrap(),
        );
        let node = DeclarativePlanNode::Scan {
            format: ScanFileFormat::Parquet,
            files: vec![FileMeta {
                location: make_url("memory:///part-1.parquet"),
                last_modified: 42,
                size: 1234,
            }],
            physical_schema: schema.clone(),
            predicate: None,
        };
        let bytes = encode_plan(node);
        let decoded = decode_plan(&bytes).unwrap();
        let DeclarativePlanNode::Scan {
            format,
            files,
            physical_schema,
            predicate,
        } = decoded
        else {
            panic!("expected Scan");
        };
        assert_eq!(format, ScanFileFormat::Parquet);
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].location.as_str(), "memory:///part-1.parquet");
        assert_eq!(files[0].last_modified, 42);
        assert_eq!(files[0].size, 1234);
        assert_eq!(physical_schema, schema);
        assert!(predicate.is_none());
    }
}
