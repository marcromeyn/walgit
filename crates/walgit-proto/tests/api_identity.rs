//! One public-API fixture compiled by both generation paths.
//!
//! Cargo builds `walgit_proto` through build.rs/prost-build. Bazel builds the
//! same crate API through `rust_prost_library`. Any generated name, field type,
//! nested enum, or message relationship used here must compile in both lanes.

use walgit_proto::Message;
use walgit_proto::v1::{
    Checkpoint, EntryKind, Lease, LogEntry, Manifest, PackRef, RefSnapshot, RefTransaction,
    RefUpdate,
};

#[test]
fn generated_api_is_identical_across_cargo_and_bazel() {
    let update = RefUpdate {
        name: "refs/heads/main".into(),
        old_oid: "0".repeat(40),
        new_oid: "1".repeat(40),
        ..Default::default()
    };
    let txn = RefTransaction {
        updates: vec![update],
        ..Default::default()
    };
    let entry = LogEntry {
        seq: 7,
        kind: EntryKind::Push as i32,
        txn: Some(txn),
        ..Default::default()
    };
    let manifest = Manifest {
        format_version: walgit_proto::WAL_FORMAT_VERSION,
        head_seq: entry.seq,
        packs: vec![PackRef::default()],
        ..Default::default()
    };
    let checkpoint = Checkpoint {
        seq: manifest.head_seq,
        packs: manifest.packs.clone(),
        ..Default::default()
    };
    let _snapshot = RefSnapshot::default();
    let _lease = Lease::default();

    let encoded = entry.encode_to_vec();
    assert_eq!(LogEntry::decode(encoded.as_slice()).unwrap(), entry);
    assert_eq!(checkpoint.seq, manifest.head_seq);
}
