//! Generated protobuf types for walgit's on-store formats.
//!
//! Schema lives in `proto/walgit/v1/wal.proto`; it is the contract between
//! every walgit instance and must only evolve backward-compatibly.

// Match rust_prost_library's package module layout so support.rs is identical
// in both build lanes.
pub mod walgit {
    // Documentation in this module is emitted by prost.
    #[allow(clippy::doc_markdown)]
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/walgit.v1.rs"));
    }
}

include!("support.rs");
