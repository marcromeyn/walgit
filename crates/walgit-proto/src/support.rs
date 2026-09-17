// Handwritten API compiled into the same crate as prost's generated messages.
// Cargo includes this after OUT_DIR/walgit.v1.rs; Bazel appends it with
// rust_prost_transform. One source owns every helper in both build lanes.

pub use walgit::v1;

pub mod inventory {
    //! Exact membership helpers. Retired inventory is download history, never live proof.
    use std::collections::BTreeSet;

    use crate::v1::{Manifest, RetiredPack};

    impl Manifest {
        pub fn serves_pack(&self, checksum: &str) -> bool {
            self.packs.iter().any(|p| p.checksum == checksum)
                || self.retired_packs.iter().any(|p| p.checksum == checksum)
        }

        /// Record currently live members before removing them. Never expires or truncates history.
        pub fn retire_packs(&mut self, checksums: &[String], seq: u64) {
            self.retire_packs_at(checksums, seq, crate::time::now());
        }

        pub fn retire_packs_at(&mut self, checksums: &[String], seq: u64, at: prost_types::Timestamp) {
            let requested: BTreeSet<_> = checksums.iter().collect();
            let mut recorded: BTreeSet<_> = self
                .retired_packs
                .iter()
                .map(|p| p.checksum.clone())
                .collect();
            for pack in &self.packs {
                if requested.contains(&pack.checksum) && recorded.insert(pack.checksum.clone()) {
                    self.retired_packs.push(RetiredPack {
                        checksum: pack.checksum.clone(),
                        retired_seq: seq,
                        retired_at: Some(at),
                    });
                }
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use crate::{
            Message,
            v1::{Manifest, PackRef},
        };
        #[test]
        fn retirement_is_exact_deduplicated_and_survives_reencoding() {
            let mut m = Manifest {
                packs: vec![PackRef {
                    checksum: "a".repeat(40),
                    ..Default::default()
                }],
                ..Default::default()
            };
            m.retire_packs(&["a".repeat(40), "b".repeat(40)], 7);
            m.retire_packs(&["a".repeat(40)], 9);
            m.packs.clear();
            let m = Manifest::decode(m.encode_to_vec().as_slice()).unwrap();
            assert_eq!(m.retired_packs.len(), 1);
            assert_eq!(m.retired_packs[0].retired_seq, 7);
            assert!(m.serves_pack(&"a".repeat(40)));
            assert!(!m.serves_pack(&"b".repeat(40)));
        }
        #[test]
        fn old_wire_messages_keep_new_fields_empty() {
            // Synthetic pre-extension wire bytes: format_version=1, object_format=sha1/sha256.
            for format in ["sha1", "sha256"] {
                let mut bytes = vec![8, 1, 26, u8::try_from(format.len()).unwrap()];
                bytes.extend_from_slice(format.as_bytes());
                let m = Manifest::decode(bytes.as_slice()).unwrap();
                assert_eq!(m.object_format, format);
                assert!(m.retired_packs.is_empty());
                assert_eq!(m.encode_to_vec(), bytes);
            }
            let p = PackRef::decode(&[56, 7, 64, 2][..]).unwrap();
            assert_eq!(p.seq, 7);
            assert_eq!(p.tier, 2);
            assert!(p.group_coverages.is_empty());
            assert_eq!(p.audience, 0);
        }
    }
}

pub mod snapshot {
    //! Canonical ref snapshots. Exact content keys are authority only after publication.
    use anyhow::{Result, ensure};
    use prost::Message;
    use sha2::{Digest, Sha256};

    use crate::v1::RefSnapshot;

    #[expect(
        clippy::case_sensitive_file_extension_comparisons,
        reason = "Git refname rules reject the case-sensitive .lock suffix"
    )]
    fn valid_ref(name: &str) -> bool {
        name.starts_with("refs/")
            && !name.ends_with('/')
            && !name.ends_with('.')
            && !name.contains("..")
            && !name.contains("@{")
            && !name
                .bytes()
                .any(|b| b <= b' ' || b == 127 || b"~^:?*[\\".contains(&b))
            && name
                .split('/')
                .all(|p| !p.is_empty() && !p.starts_with('.') && !p.ends_with(".lock"))
    }

    fn valid_oid(oid: &str, len: usize) -> bool {
        oid.len() == len
            && oid
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            && oid.bytes().any(|b| b != b'0')
    }

    /// Validate object format, ref names/OIDs, unique names and symbolic HEAD syntax.
    /// An unborn symbolic HEAD may name a ref absent from the snapshot.
    pub fn validate(snapshot: &RefSnapshot) -> Result<()> {
        let oid_len = match snapshot.object_format.as_str() {
            "sha1" => 40,
            "sha256" => 64,
            other => anyhow::bail!("unsupported snapshot object format {other:?}"),
        };
        ensure!(
            snapshot.head_target.is_empty() || valid_ref(&snapshot.head_target),
            "invalid snapshot symbolic HEAD"
        );
        let mut names = std::collections::BTreeSet::new();
        for r in &snapshot.refs {
            ensure!(valid_ref(&r.name), "invalid snapshot ref name {:?}", r.name);
            ensure!(names.insert(&r.name), "duplicate snapshot ref {:?}", r.name);
            ensure!(
                valid_oid(&r.oid, oid_len),
                "invalid snapshot OID for {}",
                r.name
            );
            ensure!(
                r.peeled.is_empty()
                    || (r.name.starts_with("refs/tags/") && valid_oid(&r.peeled, oid_len)),
                "invalid snapshot peeled target for {}",
                r.name
            );
        }
        Ok(())
    }

    /// Sort refs and omit incidental creation time. Duplicate/conflicting refs are errors.
    pub fn canonicalize(snapshot: &RefSnapshot) -> Result<RefSnapshot> {
        validate(snapshot)?;
        let mut canonical = snapshot.clone();
        canonical.refs.sort_by(|a, b| a.name.cmp(&b.name));
        canonical.created_at = None;
        Ok(canonical)
    }

    pub fn encode(snapshot: &RefSnapshot) -> Result<Vec<u8>> {
        Ok(canonicalize(snapshot)?.encode_to_vec())
    }

    pub fn key(bytes: &[u8]) -> String {
        format!("checkpoints/refs/{}.pb", hex::encode(Sha256::digest(bytes)))
    }

    /// Verify exact canonical encoding as well as the content-addressed key.
    pub fn decode_verified(expected_key: &str, bytes: &[u8]) -> Result<RefSnapshot> {
        ensure!(key(bytes) == expected_key, "snapshot content key mismatch");
        let snapshot = RefSnapshot::decode(bytes)?;
        ensure!(
            encode(&snapshot)? == bytes,
            "snapshot encoding is not canonical"
        );
        Ok(snapshot)
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use crate::v1::Ref;
        fn fixture(format: &str) -> RefSnapshot {
            let len = if format == "sha1" { 40 } else { 64 };
            RefSnapshot {
                seq: 7,
                object_format: format.into(),
                head_target: "refs/heads/main".into(),
                created_at: Some(crate::time::now()),
                refs: vec![
                    Ref {
                        name: "refs/tags/v1".into(),
                        oid: "b".repeat(len),
                        peeled: "a".repeat(len),
                    },
                    Ref {
                        name: "refs/heads/main".into(),
                        oid: "a".repeat(len),
                        peeled: String::new(),
                    },
                ],
            }
        }
        #[test]
        fn sha1_and_sha256_canonicalize_independently_of_time_and_order() {
            for format in ["sha1", "sha256"] {
                let mut s = fixture(format);
                let bytes = encode(&s).unwrap();
                s.refs.reverse();
                s.created_at = None;
                assert_eq!(bytes, encode(&s).unwrap());
                assert_eq!(
                    decode_verified(&key(&bytes), &bytes).unwrap(),
                    canonicalize(&s).unwrap()
                );
                let mut conflict = s.clone();
                conflict.refs[0].oid = "c".repeat(conflict.refs[0].oid.len());
                assert_ne!(key(&bytes), key(&encode(&conflict).unwrap()));
                assert!(decode_verified(&key(&bytes), &encode(&conflict).unwrap()).is_err());
            }
        }
        #[test]
        fn malformed_duplicate_and_noncanonical_snapshots_fail() {
            let mut s = fixture("sha1");
            s.refs.push(s.refs[0].clone());
            assert!(encode(&s).is_err());
            let mut s = fixture("sha1");
            s.refs[0].oid = "0".repeat(40);
            assert!(encode(&s).is_err());
            let mut s = fixture("sha1");
            s.refs[0].name = "refs/../x".into();
            assert!(encode(&s).is_err());
            let mut s = fixture("sha1");
            s.head_target = "main".into();
            assert!(encode(&s).is_err());
            let s = fixture("sha1");
            let noncanonical = s.encode_to_vec();
            assert!(decode_verified(&key(&noncanonical), &noncanonical).is_err());
            assert!(decode_verified("checkpoints/refs/not-a-digest.pb", &encode(&s).unwrap()).is_err());
        }
    }
}

pub use prost;
pub use prost::Message;
pub use prost_types;

/// Current `Manifest.format_version`.
pub const WAL_FORMAT_VERSION: u32 = 1;

/// Repo-relative object keys. Everything is under `repos/<owner>/<repo>/`.
pub mod keys {
    /// Prefix for a repository, always with trailing slash.
    pub fn repo_prefix(owner: &str, name: &str) -> String {
        format!("repos/{owner}/{name}/")
    }
    pub const MANIFEST: &str = "manifest.pb";
    pub const LOG_DIR: &str = "log/";
    pub const WAL_DIR: &str = "wal/";
    pub const CHECKPOINTS_DIR: &str = "checkpoints/";
    pub const LEASES_DIR: &str = "leases/";
    pub const BUNDLES_DIR: &str = "bundles/";
    pub const BUNDLE_LIST: &str = "bundles/list.pb";
    /// Bucket-root prefix of maintainer heartbeats (not under a repo).
    pub const MAINTAIN_DIR: &str = "maintain/";
    pub fn maintainer_key(host: &str) -> String {
        format!("{MAINTAIN_DIR}{host}.pb")
    }
    pub const LFS_DIR: &str = "lfs/objects/";
    /// Per-repo connectivity audit result (`FsckReport`). Overwritten, not WAL.
    pub const FSCK: &str = "fsck.pb";
    pub const CATALOG: &str = "meta/repos.pb";
    /// Per-repo push policy (JSON). Not on the WAL; CAS'd independently.
    pub const POLICY: &str = "policy.json";

    pub fn policy_key(owner: &str, name: &str) -> String {
        format!("{}{POLICY}", repo_prefix(owner, name))
    }

    /// `log/<first_seq:016x>.pb`
    pub fn log_segment_key(first_seq: u64) -> String {
        format!("{LOG_DIR}{first_seq:016x}.pb")
    }
    pub fn pack_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.pack")
    }
    pub fn idx_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.idx")
    }
    pub fn rev_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.rev")
    }
    pub fn bitmap_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.bitmap")
    }
    pub fn commit_graph_key(checksum_hex: &str) -> String {
        format!("{WAL_DIR}{checksum_hex}.commit-graph")
    }
    pub fn checkpoint_dir(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/")
    }
    pub fn checkpoint_key(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/checkpoint.pb")
    }
    /// Unique candidate metadata; attempt must be a caller-generated safe identifier.
    pub fn checkpoint_attempt_key(seq: u64, attempt: &str) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/attempts/{attempt}/checkpoint.pb")
    }
    pub fn checkpoint_refs_key(seq: u64) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/refs.pb")
    }
    pub fn checkpoint_bundle_key(seq: u64, checksum_hex: &str) -> String {
        format!("{CHECKPOINTS_DIR}{seq:016x}/{checksum_hex}.bundle")
    }
    pub fn lease_key(name: &str) -> String {
        format!("{LEASES_DIR}{name}.pb")
    }
    /// Git LFS oid: 64 hex characters (sha256).
    pub fn lfs_oid_ok(oid: &str) -> bool {
        oid.len() == 64 && oid.bytes().all(|b| b.is_ascii_hexdigit())
    }

    pub fn lfs_key(oid: &str) -> String {
        let (aa, bb) = match (oid.get(..2), oid.get(2..4)) {
            (Some(a), Some(b)) => (a, b),
            _ => ("", ""),
        };
        format!("{LFS_DIR}{aa}/{bb}/{oid}")
    }
}

/// Length-prefixed framing for log objects: `uvarint(len) || LogEntry`.
/// Appendable objects grow by appending frames; readers stop at the first
/// incomplete trailing frame.
pub mod frame {
    use bytes::{Bytes, BytesMut};
    use prost::Message;

    use crate::v1::LogEntry;

    pub fn encode_entry(e: &LogEntry, out: &mut BytesMut) {
        let len = e.encoded_len();
        prost::encoding::encode_varint(len as u64, out);
        out.reserve(len);
        // BytesMut grows as needed; encode_raw has no fallible capacity check.
        e.encode_raw(out);
    }

    pub fn encode_entries<'a>(entries: impl IntoIterator<Item = &'a LogEntry>) -> Bytes {
        let mut b = BytesMut::new();
        for e in entries {
            encode_entry(e, &mut b);
        }
        b.freeze()
    }

    /// Decode all complete frames. Returns entries and the number of bytes
    /// consumed (a trailing partial frame is left unconsumed, not an error).
    pub fn decode_entries(buf: &[u8]) -> Result<(Vec<LogEntry>, usize), prost::DecodeError> {
        let mut out = Vec::new();
        let mut pos = 0usize;
        while let Some(mut probe) = buf.get(pos..) {
            let Ok(len) = prost::encoding::decode_varint(&mut probe) else {
                break;
            };
            let Ok(len) = usize::try_from(len) else {
                break;
            };
            let Some(frame) = probe.get(..len) else {
                break;
            };
            out.push(LogEntry::decode(frame)?);
            pos = buf.len() - probe.len() + len;
        }
        Ok((out, pos))
    }
}

/// Convert to/from prost timestamps.
pub mod time {
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn now() -> prost_types::Timestamp {
        from_system(SystemTime::now())
    }
    pub fn from_system(t: SystemTime) -> prost_types::Timestamp {
        let d = t.duration_since(UNIX_EPOCH).unwrap_or_default();
        prost_types::Timestamp {
            seconds: i64::try_from(d.as_secs()).unwrap_or(i64::MAX),
            nanos: i32::try_from(d.subsec_nanos()).unwrap_or(999_999_999),
        }
    }
    pub fn to_system(t: &prost_types::Timestamp) -> SystemTime {
        UNIX_EPOCH
            + Duration::new(
                t.seconds.max(0).cast_unsigned(),
                t.nanos.max(0).cast_unsigned(),
            )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn roundtrip_manifest() {
        let m = v1::Manifest {
            format_version: WAL_FORMAT_VERSION,
            repo: "acme/monorepo".into(),
            object_format: "sha1".into(),
            head_seq: 3,
            ..Default::default()
        };
        let bytes = m.encode_to_vec();
        let back = v1::Manifest::decode(bytes.as_slice()).unwrap();
        assert_eq!(back, m);
    }
    #[test]
    fn keys() {
        assert_eq!(keys::log_segment_key(66), "log/0000000000000042.pb");
        assert_eq!(keys::pack_key("x"), "wal/x.pack");
        assert_eq!(
            keys::checkpoint_key(1),
            "checkpoints/0000000000000001/checkpoint.pb"
        );
        assert_eq!(keys::lfs_key("abcdef"), "lfs/objects/ab/cd/abcdef");
        assert!(!keys::lfs_oid_ok("ab"));
        assert!(!keys::lfs_oid_ok("abcdef"));
        assert!(keys::lfs_oid_ok(&"a".repeat(64)));
        assert_eq!(keys::lfs_key("ab"), "lfs/objects///ab");
    }
    #[test]
    fn frames_roundtrip_and_partial() {
        let e1 = v1::LogEntry {
            seq: 1,
            kind: v1::EntryKind::Push as i32,
            ..Default::default()
        };
        let e2 = v1::LogEntry {
            seq: 2,
            kind: v1::EntryKind::Compact as i32,
            supersedes: vec!["a".into(); 40],
            ..Default::default()
        };
        let all = frame::encode_entries([&e1, &e2]);
        let (got, used) = frame::decode_entries(&all).unwrap();
        assert_eq!(got, vec![e1.clone(), e2.clone()]);
        assert_eq!(used, all.len());
        // Truncated tail: only e1 decodes, consumed = e1 frame length.
        let cut = &all[..all.len() - 5];
        let (got, used) = frame::decode_entries(cut).unwrap();
        assert_eq!(got, vec![e1.clone()]);
        assert_eq!(used, frame::encode_entries([&e1]).len());
    }
}
