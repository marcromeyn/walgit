//! Durable POSIX implementation of [`ObjectStore`].
//!
//! `PutMode::Create` is the only cross-client atomic conditional operation: a
//! complete, fsynced same-directory temporary file is published with `hard_link`,
//! which cannot replace a competing name, and then the file and directory chain are
//! synced. `Overwrite` is a durable unconditional rename. `Update(Version)` and
//! conditional delete are deliberately refused with [`StoreError::InvalidArgument`]
//! until a version-file protocol exists.
//!
//! Operations are reactor-independent and perform their POSIX work while polled.
//! Async hosts must invoke this backend from a blocking worker.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::{
    BoxStream, GetOptions, GetResult, ObjectMeta, ObjectStore, PutBody, PutMode, PutOptions,
    Result, StoreError, Version,
};
use bytes::Bytes;
use futures::StreamExt;
use sha2::{Digest, Sha256};

static TEMP_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Debug)]
pub struct PosixStore {
    root: PathBuf,
}

impl PosixStore {
    pub fn open(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        std::fs::create_dir_all(&root).map_err(StoreError::other)?;
        sync_directory(&root)?;
        Ok(Self { root })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    fn path(&self, key: &str) -> Result<PathBuf> {
        validate_key(key)?;
        Ok(self.root.join(key))
    }

    fn ensure_parent<'a>(&self, final_path: &'a Path) -> Result<&'a Path> {
        let parent = final_path
            .parent()
            .ok_or_else(|| StoreError::InvalidArgument(final_path.display().to_string()))?;
        std::fs::create_dir_all(parent).map_err(StoreError::other)?;
        self.sync_ancestors(parent)?;
        Ok(parent)
    }

    fn staged(&self, final_path: &Path, value: &[u8]) -> Result<PathBuf> {
        let parent = self.ensure_parent(final_path)?;
        for _ in 0..1024 {
            let id = TEMP_ID.fetch_add(1, Ordering::Relaxed);
            let temp = parent.join(format!(".walgit-posix-{}-{id}.tmp", std::process::id()));
            match OpenOptions::new().write(true).create_new(true).open(&temp) {
                Ok(mut file) => {
                    if let Err(error) = file.write_all(value).and_then(|()| file.sync_all()) {
                        let _ = std::fs::remove_file(&temp);
                        return Err(StoreError::other(error));
                    }
                    return Ok(temp);
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
                Err(error) => return Err(StoreError::other(error)),
            }
        }
        Err(StoreError::other(std::io::Error::other(
            "could not allocate an exclusive temporary object",
        )))
    }

    fn sync_ancestors(&self, directory: &Path) -> Result<()> {
        let mut current = directory;
        loop {
            sync_directory(current)?;
            if current == self.root {
                return Ok(());
            }
            current = current.parent().ok_or_else(|| {
                StoreError::InvalidArgument(format!(
                    "{} is outside {}",
                    directory.display(),
                    self.root.display()
                ))
            })?;
        }
    }

    fn read_object(&self, key: &str) -> Result<Option<(Bytes, Version)>> {
        let path = self.path(key)?;
        match std::fs::read(path) {
            Ok(value) => {
                let bytes = Bytes::from(value);
                let version = content_version(&bytes);
                Ok(Some((bytes, version)))
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(StoreError::other(error)),
        }
    }

    fn overwrite(&self, key: &str, value: &[u8]) -> Result<ObjectMeta> {
        let path = self.path(key)?;
        let temp = self.staged(&path, value)?;
        if let Err(error) = std::fs::rename(&temp, &path) {
            let _ = std::fs::remove_file(&temp);
            return Err(StoreError::other(error));
        }
        File::open(&path)
            .and_then(|file| file.sync_all())
            .map_err(StoreError::other)?;
        self.sync_ancestors(validated_parent(&path)?)?;
        Ok(object_meta(key, value))
    }

    fn create(&self, key: &str, value: &[u8]) -> Result<ObjectMeta> {
        let path = self.path(key)?;
        let temp = self.staged(&path, value)?;
        let parent = validated_parent(&path)?;
        match std::fs::hard_link(&temp, &path) {
            Ok(()) => {
                let synced = File::open(&path).and_then(|file| file.sync_all());
                let _ = std::fs::remove_file(&temp);
                synced.map_err(StoreError::other)?;
                self.sync_ancestors(parent)?;
                Ok(object_meta(key, value))
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let synced = File::open(&path).and_then(|file| file.sync_all());
                let _ = std::fs::remove_file(&temp);
                synced.map_err(StoreError::other)?;
                self.sync_ancestors(parent)?;
                let current = self.read_object(key)?.map(|(_, version)| version);
                Err(StoreError::PreconditionFailed {
                    key: key.into(),
                    current,
                })
            }
            Err(error) => {
                let _ = std::fs::remove_file(&temp);
                Err(StoreError::other(error))
            }
        }
    }
}

async fn body_bytes(body: PutBody) -> Result<Bytes> {
    match body {
        PutBody::Bytes(bytes) => Ok(bytes),
        PutBody::Stream { len, stream } => {
            crate::util::collect(stream, usize::try_from(len).map_err(StoreError::other)?).await
        }
        PutBody::File(path) => std::fs::read(path)
            .map(Bytes::from)
            .map_err(StoreError::other),
    }
}

#[async_trait::async_trait]
impl ObjectStore for PosixStore {
    fn backend(&self) -> &'static str {
        "posix"
    }

    async fn get(&self, key: &str, opts: GetOptions) -> Result<GetResult> {
        let (bytes, version) = self
            .read_object(key)?
            .ok_or_else(|| StoreError::NotFound { key: key.into() })?;
        if opts.if_match.as_ref().is_some_and(|want| want != &version) {
            return Err(StoreError::PreconditionFailed {
                key: key.into(),
                current: Some(version),
            });
        }
        if opts.if_none_match.as_ref() == Some(&version) {
            return Ok(GetResult::NotModified { version });
        }
        let size = bytes.len() as u64;
        let body = if let Some(range) = opts.range {
            if range.start > range.end {
                return Err(StoreError::InvalidArgument(format!(
                    "bad range {range:?} for size {size}"
                )));
            }
            let start = usize::try_from(range.start.min(size)).map_err(StoreError::other)?;
            let end = usize::try_from(range.end.min(size)).map_err(StoreError::other)?;
            bytes.slice(start..end)
        } else {
            bytes
        };
        Ok(GetResult::Object {
            meta: ObjectMeta {
                key: key.into(),
                size,
                version,
            },
            body: crate::util::once(body),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<ObjectMeta>> {
        Ok(self.read_object(key)?.map(|(bytes, version)| ObjectMeta {
            key: key.into(),
            size: bytes.len() as u64,
            version,
        }))
    }

    async fn put(&self, key: &str, body: PutBody, opts: PutOptions) -> Result<ObjectMeta> {
        let bytes = body_bytes(body).await?;
        match opts.mode {
            PutMode::Overwrite => self.overwrite(key, &bytes),
            PutMode::Create => self.create(key, &bytes),
            PutMode::Update(_) => Err(StoreError::InvalidArgument(
                "POSIX object store does not support PutMode::Update; use create-only immutable objects"
                    .into(),
            )),
        }
    }

    async fn delete(&self, key: &str, if_version: Option<Version>) -> Result<()> {
        let path = self.path(key)?;
        if if_version.is_some() {
            return Err(StoreError::InvalidArgument(
                "POSIX object store does not support conditional delete".into(),
            ));
        }
        match std::fs::remove_file(&path) {
            Ok(()) => self.sync_ancestors(validated_parent(&path)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(StoreError::other(error)),
        }
    }

    fn list(
        &self,
        prefix: &str,
        start_after: Option<&str>,
    ) -> BoxStream<'static, Result<ObjectMeta>> {
        let result = (|| {
            validate_prefix(prefix)?;
            let mut keys = Vec::new();
            collect(&self.root, &self.root, &mut keys)?;
            keys.retain(|key| {
                key.starts_with(prefix) && start_after.is_none_or(|start| key.as_str() > start)
            });
            keys.sort();
            keys.into_iter()
                .map(|key| {
                    let (bytes, version) = self
                        .read_object(&key)?
                        .ok_or_else(|| StoreError::NotFound { key: key.clone() })?;
                    Ok(ObjectMeta {
                        key,
                        size: bytes.len() as u64,
                        version,
                    })
                })
                .collect::<Result<Vec<_>>>()
        })();
        match result {
            Ok(items) => futures::stream::iter(items.into_iter().map(Ok)).boxed(),
            Err(error) => futures::stream::once(async move { Err(error) }).boxed(),
        }
    }

    async fn list_prefixes(&self, prefix: &str) -> Result<Vec<String>> {
        validate_prefix(prefix)?;
        if !prefix.is_empty() && !prefix.ends_with('/') {
            return Err(StoreError::InvalidArgument(format!(
                "list-prefixes prefix must end in '/': {prefix:?}"
            )));
        }
        let mut prefixes = Vec::new();
        let mut stream = self.list(prefix, None);
        while let Some(meta) = stream.next().await {
            let key = meta?.key;
            if let Some(rest) = key.strip_prefix(prefix)
                && let Some((head, _)) = rest.split_once('/')
            {
                prefixes.push(format!("{prefix}{head}/"));
            }
        }
        prefixes.sort();
        prefixes.dedup();
        Ok(prefixes)
    }
}

fn content_version(value: &[u8]) -> Version {
    Version::new(format!("sha256:{:x}", Sha256::digest(value)))
}

fn object_meta(key: &str, value: &[u8]) -> ObjectMeta {
    ObjectMeta {
        key: key.into(),
        size: value.len() as u64,
        version: content_version(value),
    }
}

fn validate_prefix(prefix: &str) -> Result<()> {
    if prefix.starts_with('/') || prefix.split('/').any(|part| part == "." || part == "..") {
        return Err(StoreError::InvalidArgument(format!(
            "invalid store prefix {prefix:?}"
        )));
    }
    Ok(())
}

fn validate_key(key: &str) -> Result<()> {
    if key.is_empty()
        || key.starts_with('/')
        || key.ends_with('/')
        || key.split('/').any(|part| {
            part.is_empty()
                || part == "."
                || part == ".."
                || part.starts_with(".walgit-posix-")
                || !part
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
        })
    {
        return Err(StoreError::InvalidArgument(format!(
            "invalid store key {key:?}"
        )));
    }
    Ok(())
}

fn validated_parent(path: &Path) -> Result<&Path> {
    path.parent()
        .ok_or_else(|| StoreError::InvalidArgument(path.display().to_string()))
}

fn collect(root: &Path, directory: &Path, output: &mut Vec<String>) -> Result<()> {
    if !directory.exists() {
        return Ok(());
    }
    for entry in std::fs::read_dir(directory).map_err(StoreError::other)? {
        let entry = entry.map_err(StoreError::other)?;
        if entry
            .file_name()
            .to_string_lossy()
            .starts_with(".walgit-posix-")
        {
            continue;
        }
        let path = entry.path();
        if entry.file_type().map_err(StoreError::other)?.is_dir() {
            collect(root, &path, output)?;
        } else {
            output.push(
                path.strip_prefix(root)
                    .map_err(StoreError::other)?
                    .to_string_lossy()
                    .replace(std::path::MAIN_SEPARATOR, "/"),
            );
        }
    }
    Ok(())
}

fn sync_directory(path: &Path) -> Result<()> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(StoreError::other)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ObjectStoreExt;

    fn digest(value: &[u8]) -> String {
        format!("{:x}", Sha256::digest(value))
    }

    async fn read(store: &PosixStore, key: &str) -> Bytes {
        store.get_bytes(key).await.unwrap().unwrap().1
    }

    #[tokio::test]
    async fn two_writers_publish_exactly_one_manifest() {
        let root = tempfile::tempdir().unwrap();
        let left = PosixStore::open(root.path()).unwrap();
        let right = PosixStore::open(root.path()).unwrap();
        let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
        let run = |store: PosixStore,
                   value: &'static [u8],
                   barrier: std::sync::Arc<std::sync::Barrier>| {
            std::thread::spawn(move || {
                barrier.wait();
                futures::executor::block_on(store.put_bytes(
                    "manifest/1",
                    Bytes::from_static(value),
                    PutMode::Create,
                ))
            })
        };
        let a = run(left.clone(), b"left", barrier.clone());
        let b = run(right, b"right", barrier);
        let outcomes = [a.join().unwrap(), b.join().unwrap()];
        assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
        assert_eq!(
            outcomes
                .iter()
                .filter(|outcome| outcome
                    .as_ref()
                    .is_err_and(StoreError::is_precondition_failed))
                .count(),
            1
        );
        let reopened = PosixStore::open(root.path()).unwrap();
        let bytes = read(&reopened, "manifest/1").await;
        assert!(matches!(bytes.as_ref(), b"left" | b"right"));
        let keys: Vec<_> = reopened
            .list("manifest", None)
            .map(|item| item.unwrap().key)
            .collect()
            .await;
        assert_eq!(keys, ["manifest/1"]);
    }

    #[tokio::test]
    async fn payload_conflict_preserves_original_bytes() {
        let root = tempfile::tempdir().unwrap();
        let store = PosixStore::open(root.path()).unwrap();
        let original = b"immutable payload";
        let key = format!("payload/{}", digest(original));
        let meta = store
            .put_bytes(&key, original.as_slice(), PutMode::Create)
            .await
            .unwrap();

        let conflict = store
            .put_bytes(&key, b"different payload".as_slice(), PutMode::Create)
            .await
            .unwrap_err();
        assert!(conflict.is_precondition_failed());
        assert_eq!(read(&store, &key).await.as_ref(), original);
        assert!(
            store
                .get_if_changed(&key, &meta.version)
                .await
                .unwrap()
                .is_none()
        );
    }

    #[tokio::test]
    async fn orphaned_loser_payload_is_never_manifest_truth() {
        let root = tempfile::tempdir().unwrap();
        let winner = PosixStore::open(root.path()).unwrap();
        let loser = PosixStore::open(root.path()).unwrap();
        let winner_digest = digest(b"winner payload");
        let loser_digest = digest(b"loser payload");
        winner
            .put_bytes(
                &format!("payload/{winner_digest}"),
                b"winner payload".as_slice(),
                PutMode::Create,
            )
            .await
            .unwrap();
        loser
            .put_bytes(
                &format!("payload/{loser_digest}"),
                b"loser payload".as_slice(),
                PutMode::Create,
            )
            .await
            .unwrap();

        winner
            .put_bytes(
                "manifest/1",
                Bytes::copy_from_slice(winner_digest.as_bytes()),
                PutMode::Create,
            )
            .await
            .unwrap();
        let lost = loser
            .put_bytes(
                "manifest/1",
                Bytes::copy_from_slice(loser_digest.as_bytes()),
                PutMode::Create,
            )
            .await
            .unwrap_err();
        assert!(lost.is_precondition_failed());

        let reopened = PosixStore::open(root.path()).unwrap();
        assert_eq!(
            read(&reopened, "manifest/1").await.as_ref(),
            winner_digest.as_bytes()
        );
        assert_eq!(
            read(&reopened, &format!("payload/{winner_digest}"))
                .await
                .as_ref(),
            b"winner payload"
        );
        assert!(
            reopened
                .head(&format!("payload/{loser_digest}"))
                .await
                .unwrap()
                .is_some(),
            "the orphan may remain as garbage but cannot select truth"
        );
    }

    #[tokio::test]
    async fn lost_successful_reply_resolves_from_manifest_digest() {
        let root = tempfile::tempdir().unwrap();
        let store = PosixStore::open(root.path()).unwrap();
        let payload = b"durable payload";
        let payload_digest = digest(payload);
        store
            .put_bytes(
                &format!("payload/{payload_digest}"),
                payload.as_slice(),
                PutMode::Create,
            )
            .await
            .unwrap();

        // Model a transport dropping the successful response: publication ran to
        // completion, but the caller discards the returned ObjectMeta.
        let _lost_reply = store
            .put_bytes(
                "manifest/1",
                Bytes::copy_from_slice(payload_digest.as_bytes()),
                PutMode::Create,
            )
            .await
            .unwrap();
        drop(store);

        let retry = PosixStore::open(root.path()).unwrap();
        assert_eq!(
            read(&retry, "manifest/1").await.as_ref(),
            payload_digest.as_bytes()
        );
    }

    #[tokio::test]
    async fn partial_write_never_becomes_visible() {
        let root = tempfile::tempdir().unwrap();
        let store = PosixStore::open(root.path()).unwrap();
        let manifest_dir = root.path().join("manifest");
        std::fs::create_dir_all(&manifest_dir).unwrap();
        let partial = manifest_dir.join(".walgit-posix-999-1.tmp");
        let mut file = File::create(&partial).unwrap();
        file.write_all(b"partial").unwrap();
        file.sync_all().unwrap();
        sync_directory(&manifest_dir).unwrap();
        drop(file);
        drop(store);

        let restarted = PosixStore::open(root.path()).unwrap();
        assert!(restarted.head("manifest/1").await.unwrap().is_none());
        assert!(
            restarted
                .list("manifest", None)
                .collect::<Vec<_>>()
                .await
                .is_empty()
        );
        restarted
            .put_bytes("manifest/1", b"complete".as_slice(), PutMode::Create)
            .await
            .unwrap();
        assert_eq!(read(&restarted, "manifest/1").await.as_ref(), b"complete");
    }

    #[tokio::test]
    async fn restart_recovers_the_same_head() {
        let root = tempfile::tempdir().unwrap();
        let store = PosixStore::open(root.path()).unwrap();
        let payload = b"head payload";
        let payload_digest = digest(payload);
        store
            .put_bytes(
                &format!("wal/1-{payload_digest}"),
                payload.as_slice(),
                PutMode::Create,
            )
            .await
            .unwrap();
        store
            .put_bytes(
                "manifest/1",
                Bytes::copy_from_slice(payload_digest.as_bytes()),
                PutMode::Create,
            )
            .await
            .unwrap();
        let before = store.head("manifest/1").await.unwrap().unwrap();
        drop(store);

        let restarted = PosixStore::open(root.path()).unwrap();
        let after = restarted.head("manifest/1").await.unwrap().unwrap();
        assert_eq!(after, before);
        assert_eq!(
            read(&restarted, "manifest/1").await.as_ref(),
            payload_digest.as_bytes()
        );
        assert_eq!(
            read(&restarted, &format!("wal/1-{payload_digest}"))
                .await
                .as_ref(),
            payload
        );
    }

    #[tokio::test]
    async fn update_is_refused_with_a_typed_error() {
        let root = tempfile::tempdir().unwrap();
        let store = PosixStore::open(root.path()).unwrap();
        let initial = store
            .put_bytes("manifest/1", b"first".as_slice(), PutMode::Create)
            .await
            .unwrap();
        let error = store
            .put_bytes(
                "manifest/1",
                b"updated".as_slice(),
                PutMode::Update(initial.version),
            )
            .await
            .unwrap_err();
        assert!(matches!(error, StoreError::InvalidArgument(_)));
        assert_eq!(read(&store, "manifest/1").await.as_ref(), b"first");
    }
}
