use std::fs;
use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use serde::Serialize;
use walgit_proto::v1::EntryKind;

use crate::AppState;
use crate::error::ApiError;

/// Repository overview, maintenance operations, task listing, and task SSE.
/// These JSON/SSE routes are protocol surface and remain available without the web UI.
pub fn router(state: Arc<AppState>) -> Router {
    let mut router = Router::new();
    for base in crate::web::api::REPO_API_BASES {
        router = router
            .route(&format!("{base}/overview"), get(overview))
            .route(&format!("{base}/ops"), get(ops_list))
            .route(
                &format!("{base}/ops/{{op}}"),
                axum::routing::post(ops_start),
            )
            .route(&format!("{base}/tasks"), get(tasks_list))
            .route(&format!("{base}/tasks/{{id}}"), get(task_stream));
    }
    router.with_state(state)
}

#[derive(Serialize)]
struct Overview {
    repo: String,
    /// Which instance rendered this page (kind, name, shape, build).
    instance: crate::instance::InstanceInfo,
    clone_url: String,
    /// One-time git setup for this host (credential helper), multi-line.
    setup: String,
    /// `curl -fsSL …/services/public/install.sh | sh` one-liner (the open lane; no token needed).
    install: String,
    /// Absolute URL of the installer (browser download is already signed in).
    install_url: String,
    hostname: String,
    health: Health,
    manifest: ManifestInfo,
    local: LocalInfo,
    packs: PacksInfo,
    maintenance: MaintenanceInfo,
    compactions: Vec<CompactionInfo>,
    node: serde_json::Map<String, serde_json::Value>,
    ops: OpsInfo,
    /// Ready-to-paste git invocations for this repo.
    clone: CloneInfo,
}

#[derive(Serialize, Default)]
struct MaintenanceInfo {
    maintainers: Vec<MaintainerInfo>,
    orphaned: bool,
}

#[derive(Serialize)]
struct MaintainerInfo {
    host: String,
    disk: String,
    max_pack_bytes: u64,
    last_pass_age_secs: Option<u64>,
    alive: bool,
    passes: u64,
    last_unit: String,
}

#[derive(Serialize)]
struct CloneInfo {
    /// `git -c http.extraHeader="Authorization: Bearer $WALGIT_TOKEN" … clone <url>`: per-command, no installer.
    manual: String,
    /// Plain clone (after the installer ran).
    plain: String,
}

#[derive(Serialize)]
struct Health {
    status: &'static str,
    issues: Vec<String>,
    /// The last connectivity audit as the maintainer recorded it in `fsck.pb` (any host), else
    /// "never audited".
    deep: String,
    /// Maintenance this repository is missing; each maps to an op. `auto` says when the
    /// maintainer loop does it by itself — then the button is a "do it now", not a chore.
    suggestions: Vec<Suggestion>,
}

#[derive(Serialize)]
struct Suggestion {
    op: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    params: Option<String>,
    reason: String,
    /// How/when the maintainer loop performs this without anyone asking (None: a human must).
    #[serde(skip_serializing_if = "Option::is_none")]
    auto: Option<String>,
}

#[derive(Serialize)]
struct OpsInfo {
    available: Vec<crate::ops::OpSpec>,
    /// Recent + running tasks on this instance (ops and automatic ones:
    /// materialize, remote-index).
    recent: Vec<walgit_wal::TaskRecord>,
}

#[derive(Serialize)]
struct ManifestInfo {
    version: String,
    next_seq: u64,
    min_seq: u64,
    segments: Vec<SegmentInfo>,
    tail_entries: usize,
    entries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    packset: Option<PacksetInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    last_push: Option<String>,
}

#[derive(Serialize)]
struct SegmentInfo {
    key: String,
    first_seq: u64,
    last_seq: u64,
    size: u64,
}

#[derive(Serialize)]
struct PacksetInfo {
    at_seq: u64,
    packs: usize,
    bytes: u64,
    created: String,
    creator: String,
}

#[derive(Serialize)]
struct LocalInfo {
    version: String,
    next_seq: u64,
    bootstrap: u64,
    reconciled: bool,
    size_bytes: u64,
    /// How objects are served here: `local` (packs on disk), `remote` (pack set
    /// too large: indexes local, data by range read), `pending` (packs not yet
    /// downloaded).
    objects: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    remote: Option<RemoteInfo>,
}

#[derive(Serialize)]
struct RemoteInfo {
    packs: usize,
    objects: u64,
    decoded: u64,
    block_range_reads: u64,
    block_bytes_read: u64,
    block_cache_bytes: u64,
}

#[derive(Serialize)]
struct PacksInfo {
    live: usize,
    live_bytes: u64,
    pushes: usize,
}

#[derive(Serialize)]
struct CompactionInfo {
    seq: u64,
    level: u32,
    first_seq: u64,
    last_seq: u64,
    pack_size: u64,
    superseded_packs: usize,
    superseded_bytes: u64,
    at: String,
    primary: String,
}

async fn overview(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let handle = state.registry.open(&id).await.map_err(wal_err)?;
    // read_log performs its own freshness check; acquire the read guard only
    // after it has completed because read_log may need the write lock.
    let entries = handle.read_log(1, None).await.map_err(wal_err)?;
    // Refs-level sync only: the overview must render for repos whose packs do
    // not fit this instance (that is exactly when people look at it).
    let _guard = handle.sync_refs().await.map_err(wal_err)?;
    let manifest = handle.manifest();
    let version = handle
        .manifest_version()
        .map(|version| version.to_string())
        .unwrap_or_default();
    let base_url = crate::smart::request_base_url(&state, &headers);
    let clone_url = format!("{base_url}/{id}.git");
    let recipes = crate::setup::recipes(&state.cfg, &base_url, Some(&id.to_string()));
    let setup = recipes.setup_text.clone();

    let created = manifest
        .updated_at
        .as_ref()
        .map(timestamp)
        .unwrap_or_default();
    let packs_bytes = manifest.packs.iter().map(|pack| pack.pack_size).sum();
    let packset = if manifest.packs.is_empty() {
        None
    } else {
        Some(PacksetInfo {
            at_seq: manifest.head_seq,
            packs: manifest.packs.len(),
            bytes: packs_bytes,
            created: created.clone(),
            creator: manifest.writer.clone(),
        })
    };
    let last_push = entries
        .iter()
        .filter(|entry| entry.kind() == EntryKind::Push)
        .filter_map(|entry| entry.created_at.as_ref().map(timestamp))
        .next_back();
    let mut push_count = 0;
    let mut compactions = Vec::new();
    let mut pack_by_checksum = std::collections::HashMap::new();
    for entry in &entries {
        if let Some(pack) = &entry.pack {
            pack_by_checksum.insert(pack.checksum.as_str(), (pack.seq, pack.pack_size));
        }
        if entry.kind() == EntryKind::Push && entry.pack.is_some() {
            push_count += 1;
        }
        if entry.kind() == EntryKind::Compact {
            let mut first = u64::MAX;
            let mut last = 0;
            let mut bytes = 0;
            for checksum in &entry.supersedes {
                if let Some((seq, size)) = pack_by_checksum.get(checksum.as_str()) {
                    first = first.min(*seq);
                    last = last.max(*seq);
                    bytes += *size;
                }
            }
            compactions.push(CompactionInfo {
                seq: entry.seq,
                level: entry.pack.as_ref().map_or(0, |pack| pack.tier),
                first_seq: if first == u64::MAX { 0 } else { first },
                last_seq: last,
                pack_size: entry.pack.as_ref().map_or(0, |pack| pack.pack_size),
                superseded_packs: entry.supersedes.len(),
                superseded_bytes: bytes,
                at: entry.created_at.as_ref().map(timestamp).unwrap_or_default(),
                primary: entry.writer.clone(),
            });
        }
    }
    let size_bytes = repo_size(handle.local().path()).await;
    let local_version = handle.local_version().unwrap_or_default();
    let reconciled = local_version == version && handle.applied_seq() == manifest.head_seq;
    let remote_reader = handle.remote();
    let objects_mode = if handle.packs_ready() {
        "local"
    } else if remote_reader.is_some() || !handle.packs_fit() {
        "remote"
    } else {
        "pending"
    };
    let remote_info = remote_reader.map(|r| {
        let (reads, bytes, cached) = state.registry.blocks().stats();
        RemoteInfo {
            packs: r.pack_count(),
            objects: r.total_objects(),
            decoded: r.objects_decoded.load(std::sync::atomic::Ordering::Relaxed),
            block_range_reads: reads,
            block_bytes_read: bytes,
            block_cache_bytes: cached,
        }
    });

    // Health + suggestions.
    let mut issues = Vec::new();
    let mut suggestions = Vec::new();
    let disk_mode_note = state.cfg.cache_is_disk().then(|| {
        format!(
            "local packs · {} on {} (disk mode, no cache budget; eviction only above {:.0}% disk use)",
            walgit_wal::remote::human_bytes(manifest.packs.iter().map(|p| p.pack_size + p.idx_size).sum()),
            state.cfg.cache.dir.display(),
            state.cfg.cache.disk_high_watermark * 100.0
        )
    });
    if disk_mode_note.is_some() {
        // D25: no budget on the SSD host — never the too-large path.
    } else if !handle.packs_fit() {
        issues.push(format!(
            "pack set ({}) exceeds this instance's cache limit ({}); objects are read from the store by range",
            walgit_wal::remote::human_bytes(manifest.packs.iter().map(|p| p.pack_size + p.idx_size).sum()),
            walgit_wal::remote::human_bytes(state.cfg.cache.max_bytes.as_u64())
        ));
    }
    if manifest.head_seq > 0 && !reconciled {
        issues.push(format!(
            "local copy on {} is at seq {} but the WAL head is {}",
            walgit_store::coord::instance_id(),
            handle.applied_seq(),
            manifest.head_seq
        ));
        suggestions.push(Suggestion {
            op: "sync",
            params: None,
            reason: "catch the local copy up to the WAL head".into(),
            auto: Some(
                "the next request to this instance revalidates (one conditional GET)".into(),
            ),
        });
    }
    let fresh = manifest.packs.iter().filter(|p| p.tier == 0).count();
    let ecfg = handle.validated_effective_config().map_err(wal_err)?;
    let maintenance_on = ecfg.packs.enabled && state.cfg.has_role(walgit_config::Role::Compact);
    if fresh >= ecfg.packs.fold_needs_at_least_packs {
        suggestions.push(Suggestion {
            op: "compact",
            params: None,
            reason: format!("{fresh} fresh packs available for maintenance"),
            auto: maintenance_on.then(|| {
                "the maintainer evaluates size and age within each compatible pack family".into()
            }),
        });
    }
    if manifest.head_seq > 0 {
        let cp_seq = manifest.checkpoint.as_ref().map_or(0, |c| c.seq);
        let behind = manifest.head_seq.saturating_sub(cp_seq);
        if behind >= state.cfg.wal.snapshot_every_entries.max(1) || (cp_seq == 0 && behind > 0) {
            suggestions.push(Suggestion {
                op: "checkpoint",
                params: None,
                reason: if cp_seq == 0 {
                    "no checkpoint yet: cold materialize replays the whole log".into()
                } else {
                    format!("checkpoint is {behind} entries behind the head")
                },
                auto: Some(format!(
                    "first unit of the maintainer's next pass (every {} entries / {} / {} of tail)",
                    state.cfg.wal.snapshot_every_entries,
                    humantime::format_duration(state.cfg.wal.checkpoint_interval),
                    state.cfg.wal.checkpoint_tail_bytes
                )),
            });
        }
    }
    // The audit verdict lives in the store (`fsck.pb`, written by whichever maintainer ran it),
    // not in this instance's task memory.
    let fsck_report = crate::ops::read_fsck(&handle).await.ok().flatten();
    let deep = match &fsck_report {
        Some(r) => {
            let when =
                r.at.as_ref()
                    .map(|t| t.seconds)
                    .map(|s| {
                        chrono::DateTime::from_timestamp(s, 0)
                            .map(|d| d.format("%Y-%m-%d %H:%MZ").to_string())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default();
            let verdict = if r.missing_total == 0 && r.problems == 0 {
                "clean".to_string()
            } else {
                format!(
                    "{} missing object(s), {} other problem(s){}",
                    r.missing_total,
                    r.problems,
                    if r.repaired_seq > 0 {
                        format!("; repaired at seq {}", r.repaired_seq)
                    } else {
                        String::new()
                    }
                )
            };
            format!(
                "{verdict} at seq {} ({when}, {}, {:.1}s)",
                r.seq, r.host, r.elapsed_secs
            )
        }
        None => "never audited".into(),
    };
    let fsck_every = ecfg.maintenance.fsck_interval;
    if fsck_report.is_none() && manifest.head_seq > 0 {
        suggestions.push(Suggestion {
            op: "fsck",
            params: Some("connectivity=1".into()),
            reason: "connectivity never audited".into(),
            auto: (!fsck_every.is_zero()).then(|| {
                format!(
                    "lowest-priority unit: runs when nothing else is due, then every {}",
                    humantime::format_duration(fsck_every)
                )
            }),
        });
    } else if let Some(r) = &fsck_report
        && (r.missing_total > 0 || r.problems > 0)
        && r.repaired_seq == 0
    {
        issues.push(format!(
            "last fsck found {} missing object(s), {} other problem(s)",
            r.missing_total, r.problems
        ));
        suggestions.push(Suggestion {
            op: "repair",
            params: None,
            reason: "fetch the missing objects from upstream.git and publish them".into(),
            auto: ecfg.upstream.git.as_ref().map(|_| {
                "the repair unit, right after checkpoints in the maintainer's priority".to_string()
            }),
        });
    }
    let status = if issues.iter().any(|i| i.starts_with("last fsck found")) {
        "error"
    } else if !issues.is_empty() {
        "degraded"
    } else {
        "ok"
    };
    let ops = OpsInfo {
        available: crate::ops::OPS.to_vec(),
        recent: state.registry.tasks().recent(&id.to_string()),
    };
    let clone = CloneInfo {
        manual: recipes.manual_clone.clone(),
        plain: recipes.plain_clone.clone(),
    };
    let maintenance = {
        let hbs_all = crate::maintain::heartbeats(&state)
            .await
            .unwrap_or_default();
        let maintainers: Vec<MaintainerInfo> = match Ok::<_, anyhow::Error>(hbs_all) {
            Ok(hbs) => hbs
                .into_iter()
                .filter(|h| {
                    walgit_config::repo_listed(&h.repos, id.owner(), id.name())
                        && !walgit_config::repo_listed(&h.exclude, id.owner(), id.name())
                })
                .map(|h| {
                    let age = h
                        .last_pass_at
                        .as_ref()
                        .map(walgit_proto::time::to_system)
                        .and_then(|t| std::time::SystemTime::now().duration_since(t).ok())
                        .map(|d| d.as_secs());
                    MaintainerInfo {
                        host: h.host,
                        disk: h.disk,
                        max_pack_bytes: h.max_pack_bytes,
                        last_pass_age_secs: age,
                        alive: age.is_some_and(|a| a < 600),
                        passes: h.passes,
                        last_unit: h.last_unit,
                    }
                })
                .collect(),
            Err(_) => Vec::new(),
        };
        let orphaned = !maintainers.iter().any(|m| m.alive);
        MaintenanceInfo {
            maintainers,
            orphaned,
        }
    };
    let body = Overview {
        repo: id.to_string(),
        instance: crate::instance::info(&state.cfg),
        clone_url,
        setup,
        install: recipes.install.clone(),
        install_url: recipes.install_url.clone(),
        hostname: walgit_store::coord::instance_id().to_string(),
        health: Health {
            status,
            issues,
            deep,
            suggestions,
        },
        manifest: ManifestInfo {
            version: version.clone(),
            next_seq: manifest.head_seq.saturating_add(1),
            min_seq: manifest.min_seq,
            segments: manifest
                .log_segments
                .iter()
                .map(|segment| SegmentInfo {
                    key: segment.key.clone(),
                    first_seq: segment.first_seq,
                    last_seq: segment.last_seq,
                    size: segment.size,
                })
                .collect(),
            tail_entries: entries.len(),
            entries: entries.len(),
            packset,
            last_push,
        },
        local: LocalInfo {
            version: local_version.clone(),
            next_seq: handle.applied_seq().saturating_add(1),
            bootstrap: handle.applied_seq(),
            reconciled,
            size_bytes,
            objects: objects_mode,
            remote: remote_info,
        },
        packs: PacksInfo {
            live: manifest.packs.len(),
            live_bytes: packs_bytes,
            pushes: push_count,
        },
        maintenance,
        compactions,
        node: {
            let mut m = serde_json::Map::new();
            if let Some(n) = disk_mode_note {
                m.insert("storage".into(), serde_json::Value::String(n));
            }
            m
        },
        ops,
        clone,
    };
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `GET …/ops` — available ops + recent outcomes on this instance.
async fn ops_list(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let body = OpsInfo {
        available: crate::ops::OPS.to_vec(),
        recent: state.registry.tasks().recent(&id.to_string()),
    };
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `POST …/ops/{op}?<params>` — run a maintenance op on this instance as a
/// background task and stream it (SSE envelope: `task`, `notice`, `progress`,
/// then `result` `{"task","value"}` or `error`). Write permission required.
/// If the same op is already running here the response attaches to that task
/// instead (same stream shape; its `task.id` tells you which).
async fn ops_start(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo, op)): AxumPath<(String, String, String)>,
    axum::extract::Query(params): axum::extract::Query<std::collections::HashMap<String, String>>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    let principal = state.auth.require_write(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    // Make sure the repo exists before spawning anything.
    state.registry.open(&id).await.map_err(wal_err)?;
    tracing::info!(repo = %id, op = %op, by = %principal.name, ?params, "ops.start");
    let task = match crate::ops::start(state.clone(), id, &op, params).await {
        Ok(t) => t,
        Err(crate::ops::StartError::UnknownOp) => {
            return Err(ApiError::NotFound(format!("unknown op {op}")));
        }
        Err(crate::ops::StartError::AlreadyRunning(existing)) => existing,
    };
    Ok(crate::sse::task_stream(task))
}

/// `GET …/tasks` — running + recent background tasks of this repo on this
/// instance (materialize, remote-index, fsck, compact, ...). The UI
/// polls this to show what is happening to a repo.
async fn tasks_list(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo)): AxumPath<(String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let tasks = state.registry.tasks();
    let body = serde_json::json!({
        "hostname": walgit_store::coord::instance_id(),
        "running": tasks.running(&id.to_string()),
        "recent": tasks.recent(&id.to_string()),
    });
    Ok((
        [
            (header::CONTENT_TYPE, "application/json"),
            (header::CACHE_CONTROL, "no-store"),
        ],
        serde_json::to_vec(&body).map_err(|e| ApiError::Internal(e.to_string()))?,
    )
        .into_response())
}

/// `GET …/tasks/{id}` — attach to a task: SSE replay of its packets so far,
/// then live, then the terminal `result`/`error`. JSON (no SSE accept) returns
/// the record.
async fn task_stream(
    State(state): State<Arc<AppState>>,
    AxumPath((owner, repo, task_id)): AxumPath<(String, String, String)>,
    headers: HeaderMap,
) -> Result<Response, ApiError> {
    state.auth.require_read(&headers).await.map_err(auth_err)?;
    let id =
        walgit_git::RepoId::new(&owner, &repo).map_err(|e| ApiError::NotFound(e.to_string()))?;
    let task = state
        .registry
        .tasks()
        .get(&task_id)
        .filter(|t| t.record().repo == id.to_string())
        .ok_or_else(|| ApiError::NotFound(format!("task {task_id} (tasks are per instance; this one may have run elsewhere or aged out)")))?;
    if !crate::sse::wants_sse(&headers) {
        return Ok((
            [
                (header::CONTENT_TYPE, "application/json"),
                (header::CACHE_CONTROL, "no-store"),
            ],
            serde_json::to_vec(&task.record()).map_err(|e| ApiError::Internal(e.to_string()))?,
        )
            .into_response());
    }
    Ok(crate::sse::task_stream(task))
}

fn timestamp(value: &prost_types::Timestamp) -> String {
    chrono::DateTime::<chrono::Utc>::from_timestamp(value.seconds, value.nanos as u32)
        .map(|date| date.to_rfc3339())
        .unwrap_or_default()
}

async fn repo_size(path: &Path) -> u64 {
    let path = path.to_owned();
    tokio::task::spawn_blocking(move || size_recursive(&path))
        .await
        .unwrap_or(0)
}

fn size_recursive(path: &Path) -> u64 {
    let Ok(metadata) = fs::symlink_metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    if !metadata.is_dir() {
        return 0;
    }
    fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| size_recursive(&entry.path()))
        .sum()
}

fn wal_err(error: walgit_wal::WalError) -> ApiError {
    match error {
        walgit_wal::WalError::NotFound => ApiError::NotFound("repository not found".into()),
        other => ApiError::Internal(format!("wal: {other}")),
    }
}

fn auth_err(error: crate::auth::AuthError) -> ApiError {
    match error {
        crate::auth::AuthError::Invalid | crate::auth::AuthError::Unauthorized => {
            ApiError::Unauthorized
        }
        crate::auth::AuthError::Forbidden => ApiError::Forbidden,
        crate::auth::AuthError::Unavailable => {
            ApiError::ServiceUnavailable("auth provider unavailable".into())
        }
    }
}
