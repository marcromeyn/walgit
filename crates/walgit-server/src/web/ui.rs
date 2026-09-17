use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use rust_embed::RustEmbed;

use crate::AppState;
#[derive(RustEmbed)]
#[folder = "../../web/dist"]
// Debug builds read the folder at runtime; `allow_missing` lets the crate
// compile where the web build is absent (distributed sccache workers).
// Release images assert the optimized artefacts exist (Containerfile).
#[allow_missing = true]
struct Assets;

/// Look up a UI asset. Release builds embed `web/dist`; debug builds read it
/// from disk. When the crate was compiled on a machine without `web/dist`
/// (distributed sccache), rust-embed's baked-in folder path does not
/// canonicalize, so fall back to reading relative to this crate's manifest.
fn embedded(path: &str) -> Option<rust_embed::EmbeddedFile> {
    if let Some(f) = Assets::get(path) {
        return Some(f);
    }
    if cfg!(debug_assertions) {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../web/dist")
            .canonicalize()
            .ok()?;
        let file = root.join(path).canonicalize().ok()?;
        if !file.starts_with(&root) {
            return None;
        }
        return rust_embed::utils::read_file_from_fs(&file).ok();
    }
    None
}

const INDEX: &str = "index.html";
const IMMUTABLE: &str = "public, max-age=31536000, immutable";

/// Routes for the optional embedded SPA.
pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/", get(root))
        .route("/_ui/{*path}", get(asset))
        .route("/services/setup.json", get(setup_json))
        // "API" docs page (SPA route); `/api/v1` is the JSON discovery document (D20).
        .route("/api", get(index_route))
        .route("/{owner}", get(index_route))
        .route(
            "/{owner}/{repo}",
            get(index_route)
                .put(crate::dispatch)
                .delete(crate::dispatch),
        )
        .route("/{owner}/{repo}/tree/{*rest}", get(index_route))
        .route("/{owner}/{repo}/blob/{*rest}", get(index_route))
        .route("/{owner}/{repo}/commits", get(index_route))
        .route("/{owner}/{repo}/commits/{*rest}", get(index_route))
        .route("/{owner}/{repo}/commit/{*rest}", get(index_route))
        .route("/{owner}/{repo}/wal", get(index_route))
        .route("/{owner}/{repo}/settings", get(index_route))
        .with_state(state)
}

/// The installer a not-yet-signed-in user needs — the only route on the open
/// `/services/public/*` prefix;
/// nothing under this router is gated, so nothing with data may ever be added to it.
pub fn public_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/services/public/install.sh", get(install_sh))
        // The certificate this process presents (self_signed/files, D39): public
        // material, what the installer pins for git. 404 behind an edge (h2c).
        .route("/services/public/ca.pem", get(ca_pem))
        // Nothing else lives on the public lane: explicit 404 so no gated route can ever be
        // reached through it by accident.
        .route(
            "/services/public/{*rest}",
            get(|| async { StatusCode::NOT_FOUND }),
        )
        .with_state(state)
}

async fn ca_pem(State(state): State<Arc<AppState>>) -> Response {
    match &state.tls {
        Some(t) => (
            [
                (header::CONTENT_TYPE, "application/x-pem-file"),
                (header::CACHE_CONTROL, "no-cache"),
                (header::CONTENT_DISPOSITION, "inline; filename=\"ca.pem\""),
            ],
            t.cert_pem.clone(),
        )
            .into_response(),
        None => (
            StatusCode::NOT_FOUND,
            "this host does not terminate TLS itself",
        )
            .into_response(),
    }
}

async fn root(State(state): State<Arc<AppState>>, req: Request<Body>) -> Response {
    let query_text = req
        .uri()
        .query()
        .is_some_and(|query| query.split('&').any(|part| part == "format=text"));
    let accept = req
        .headers()
        .get(header::ACCEPT)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default();
    if query_text || (accept.contains("text/plain") && !accept.contains("text/html")) {
        return match crate::admin::list_repos(&state, req.headers()).await {
            Ok(response) => response,
            Err(error) => error.into_response(),
        };
    }
    index(req.method(), req.headers())
}

/// SPA entry point for every page route. `no-cache` (not `no-store`): the
/// browser keeps it and revalidates with `If-None-Match`; a deploy that only
/// changes the import map costs one 304-or-tiny-200 round trip.
async fn index_route(req: Request<Body>) -> Response {
    index(req.method(), req.headers())
}

fn index(method: &Method, headers: &HeaderMap) -> Response {
    match embedded(INDEX) {
        Some(file) => embedded_response(INDEX, file, method, headers, "no-cache"),
        None => (StatusCode::INTERNAL_SERVER_ERROR, "web assets are missing").into_response(),
    }
}

#[derive(serde::Deserialize, Default)]
pub struct InstallQuery {
    /// `owner/name` — the script ends by exec'ing `git clone` of that repository.
    repo: Option<String>,
    /// Alias of `repo`.
    tree: Option<String>,
}

/// `/services/public/install.sh[?repo=owner/name]`: the ONE idempotent client setup command — token, credential helper, self-test, and with
/// `repo` the clone. Open at the app (no credential exists yet when it is fetched).
pub async fn install_sh(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<InstallQuery>,
) -> Response {
    let base_url = crate::smart::request_base_url(&state, &headers);
    let repo = q
        .repo
        .or(q.tree)
        .as_deref()
        .map(|r| r.trim_matches('/').trim_end_matches(".git").to_string())
        .filter(|r| {
            r.split_once('/')
                .is_some_and(|(o, n)| walgit_git::RepoId::new(o, n).is_ok())
        });
    (
        [
            (header::CONTENT_TYPE, "text/x-shellscript; charset=utf-8"),
            (header::CACHE_CONTROL, "public, max-age=300"),
            (
                header::CONTENT_DISPOSITION,
                "inline; filename=\"install.sh\"",
            ),
        ],
        crate::setup::install_script(&state.cfg, &base_url, repo.as_deref()),
    )
        .into_response()
}

/// `/services/setup.json[?repo=owner/name]`: the clone/setup recipes the Clone menu and
/// the API page render (`setup::Recipes`) — one source of truth for the one-liners, so the
/// UI never re-derives the token command or the OAuth client.
async fn setup_json(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    axum::extract::Query(q): axum::extract::Query<InstallQuery>,
) -> Response {
    let base_url = crate::smart::request_base_url(&state, &headers);
    let repo = q
        .repo
        .or(q.tree)
        .as_deref()
        .map(|r| r.trim_matches('/').trim_end_matches(".git").to_string())
        .filter(|r| {
            r.split_once('/')
                .is_some_and(|(o, n)| walgit_git::RepoId::new(o, n).is_ok())
        });
    (
        [(header::CACHE_CONTROL, "no-cache")],
        axum::Json(crate::setup::recipes(
            &state.cfg,
            &base_url,
            repo.as_deref(),
        )),
    )
        .into_response()
}

/// `GET|HEAD /repos.js` | `/repos.mjs` — the browser SDK (`web/sdk/`, built
/// into `web/dist/` by `pnpm run build`). Permanent URL, so `no-cache` +
/// strong `ETag` (revalidated per deploy), precompressed like every asset.
pub async fn sdk_asset(req: Request<Body>) -> Response {
    let name = req.uri().path().trim_start_matches('/');
    match embedded(name) {
        Some(file) => embedded_response(name, file, req.method(), req.headers(), "no-cache"),
        None => (
            StatusCode::NOT_FOUND,
            "sdk not built (web/dist/repos.js missing)",
        )
            .into_response(),
    }
}

/// `GET|HEAD /_ui/{path}` — embedded build output.
///
/// * `assets/*` carry a content hash in their name → `immutable` for a year.
///   Anything else (none today besides `index.html`) is `no-cache`.
/// * Strong `ETag` (build-time sha256 of the bytes) on everything,
///   `If-None-Match` → `304`.
/// * Brotli/gzip: the build emits `.br`/`.gz` siblings (max quality, once);
///   the best encoding the client accepts is served byte-for-byte with
///   `Content-Encoding` + `Vary: Accept-Encoding`. Nothing is compressed at
///   request time.
/// * `Content-Length` always; `HEAD` answered without a body.
async fn asset(AxumPath(path): AxumPath<String>, req: Request<Body>) -> Response {
    let path = path.trim_start_matches('/');
    // Never hand out the precompressed siblings directly: their identity is the
    // uncompressed asset (content negotiation picks the encoding).
    if path.ends_with(".br") || path.ends_with(".gz") {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    }
    let Some(content) = embedded(path) else {
        return (StatusCode::NOT_FOUND, "asset not found").into_response();
    };
    let cache = if path.starts_with("assets/") {
        IMMUTABLE
    } else {
        "no-cache"
    };
    embedded_response(path, content, req.method(), req.headers(), cache)
}

fn embedded_response(
    path: &str,
    file: rust_embed::EmbeddedFile,
    method: &Method,
    headers: &HeaderMap,
    cache: &'static str,
) -> Response {
    let etag = format!("\"{}\"", hex::encode(&file.metadata.sha256_hash()[..16]));
    let etag_hit = headers
        .get_all(header::IF_NONE_MATCH)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(|t| t.trim().trim_start_matches("W/"))
        .any(|t| t == "*" || t == etag);
    let mut resp = Response::new(Body::empty());
    {
        let h = resp.headers_mut();
        h.insert(header::ETAG, HeaderValue::from_str(&etag).unwrap());
        h.insert(header::CACHE_CONTROL, HeaderValue::from_static(cache));
        h.insert(header::VARY, HeaderValue::from_static("Accept-Encoding"));
        h.insert(
            header::X_CONTENT_TYPE_OPTIONS,
            HeaderValue::from_static("nosniff"),
        );
    }
    if etag_hit {
        *resp.status_mut() = StatusCode::NOT_MODIFIED;
        return resp;
    }
    let (encoding, data) = negotiate_encoding(path, headers).unwrap_or((None, file.data));
    let h = resp.headers_mut();
    h.insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static(content_type(path)),
    );
    h.insert(header::CONTENT_LENGTH, HeaderValue::from(data.len()));
    if let Some(enc) = encoding {
        h.insert(header::CONTENT_ENCODING, HeaderValue::from_static(enc));
    }
    if method != Method::HEAD {
        *resp.body_mut() = Body::from(data);
    }
    resp
}

/// Pick the best precompressed variant the client accepts (`br` > `gzip`).
/// Returns `None` when the client accepts neither or no sibling was built.
fn negotiate_encoding(
    path: &str,
    headers: &HeaderMap,
) -> Option<(Option<&'static str>, std::borrow::Cow<'static, [u8]>)> {
    let accept = headers
        .get_all(header::ACCEPT_ENCODING)
        .iter()
        .filter_map(|v| v.to_str().ok())
        .flat_map(|v| v.split(','))
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .collect::<Vec<_>>();
    let accepts = |name: &str| {
        accept.iter().any(|t| {
            let (coding, q) = t.split_once(';').map_or((*t, None), |(c, q)| (c, Some(q)));
            coding.trim().eq_ignore_ascii_case(name)
                && q.is_none_or(|q| q.trim().trim_start_matches("q=").trim() != "0")
        })
    };
    for (name, ext) in [("br", ".br"), ("gzip", ".gz")] {
        if accepts(name)
            && let Some(f) = embedded(&format!("{path}{ext}"))
        {
            return Some((Some(name), f.data));
        }
    }
    None
}

fn content_type(path: &str) -> &'static str {
    match Path::new(path).extension().and_then(|e| e.to_str()) {
        Some("css") => "text/css; charset=utf-8",
        Some("js" | "mjs") => "text/javascript; charset=utf-8",
        Some("json" | "map") => "application/json; charset=utf-8",
        Some("html") => "text/html; charset=utf-8",
        Some("svg") => "image/svg+xml",
        Some("png") => "image/png",
        Some("jpg" | "jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("ico") => "image/x-icon",
        Some("woff") => "font/woff",
        Some("woff2") => "font/woff2",
        Some("txt") => "text/plain; charset=utf-8",
        _ => "application/octet-stream",
    }
}
