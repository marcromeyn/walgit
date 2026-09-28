// Test fixtures use panics to fail the test, including shared helper functions.
#![allow(clippy::unwrap_used, clippy::indexing_slicing)]

//! A consumer resumes the WAL ref stream on a fresh server process and sees an
//! explicit gap after checkpoint retention advances past its cursor.
mod harness;

use harness::{Server, TestRepo, git_in};
use serde_json::Value;

async fn page(server: &Server, after: u64) -> anyhow::Result<(reqwest::StatusCode, Value)> {
    let response = reqwest::Client::new()
        .get(format!(
            "{}/t/r/api/ref-events?family=refs%2Fv1&after={after}",
            server.base_url
        ))
        .send()
        .await?;
    let status = response.status();
    anyhow::ensure!(
        response
            .headers()
            .get("cache-control")
            .and_then(|v| v.to_str().ok())
            == Some("no-store"),
        "cursor responses must not be cached"
    );
    Ok((status, response.json().await?))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn resumes_after_restart_without_gap_or_replay_then_reports_retention_gap()
-> anyhow::Result<()> {
    let first = Server::start().await?;
    first.put_repo("t", "r").await?;
    let src = TestRepo::synthetic(1, 1)?;
    git_in(&src, &["branch", "-M", "main"])?;
    git_in(
        &src,
        &["remote", "add", "origin", &first.repo_url("t", "r")],
    )?;
    git_in(&src, &["push", "-u", "origin", "main"])?;

    let (status, initial) = page(&first, 0).await?;
    assert_eq!(status, 200);
    assert_eq!(initial["cursor"]["family"], "refs/v1");
    assert_eq!(initial["cursor"]["seq"], "1");
    assert_eq!(initial["events"].as_array().unwrap().len(), 1);
    assert_eq!(initial["events"][0]["_walgit"]["seq"], "1");
    let (status, empty) = page(&first, 1).await?;
    assert_eq!(status, 200);
    assert_eq!(empty["events"].as_array().unwrap().len(), 0);

    // A distinct server with a fresh registry and cache serves the same WAL.
    let restarted = first.start_sibling_with(|_| {}).await?;
    drop(first);
    git_in(
        &src,
        &["remote", "set-url", "origin", &restarted.repo_url("t", "r")],
    )?;
    git_in(&src, &["commit", "--allow-empty", "-m", "second"])?;
    git_in(&src, &["push", "origin", "main"])?;
    let (status, resumed) = page(&restarted, 1).await?;
    assert_eq!(status, 200);
    assert_eq!(resumed["cursor"]["seq"], "2");
    assert_eq!(resumed["events"].as_array().unwrap().len(), 1);
    assert_eq!(resumed["events"][0]["_walgit"]["seq"], "2");
    assert_eq!(
        page(&restarted, 2).await?.1["events"]
            .as_array()
            .unwrap()
            .len(),
        0
    );
    let (status, ahead) = page(&restarted, 3).await?;
    assert_eq!(status, 409, "a future cursor cannot silently resume");
    assert_eq!(ahead["code"], "WAL_GAP");

    // The normal WAL checkpoint folds the entries. The route must refuse an
    // old cursor, never return an empty successful page or skip the loss.
    let id = walgit_git::RepoId::new("t", "r")?;
    let handle = restarted.state.registry.open(&id).await?;
    handle.write_checkpoint().await?;
    let (status, gap) = page(&restarted, 0).await?;
    assert_eq!(status, 409);
    assert_eq!(gap["code"], "WAL_GAP");
    assert_eq!(gap["family"], "refs/v1");
    assert_eq!(gap["requested"], "0");
    assert_eq!(gap["oldest"], "3");
    assert_eq!(gap["head"], "2");
    Ok(())
}
