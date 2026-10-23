//! Embeds the source commit for `rusticmq_build_info`. Docker builds have no
//! `.git` in their context and pass `RUSTICMQ_COMMIT` as a build argument.
use std::{fs, path::Path, process::Command};

fn main() {
    println!("cargo:rerun-if-env-changed=RUSTICMQ_COMMIT");
    let commit = std::env::var("RUSTICMQ_COMMIT")
        .ok()
        .filter(|commit| !commit.is_empty())
        .or_else(git_commit)
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=RUSTICMQ_COMMIT={commit}");
}

fn git_commit() -> Option<String> {
    let head = Path::new(".git/HEAD");
    if !head.exists() {
        return None;
    }
    // Rerun when HEAD moves to another branch or its branch gets a new commit.
    // Cargo treats a missing watched path as always changed, so skip those.
    let mut watched = vec![".git/HEAD".to_owned(), ".git/packed-refs".to_owned()];
    if let Some(reference) = fs::read_to_string(head).ok()?.strip_prefix("ref: ") {
        watched.push(format!(".git/{}", reference.trim()));
    }
    for path in watched.iter().filter(|path| Path::new(path).exists()) {
        println!("cargo:rerun-if-changed={path}");
    }
    let output = Command::new("git")
        .args(["rev-parse", "--short=7", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())?;
    Some(String::from_utf8(output.stdout).ok()?.trim().to_owned())
}
