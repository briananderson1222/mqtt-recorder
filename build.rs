fn main() {
    // Short git hash for --version; "unknown" when building outside a git
    // checkout (e.g. from a source tarball).
    let hash = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_HASH={}", hash);
    println!("cargo:rerun-if-changed=.git/HEAD");
    // .git/HEAD only changes on branch switches; commits move the branch ref,
    // so track that file too or incremental rebuilds keep a stale hash.
    if let Ok(head) = std::fs::read_to_string(".git/HEAD") {
        if let Some(reference) = head.trim().strip_prefix("ref: ") {
            println!("cargo:rerun-if-changed=.git/{}", reference);
        }
    }
}
