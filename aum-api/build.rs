//! Build script for aum-api: optionally bundles the Svelte frontend.

fn main() {
    #[cfg(feature = "bundle-frontend")]
    bundle_frontend();
}

#[cfg(feature = "bundle-frontend")]
fn bundle_frontend() {
    use std::process::Command;

    let frontend_dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../frontend");

    // Rerun if frontend sources change.
    println!(
        "cargo:rerun-if-changed={}",
        frontend_dir.join("src").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        frontend_dir.join("package.json").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        frontend_dir.join("vite.config.ts").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        frontend_dir.join("svelte.config.js").display()
    );
    println!(
        "cargo:rerun-if-changed={}",
        frontend_dir.join("index.html").display()
    );

    let npm = if cfg!(windows) { "npm.cmd" } else { "npm" };

    let status = Command::new(npm)
        .args(["ci"])
        .current_dir(&frontend_dir)
        .status()
        .expect("failed to run npm ci");
    assert!(status.success(), "npm ci failed");

    let status = Command::new(npm)
        .args(["run", "build"])
        .current_dir(&frontend_dir)
        .status()
        .expect("failed to run npm run build");
    assert!(status.success(), "npm run build failed");
}
