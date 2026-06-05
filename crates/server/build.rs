use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=../../.git/HEAD");
    println!("cargo:rerun-if-env-changed=VAYLIX_GIT_SHA");

    if let Ok(sha) = std::env::var("VAYLIX_GIT_SHA") {
        println!("cargo:rustc-env=VAYLIX_GIT_SHA={sha}");
        return;
    }

    let sha = Command::new("git")
        .args(["rev-parse", "--verify", "HEAD"])
        .output()
        .ok()
        .filter(|output| output.status.success())
        .and_then(|output| String::from_utf8(output.stdout).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "unknown".to_string());

    println!("cargo:rustc-env=VAYLIX_GIT_SHA={sha}");
}
