//! Records the git commit the benchmark binary is built from, so every report can name the code it measured.

use std::process::Command;

fn main() {
    let git = |args: &[&str]| {
        Command::new("git")
            .args(args)
            .output()
            .ok()
            .filter(|output| output.status.success())
            .and_then(|output| String::from_utf8(output.stdout).ok())
            .map(|text| text.trim().to_owned())
    };
    // Rebuild the stamp when the checked-out commit, its branch, or the index moves. A tree without git — a source
    // archive — leaves the commit unset rather than failing the build.
    let mut watched: Vec<String> = ["HEAD", "index"]
        .iter()
        .filter_map(|path| git(&["rev-parse", "--git-path", path]))
        .collect();
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"]).and_then(|name| git(&["rev-parse", "--git-path", &name]))
    {
        watched.push(branch);
    }
    for path in watched {
        println!("cargo:rerun-if-changed={path}");
    }
    if let Some(commit) = git(&["describe", "--always", "--dirty", "--abbrev=12", "--exclude=*"]) {
        println!("cargo:rustc-env=OE_BENCHMARK_COMMIT={commit}");
    }
}
