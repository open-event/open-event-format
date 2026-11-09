
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
