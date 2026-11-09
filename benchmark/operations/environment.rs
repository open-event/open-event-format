
use crate::allocation::counting_enabled;
use crate::model::Environment;
use std::process::Command;

pub fn capture() -> Environment {
    let mut controls: Vec<String> = std::env::vars()
        .filter(|(name, _)| name.starts_with("OE_BENCH_"))
        .map(|(name, value)| format!("{name}={value}"))
        .collect();
    controls.sort();
    Environment {
        alloc_counting: counting_enabled(),
        arch: std::env::consts::ARCH.to_owned(),
        commit: option_env!("OE_BENCHMARK_COMMIT").map(str::to_owned),
        controls,
        cpu: cpu_model(),
        hostname: first_line("hostname", &[]),
        kernel: first_line("uname", &["-sr"]),
        os: std::env::consts::OS.to_owned(),
    }
}

fn cpu_model() -> Option<String> {
    if cfg!(target_os = "macos") {
        return first_line("sysctl", &["-n", "machdep.cpu.brand_string"]);
    }
    let cpuinfo = std::fs::read_to_string("/proc/cpuinfo").ok()?;
    cpuinfo
        .lines()
        .find_map(|line| line.strip_prefix("model name"))
        .and_then(|rest| rest.split_once(':'))
        .map(|(_, name)| name.trim().to_owned())
}

fn first_line(program: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(program).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    let text = String::from_utf8(output.stdout).ok()?;
    let line = text.lines().next()?.trim();
    (!line.is_empty()).then(|| line.to_owned())
}
