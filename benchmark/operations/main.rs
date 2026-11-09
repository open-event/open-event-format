
use anyhow::{Context, Result};
use oe_operations_benchmark::cli::parse_args;
use oe_operations_benchmark::{
    BenchmarkReport, BenchmarkSpec, append_run, check_memory_regressions, compare_selected, render_table,
};

fn main() -> Result<()> {
    let args = parse_args(std::env::args().skip(1))?;
    let mut spec = match args.spec_path.as_ref() {
        Some(path) => {
            let text =
                std::fs::read_to_string(path).with_context(|| format!("reading the spec at {}", path.display()))?;
            toml::from_str::<BenchmarkSpec>(&text).with_context(|| format!("parsing the spec at {}", path.display()))?
        }
        None => BenchmarkSpec::default(),
    };
    if let Some(iterations) = args.iterations {
        spec.iterations = iterations;
    }

    std::fs::create_dir_all(&args.dir).with_context(|| format!("creating {}", args.dir.display()))?;
    let report = compare_selected(&spec, &args.dir, &args.selection)?;

    if let Some(path) = args.results_file.as_ref() {
        append_run(&report, path).with_context(|| format!("appending the run to {}", path.display()))?;
    }

    if let (Some(path), Some(percent)) = (args.memory_baseline.as_ref(), args.max_memory_regression_percent) {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading the memory baseline at {}", path.display()))?;
        let baseline: BenchmarkReport =
            toml::from_str(&text).with_context(|| format!("parsing the memory baseline at {}", path.display()))?;
        check_memory_regressions(&baseline, &report, percent / 100.0)?;
    }

    if args.as_toml {
        println!("{}", toml::to_string_pretty(&report).context("rendering the report")?);
    } else {
        print!("{}", render_table(&report));
    }
    Ok(())
}
