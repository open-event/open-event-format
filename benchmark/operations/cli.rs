
use crate::model::{BenchmarkFormat, Operation};
use crate::run::BenchmarkSelection;
use anyhow::{Result, anyhow, bail};
use std::path::PathBuf;
use std::str::FromStr;

pub const USAGE: &str = "usage: oe-vs-parquet <work-dir> [spec.toml] [--toml] [--format <name>]... \
    [--operation <name>]... [--iterations <count>] [--phases] [--no-check-answers] \
    [--memory-baseline <report.toml> --max-memory-regression-percent <percent>] \
    [--results-file <path.jsonl>]";

#[derive(Clone, Debug, PartialEq)]
pub struct CliArgs {
    pub as_toml: bool,
    pub dir: PathBuf,
    pub iterations: Option<usize>,
    pub max_memory_regression_percent: Option<f64>,
    pub memory_baseline: Option<PathBuf>,
    pub results_file: Option<PathBuf>,
    pub selection: BenchmarkSelection,
    pub spec_path: Option<PathBuf>,
}

pub fn parse_args(args: impl IntoIterator<Item = String>) -> Result<CliArgs> {
    let mut args = args.into_iter();
    let mut as_toml = false;
    let mut formats = Vec::new();
    let mut iterations = None;
    let mut max_memory_regression_percent = None;
    let mut memory_baseline = None;
    let mut operations = Vec::new();
    let mut phase_timings = false;
    let mut check_answers = true;
    let mut results_file = None;
    let mut positional = Vec::new();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--toml" => as_toml = true,
            "--phases" => phase_timings = true,
            "--no-check-answers" => check_answers = false,
            "--memory-baseline" => {
                memory_baseline = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--memory-baseline needs a value\n{USAGE}"))?,
                ));
            }
            "--max-memory-regression-percent" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--max-memory-regression-percent needs a value\n{USAGE}"))?;
                max_memory_regression_percent = Some(parse_non_negative_percent(&value)?);
            }
            "--results-file" => {
                results_file = Some(PathBuf::from(
                    args.next()
                        .ok_or_else(|| anyhow!("--results-file needs a value\n{USAGE}"))?,
                ));
            }
            "--format" => {
                let value = args.next().ok_or_else(|| anyhow!("--format needs a value\n{USAGE}"))?;
                push_unique(&mut formats, BenchmarkFormat::from_str(&value)?);
            }
            "--operation" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--operation needs a value\n{USAGE}"))?;
                push_unique(&mut operations, Operation::from_str(&value)?);
            }
            "--iterations" => {
                let value = args
                    .next()
                    .ok_or_else(|| anyhow!("--iterations needs a value\n{USAGE}"))?;
                iterations = Some(parse_iterations(&value)?);
            }
            _ if arg.starts_with("--format=") => {
                let value = arg.trim_start_matches("--format=");
                push_unique(&mut formats, BenchmarkFormat::from_str(value)?);
            }
            _ if arg.starts_with("--operation=") => {
                let value = arg.trim_start_matches("--operation=");
                push_unique(&mut operations, Operation::from_str(value)?);
            }
            _ if arg.starts_with("--iterations=") => {
                iterations = Some(parse_iterations(arg.trim_start_matches("--iterations="))?);
            }
            _ if arg.starts_with("--memory-baseline=") => {
                memory_baseline = Some(PathBuf::from(arg.trim_start_matches("--memory-baseline=")));
            }
            _ if arg.starts_with("--max-memory-regression-percent=") => {
                max_memory_regression_percent = Some(parse_non_negative_percent(
                    arg.trim_start_matches("--max-memory-regression-percent="),
                )?);
            }
            _ if arg.starts_with("--results-file=") => {
                results_file = Some(PathBuf::from(arg.trim_start_matches("--results-file=")));
            }
            _ if arg.starts_with('-') => bail!("unknown option `{arg}`\n{USAGE}"),
            _ => positional.push(PathBuf::from(arg)),
        }
    }

    if positional.len() > 2 {
        bail!(USAGE);
    }
    let dir = positional.first().cloned().ok_or_else(|| anyhow!(USAGE))?;
    let spec_path = positional.get(1).cloned();
    if memory_baseline.is_some() != max_memory_regression_percent.is_some() {
        bail!("--memory-baseline and --max-memory-regression-percent must be supplied together\n{USAGE}");
    }
    Ok(CliArgs {
        as_toml,
        dir,
        iterations,
        max_memory_regression_percent,
        memory_baseline,
        results_file,
        selection: BenchmarkSelection {
            check_answers,
            formats: if formats.is_empty() {
                BenchmarkSelection::default().formats
            } else {
                formats
            },
            operations: if operations.is_empty() {
                BenchmarkSelection::default().operations
            } else {
                operations
            },
            phase_timings,
        },
        spec_path,
    })
}

fn parse_non_negative_percent(value: &str) -> Result<f64> {
    let percent: f64 = value
        .parse()
        .map_err(|_| anyhow!("invalid memory regression percent `{value}`"))?;
    if !percent.is_finite() || percent < 0.0 {
        bail!("memory regression percent must be finite and non-negative");
    }
    Ok(percent)
}

fn parse_iterations(value: &str) -> Result<usize> {
    value.parse().map_err(|_| anyhow!("invalid iteration count `{value}`"))
}

fn push_unique<T: Eq>(values: &mut Vec<T>, value: T) {
    if !values.contains(&value) {
        values.push(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_filters_and_equals_forms_are_accepted() {
        let parsed = parse_args([
            "work".to_owned(),
            "spec.toml".to_owned(),
            "--format".to_owned(),
            "oe".to_owned(),
            "--format=parquet".to_owned(),
            "--operation=create_write".to_owned(),
            "--operation".to_owned(),
            "update_rows".to_owned(),
            "--phases".to_owned(),
            "--toml".to_owned(),
            "--no-check-answers".to_owned(),
            "--memory-baseline=baseline.toml".to_owned(),
            "--max-memory-regression-percent".to_owned(),
            "5".to_owned(),
            "--iterations".to_owned(),
            "50".to_owned(),
            "--results-file".to_owned(),
            "results.jsonl".to_owned(),
        ])
        .unwrap();
        assert_eq!(parsed.dir, PathBuf::from("work"));
        assert_eq!(parsed.iterations, Some(50));
        assert_eq!(parsed.spec_path, Some(PathBuf::from("spec.toml")));
        assert_eq!(
            parsed.selection.formats,
            vec![BenchmarkFormat::Oe, BenchmarkFormat::Parquet]
        );
        assert_eq!(
            parsed.selection.operations,
            vec![Operation::CreateWrite, Operation::UpdateRows]
        );
        assert!(parsed.selection.phase_timings);
        assert!(!parsed.selection.check_answers);
        assert!(parsed.as_toml);
        assert_eq!(parsed.memory_baseline, Some(PathBuf::from("baseline.toml")));
        assert_eq!(parsed.max_memory_regression_percent, Some(5.0));
        assert_eq!(parsed.results_file, Some(PathBuf::from("results.jsonl")));
    }

    #[test]
    fn iterations_default_to_the_spec_and_take_the_equals_form() {
        assert_eq!(parse_args(["work".to_owned()]).unwrap().iterations, None);
        assert_eq!(
            parse_args(["work".to_owned(), "--iterations=7".to_owned()])
                .unwrap()
                .iterations,
            Some(7)
        );
    }

    #[test]
    fn results_file_takes_the_equals_form() {
        assert_eq!(parse_args(["work".to_owned()]).unwrap().results_file, None);
        assert_eq!(
            parse_args(["work".to_owned(), "--results-file=results.jsonl".to_owned()])
                .unwrap()
                .results_file,
            Some(PathBuf::from("results.jsonl"))
        );
    }

    #[test]
    fn unknown_and_unvalued_filters_are_rejected() {
        assert!(parse_args(["work".to_owned(), "--format".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--iterations".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--iterations=many".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--operation=nope".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--unknown".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--results-file".to_owned()]).is_err());
        assert!(parse_args(["work".to_owned(), "--memory-baseline=baseline.toml".to_owned()]).is_err());
        assert!(
            parse_args([
                "work".to_owned(),
                "--memory-baseline=baseline.toml".to_owned(),
                "--max-memory-regression-percent=-1".to_owned(),
            ])
            .is_err()
        );
    }
}
