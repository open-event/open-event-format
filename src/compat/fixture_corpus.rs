use crate::error::FormatError;
use crate::layout::reader::OeFile;
use std::path::{Path, PathBuf};

pub const FIXTURE_CORPUS_DIR: &str = "test_data/oe-fixtures";

pub const FIXTURE_WRITER_VERSION: (u16, u16) = (1, 0);

#[derive(Debug)]
pub struct FixtureSweep {
    pub failures: Vec<(String, FormatError)>,
    pub opened: Vec<String>,
}

pub fn read_fixture_corpus(dir: &Path) -> std::io::Result<FixtureSweep> {
    let mut sweep = FixtureSweep {
        failures: Vec::new(),
        opened: Vec::new(),
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(sweep),
        Err(error) => return Err(error),
    };
    let mut paths: Vec<PathBuf> = entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.extension().is_some_and(|extension| extension == "oe"))
        .collect();
    paths.sort();
    for path in paths {
        let name = path
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .unwrap_or_default();
        let bytes = std::fs::read(&path)?;
        match OeFile::open(bytes, None) {
            Ok(_) => sweep.opened.push(name),
            Err(error) => sweep.failures.push((name, error)),
        }
    }
    Ok(sweep)
}

#[cfg(feature = "write")]
pub fn generate_fixture(dir: &Path, label: &str, built: &crate::writer::build::BuiltOe) -> std::io::Result<PathBuf> {
    if built.footer.format_version != FIXTURE_WRITER_VERSION {
        return Err(std::io::Error::other(
            "fixture generator writer version disagrees with the built file's declared format version",
        ));
    }
    std::fs::create_dir_all(dir)?;
    let (major, minor) = FIXTURE_WRITER_VERSION;
    let path = dir.join(format!("oe-v{major}.{minor}-{label}.oe"));
    std::fs::write(&path, &built.bytes)?;
    Ok(path)
}

#[cfg(all(test, feature = "write"))]
#[path = "test/fixture_corpus.rs"]
mod tests;
