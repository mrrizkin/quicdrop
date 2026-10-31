//! Expand paths into the file list a sender offers: folders are walked, and each file
//! carries a relative path the receiver recreates.

use anyhow::{bail, Context, Result};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

use crate::proto::{MAX_NAME_LEN, MAX_OFFER_FILES};
use crate::util::wire_path;

const MAX_DEPTH: usize = 64;

#[derive(Debug, Clone)]
pub struct SourceFile {
    /// Absolute or user supplied path on this machine.
    pub path: PathBuf,
    /// Relative, `/` separated name announced to the receiver.
    pub name: String,
    pub size: u64,
}

/// Identity of a file on disk, used to drop duplicates. On unix this is
/// `(st_dev, st_ino)` from metadata the walk already read.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct FileKey {
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(not(unix))]
    path: PathBuf,
}

impl FileKey {
    #[cfg(unix)]
    fn of(_path: &Path, meta: &std::fs::Metadata) -> Self {
        use std::os::unix::fs::MetadataExt;
        Self {
            dev: meta.dev(),
            ino: meta.ino(),
        }
    }

    #[cfg(not(unix))]
    fn of(path: &Path, _meta: &std::fs::Metadata) -> Self {
        Self {
            path: std::fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf()),
        }
    }
}

/// Expand files and folders into a deterministic list of [`SourceFile`]s.
///
/// Directories are walked recursively without following symlinks; a folder named
/// `photos` containing `2024/a.jpg` becomes the file `photos/2024/a.jpg`.
pub fn expand(inputs: &[PathBuf]) -> Result<Vec<SourceFile>> {
    let mut out = Vec::new();
    let mut seen: HashSet<FileKey> = HashSet::new();

    for input in inputs {
        let meta =
            std::fs::metadata(input).with_context(|| format!("cannot read {}", input.display()))?;
        if meta.is_file() {
            add_file(
                &mut out,
                &mut seen,
                input.clone(),
                root_name(input, "file"),
                &meta,
            )?;
        } else if meta.is_dir() {
            let root = root_name(input, "folder");
            walk(input, Path::new(&root), 0, &mut out, &mut seen)?;
        } else {
            bail!("not a file or folder: {}", input.display());
        }
    }

    if out.is_empty() {
        bail!("nothing to send (folders were empty)");
    }
    Ok(out)
}

fn root_name(path: &Path, fallback: &str) -> String {
    path.file_name()
        .map(|n| n.to_string_lossy().into_owned())
        .filter(|n| !n.is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn walk(
    dir: &Path,
    rel: &Path,
    depth: usize,
    out: &mut Vec<SourceFile>,
    seen: &mut HashSet<FileKey>,
) -> Result<()> {
    if depth > MAX_DEPTH {
        bail!("folder nesting deeper than {MAX_DEPTH}: {}", dir.display());
    }
    let mut entries: Vec<std::fs::DirEntry> = std::fs::read_dir(dir)
        .with_context(|| format!("cannot list {}", dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()
        .with_context(|| format!("cannot list {}", dir.display()))?;
    // One key per entry, not one per comparison.
    entries.sort_by_cached_key(std::fs::DirEntry::file_name);

    for entry in entries {
        let file_type = entry
            .file_type()
            .with_context(|| format!("cannot inspect {}", entry.path().display()))?;
        let rel_child = rel.join(entry.file_name());
        if file_type.is_symlink() {
            // Never follow: a link could point outside the selected folder or loop.
            continue;
        }
        if file_type.is_dir() {
            walk(&entry.path(), &rel_child, depth + 1, out, seen)?;
        } else if file_type.is_file() {
            let meta = entry
                .metadata()
                .with_context(|| format!("cannot inspect {}", entry.path().display()))?;
            add_file(out, seen, entry.path(), wire_path(&rel_child), &meta)?;
        }
    }
    Ok(())
}

fn add_file(
    out: &mut Vec<SourceFile>,
    seen: &mut HashSet<FileKey>,
    path: PathBuf,
    name: String,
    meta: &std::fs::Metadata,
) -> Result<()> {
    if !seen.insert(FileKey::of(&path, meta)) {
        // The same file was selected twice (e.g. a folder and one of its files).
        return Ok(());
    }
    if name.len() > MAX_NAME_LEN {
        bail!(
            "path is {} bytes, limit is {MAX_NAME_LEN}: {}",
            name.len(),
            path.display()
        );
    }
    if out.len() >= MAX_OFFER_FILES {
        bail!("too many files (limit {MAX_OFFER_FILES})");
    }
    out.push(SourceFile {
        path,
        name,
        size: meta.len(),
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "qd-source-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn expands_folder_recursively() {
        let dir = temp_dir("tree");
        let tree = dir.join("photos");
        std::fs::create_dir_all(tree.join("2024/raw")).unwrap();
        std::fs::write(tree.join("a.jpg"), b"a").unwrap();
        std::fs::write(tree.join("2024/b.jpg"), b"bb").unwrap();
        std::fs::write(tree.join("2024/raw/c.jpg"), b"ccc").unwrap();

        let files = expand(&[tree]).unwrap();
        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            vec!["photos/2024/b.jpg", "photos/2024/raw/c.jpg", "photos/a.jpg"]
        );
        assert_eq!(files.iter().map(|f| f.size).sum::<u64>(), 6);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn mixes_files_and_folders_and_dedupes() {
        let dir = temp_dir("mixed");
        std::fs::write(dir.join("loose.txt"), b"x").unwrap();
        let folder = dir.join("folder");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("inner.txt"), b"y").unwrap();

        let files = expand(&[
            dir.join("loose.txt"),
            folder.clone(),
            folder.join("inner.txt"),
        ])
        .unwrap();
        let names: Vec<&str> = files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(names, vec!["loose.txt", "folder/inner.txt"]);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn rejects_empty_folder() {
        let dir = temp_dir("empty");
        let err = expand(std::slice::from_ref(&dir)).unwrap_err().to_string();
        assert!(err.contains("nothing to send"), "{err}");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn skips_symlinks() {
        let dir = temp_dir("link");
        let folder = dir.join("d");
        std::fs::create_dir_all(&folder).unwrap();
        std::fs::write(folder.join("real.txt"), b"x").unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(dir.join("d"), folder.join("loop")).unwrap();

        let files = expand(&[folder]).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].name, "d/real.txt");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
