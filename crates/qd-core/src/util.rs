use std::path::{Path, PathBuf};

/// Human readable byte size, e.g. `1.4 MiB`.
pub fn human_bytes(n: u64) -> String {
    const UNITS: [&str; 6] = ["B", "KiB", "MiB", "GiB", "TiB", "PiB"];
    let mut v = n as f64;
    let mut i = 0;
    while v >= 1024.0 && i < UNITS.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{} {}", n, UNITS[0])
    } else {
        format!("{v:.2} {}", UNITS[i])
    }
}

pub fn human_rate(bytes_per_sec: f64) -> String {
    format!("{}/s", human_bytes(bytes_per_sec as u64))
}

/// Default receive directory: `~/Downloads/QuicDrop`.
pub fn default_download_dir() -> PathBuf {
    dirs::download_dir()
        .or_else(dirs::home_dir)
        .unwrap_or_else(|| PathBuf::from("."))
        .join("QuicDrop")
}

/// Relative age of a unix timestamp, e.g. `3m ago`.
pub fn human_age(unix_secs: u64) -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let elapsed = now.saturating_sub(unix_secs);
    match elapsed {
        0..=59 => "just now".to_string(),
        60..=3599 => format!("{}m ago", elapsed / 60),
        3600..=86_399 => format!("{}h ago", elapsed / 3600),
        86_400..=604_799 => format!("{}d ago", elapsed / 86_400),
        _ => format!("{}w ago", elapsed / 604_800),
    }
}

/// Maximum nesting depth accepted from a peer supplied relative path.
const MAX_PATH_DEPTH: usize = 64;

/// Turn a peer supplied relative path into a path that is safe to join onto the
/// receive directory.
///
/// A receiver must never trust the sender's name: `../../.ssh/authorized_keys` is a
/// perfectly valid name to receive over the wire. Empty, `.` and `..` components are
/// dropped, so the result can only ever point inside the directory it is joined onto.
/// Returns `unnamed` when nothing usable is left.
pub fn sanitize_rel_path(name: &str) -> PathBuf {
    let mut out = PathBuf::new();
    let mut depth = 0usize;
    for raw in name.split(['/', '\\']) {
        if depth >= MAX_PATH_DEPTH {
            break;
        }
        let part = raw.trim_matches('\0').trim();
        if part.is_empty() || part == "." || part == ".." {
            continue;
        }
        let mut cleaned: String = part
            .chars()
            .filter(|c| !c.is_control() && *c != '\0')
            .collect();
        // Windows refuses these in a file name, and a drive prefix must never survive.
        if cfg!(windows) {
            cleaned = cleaned
                .chars()
                .filter(|c| !matches!(c, '<' | '>' | ':' | '"' | '|' | '?' | '*'))
                .collect();
        }
        let cleaned = cleaned.trim_start_matches('.').trim();
        if cleaned.is_empty() {
            continue;
        }
        out.push(cleaned);
        depth += 1;
    }
    if out.as_os_str().is_empty() {
        PathBuf::from("unnamed")
    } else {
        out
    }
}

/// Wire (forward slash) form of a relative path.
pub fn wire_path(path: &Path) -> String {
    path.components()
        .filter_map(|c| match c {
            std::path::Component::Normal(s) => Some(s.to_string_lossy().into_owned()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("/")
}

/// Strip any directory component and hostile characters from a peer supplied name.
pub fn sanitize_file_name(name: &str) -> String {
    sanitize_rel_path(name)
        .file_name()
        .and_then(|n| n.to_str())
        .map(|s| s.to_string())
        .unwrap_or_else(|| "unnamed".to_string())
}

/// Pick a non existing path by appending ` (n)` before the extension.
pub fn unique_path(dir: &Path, name: &str) -> PathBuf {
    let candidate = dir.join(name);
    if !candidate.exists() {
        return candidate;
    }
    let path = Path::new(name);
    let stem = path.file_stem().and_then(|s| s.to_str()).unwrap_or(name);
    let ext = path.extension().and_then(|s| s.to_str());
    for n in 1..10_000u32 {
        let candidate = match ext {
            Some(ext) => dir.join(format!("{stem} ({n}).{ext}")),
            None => dir.join(format!("{stem} ({n})")),
        };
        if !candidate.exists() {
            return candidate;
        }
    }
    dir.join(format!("{stem}-{name}"))
}

/// Best effort hostname for display purposes.
pub fn hostname() -> String {
    if let Ok(h) = std::env::var("QD_NAME") {
        if !h.trim().is_empty() {
            return h;
        }
    }
    if let Ok(h) = std::fs::read_to_string("/etc/hostname") {
        let h = h.trim();
        if !h.is_empty() {
            return h.to_string();
        }
    }
    if let Ok(h) = std::env::var("COMPUTERNAME") {
        if !h.trim().is_empty() {
            return h;
        }
    }
    std::env::var("USER").unwrap_or_else(|_| "quicdrop".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_path_traversal() {
        assert_eq!(
            sanitize_file_name("../../.ssh/authorized_keys"),
            "authorized_keys"
        );
        assert_eq!(sanitize_file_name("/etc/passwd"), "passwd");
        assert_eq!(sanitize_file_name(".."), "unnamed");
        assert_eq!(sanitize_file_name(""), "unnamed");
        assert_eq!(sanitize_file_name("a\u{0}b.txt"), "ab.txt");
    }

    #[test]
    fn keeps_plain_names() {
        assert_eq!(sanitize_file_name("holiday photo.jpg"), "holiday photo.jpg");
    }

    #[test]
    fn keeps_safe_relative_paths() {
        assert_eq!(
            sanitize_rel_path("photos/2024/a.jpg"),
            Path::new("photos").join("2024").join("a.jpg")
        );
        assert_eq!(wire_path(&sanitize_rel_path("a\\b\\c.txt")), "a/b/c.txt");
    }

    #[test]
    fn neutralises_traversal_in_relative_paths() {
        assert_eq!(
            sanitize_rel_path("../../etc/passwd"),
            Path::new("etc").join("passwd")
        );
        assert_eq!(sanitize_rel_path("/"), PathBuf::from("unnamed"));
        assert_eq!(sanitize_rel_path("a/./../b"), PathBuf::from("a").join("b"));
        assert_eq!(sanitize_rel_path(".hidden/x"), PathBuf::from("hidden").join("x"));
    }

    #[test]
    fn caps_relative_depth() {
        let deep = vec!["d"; 200].join("/");
        let path = sanitize_rel_path(&deep);
        assert_eq!(path.components().count(), MAX_PATH_DEPTH);
    }

    #[test]
    fn avoids_overwriting() {
        let dir = std::env::temp_dir().join(format!("qd-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let first = unique_path(&dir, "report.pdf");
        std::fs::write(&first, b"x").unwrap();
        let second = unique_path(&dir, "report.pdf");
        assert_eq!(second.file_name().unwrap(), "report (1).pdf");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn formats_sizes() {
        assert_eq!(human_bytes(512), "512 B");
        assert_eq!(human_bytes(1536), "1.50 KiB");
    }

    #[test]
    fn formats_ages() {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert_eq!(human_age(now), "just now");
        assert_eq!(human_age(now - 120), "2m ago");
        assert_eq!(human_age(now - 7200), "2h ago");
        assert_eq!(human_age(now - 172_800), "2d ago");
    }
}
