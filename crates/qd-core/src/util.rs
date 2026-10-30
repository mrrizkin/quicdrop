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

/// Strip any directory component and hostile characters from a peer supplied name.
///
/// A receiver must never trust the sender's filename: `../../.ssh/authorized_keys` is a
/// perfectly valid name to receive over the wire.
pub fn sanitize_file_name(name: &str) -> String {
    let base = name
        .rsplit(['/', '\\'])
        .next()
        .unwrap_or("file")
        .trim_matches('\0')
        .trim();
    let cleaned: String = base
        .chars()
        .filter(|c| !c.is_control() && *c != '\0')
        .collect();
    let cleaned = cleaned.trim_start_matches('.').trim();
    if cleaned.is_empty() {
        "unnamed".to_string()
    } else {
        cleaned.to_string()
    }
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
}
