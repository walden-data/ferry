use std::collections::HashMap;
use std::path::Path;

use crate::error::FerryError;

/// Secrets loaded from `secrets.toml`.
///
/// The TOML format uses sections like `[source.duckdb]` or `[destination.braze]`
/// with key-value pairs inside each section.
#[derive(Debug)]
pub struct Secrets {
    sections: HashMap<String, HashMap<String, String>>,
}

impl Secrets {
    /// Load secrets from a TOML file.
    ///
    /// Returns `None` if the file does not exist.
    /// On Unix, refuses to read if file permissions are not `0o600`.
    pub fn load(path: &Path) -> Result<Option<Self>, FerryError> {
        if !path.exists() {
            return Ok(None);
        }

        // Check permissions on Unix — must be 0600
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = std::fs::metadata(path).map_err(|e| {
                FerryError::Config(format!("Cannot read metadata for secrets file: {e}"))
            })?;
            let mode = metadata.permissions().mode() & 0o777;
            if mode != 0o600 {
                return Err(FerryError::Config(format!(
                    "secrets.toml has insecure permissions {mode:o}; must be 600"
                )));
            }
        }

        let content = std::fs::read_to_string(path)
            .map_err(|e| FerryError::Config(format!("Cannot read secrets file: {e}")))?;

        // Parse as toml::Table first, then convert to our flat section/key structure
        let table: toml::Table = toml::from_str(&content)
            .map_err(|e| FerryError::Config(format!("Cannot parse secrets.toml: {e}")))?;

        let mut sections: HashMap<String, HashMap<String, String>> = HashMap::new();

        // TOML [source.duckdb] creates nested tables: {source: {duckdb: {path: ...}}}
        // We flatten these to "source.duckdb" as the section key
        for (top_key, top_value) in &table {
            if let Some(sub_table) = top_value.as_table() {
                for (sub_key, sub_value) in sub_table {
                    if let Some(inner_table) = sub_value.as_table() {
                        let section_name = format!("{}.{}", top_key, sub_key);
                        let mut section_map: HashMap<String, String> = HashMap::new();
                        for (key, value) in inner_table {
                            if let Some(s) = value.as_str() {
                                section_map.insert(key.clone(), s.to_string());
                            }
                        }
                        sections.insert(section_name, section_map);
                    }
                }
            }
        }

        Ok(Some(Secrets { sections }))
    }

    /// Resolve a secret value by section and key.
    ///
    /// Sections are formatted as `source.duckdb` or `destination.braze`.
    /// Returns `None` if the section or key is not found.
    pub fn resolve(&self, section: &str, key: &str) -> Option<String> {
        self.sections
            .get(section)
            .and_then(|section_map| section_map.get(key))
            .cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    #[test]
    fn test_load_secrets() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file_mut()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        write!(
            file,
            r#"
[source.duckdb]
path = "/data/warehouse.db"

[destination.braze]
api_key = "abc123"
endpoint = "https://rest.braze.com"
"#
        )
        .unwrap();

        let secrets = Secrets::load(file.path())
            .unwrap()
            .expect("Expected secrets");
        assert_eq!(
            secrets.resolve("source.duckdb", "path"),
            Some("/data/warehouse.db".to_string())
        );
        assert_eq!(
            secrets.resolve("destination.braze", "api_key"),
            Some("abc123".to_string())
        );
        assert_eq!(
            secrets.resolve("destination.braze", "endpoint"),
            Some("https://rest.braze.com".to_string())
        );
    }

    #[test]
    fn test_load_missing_file() {
        let path = Path::new("/tmp/ferry_test_nonexistent_secrets_xyz.toml");
        // Ensure it doesn't exist
        let _ = std::fs::remove_file(path);
        let result = Secrets::load(path).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_resolve_secret() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file_mut()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        write!(
            file,
            r#"
[source.duckdb]
password = "s3cret"
"#
        )
        .unwrap();

        let secrets = Secrets::load(file.path())
            .unwrap()
            .expect("Expected secrets");
        assert_eq!(
            secrets.resolve("source.duckdb", "password"),
            Some("s3cret".to_string())
        );
    }

    #[test]
    fn test_resolve_missing_secret() {
        let mut file = tempfile::NamedTempFile::new().unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            file.as_file_mut()
                .set_permissions(std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        write!(
            file,
            r#"
[source.duckdb]
path = "/data/db.duckdb"
"#
        )
        .unwrap();

        let secrets = Secrets::load(file.path())
            .unwrap()
            .expect("Expected secrets");
        assert_eq!(secrets.resolve("source.duckdb", "nonexistent"), None);
        assert_eq!(secrets.resolve("nonexistent.section", "key"), None);
    }

    #[test]
    fn test_load_secrets_bad_permissions() {
        // Only meaningful on Unix
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mut file = tempfile::NamedTempFile::new().unwrap();
            // Set permissions to 644 (too permissive)
            file.as_file_mut()
                .set_permissions(std::fs::Permissions::from_mode(0o644))
                .unwrap();
            write!(
                file,
                r#"[source.duckdb]
key = "val""#
            )
            .unwrap();

            let result = Secrets::load(file.path());
            assert!(result.is_err());
            let err = result.unwrap_err();
            match err {
                FerryError::Config(msg) => {
                    assert!(msg.contains("insecure permissions") || msg.contains("600"));
                }
                _ => panic!("Expected Config error, got {:?}", err),
            }
        }
    }
}
