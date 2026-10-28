use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::process::Command;
use std::sync::{OnceLock, RwLock};

fn cache() -> &'static RwLock<Vec<(OsString, OsString)>> {
    static CACHE: OnceLock<RwLock<Vec<(OsString, OsString)>>> = OnceLock::new();
    CACHE.get_or_init(|| RwLock::new(filtered(std::env::vars_os())))
}

fn filtered<I>(environment: I) -> Vec<(OsString, OsString)>
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    environment
        .into_iter()
        .filter(|(key, _)| !is_reserved(key))
        .collect()
}

pub(crate) fn initialize() {
    replace(std::env::vars_os());
}

pub(crate) fn refresh_from_map(environment: &HashMap<String, String>) {
    // A restore supplies new control values, not a replacement application
    // environment. Publish one merged snapshot to concurrent command readers.
    // A poisoned cache is an invariant failure, as in snapshot and replace.
    let mut cached = cache().write().expect("child environment cache poisoned");
    *cached = restored_environment(&cached, environment);
}

fn restored_environment(
    original: &[(OsString, OsString)],
    environment: &HashMap<String, String>,
) -> Vec<(OsString, OsString)> {
    let mut restored: HashMap<OsString, OsString> =
        filtered(original.iter().cloned()).into_iter().collect();
    restored.extend(filtered(
        environment
            .iter()
            .map(|(key, value)| (OsString::from(key), OsString::from(value))),
    ));
    restored.into_iter().collect()
}

fn replace<I>(environment: I)
where
    I: IntoIterator<Item = (OsString, OsString)>,
{
    *cache().write().expect("child environment cache poisoned") = filtered(environment);
}

pub(crate) fn snapshot() -> Vec<(OsString, OsString)> {
    cache()
        .read()
        .expect("child environment cache poisoned")
        .clone()
}

pub(crate) fn apply(command: &mut Command) {
    command.env_clear();
    command.envs(snapshot());
}

pub(crate) fn apply_tokio(command: &mut tokio::process::Command) {
    command.env_clear();
    command.envs(snapshot());
}

pub(crate) fn apply_pty(command: &mut portable_pty::CommandBuilder) {
    command.env_clear();
    for (key, value) in snapshot() {
        command.env(key, value);
    }
}

pub(crate) fn is_reserved(key: &OsStr) -> bool {
    key.to_string_lossy().starts_with("ADX_")
}

pub(crate) fn validate_override(key: &str) -> Result<(), String> {
    if is_reserved(OsStr::new(key)) {
        return Err(format!(
            "environment variable {key} uses the reserved ADX_ prefix"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filters_reserved_environment() {
        let values = filtered([
            (OsString::from("PATH"), OsString::from("/bin")),
            (
                OsString::from("ADX_IMAGE_PROCESS_CONFIG"),
                OsString::from("/tmp/spec"),
            ),
            (OsString::from("USER_VALUE"), OsString::from("ok")),
        ]);
        assert_eq!(values.len(), 2);
        assert!(values.iter().all(|(key, _)| !is_reserved(key)));
    }

    #[test]
    fn rejects_reserved_user_override() {
        assert!(validate_override("ADX_INTERNAL").is_err());
        assert!(validate_override("USER_VALUE").is_ok());
    }
}

#[cfg(test)]
mod restoration_environment_contract {
    use super::*;

    #[test]
    fn restore_keeps_original_image_home_and_path_when_update_omits_them() {
        let original = vec![
            (OsString::from("HOME"), OsString::from("/original-home")),
            (OsString::from("PATH"), OsString::from("/original-bin:/bin")),
            (
                OsString::from("EXECD_HTTP_TOKEN"),
                OsString::from("synthetic-old-control"),
            ),
        ];
        let update = HashMap::from([
            ("ADX_RUNTIME_ID".to_owned(), "target-runtime".to_owned()),
            (
                "EXECD_HTTP_TOKEN".to_owned(),
                "synthetic-new-control".to_owned(),
            ),
        ]);
        let observed: HashMap<_, _> = restored_environment(&original, &update)
            .into_iter()
            .collect();
        assert_eq!(
            observed.get(OsStr::new("HOME")),
            Some(&OsString::from("/original-home"))
        );
        assert_eq!(
            observed.get(OsStr::new("PATH")),
            Some(&OsString::from("/original-bin:/bin"))
        );
        assert_eq!(
            observed.get(OsStr::new("EXECD_HTTP_TOKEN")),
            Some(&OsString::from("synthetic-new-control"))
        );
        assert!(!observed.contains_key(OsStr::new("ADX_RUNTIME_ID")));
    }

    #[test]
    fn restore_replaces_explicit_values_including_empty_without_dropping_other_keys() {
        let original = vec![
            (OsString::from("HOME"), OsString::from("/original-home")),
            (OsString::from("KEEP_IMAGE_VALUE"), OsString::from("kept")),
        ];
        let update = HashMap::from([
            ("HOME".to_owned(), "".to_owned()),
            ("PATH".to_owned(), "/new-bin".to_owned()),
        ]);
        let observed: HashMap<_, _> = restored_environment(&original, &update)
            .into_iter()
            .collect();
        assert_eq!(observed.get(OsStr::new("HOME")), Some(&OsString::new()));
        assert_eq!(
            observed.get(OsStr::new("PATH")),
            Some(&OsString::from("/new-bin"))
        );
        assert_eq!(
            observed.get(OsStr::new("KEEP_IMAGE_VALUE")),
            Some(&OsString::from("kept"))
        );
    }

    #[test]
    fn repeated_restore_keeps_original_context_and_filters_reserved_keys() {
        let original = vec![
            (OsString::from("HOME"), OsString::from("/original-home")),
            (
                OsString::from("ADX_ORIGIN"),
                OsString::from("source-runtime"),
            ),
        ];
        let update = HashMap::from([
            ("ADX_RUNTIME_ID".to_owned(), "target-runtime".to_owned()),
            ("VISIBLE".to_owned(), "new".to_owned()),
        ]);
        let first = restored_environment(&original, &update);
        let observed: HashMap<_, _> = restored_environment(&first, &update).into_iter().collect();
        assert_eq!(
            observed.get(OsStr::new("HOME")),
            Some(&OsString::from("/original-home"))
        );
        assert_eq!(
            observed.get(OsStr::new("VISIBLE")),
            Some(&OsString::from("new"))
        );
        assert!(!observed.contains_key(OsStr::new("ADX_ORIGIN")));
        assert!(!observed.contains_key(OsStr::new("ADX_RUNTIME_ID")));
    }
}
