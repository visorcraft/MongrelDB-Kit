//! Key encoding for Kit indexes, guards, and primary keys.
//!
//! # Format
//!
//! Each encoded key is a colon-separated (`:`) string of typed components.
//! A component is one of:
//!
//! * `s:<text>`  – a string value. Colons and backslashes in `<text>` are
//!   escaped as `\:` and `\\` respectively.
//! * `n:null`    – an explicit SQL `NULL` marker.
//!
//! `encode_pk` joins components directly with `:` and is suitable for use as
//! a primary-key literal inside other keys. `encode_unique_key` prefixes the
//! components with `uq:<version>:<constraint_name>:` so the resulting string
//! is globally unique per constraint. `encode_row_guard_key` prefixes an
//! already-encoded primary key with `rg:<table_name>:`.
//!
//! This matches the TypeScript kit (`src/keys.ts`) so existing databases can
//! continue to resolve guards and unique-key entries.

/// Escape a string so it can safely appear in a typed component body.
fn escape_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace(':', "\\:")
}

/// Encode a single scalar value as a typed component.
fn encode_component(value: Option<&str>) -> String {
    match value {
        Some(s) => format!("s:{}", escape_string(s)),
        None => "n:null".into(),
    }
}

/// Encode a primary key value.
///
/// For single-column keys this returns a single component; for composite keys
/// the components are joined with `:`.
///
/// # Example
///
/// ```
/// use mongreldb_kit_core::keys::encode_pk;
/// assert_eq!(encode_pk(&["hello"]), "s:hello");
/// assert_eq!(encode_pk(&["a", "b"]), "s:a:s:b");
/// ```
pub fn encode_pk(values: &[&str]) -> String {
    values.iter().map(|&v| encode_component(Some(v))).collect::<Vec<_>>().join(":")
}

/// Encode a unique-constraint key.
///
/// The format is `uq:<version>:<constraint_name>:<component1>:...`.
///
/// # Example
///
/// ```
/// use mongreldb_kit_core::keys::encode_unique_key;
/// assert_eq!(
///     encode_unique_key(1, "uq_user_email", &[Some("foo@bar.com")]),
///     "uq:1:uq_user_email:s:foo@bar.com"
/// );
/// ```
pub fn encode_unique_key(kit_version: u32, constraint_name: &str, values: &[Option<&str>]) -> String {
    let components = values
        .iter()
        .map(|v| encode_component(*v))
        .collect::<Vec<_>>()
        .join(":");
    format!("uq:{kit_version}:{constraint_name}:{components}")
}

/// Encode a row-guard key for optimistic foreign-key checks.
///
/// The format is `rg:<table_name>:<encoded_pk>`.
pub fn encode_row_guard_key(table_name: &str, pk: &str) -> String {
    format!("rg:{table_name}:{pk}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_pk_scalar() {
        assert_eq!(encode_pk(&["hello"]), "s:hello");
    }

    #[test]
    fn encode_pk_composite() {
        assert_eq!(encode_pk(&["a", "b"]), "s:a:s:b");
    }

    #[test]
    fn encode_unique_key_basic() {
        assert_eq!(
            encode_unique_key(1, "uq_user_email", &[Some("foo@bar.com")]),
            "uq:1:uq_user_email:s:foo@bar.com"
        );
    }

    #[test]
    fn encode_unique_key_null() {
        assert_eq!(
            encode_unique_key(1, "uq_optional", &[None]),
            "uq:1:uq_optional:n:null"
        );
    }

    #[test]
    fn encode_unique_key_escapes_colons_and_backslashes() {
        assert_eq!(
            encode_unique_key(1, "uq_path", &[Some("a:b\\c")]),
            "uq:1:uq_path:s:a\\:b\\\\c"
        );
    }

    #[test]
    fn encode_row_guard_key_basic() {
        assert_eq!(
            encode_row_guard_key("orders", "s:order-123"),
            "rg:orders:s:order-123"
        );
    }
}
