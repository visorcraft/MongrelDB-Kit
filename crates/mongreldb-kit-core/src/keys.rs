//! Key encoding for Kit indexes, guards, and primary keys.
//!
//! # Format
//!
//! Each encoded key is a colon-separated (`:`) string of typed components.
//! A component is one of:
//!
//! * `i:<value>` – a signed integer value (mirrors a TypeScript `bigint`).
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
//! This matches the TypeScript kit (`src/keys.ts`) byte-for-byte so the three
//! languages produce identical guard and unique-key entries. In particular the
//! typed `i:`/`s:` prefixes guarantee that the integer `1` and the text `"1"`
//! never collide.

/// Stable on-disk format version embedded in unique-key encodings.
pub const KIT_KEY_VERSION: u32 = 1;

/// A single typed key component.
///
/// Mirrors the TypeScript `string | bigint | null` union used by
/// `encodeKeyComponent`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyComponent {
    /// A signed integer (TypeScript `bigint`), encoded as `i:<value>`.
    Int(i64),
    /// A UTF-8 string, encoded as `s:<escaped>`.
    Text(String),
    /// An explicit SQL `NULL`, encoded as `n:null`.
    Null,
}

impl KeyComponent {
    /// Build a text component from anything string-like.
    pub fn text(value: impl Into<String>) -> Self {
        KeyComponent::Text(value.into())
    }
}

/// Escape a string so it can safely appear in a typed component body.
fn escape_string(value: &str) -> String {
    value.replace('\\', "\\\\").replace(':', "\\:")
}

/// Inverse of [`escape_string`]. Mirrors the (intentionally naive) TypeScript
/// `unescapeString` so round-tripping matches across languages.
fn unescape_string(value: &str) -> String {
    value.replace("\\:", ":").replace("\\\\", "\\")
}

/// Encode a single typed component.
pub fn encode_component(value: &KeyComponent) -> String {
    match value {
        KeyComponent::Null => "n:null".to_string(),
        KeyComponent::Int(i) => format!("i:{i}"),
        KeyComponent::Text(s) => format!("s:{}", escape_string(s)),
    }
}

/// Decode a single typed token such as `i:42`, `s:foo`, or `n:null`.
///
/// Uses `strip_prefix`/`starts_with` rather than byte-index slicing so that a
/// malformed token beginning with a multi-byte character cannot panic.
fn decode_component(token: &str) -> KeyComponent {
    if let Some(body) = token.strip_prefix("s:") {
        KeyComponent::Text(unescape_string(body))
    } else if let Some(body) = token.strip_prefix("i:") {
        // A non-numeric body is malformed input; fall back to text rather than
        // silently decoding it as 0.
        body.parse::<i64>()
            .map(KeyComponent::Int)
            .unwrap_or_else(|_| KeyComponent::Text(unescape_string(token)))
    } else if token.strip_prefix("n:").is_some() {
        KeyComponent::Null
    } else {
        KeyComponent::Text(unescape_string(token))
    }
}

/// Split an encoded key into its typed component tokens.
///
/// Mirrors the TypeScript `decodeKeyComponents`: a token boundary is an
/// unescaped `:` immediately following a complete typed component
/// (`s:`/`i:`/`n:` plus a body).
fn split_components(encoded: &str) -> Vec<String> {
    let mut tokens: Vec<String> = Vec::new();
    let mut current = String::new();
    let mut escaped = false;
    for ch in encoded.chars() {
        if escaped {
            current.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            current.push(ch);
            escaped = true;
            continue;
        }
        if ch == ':' {
            let starts_typed = current.starts_with("s:")
                || current.starts_with("i:")
                || current.starts_with("n:");
            if starts_typed {
                tokens.push(std::mem::take(&mut current));
                continue;
            }
        }
        current.push(ch);
    }
    if current.len() >= 2 {
        tokens.push(current);
    }
    tokens
}

/// Decode an encoded primary key into its typed components.
pub fn decode_pk(encoded: &str) -> Vec<KeyComponent> {
    split_components(encoded)
        .iter()
        .map(|t| decode_component(t))
        .collect()
}

/// Encode a primary key value.
///
/// For single-column keys this returns a single component; for composite keys
/// the components are joined with `:`.
///
/// # Example
///
/// ```
/// use mongreldb_kit_core::keys::{encode_pk, KeyComponent};
/// assert_eq!(encode_pk(&[KeyComponent::text("hello")]), "s:hello");
/// assert_eq!(encode_pk(&[KeyComponent::Int(1)]), "i:1");
/// assert_eq!(
///     encode_pk(&[KeyComponent::text("a"), KeyComponent::text("b")]),
///     "s:a:s:b"
/// );
/// ```
pub fn encode_pk(values: &[KeyComponent]) -> String {
    values
        .iter()
        .map(encode_component)
        .collect::<Vec<_>>()
        .join(":")
}

/// Encode a unique-constraint key.
///
/// The format is `uq:<version>:<constraint_name>:<component1>:...`.
///
/// # Example
///
/// ```
/// use mongreldb_kit_core::keys::{encode_unique_key, KeyComponent};
/// assert_eq!(
///     encode_unique_key(1, "uq_user_email", &[KeyComponent::text("foo@bar.com")]),
///     "uq:1:uq_user_email:s:foo@bar.com"
/// );
/// ```
pub fn encode_unique_key(
    kit_version: u32,
    constraint_name: &str,
    values: &[KeyComponent],
) -> String {
    let components = values
        .iter()
        .map(encode_component)
        .collect::<Vec<_>>()
        .join(":");
    format!("uq:{kit_version}:{constraint_name}:{components}")
}

/// Encode a row-guard key for optimistic foreign-key checks.
///
/// The format is `rg:<table_name>:<encoded_pk>`, where `encoded_pk` is the
/// output of [`encode_pk`].
pub fn encode_row_guard_key(table_name: &str, encoded_pk: &str) -> String {
    format!("rg:{table_name}:{encoded_pk}")
}

#[cfg(test)]
mod tests {
    use super::*;

    // The following vectors are byte-identical to the TypeScript kit
    // (`packages/kit/src/constraints.test.ts`) and guarantee cross-language
    // parity for the encoding.

    #[test]
    fn int_and_text_do_not_collide() {
        assert_eq!(encode_pk(&[KeyComponent::Int(1)]), "i:1");
        assert_eq!(encode_pk(&[KeyComponent::text("1")]), "s:1");
        assert_ne!(
            encode_pk(&[KeyComponent::Int(1)]),
            encode_pk(&[KeyComponent::text("1")])
        );
    }

    #[test]
    fn encode_pk_scalar_and_composite() {
        assert_eq!(encode_pk(&[KeyComponent::text("hello")]), "s:hello");
        assert_eq!(
            encode_pk(&[KeyComponent::text("a"), KeyComponent::text("b")]),
            "s:a:s:b"
        );
        assert_eq!(
            encode_pk(&[KeyComponent::Int(42), KeyComponent::Int(7)]),
            "i:42:i:7"
        );
    }

    #[test]
    fn encode_unique_key_matches_typescript_vectors() {
        assert_eq!(
            encode_unique_key(1, "users_email_uq", &[KeyComponent::text("a@example.com")]),
            "uq:1:users_email_uq:s:a@example.com"
        );
        assert_eq!(
            encode_unique_key(
                1,
                "shares_trip_user_uq",
                &[KeyComponent::Int(42), KeyComponent::Int(7)]
            ),
            "uq:1:shares_trip_user_uq:i:42:i:7"
        );
        assert_eq!(
            encode_unique_key(1, "uq_esc", &[KeyComponent::text("a:b\\c")]),
            "uq:1:uq_esc:s:a\\:b\\\\c"
        );
        assert_eq!(
            encode_unique_key(1, "uq_null", &[KeyComponent::Null]),
            "uq:1:uq_null:n:null"
        );
    }

    #[test]
    fn encode_row_guard_key_matches_typescript_vectors() {
        assert_eq!(
            encode_row_guard_key("trips", &encode_pk(&[KeyComponent::Int(5)])),
            "rg:trips:i:5"
        );
        assert_eq!(
            encode_row_guard_key("users", &encode_pk(&[KeyComponent::text("alpha")])),
            "rg:users:s:alpha"
        );
    }

    #[test]
    fn pk_round_trips_through_decode() {
        let cases = vec![
            vec![KeyComponent::Int(1)],
            vec![KeyComponent::Int(-99)],
            vec![KeyComponent::text("alpha")],
            vec![KeyComponent::text("a"), KeyComponent::text("b")],
            vec![KeyComponent::Int(1), KeyComponent::text("x")],
        ];
        for case in cases {
            let encoded = encode_pk(&case);
            assert_eq!(decode_pk(&encoded), case, "round-trip failed for {encoded}");
        }
    }

    #[test]
    fn decode_handles_typed_prefixes() {
        assert_eq!(decode_pk("i:5"), vec![KeyComponent::Int(5)]);
        assert_eq!(decode_pk("s:hi"), vec![KeyComponent::text("hi")]);
        assert_eq!(decode_pk("n:null"), vec![KeyComponent::Null]);
    }

    #[test]
    fn decode_does_not_panic_on_malformed_multibyte_input() {
        // Multi-byte leading chars must not panic the byte-unaware decoder, and
        // an unparseable `i:` body falls back to text rather than 0.
        let _ = decode_pk("中");
        let _ = decode_pk("中:foo");
        let _ = decode_pk("");
        assert_eq!(decode_pk("中"), vec![KeyComponent::text("中")]);
        assert_eq!(decode_pk("i:abc"), vec![KeyComponent::text("i:abc")]);
    }
}
