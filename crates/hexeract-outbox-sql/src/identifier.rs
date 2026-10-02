//! The single anti-injection guard for SQL table identifiers.
//!
//! Table names reach the SQL layer from configuration and are concatenated
//! into generated DDL and statements, so they cannot be bound as parameters.
//! The guard below is the one place that decides which names are safe to
//! concatenate, for every crate in the workspace that generates SQL.
//!
//! The module is hidden from the rendered documentation, which keeps it out of
//! the public API as defined by `docs/SEMVER_POLICY.md`, while remaining
//! reachable by the sibling crates that must apply the same rule.

/// Maximum byte length of a SQL identifier before the server silently
/// truncates it (PostgreSQL `NAMEDATALEN` minus one).
///
/// Identifiers longer than this are rejected so that names derived from the
/// table name, such as `idx_{table}_pending`, cannot collide with another
/// name after server-side truncation.
pub const MAX_IDENTIFIER_LEN: usize = 63;

/// Why a table name was rejected by [`validate_table_name`].
///
/// One variant per validation branch, so that a caller can report the precise
/// cause and so that each branch stays independently observable.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InvalidTableName {
    /// The name was empty.
    Empty,
    /// The name exceeded [`MAX_IDENTIFIER_LEN`] bytes.
    TooLong {
        /// Byte length of the rejected name.
        length: usize,
    },
    /// The first character was neither an ASCII letter nor an underscore.
    InvalidLeadingCharacter {
        /// The offending first character.
        character: char,
    },
    /// A character after the first was neither ASCII alphanumeric nor an
    /// underscore.
    InvalidCharacter {
        /// The first offending character found after the leading one.
        character: char,
    },
}

impl core::fmt::Display for InvalidTableName {
    fn fmt(&self, formatter: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        match self {
            Self::Empty => formatter.write_str("must not be empty"),
            Self::TooLong { length } => write!(
                formatter,
                "is {length} bytes long, which exceeds the maximum identifier length of {MAX_IDENTIFIER_LEN} bytes"
            ),
            Self::InvalidLeadingCharacter { character } => {
                write!(formatter, "must start with [a-zA-Z_], found {character:?}")
            }
            Self::InvalidCharacter { character } => write!(
                formatter,
                "must match [a-zA-Z_][a-zA-Z0-9_]*, found {character:?}"
            ),
        }
    }
}

impl core::error::Error for InvalidTableName {}

/// Reject any table name that is not a safe SQL identifier or that would
/// overflow the server's identifier length limit.
///
/// The accepted subset is `^[a-zA-Z_][a-zA-Z0-9_]*$`, which prevents SQL
/// injection through a name that is concatenated into generated statements,
/// bounded to [`MAX_IDENTIFIER_LEN`] bytes so that derived names are not
/// silently truncated.
///
/// Passing this check does not make a name safe to embed unquoted: reserved
/// words are valid identifiers and are accepted here, so callers must still
/// quote the name through [`crate::Dialect`].
pub fn validate_table_name(name: &str) -> Result<(), InvalidTableName> {
    let mut characters = name.chars();
    let Some(first) = characters.next() else {
        return Err(InvalidTableName::Empty);
    };
    if name.len() > MAX_IDENTIFIER_LEN {
        return Err(InvalidTableName::TooLong { length: name.len() });
    }
    if !(first.is_ascii_alphabetic() || first == '_') {
        return Err(InvalidTableName::InvalidLeadingCharacter { character: first });
    }
    if let Some(character) = characters.find(|c| !(c.is_ascii_alphanumeric() || *c == '_')) {
        return Err(InvalidTableName::InvalidCharacter { character });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accepts_valid_names() {
        for name in [
            "audit_outbox",
            "scheduled_messages",
            "_internal",
            "outbox_v2",
            "schedules_v2",
            "A",
            "_",
        ] {
            assert_eq!(validate_table_name(name), Ok(()), "name {name:?}");
        }
    }

    // Union of the two lists that existed before the single source: the
    // `"tbl\u{0000}"` case was tested by `hexeract-outbox-sql` but not by
    // `hexeract-scheduler-sql`, so keeping it proves that merging the two
    // lists loses no case.
    #[test]
    fn rejects_every_name_of_the_union_list() {
        let rejected = [
            "",
            "1starts_with_digit",
            "has space",
            "has-dash",
            "has;semicolon",
            "drop_table\"; DROP",
            "a.b",
            "tbl\u{0000}",
        ];
        assert_eq!(rejected.len(), 8);
        for name in rejected {
            assert!(validate_table_name(name).is_err(), "name {name:?}");
        }
    }

    #[test]
    fn empty_name_is_empty_variant() {
        assert_eq!(validate_table_name(""), Err(InvalidTableName::Empty));
    }

    #[test]
    fn length_bound_is_checked_on_both_sides() {
        let at_limit = "a".repeat(MAX_IDENTIFIER_LEN);
        let over_limit = "a".repeat(MAX_IDENTIFIER_LEN + 1);
        assert_eq!(at_limit.len(), 63);
        assert_eq!(validate_table_name(&at_limit), Ok(()));
        assert_eq!(
            validate_table_name(&over_limit),
            Err(InvalidTableName::TooLong { length: 64 })
        );
    }

    #[test]
    fn digit_first_is_invalid_leading_character() {
        assert_eq!(
            validate_table_name("1abc"),
            Err(InvalidTableName::InvalidLeadingCharacter { character: '1' })
        );
    }

    #[test]
    fn dot_is_invalid_character() {
        assert_eq!(
            validate_table_name("a.b"),
            Err(InvalidTableName::InvalidCharacter { character: '.' })
        );
    }

    // Reserved words are valid identifiers as far as characters go. Quoting
    // through `Dialect` is what makes them safe, not this function, so
    // accepting them here is intended and not an oversight.
    #[test]
    fn reserved_words_are_accepted() {
        for name in ["select", "user", "order", "table"] {
            assert_eq!(validate_table_name(name), Ok(()), "name {name:?}");
        }
    }

    // Only the rule is asserted, not the full text, so that rewording the
    // message does not break a test that would prove nothing more.
    #[test]
    fn display_mentions_the_accepted_rule() {
        let message = InvalidTableName::InvalidCharacter { character: '.' }.to_string();
        assert!(!message.is_empty());
        assert!(
            message.contains("[a-zA-Z_][a-zA-Z0-9_]*"),
            "got {message:?}"
        );
    }
}
