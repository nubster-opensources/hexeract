//! Safe rendering of untrusted text for terminals and logs.

use std::borrow::Cow;

/// Escape untrusted characters that can alter how terminal output is displayed.
pub(crate) fn escape_unsafe_characters(value: &str) -> Cow<'_, str> {
    if !value.chars().any(is_unsafe_character) {
        return Cow::Borrowed(value);
    }

    let mut escaped = String::with_capacity(value.len());
    for character in value.chars() {
        if is_unsafe_character(character) {
            escaped.extend(character.escape_debug());
        } else {
            escaped.push(character);
        }
    }
    Cow::Owned(escaped)
}

/// Whether `character` must be escaped before reaching a terminal or log.
///
/// Covers control characters, bidirectional formatting, line and paragraph
/// separators, invisible operators and the byte order mark, which can
/// inject terminal sequences, reorder or hide displayed text. The backslash
/// is escaped too, so an escaped sequence stays distinguishable from the
/// same literal text. Joiners, combining marks and variation selectors are
/// kept: printable scripts and emoji need them to render.
fn is_unsafe_character(character: char) -> bool {
    character.is_control()
        || matches!(
            character,
            '\\' | '\u{061C}'
                | '\u{200B}'
                | '\u{200E}'..='\u{200F}'
                | '\u{2028}'..='\u{202E}'
                | '\u{2060}'..='\u{2064}'
                | '\u{2066}'..='\u{2069}'
                | '\u{FEFF}'
        )
}
