//! COPY data format codec — PostgreSQL text and CSV formats.
//!
//! Text format: one row per line, fields separated by a delimiter (default
//! TAB); backslash escapes (`\\`, `\t`, `\n`, `\r`, `\b`, `\f`, `\v`); the
//! NULL string is matched against the RAW field before unescaping.
//! CSV: RFC 4180 with PG's rule that an UNQUOTED empty field is NULL (a
//! quoted empty field is the empty string).

use crate::engine::Value;

/// Splits one text-format line into RAW (still escaped) fields.
fn split_raw_line(line: &str, delim: char) -> Result<Vec<String>, String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut escaped = false;
    for ch in line.chars() {
        if escaped {
            cur.push(ch);
            escaped = false;
        } else if ch == '\\' {
            cur.push(ch);
            escaped = true;
        } else if ch == delim {
            fields.push(std::mem::take(&mut cur));
        } else {
            cur.push(ch);
        }
    }
    if escaped {
        return Err("COPY: dangling backslash at end of line".into());
    }
    fields.push(cur);
    Ok(fields)
}

/// Unescapes a raw text-format field (PG escapes only).
fn unescape(raw: &str) -> Result<String, String> {
    let mut out = String::with_capacity(raw.len());
    let mut escaped = false;
    for ch in raw.chars() {
        if escaped {
            match ch {
                '\\' => out.push('\\'),
                't' => out.push('\t'),
                'n' => out.push('\n'),
                'r' => out.push('\r'),
                'b' => out.push('\u{8}'),
                'f' => out.push('\u{c}'),
                'v' => out.push('\u{b}'),
                other => {
                    return Err(format!("COPY: invalid escape sequence \\{}", other))
                }
            }
            escaped = false;
        } else if ch == '\\' {
            escaped = true;
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

/// Parses COPY text-format data into rows of optional fields (None = NULL).
pub fn parse_text(
    data: &str,
    delim: char,
    null: &str,
    header: bool,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let lines: Vec<&str> = data.split('\n').collect();
    let n = lines.len();
    let mut first = true;
    for (i, line) in lines.iter().enumerate() {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            if i + 1 == n {
                continue; // artifact of the trailing newline
            }
            // empty line mid-stream: PG errors with missing data
            return Err("COPY: missing data for row".into());
        }
        if header && first {
            first = false;
            continue;
        }
        first = false;
        let raws = split_raw_line(line, delim)?;
        let mut fields = Vec::with_capacity(raws.len());
        for raw in &raws {
            if raw == null {
                fields.push(None);
            } else {
                fields.push(Some(unescape(raw)?));
            }
        }
        rows.push(fields);
    }
    Ok(rows)
}

/// Parses COPY CSV data.
pub fn parse_csv(
    data: &str,
    delim: char,
    quote: char,
    header: bool,
) -> Result<Vec<Vec<Option<String>>>, String> {
    let mut rows: Vec<Vec<Option<String>>> = Vec::new();
    let mut fields: Vec<Option<String>> = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut field_was_quoted = false;
    let mut field_has_chars = false;
    let mut chars = data.chars().peekable();

    macro_rules! end_field {
        () => {{
            if in_quotes {
                return Err("COPY: unterminated quoted field".into());
            }
            // PG CSV: unquoted empty = NULL; quoted empty = ""
            if field_was_quoted || field_has_chars {
                fields.push(Some(cur.clone()));
            } else {
                fields.push(None);
            }
            cur.clear();
        }};
    }

    while let Some(ch) = chars.next() {
        if in_quotes {
            if ch == quote {
                if chars.peek() == Some(&quote) {
                    chars.next();
                    cur.push(quote);
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(ch);
            }
            continue;
        }
        if ch == quote && !field_has_chars {
            in_quotes = true;
            field_was_quoted = true;
            field_has_chars = true;
        } else if ch == delim {
            end_field!();
            field_was_quoted = false;
            field_has_chars = false;
        } else if ch == '\n' {
            if cur.ends_with('\r') {
                cur.pop();
            }
            end_field!();
            rows.push(std::mem::take(&mut fields));
            field_was_quoted = false;
            field_has_chars = false;
        } else if ch == '\r' && chars.peek() == Some(&'\n') {
            continue; // consumed with the \n
        } else {
            cur.push(ch);
            field_has_chars = true;
        }
    }
    if in_quotes {
        return Err("COPY: unterminated quoted field".into());
    }
    // final row without trailing newline
    if !fields.is_empty() || !cur.is_empty() || field_has_chars {
        end_field!();
        rows.push(fields);
    }
    if header && !rows.is_empty() {
        rows.remove(0);
    }
    Ok(rows)
}

/// Encodes one value for COPY text format.
fn enc_text(v: &Value, out: &mut String, null: &str) {
    match v {
        Value::Null => out.push_str(null),
        _ => {
            let s = crate::engine::value_to_text(v).unwrap_or_default();
            for c in s.chars() {
                match c {
                    '\\' => out.push_str("\\\\"),
                    '\t' => out.push_str("\\t"),
                    '\n' => out.push_str("\\n"),
                    '\r' => out.push_str("\\r"),
                    '\u{8}' => out.push_str("\\b"),
                    '\u{c}' => out.push_str("\\f"),
                    '\u{b}' => out.push_str("\\v"),
                    c => out.push(c),
                }
            }
        }
    }
}

/// Encodes a row in COPY text format.
pub fn encode_text_row(row: &[Value], delim: char, null: &str) -> String {
    let mut out = String::new();
    for (i, v) in row.iter().enumerate() {
        if i > 0 {
            out.push(delim);
        }
        enc_text(v, &mut out, null);
    }
    out.push('\n');
    out
}

/// Encodes a row in COPY CSV format.
pub fn encode_csv_row(row: &[Value], delim: char, quote: char) -> String {
    let mut out = String::new();
    for (i, v) in row.iter().enumerate() {
        if i > 0 {
            out.push(delim);
        }
        match v {
            Value::Null => {} // unquoted empty = NULL
            _ => {
                let s = crate::engine::value_to_text(v).unwrap_or_default();
                let needs_quote =
                    s.contains(delim) || s.contains(quote) || s.contains('\n') || s.contains('\r');
                if needs_quote {
                    out.push(quote);
                    for c in s.chars() {
                        if c == quote {
                            out.push(quote);
                        }
                        out.push(c);
                    }
                    out.push(quote);
                } else {
                    out.push_str(&s);
                }
            }
        }
    }
    out.push('\n');
    out
}
