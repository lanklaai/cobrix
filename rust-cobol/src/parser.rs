use crate::schema::{Field, Picture, Schema, Usage};
use crate::{CobolError, Result};

#[derive(Debug, Clone)]
pub struct ParserConfig {
    pub allow_comments: bool,
}

impl Default for ParserConfig {
    fn default() -> Self {
        Self {
            allow_comments: true,
        }
    }
}

pub fn parse_copybook(copybook_text: &str, cfg: &ParserConfig) -> Result<Schema> {
    let logical_lines = logical_lines(copybook_text, cfg.allow_comments);
    let mut fields = Vec::new();
    let mut occurs_stack: Vec<(u8, usize)> = Vec::new();

    for line in logical_lines {
        let Some((level, name, rest)) = split_line(&line) else {
            continue;
        };

        while occurs_stack.last().is_some_and(|(lvl, _)| *lvl >= level) {
            occurs_stack.pop();
        }
        let parent_occurs = occurs_stack.last().map(|(_, mult)| *mult).unwrap_or(1);

        let occurs = parse_occurs(&rest).unwrap_or(1);
        let effective_occurs = parent_occurs.saturating_mul(occurs.max(1));
        occurs_stack.push((level, effective_occurs));

        let redefines = parse_redefines(&rest);
        let depending_on = parse_depends_on(&rest);

        let picture = extract_pic_clause(&rest)
            .map(|pic| parse_picture(pic, parse_usage(&rest)))
            .transpose()?;

        if name.eq_ignore_ascii_case("FILLER") {
            continue;
        }

        fields.push(Field {
            level,
            name,
            picture,
            occurs: effective_occurs,
            redefines,
            depending_on,
        });
    }

    let leaves: Vec<Field> = fields.into_iter().filter(|f| f.is_leaf()).collect();
    if leaves.is_empty() {
        return Err(CobolError::InvalidCopybook(
            "no leaf fields with PIC found".to_string(),
        ));
    }

    Ok(Schema { fields: leaves })
}

fn logical_lines(copybook_text: &str, allow_comments: bool) -> Vec<String> {
    let mut out = Vec::new();
    let mut current = String::new();

    for line in copybook_text.lines() {
        let mut l = line.replace('\t', " ").trim().to_string();
        if l.is_empty() {
            continue;
        }
        if allow_comments && (l.starts_with('*') || l.starts_with("/") || l.starts_with("*$")) {
            continue;
        }

        if l.ends_with('') {
            l.pop();
        }

        if !current.is_empty() {
            current.push(' ');
        }
        current.push_str(&l);

        if l.contains('.') {
            for stmt in current.split('.') {
                let s = stmt.trim();
                if !s.is_empty() {
                    out.push(s.to_string());
                }
            }
            current.clear();
        }
    }

    if !current.trim().is_empty() {
        out.push(current.trim().to_string());
    }

    out
}

fn split_line(line: &str) -> Option<(u8, String, String)> {
    let mut parts = line.split_whitespace();
    let level = parts.next()?.parse::<u8>().ok()?;
    let name = parts.next()?.trim_end_matches('.').to_string();
    let rest = parts.collect::<Vec<_>>().join(" ");
    Some((level, name, rest))
}

fn parse_occurs(rest: &str) -> Option<usize> {
    parse_token_after(rest, "OCCURS")?.parse::<usize>().ok()
}

fn parse_redefines(rest: &str) -> Option<String> {
    parse_token_after(rest, "REDEFINES").map(|s| s.trim_end_matches('.').to_string())
}

fn parse_depends_on(rest: &str) -> Option<String> {
    let up = rest.to_ascii_uppercase();
    let i = up.find("DEPENDING ON")?;
    let tail = rest.get(i + "DEPENDING ON".len()..)?.trim();
    Some(
        tail.split_whitespace()
            .next()?
            .trim_end_matches('.')
            .to_string(),
    )
}

fn parse_token_after<'a>(rest: &'a str, keyword: &str) -> Option<&'a str> {
    let up = rest.to_ascii_uppercase();
    let idx = up.find(keyword)?;
    let tail = rest.get(idx + keyword.len()..)?.trim();
    tail.split_whitespace().next()
}

fn parse_usage(rest: &str) -> Usage {
    let up = rest.to_ascii_uppercase();
    if up.contains("COMP-3") {
        Usage::Comp3
    } else if up.contains("COMP-1") {
        Usage::Comp1
    } else if up.contains("COMP-2") {
        Usage::Comp2
    } else if up.contains("COMP-4") {
        Usage::Comp4
    } else if up.contains("COMP-5") {
        Usage::Comp5
    } else if up.contains(" BINARY") {
        Usage::Binary
    } else if up.contains(" COMP") {
        Usage::Comp
    } else {
        Usage::Display
    }
}

fn extract_pic_clause(rest: &str) -> Option<String> {
    let parts: Vec<&str> = rest.split_whitespace().collect();
    let idx = parts
        .iter()
        .position(|p| p.eq_ignore_ascii_case("PIC") || p.eq_ignore_ascii_case("PICTURE"))?;

    let mut pic = String::new();
    for token in parts.iter().skip(idx + 1) {
        let u = token.to_ascii_uppercase();
        if u.starts_with("COMP")
            || u == "BINARY"
            || u == "USAGE"
            || u == "REDEFINES"
            || u == "OCCURS"
            || u == "DEPENDING"
        {
            break;
        }
        pic.push_str(token);
    }

    let pic = pic.trim().trim_end_matches('.').replace(' ', "");
    if pic.is_empty() {
        None
    } else {
        Some(pic)
    }
}

fn parse_picture(pic: String, usage: Usage) -> Result<Picture> {
    let raw = pic.trim().to_ascii_uppercase();
    if raw.starts_with('X') {
        let len = parse_repeat_count(&raw, 'X')?;
        return Ok(Picture {
            raw,
            signed: false,
            digits_before: 0,
            digits_after: 0,
            alpha_len: Some(len),
            usage,
        });
    }

    let signed = raw.starts_with('S');
    let body = if signed { &raw[1..] } else { &raw };
    let body = body.replace('.', "");
    let segments: Vec<&str> = body.split('V').collect();
    if segments.len() > 2 {
        return Err(CobolError::InvalidPicture(pic));
    }

    let digits_before = parse_numeric_digits(segments[0])?;
    let digits_after = if segments.len() == 2 {
        parse_numeric_digits(segments[1])?
    } else {
        0
    };

    if digits_before == 0 && digits_after == 0 {
        return Err(CobolError::InvalidPicture(pic));
    }

    Ok(Picture {
        raw,
        signed,
        digits_before,
        digits_after,
        alpha_len: None,
        usage,
    })
}

fn parse_repeat_count(raw: &str, token: char) -> Result<usize> {
    if raw == token.to_string() {
        return Ok(1);
    }
    if let Some(start) = raw.find('(') {
        let end = raw
            .find(')')
            .ok_or_else(|| CobolError::InvalidPicture(raw.to_string()))?;
        let num = raw[start + 1..end]
            .parse::<usize>()
            .map_err(|_| CobolError::InvalidPicture(raw.to_string()))?;
        return Ok(num);
    }
    let count = raw.chars().filter(|c| *c == token).count();
    if count == 0 {
        return Err(CobolError::InvalidPicture(raw.to_string()));
    }
    Ok(count)
}

fn parse_numeric_digits(segment: &str) -> Result<usize> {
    let s = segment.trim();
    if s.is_empty() {
        return Ok(0);
    }

    let mut total = 0usize;
    let chars: Vec<char> = s.chars().collect();
    let mut i = 0usize;
    while i < chars.len() {
        let ch = chars[i];
        let marker = ch.to_ascii_uppercase();
        if matches!(marker, '9' | 'Z' | '*' | 'P') {
            let mut repeat = 1usize;
            if i + 1 < chars.len() && chars[i + 1] == '(' {
                let mut j = i + 2;
                let mut count_str = String::new();
                while j < chars.len() && chars[j] != ')' {
                    count_str.push(chars[j]);
                    j += 1;
                }
                if j >= chars.len() {
                    return Err(CobolError::InvalidPicture(segment.to_string()));
                }
                repeat = count_str
                    .parse::<usize>()
                    .map_err(|_| CobolError::InvalidPicture(segment.to_string()))?;
                i = j;
            }
            if marker != 'P' {
                total += repeat;
            }
        }
        i += 1;
    }

    if total == 0 {
        return Err(CobolError::InvalidPicture(segment.to_string()));
    }
    Ok(total)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_copybook() {
        let cb = "01 REC.\n 05 ID PIC 9(4).\n 05 NAME PIC X(10).";
        let schema = parse_copybook(cb, &ParserConfig::default()).expect("parse ok");
        assert_eq!(schema.fields.len(), 2);
        assert_eq!(schema.fixed_record_len(), 14);
    }

    #[test]
    fn parses_real_cobrix_copybooks() {
        let cb1 = include_str!("../../data/test1_copybook.cob");
        let cb6 = include_str!("../../data/test6_copybook.cob");

        let schema1 = parse_copybook(cb1, &ParserConfig::default()).expect("parse test1");
        let schema6 = parse_copybook(cb6, &ParserConfig::default()).expect("parse test6");

        assert!(schema1.fields.len() > 5);
        assert!(schema6.fields.len() > 20);
        assert!(schema1.fixed_record_len() > 0);
    }
}
