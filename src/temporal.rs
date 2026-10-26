//! Temporal types: DATE (days since 1970-01-01), TIME (microseconds since
//! midnight), TIMESTAMP (microseconds since the epoch, no time zone) and
//! INTERVAL (months + days + microseconds, like PG — months need their own
//! component because their length varies).
//!
//! Civil-date arithmetic uses Howard Hinnant's algorithms (public domain):
//! exact, branch-cheap, valid over the whole year range we accept.

use std::time::{SystemTime, UNIX_EPOCH};

pub const US_PER_DAY: i64 = 86_400_000_000;
/// PG-compatible nominal month length for interval comparisons.
pub const US_PER_NOMINAL_MONTH: i64 = 30 * US_PER_DAY;

// ------------------------------------------------------------ civil dates --

pub fn days_from_civil(y: i32, m: u32, d: u32) -> i32 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = if y >= 0 { y } else { y - 399 } / 400;
    let yoe = (y - era * 400) as i64; // [0, 399]
    let mp = (m as i64) + if m > 2 { -3 } else { 9 }; // [0, 11]
    let doy = (153 * mp + 2) / 5 + (d as i64) - 1; // [0, 365]
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy; // [0, 146096]
    (era as i64 * 146_097 + doe - 719_468) as i32
}

pub fn civil_from_days(z: i32) -> (i32, u32, u32) {
    let z = z as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (mp + if mp < 10 { 3 } else { -9 }) as u32; // [1, 12]
    ((y + if m <= 2 { 1 } else { 0 }) as i32, m, d)
}

pub fn days_in_month(y: i32, m: u32) -> u32 {
    match m {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if (y % 4 == 0 && y % 100 != 0) || y % 400 == 0 {
                29
            } else {
                28
            }
        }
        _ => 0,
    }
}

/// Splits a timestamp into (days, time-of-day micros).
fn split_ts(us: i64) -> (i32, i64) {
    let days = us.div_euclid(US_PER_DAY);
    let tod = us.rem_euclid(US_PER_DAY);
    (days as i32, tod)
}

fn join_ts(days: i32, tod_us: i64) -> i64 {
    (days as i64) * US_PER_DAY + tod_us
}

/// Adds calendar months to a timestamp, clamping the day like PG
/// (Jan 31 + 1 mon = Feb 28/29).
pub fn add_months(us: i64, n: i32) -> i64 {
    let (days, tod) = split_ts(us);
    let (y, m, d) = civil_from_days(days);
    let total = (y as i64) * 12 + (m as i64) - 1 + (n as i64);
    let ny = total.div_euclid(12) as i32;
    let nm = total.rem_euclid(12) as u32 + 1;
    let nd = d.min(days_in_month(ny, nm));
    join_ts(days_from_civil(ny, nm, nd), tod)
}

// ---------------------------------------------------------------- parsing --

fn digits(b: &[u8]) -> Option<u64> {
    if b.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &c in b {
        if !c.is_ascii_digit() {
            return None;
        }
        v = v.checked_mul(10)? + (c - b'0') as u64;
    }
    Some(v)
}

/// Parses YYYY-MM-DD (1-2 digit month/day tolerated). Returns days.
pub fn parse_date(s: &str) -> Result<i32, String> {
    let s = s.trim();
    let b = s.as_bytes();
    let sep1 = b.iter().position(|&c| c == b'-');
    let Some(sep1) = sep1 else {
        return Err(format!("invalid input syntax for type date: \"{s}\""));
    };
    let year = digits(&b[..sep1])
        .filter(|y| (0..=999_999).contains(y))
        .ok_or_else(|| format!("invalid input syntax for type date: \"{s}\""))?;
    let rest = &b[sep1 + 1..];
    let Some(sep2) = rest.iter().position(|&c| c == b'-') else {
        return Err(format!("invalid input syntax for type date: \"{s}\""));
    };
    let month = digits(&rest[..sep2]).filter(|m| (1..=12).contains(m));
    let day = digits(&rest[sep2 + 1..]).filter(|d| (1..=31).contains(d));
    let (Some(month), Some(day)) = (month, day) else {
        return Err(format!("invalid input syntax for type date: \"{s}\""));
    };
    let y = year as i32;
    let m = month as u32;
    let d = day as u32;
    if d > days_in_month(y, m) {
        return Err(format!("date/time field value out of range: \"{s}\""));
    }
    Ok(days_from_civil(y, m, d))
}

/// Parses HH:MM[:SS[.ffffff]] (1-2 digit fields tolerated).
/// Returns microseconds since midnight.
pub fn parse_time(s: &str) -> Result<i64, String> {
    let bad = || format!("invalid input syntax for type time: \"{s}\"");
    let t = s.trim();
    let mut parts = t.split(':');
    let h: u64 = parts.next().and_then(|p| digits(p.as_bytes())).ok_or_else(bad)?;
    let m: u64 = parts.next().and_then(|p| digits(p.as_bytes())).ok_or_else(bad)?;
    let (sec, frac_us) = match parts.next() {
        None => (0u64, 0i64),
        Some(sp) => {
            let (ip, fp) = match sp.split_once('.') {
                Some((a, b)) => (a, Some(b)),
                None => (sp, None),
            };
            let sv: u64 = digits(ip.as_bytes()).ok_or_else(bad)?;
            let fv = match fp {
                None => 0i64,
                Some(f) => {
                    if f.len() > 6 || !f.bytes().all(|c| c.is_ascii_digit()) {
                        return Err(bad());
                    }
                    let mut v: i64 = 0;
                    for c in f.bytes() {
                        v = v * 10 + (c - b'0') as i64;
                    }
                    for _ in f.len()..6 {
                        v *= 10;
                    }
                    v
                }
            };
            (sv, fv)
        }
    };
    if parts.next().is_some() || h > 24 || m > 59 || sec > 61 || (h == 24 && (m > 0 || sec > 0)) {
        return Err(bad());
    }
    Ok((h as i64) * 3_600_000_000 + (m as i64) * 60_000_000 + (sec as i64) * 1_000_000 + frac_us)
}

/// Parses a timestamp: date, then optionally ' ' or 'T' and a time.
pub fn parse_timestamp(s: &str) -> Result<i64, String> {
    let s = s.trim();
    let bad = || format!("invalid input syntax for type timestamp: \"{s}\"");
    // find the date/time boundary: first ' ' or 'T' after the date part
    let b = s.as_bytes();
    let mut split = None;
    for (i, &c) in b.iter().enumerate() {
        if c == b' ' || c == b'T' || c == b't' {
            split = Some(i);
            break;
        }
    }
    let (dpart, tpart) = match split {
        None => (s, ""),
        Some(i) => (&s[..i], s[i + 1..].trim()),
    };
    let days = parse_date(dpart).map_err(|_| bad())?;
    if tpart.is_empty() {
        return Ok(join_ts(days, 0));
    }
    let tod = parse_time(tpart).map_err(|_| bad())?;
    Ok(join_ts(days, tod))
}

/// Parses an interval: "1 year 2 mons 3 days 04:05:06", "1.5 hours",
/// "-2 days", "3:30", "".
pub fn parse_interval(s: &str) -> Result<(i32, i32, i64), String> {
    let bad = || format!("invalid input syntax for type interval: \"{s}\"");
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let toks: Vec<&str> = s.split_whitespace().collect();
    let mut i = 0;
    while i < toks.len() {
        let t = toks[i];
        // standalone time-of-day form: [-]HH:MM[:SS[.f]]
        if t.contains(':') {
            if t.bytes().next().is_some_and(|c| c.is_ascii_digit() || c == b'-') {
                let neg = t.starts_with('-');
                let us = parse_time(t.trim_start_matches('-')).map_err(|_| bad())?;
                micros += if neg { -us } else { us };
                i += 1;
                continue;
            }
            return Err(bad());
        }
        // quantity [unit]: the unit may be fused ("1day") or in the next
        // whitespace token ("1 day")
        let (num_str, fused_unit) = t.split_at(t.find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+').unwrap_or(t.len()));
        if num_str.is_empty() {
            return Err(bad());
        }
        let q: f64 = num_str.parse::<f64>().map_err(|_| bad())?;
        let unit_owned;
        let unit: &str = if fused_unit.trim().is_empty() {
            // unit lives in the next token
            let Some(u) = toks.get(i + 1) else {
                return Err(bad());
            };
            unit_owned = u.to_lowercase();
            i += 1;
            &unit_owned
        } else {
            unit_owned = fused_unit.trim().to_lowercase();
            &unit_owned
        };
        let apply = |months: &mut i64, days: &mut i64, micros: &mut i64| -> Result<(), String> {
            match unit {
                "year" | "years" | "y" | "yrs" => {
                    *months += (q * 12.0).round() as i64;
                }
                "month" | "months" | "mon" | "mons" => {
                    // PG-style: integer part in months, fraction in
                    // 30-day months
                    *months += q.trunc() as i64;
                    let frach = (q - q.trunc()) * 30.0;
                    *days += frach.trunc() as i64;
                    *micros += ((frach - frach.trunc()) * US_PER_DAY as f64).round() as i64;
                }
                "week" | "weeks" | "w" => {
                    let wd = q * 7.0;
                    *days += wd.trunc() as i64;
                    *micros += ((wd - wd.trunc()) * US_PER_DAY as f64).round() as i64;
                }
                "day" | "days" | "d" => {
                    *days += q.trunc() as i64;
                    *micros += ((q - q.trunc()) * US_PER_DAY as f64).round() as i64;
                }
                "hour" | "hours" | "hr" | "hrs" | "h" => {
                    *micros += (q * 3_600_000_000.0).round() as i64;
                }
                "minute" | "minutes" | "min" | "mins" | "m" => {
                    *micros += (q * 60_000_000.0).round() as i64;
                }
                "second" | "seconds" | "sec" | "secs" | "s" => {
                    *micros += (q * 1_000_000.0).round() as i64;
                }
                "millisecond" | "milliseconds" | "msec" | "msecs" | "ms" => {
                    *micros += (q * 1_000.0).round() as i64;
                }
                "microsecond" | "microseconds" | "usec" | "usecs" | "us" => {
                    *micros += q.round() as i64;
                }
                _ => return Err(bad()),
            }
            Ok(())
        };
        apply(&mut months, &mut days, &mut micros)?;
        i += 1;
    }
    let m = i32::try_from(months).map_err(|_| "interval out of range".to_string())?;
    let d = i32::try_from(days).map_err(|_| "interval out of range".to_string())?;
    Ok((m, d, micros))
}

// -------------------------------------------------------------- formatting --

fn fmt_frac(us: i64) -> String {
    // trailing-zero-trimmed 6-digit fraction, '' when zero
    if us == 0 {
        return String::new();
    }
    let mut f = format!("{:06}", us);
    while f.ends_with('0') {
        f.pop();
    }
    if f.is_empty() {
        String::new()
    } else {
        format!(".{f}")
    }
}

pub fn fmt_date(days: i32) -> String {
    let (y, m, d) = civil_from_days(days);
    fmt_civil(y, m, d)
}

fn fmt_civil(y: i32, m: u32, d: u32) -> String {
    if (0..=9999).contains(&y) {
        format!("{y:04}-{m:02}-{d:02}")
    } else {
        format!("{y:+06}-{m:02}-{d:02}")
    }
}

pub fn fmt_time(us: i64) -> String {
    let h = us / 3_600_000_000;
    let m = (us / 60_000_000) % 60;
    let s = (us / 1_000_000) % 60;
    format!("{h:02}:{m:02}:{s:02}{}", fmt_frac(us.rem_euclid(1_000_000)))
}

pub fn fmt_timestamp(us: i64) -> String {
    let (days, tod) = split_ts(us);
    format!("{} {}", fmt_date(days), fmt_time(tod))
}

pub fn fmt_interval(months: i32, days: i32, micros: i64) -> String {
    let mut parts: Vec<String> = Vec::new();
    let y = months / 12;
    let mo = months % 12;
    if y != 0 {
        parts.push(format!("{y} year{}", if y == 1 { "" } else { "s" }));
    }
    if mo != 0 {
        parts.push(format!("{mo} mon{}", if mo == 1 { "" } else { "s" }));
    }
    if days != 0 {
        parts.push(format!("{days} day{}", if days == 1 { "" } else { "s" }));
    }
    if micros != 0 || parts.is_empty() {
        let neg = micros < 0;
        let a = micros.unsigned_abs();
        let h = a / 3_600_000_000;
        let m = (a / 60_000_000) % 60;
        let s = (a / 1_000_000) % 60;
        parts.push(format!(
            "{}{:02}:{:02}:{:02}{}",
            if neg { "-" } else { "" },
            h,
            m,
            s,
            fmt_frac((a % 1_000_000) as i64)
        ));
    }
    parts.join(" ")
}

// ------------------------------------------------------------- clock input --

pub fn now_us() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_micros() as i64)
        .unwrap_or(0)
}

/// Interval length for ordering: nominal 30-day months (PG semantics).
pub fn interval_total_us(months: i32, days: i32, micros: i64) -> i64 {
    months as i64 * US_PER_NOMINAL_MONTH + days as i64 * US_PER_DAY + micros
}

// ------------------------------------------------------- extract / trunc ---

const MONTH_NAMES: [&str; 12] = [
    "January", "February", "March", "April", "May", "June", "July", "August",
    "September", "October", "November", "December",
];
const DAY_NAMES: [&str; 7] = [
    "Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday",
];

/// Extracts a field from a temporal value given as (days, tod_us, kind)
/// where kind: 0 = date, 1 = time, 2 = timestamp, 3 = interval(m,d,us).
pub fn extract_field(field: &str, v: &crate::engine::Value) -> Result<f64, String> {
    use crate::engine::Value;
    let f = field.to_lowercase();
    let r = match v {
        Value::Date(days) => {
            let (y, m, d) = civil_from_days(*days);
            match f.as_str() {
                "year" => y as f64,
                "month" => m as f64,
                "day" => d as f64,
                "quarter" => ((m - 1) / 3 + 1) as f64,
                "dow" => day_of_week(*days) as f64,
                "doy" => day_of_year(*days) as f64,
                "epoch" => *days as f64 * 86_400.0,
                "century" => ((y - 1) / 100 + 1) as f64,
                _ => return Err(format!("interval field \"{f}\" not supported")),
            }
        }
        Value::Time(us) => match f.as_str() {
            "hour" => (us / 3_600_000_000) as f64,
            "minute" => ((us / 60_000_000) % 60) as f64,
            "second" => (us % 60_000_000) as f64 / 1e6,
            "epoch" => *us as f64 / 1e6,
            _ => return Err(format!("interval field \"{f}\" not supported")),
        },
        Value::Timestamp(us) => {
            let (days, tod) = split_ts(*us);
            let (y, m, d) = civil_from_days(days);
            match f.as_str() {
                "year" => y as f64,
                "month" => m as f64,
                "day" => d as f64,
                "quarter" => ((m - 1) / 3 + 1) as f64,
                "dow" => day_of_week(days) as f64,
                "doy" => day_of_year(days) as f64,
                "hour" => (tod / 3_600_000_000) as f64,
                "minute" => ((tod / 60_000_000) % 60) as f64,
                "second" => (tod % 60_000_000) as f64 / 1e6,
                "epoch" => *us as f64 / 1e6,
                "century" => ((y - 1) / 100 + 1) as f64,
                _ => return Err(format!("interval field \"{f}\" not supported")),
            }
        }
        Value::Interval { months, days, micros } => match f.as_str() {
            "year" => (*months / 12) as f64,
            "month" => (*months % 12) as f64,
            "day" => *days as f64,
            "hour" => (micros / 3_600_000_000) as f64,
            "minute" => ((micros / 60_000_000) % 60) as f64,
            "second" => (micros % 60_000_000) as f64 / 1e6,
            "epoch" => interval_total_us(*months, *days, *micros) as f64 / 1e6,
            _ => return Err(format!("interval field \"{f}\" not supported")),
        },
        _ => {
            return Err(format!(
                "cannot extract from {}",
                crate::engine::type_name(v)
            ))
        }
    };
    Ok(r)
}

/// 0 = Sunday .. 6 = Saturday (PG dow).
pub fn day_of_week(days: i32) -> i32 {
    // 1970-01-01 was a Thursday (4)
    (days + 4).rem_euclid(7)
}

/// 1..=366 (PG doy).
pub fn day_of_year(days: i32) -> i32 {
    let (y, _, _) = civil_from_days(days);
    days - days_from_civil(y, 1, 1) + 1
}

pub fn date_trunc(field: &str, us: i64) -> Result<i64, String> {
    let f = field.to_lowercase();
    let (days, tod) = split_ts(us);
    let (y, m, _d) = civil_from_days(days);
    let (nd, ntod) = match f.as_str() {
        "microseconds" => (days, tod),
        "milliseconds" => (days, tod / 1_000 * 1_000),
        "second" => (days, tod / 1_000_000 * 1_000_000),
        "minute" => (days, tod / 60_000_000 * 60_000_000),
        "hour" => (days, tod / 3_600_000_000 * 3_600_000_000),
        "day" => (days, 0),
        "week" => {
            // truncate to Monday
            let back = (day_of_week(days) + 6) % 7;
            (days - back, 0)
        }
        "month" => (days_from_civil(y, m, 1), 0),
        "quarter" => (days_from_civil(y, ((m - 1) / 3) * 3 + 1, 1), 0),
        "year" => (days_from_civil(y, 1, 1), 0),
        _ => return Err(format!("interval field \"{f}\" not supported")),
    };
    Ok(join_ts(nd, ntod))
}

/// to_char with the common PG tokens. Returns Err for unsupported values.
pub fn to_char(v: &crate::engine::Value, fmt: &str) -> Result<String, String> {
    use crate::engine::Value;
    let (days, tod): (Option<i32>, i64) = match v {
        Value::Date(d) => (Some(*d), 0),
        Value::Time(t) => (None, *t),
        Value::Timestamp(us) => {
            let (d, t) = split_ts(*us);
            (Some(d), t)
        }
        _ => {
            return Err(format!(
                "to_char: unsupported value type {}",
                crate::engine::type_name(v)
            ))
        }
    };
    let (y, m, d) = days.map(civil_from_days).unwrap_or((0, 0, 0));
    let h24 = tod / 3_600_000_000;
    let mi = (tod / 60_000_000) % 60;
    let ss = (tod / 1_000_000) % 60;
    let us = tod.rem_euclid(1_000_000);
    let h12 = if h24 % 12 == 0 { 12 } else { h24 % 12 };
    let ampm = |lower: bool, dots: bool| {
        let base = if h24 < 12 { "AM" } else { "PM" };
        let s = if dots {
            format!("{}.{}.", &base[..1], &base[1..])
        } else {
            base.to_string()
        };
        if lower { s.to_lowercase() } else { s }
    };
    let case_of = |pat: &str, out: &str| -> String {
        if pat.bytes().all(|c| c.is_ascii_lowercase()) {
            out.to_lowercase()
        } else if pat.bytes().all(|c| c.is_ascii_uppercase()) {
            out.to_uppercase()
        } else {
            out.to_string()
        }
    };
    let fb = fmt.as_bytes();
    let mut out = String::with_capacity(fmt.len() + 8);
    let mut i = 0;
    while i < fb.len() {
        let rest = &fmt[i..];
        let take = |n: usize, s: &str| -> Option<String> {
            if rest.len() >= n && rest[..n].eq_ignore_ascii_case(s) {
                Some(rest[..n].to_string())
            } else {
                None
            }
        };
        // longest-match table
        let tok: Option<(String, String)> = [
            ("YYYY", format!("{:04}", y.max(0))),
            ("YYY", format!("{:03}", (y.max(0)) % 1000)),
            ("YY", format!("{:02}", y.max(0) % 100)),
            ("MM", format!("{:02}", m)),
            ("DD", format!("{:02}", d)),
            ("DDD", format!("{:03}", days.map(day_of_year).unwrap_or(0))),
            ("HH24", format!("{:02}", h24)),
            ("HH12", format!("{:02}", h12)),
            ("HH", format!("{:02}", h12)),
            ("MI", format!("{:02}", mi)),
            ("SS", format!("{:02}", ss)),
            ("MS", format!("{:03}", us / 1_000)),
            ("US", format!("{:06}", us)),
            ("Month", case_of("Month", MONTH_NAMES[(m as usize).clamp(1, 12) - 1])),
            ("Mon", case_of("Mon", &MONTH_NAMES[(m as usize).clamp(1, 12) - 1][..3])),
            ("Day", case_of("Day", DAY_NAMES[days.map(day_of_week).unwrap_or(0) as usize])),
            ("Dy", case_of("Dy", &DAY_NAMES[days.map(day_of_week).unwrap_or(0) as usize][..3])),
            ("D", format!("{}", days.map(day_of_week).unwrap_or(0) + 1)),
        ]
        .iter()
        .find_map(|(p, o)| take(p.len(), p).map(|pt| (pt, o.clone())));
        if let Some((pat, rep)) = tok {
            out.push_str(&rep);
            i += pat.len();
            continue;
        }
        if let Some(body) = rest.strip_prefix('"') {
            // quoted literal
            let Some(close) = body.find('"') else {
                return Err("to_char: unterminated literal in format".into());
            };
            out.push_str(&body[..close]);
            i += close + 2;
            continue;
        }
        // AM/PM family
        let mut ampm_done = false;
        for (p, lower, dots) in [
            ("A.M.", false, true),
            ("P.M.", false, true),
            ("AM", false, false),
            ("PM", false, false),
            ("a.m.", true, true),
            ("p.m.", true, true),
            ("am", true, false),
            ("pm", true, false),
        ] {
            if rest.starts_with(p) {
                out.push_str(&ampm(lower, dots));
                i += p.len();
                ampm_done = true;
                break;
            }
        }
        if ampm_done {
            continue;
        }
        // plain char
        let ch = rest.chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

/// Calendar difference a - b, PG age() semantics: whole months first
/// (day-of-month guard), then days, then time-of-day. The span is computed
/// on the ordered pair and negated when a < b, which is what makes
/// age('2026-01-31','2026-03-01') = -1 mon -1 day (not -2 mons +30 days).
pub fn age_diff(a_us: i64, b_us: i64) -> (i32, i32, i64) {
    if a_us == b_us {
        return (0, 0, 0);
    }
    let (neg, hi, lo) = if a_us > b_us {
        (false, a_us, b_us)
    } else {
        (true, b_us, a_us)
    };
    let (hd, ht) = split_ts(hi);
    let (ld, lt) = split_ts(lo);
    let (hy, hm, hdy) = civil_from_days(hd);
    let (ly, lm, ldy) = civil_from_days(ld);
    let mut months = (hy as i64) * 12 + hm as i64 - ((ly as i64) * 12 + lm as i64);
    // a whole month beyond hi's day-of-month? then don't count it
    if (hdy, ht) < (ldy, lt) {
        months -= 1;
    }
    let base = add_months(lo, months as i32);
    let mut days = (hi - base) / US_PER_DAY;
    let mut tod = (hi - base) % US_PER_DAY;
    if tod < 0 {
        tod += US_PER_DAY;
        days -= 1;
    }
    let m = i32::try_from(months).unwrap_or(i32::MAX);
    let d = i32::try_from(days).unwrap_or(i32::MAX);
    if neg {
        (-m, -d, -tod)
    } else {
        (m, d, tod)
    }
}

// ------------------------------------------------------------------- tests --

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_roundtrip() {
        // sweep every day across ±800 years plus leap-year edges
        let start = days_from_civil(1969, 12, 30);
        for days in start..start + 600_000 {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days, "at {y}-{m}-{d}");
        }
        for &(y, m, d) in &[
            (1970, 1, 1),
            (2000, 2, 29),
            (1900, 2, 28),
            (2100, 2, 28),
            (2024, 2, 29),
            (1, 1, 1),
        ] {
            let z = days_from_civil(y, m, d);
            assert_eq!(civil_from_days(z), (y, m, d));
        }
    }

    #[test]
    fn known_dates() {
        assert_eq!(days_from_civil(1970, 1, 1), 0);
        assert_eq!(days_from_civil(1970, 1, 2), 1);
        assert_eq!(days_from_civil(1969, 12, 31), -1);
        assert_eq!(days_from_civil(2026, 10, 24), 20_750);
        // 2026-10-24 is a Saturday
        assert_eq!(day_of_week(days_from_civil(2026, 10, 24)), 6);
        assert_eq!(day_of_week(0), 4); // 1970-01-01 Thursday
        assert_eq!(day_of_year(days_from_civil(2024, 3, 1)), 61);
        assert_eq!(day_of_year(days_from_civil(2023, 3, 1)), 60);
    }

    #[test]
    fn date_parse_fmt() {
        assert_eq!(parse_date("2026-10-24").unwrap(), 20_750);
        assert_eq!(fmt_date(20_750), "2026-10-24");
        assert_eq!(parse_date("2026-1-2").unwrap(), days_from_civil(2026, 1, 2));
        assert!(parse_date("2026-02-30").is_err());
        assert!(parse_date("2026-13-01").is_err());
        assert!(parse_date("not a date").is_err());
        assert!(parse_date("").is_err());
        // leap days
        assert!(parse_date("2024-02-29").is_ok());
        assert!(parse_date("2023-02-29").is_err());
    }

    #[test]
    fn time_parse_fmt() {
        assert_eq!(parse_time("17:30:00").unwrap(), 63_000_000_000);
        assert_eq!(parse_time("17:30").unwrap(), 63_000_000_000);
        assert_eq!(parse_time("04:05:06.5").unwrap(), 14_706_500_000);
        assert_eq!(parse_time("04:05:06.123456").unwrap(), 14_706_123_456);
        assert_eq!(fmt_time(63_000_000_000), "17:30:00");
        assert_eq!(fmt_time(14_706_123_456), "04:05:06.123456");
        assert_eq!(fmt_time(14_706_500_000), "04:05:06.5");
        assert!(parse_time("25:00:00").is_err());
        assert!(parse_time("12:60").is_err());
        assert!(parse_time("").is_err());
    }

    #[test]
    fn timestamp_parse_fmt() {
        let us = parse_timestamp("2026-10-24 17:30:00").unwrap();
        assert_eq!(us, 20_750 * US_PER_DAY + 63_000_000_000);
        assert_eq!(fmt_timestamp(us), "2026-10-24 17:30:00");
        assert_eq!(parse_timestamp("2026-10-24").unwrap(), 20_750 * US_PER_DAY);
        assert_eq!(
            parse_timestamp("2026-10-24T04:05:06.123456").unwrap(),
            20_750 * US_PER_DAY + 14_706_123_456
        );
        assert_eq!(fmt_timestamp(20_750 * US_PER_DAY + 14_706_123_456),
                   "2026-10-24 04:05:06.123456");
        assert!(parse_timestamp("2026-10-24 25:00").is_err());
        assert!(parse_timestamp("garbage").is_err());
    }

    #[test]
    fn interval_parse_fmt() {
        assert_eq!(parse_interval("1 day").unwrap(), (0, 1, 0));
        assert_eq!(parse_interval("2 years 3 mons").unwrap(), (27, 0, 0));
        assert_eq!(parse_interval("-2 days").unwrap(), (0, -2, 0));
        assert_eq!(parse_interval("04:05:06").unwrap(), (0, 0, 14_706_000_000));
        assert_eq!(parse_interval("-04:05:06").unwrap(), (0, 0, -14_706_000_000));
        assert_eq!(parse_interval("1.5 hours").unwrap(), (0, 0, 5_400_000_000));
        assert_eq!(parse_interval("90 seconds").unwrap(), (0, 0, 90_000_000));
        assert_eq!(parse_interval("1 week").unwrap(), (0, 7, 0));
        assert_eq!(parse_interval("").unwrap(), (0, 0, 0));
        // fractional month: 1.5 mons = 1 mon 15 days
        assert_eq!(parse_interval("1.5 mons").unwrap(), (1, 15, 0));
        assert!(parse_interval("1 fortnight").is_err());
        assert!(parse_interval("blarg").is_err());
        assert_eq!(fmt_interval(1, 2, 3_000_000_000), "1 mon 2 days 00:50:00");
        assert_eq!(fmt_interval(0, 1, 0), "1 day");
        assert_eq!(fmt_interval(0, 0, 0), "00:00:00");
        assert_eq!(fmt_interval(13, 0, 0), "1 year 1 mon");
        assert_eq!(fmt_interval(0, 0, -14_706_000_000), "-04:05:06");
        assert_eq!(fmt_interval(24, 0, 0), "2 years");
    }

    #[test]
    fn month_arithmetic() {
        // Jan 31 + 1 mon clamps to Feb 28
        let jan31 = join_ts(days_from_civil(2026, 1, 31), 0);
        let feb = add_months(jan31, 1);
        assert_eq!(fmt_timestamp(feb), "2026-02-28 00:00:00");
        // leap year clamps to 29
        let jan31_24 = join_ts(days_from_civil(2024, 1, 31), 0);
        assert_eq!(fmt_timestamp(add_months(jan31_24, 1)), "2024-02-29 00:00:00");
        // across year boundary
        let dec = join_ts(days_from_civil(2026, 12, 15), 0);
        assert_eq!(fmt_timestamp(add_months(dec, 2)), "2027-02-15 00:00:00");
        assert_eq!(fmt_timestamp(add_months(dec, -2)), "2026-10-15 00:00:00");
    }

    #[test]
    fn trunc_extract() {
        let us = parse_timestamp("2026-10-24 17:30:45.123456").unwrap();
        assert_eq!(fmt_timestamp(date_trunc("day", us).unwrap()), "2026-10-24 00:00:00");
        assert_eq!(fmt_timestamp(date_trunc("hour", us).unwrap()), "2026-10-24 17:00:00");
        assert_eq!(fmt_timestamp(date_trunc("minute", us).unwrap()), "2026-10-24 17:30:00");
        assert_eq!(fmt_timestamp(date_trunc("second", us).unwrap()), "2026-10-24 17:30:45");
        assert_eq!(fmt_timestamp(date_trunc("month", us).unwrap()), "2026-10-01 00:00:00");
        assert_eq!(fmt_timestamp(date_trunc("year", us).unwrap()), "2026-01-01 00:00:00");
        // week truncates to Monday: 2026-10-24 Sat -> Mon 2026-10-19
        assert_eq!(fmt_timestamp(date_trunc("week", us).unwrap()), "2026-10-19 00:00:00");
        assert_eq!(fmt_timestamp(date_trunc("quarter", us).unwrap()), "2026-10-01 00:00:00");
        assert!(date_trunc("fortnight", us).is_err());

        let v = crate::engine::Value::Timestamp(us);
        assert_eq!(extract_field("year", &v).unwrap(), 2026.0);
        assert_eq!(extract_field("month", &v).unwrap(), 10.0);
        assert_eq!(extract_field("day", &v).unwrap(), 24.0);
        assert_eq!(extract_field("hour", &v).unwrap(), 17.0);
        assert_eq!(extract_field("second", &v).unwrap(), 45.123456);
        assert_eq!(extract_field("dow", &v).unwrap(), 6.0);
        let dv = crate::engine::Value::Date(days_from_civil(2024, 3, 1));
        assert_eq!(extract_field("doy", &dv).unwrap(), 61.0);
        assert!(extract_field("fortnight", &v).is_err());
    }

    #[test]
    fn to_char_cases() {
        let us = parse_timestamp("2026-10-24 17:30:45.123456").unwrap();
        let v = crate::engine::Value::Timestamp(us);
        let tc = |f: &str| to_char(&v, f).unwrap();
        assert_eq!(tc("YYYY-MM-DD"), "2026-10-24");
        assert_eq!(tc("DD Mon YYYY"), "24 Oct 2026");
        assert_eq!(tc("Day, Mon DD YYYY"), "Saturday, Oct 24 2026");
        assert_eq!(tc("HH12:MI AM"), "05:30 PM");
        assert_eq!(tc("HH24:MI:SS.US"), "17:30:45.123456");
        assert_eq!(tc("HH:MI:SS MS"), "05:30:45 123");
        assert_eq!(tc("YYYY\"FY\"MM"), "2026FY10");
        let dv = crate::engine::Value::Date(days_from_civil(2026, 1, 5));
        assert_eq!(to_char(&dv, "Dy DD Mon").unwrap(), "Mon 05 Jan");
        let tv = crate::engine::Value::Time(63_000_000_000);
        assert_eq!(to_char(&tv, "HH24:MI").unwrap(), "17:30");
    }

    fn fmt_iv(iv: (i32, i32, i64)) -> String {
        fmt_interval(iv.0, iv.1, iv.2)
    }

    #[test]
    fn age_cases() {
        let a = parse_timestamp("2026-03-01 00:00:00").unwrap();
        let b = parse_timestamp("2026-01-31 00:00:00").unwrap();
        assert_eq!(fmt_iv(age_diff(a, b)), "1 mon 1 day");
        let a2 = parse_timestamp("2026-10-24 12:00:00").unwrap();
        let b2 = parse_timestamp("2020-01-01 00:00:00").unwrap();
        assert_eq!(fmt_iv(age_diff(a2, b2)), "6 years 9 mons 23 days 12:00:00");
        let a3 = parse_timestamp("2026-01-31 00:00:00").unwrap();
        let b3 = parse_timestamp("2026-03-01 00:00:00").unwrap();
        assert_eq!(fmt_iv(age_diff(a3, b3)), "-1 mons -1 days");
    }
}
