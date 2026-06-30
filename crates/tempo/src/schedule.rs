//! A deliberately minimal schedule parser for cron jobs.
//!
//! Two forms are supported (and documented for the operator in the console):
//!
//! - `@every <N><unit>` — a fixed interval. `unit` is `s` (seconds), `m` (minutes), or `h` (hours).
//!   e.g. `@every 30s`, `@every 5m`, `@every 2h`. The job fires when at least that long has elapsed
//!   since its last run (a brand-new job with `last_run_at = 0` fires on the next tick).
//! - `HH:MM` — once per day at that UTC wall-clock time, e.g. `09:30`. The job fires once after the
//!   most recent daily occurrence has passed and it has not yet run since then.
//!
//! This is NOT a full 5-field crontab. It covers the two cases the estate actually needs (periodic
//! pings + a daily job) without dragging in a cron-expression dependency. Anything else is rejected
//! at save time, so an unparseable schedule can never silently never-fire.

/// One parsed schedule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Schedule {
    /// Fire every `N` seconds (always `> 0`).
    Every(i64),
    /// Fire once per day at `HH:MM` UTC.
    DailyAt(u8, u8),
}

const SECS_PER_DAY: i64 = 86_400;

impl Schedule {
    /// Is this schedule due to fire, given the job's `last_run_at` and the current epoch `now`?
    pub fn is_due(&self, last_run_at: i64, now: i64) -> bool {
        match *self {
            Schedule::Every(secs) => now.saturating_sub(last_run_at) >= secs,
            Schedule::DailyAt(h, m) => {
                let midnight = now.div_euclid(SECS_PER_DAY) * SECS_PER_DAY;
                let mut scheduled = midnight + (h as i64) * 3600 + (m as i64) * 60;
                // If today's occurrence is still in the future, the most recent one was yesterday.
                if scheduled > now {
                    scheduled -= SECS_PER_DAY;
                }
                last_run_at < scheduled
            }
        }
    }

    /// A short human description for the console job list.
    pub fn describe(&self) -> String {
        match *self {
            Schedule::Every(secs) => format!("every {}", human_secs(secs)),
            Schedule::DailyAt(h, m) => format!("daily at {h:02}:{m:02} UTC"),
        }
    }
}

/// Parse a schedule spec into a [`Schedule`], or `None` when it is not a form we support.
pub fn parse(spec: &str) -> Option<Schedule> {
    let s = spec.trim();
    if let Some(rest) = s.strip_prefix("@every") {
        return parse_interval(rest.trim());
    }
    parse_daily(s)
}

/// Parse a `<N><unit>` interval (`30s` / `5m` / `2h`) into a positive second count.
fn parse_interval(spec: &str) -> Option<Schedule> {
    let spec = spec.trim();
    if spec.len() < 2 {
        return None;
    }
    let (num, unit) = spec.split_at(spec.len() - 1);
    let n: i64 = num.trim().parse().ok()?;
    if n <= 0 {
        return None;
    }
    let secs = match unit {
        "s" | "S" => n,
        "m" | "M" => n.checked_mul(60)?,
        "h" | "H" => n.checked_mul(3600)?,
        _ => return None,
    };
    Some(Schedule::Every(secs))
}

/// Parse a `HH:MM` daily time.
fn parse_daily(spec: &str) -> Option<Schedule> {
    let (h, m) = spec.split_once(':')?;
    let h: u8 = h.trim().parse().ok()?;
    let m: u8 = m.trim().parse().ok()?;
    if h < 24 && m < 60 {
        Some(Schedule::DailyAt(h, m))
    } else {
        None
    }
}

/// Render a second count as a compact `30s` / `5m` / `2h` label (falling back to seconds).
fn human_secs(secs: i64) -> String {
    if secs % 3600 == 0 {
        format!("{}h", secs / 3600)
    } else if secs % 60 == 0 {
        format!("{}m", secs / 60)
    } else {
        format!("{secs}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_interval_units() {
        assert_eq!(parse("@every 30s"), Some(Schedule::Every(30)));
        assert_eq!(parse("@every 5m"), Some(Schedule::Every(300)));
        assert_eq!(parse("@every 2h"), Some(Schedule::Every(7200)));
        // Tolerates no space after @every.
        assert_eq!(parse("@every90s"), Some(Schedule::Every(90)));
    }

    #[test]
    fn parses_daily_time() {
        assert_eq!(parse("09:30"), Some(Schedule::DailyAt(9, 30)));
        assert_eq!(parse("00:00"), Some(Schedule::DailyAt(0, 0)));
        assert_eq!(parse("23:59"), Some(Schedule::DailyAt(23, 59)));
    }

    #[test]
    fn rejects_garbage_and_out_of_range() {
        assert_eq!(parse(""), None);
        assert_eq!(parse("hourly"), None);
        assert_eq!(parse("@every 0s"), None);
        assert_eq!(parse("@every -5m"), None);
        assert_eq!(parse("@every 10d"), None);
        assert_eq!(parse("24:00"), None);
        assert_eq!(parse("12:60"), None);
    }

    #[test]
    fn interval_due_respects_last_run() {
        let s = Schedule::Every(60);
        // Never run -> due.
        assert!(s.is_due(0, 1_000_000));
        // Ran 30s ago -> not yet due.
        assert!(!s.is_due(1_000_000 - 30, 1_000_000));
        // Ran exactly 60s ago -> due.
        assert!(s.is_due(1_000_000 - 60, 1_000_000));
    }

    #[test]
    fn daily_fires_once_per_occurrence() {
        // 2026-01-01 is day index 20454; pick a midnight-aligned base.
        let day = 20_454_i64 * SECS_PER_DAY; // a UTC midnight
        let s = Schedule::DailyAt(9, 0);
        let nine_am = day + 9 * 3600;
        // Before 09:00 with no run today: the most recent occurrence was yesterday 09:00, and we
        // have not run since -> due (catch-up once).
        assert!(s.is_due(0, day + 3600));
        // At/after 09:00, not run since today's 09:00 -> due.
        assert!(s.is_due(nine_am - SECS_PER_DAY, nine_am + 60));
        // Already ran at 09:00 today -> not due again until tomorrow.
        assert!(!s.is_due(nine_am, nine_am + 3600));
    }
}
