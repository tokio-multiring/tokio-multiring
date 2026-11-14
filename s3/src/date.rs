//! Dates as HTTP writes them.

const DAYS: [&str; 7] = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"];
const MONTHS: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Formats seconds since the Unix epoch as `Sun, 06 Nov 1994 08:49:37 GMT`.
pub(crate) fn http_date(seconds: i64) -> String {
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    // 1970-01-01 was a Thursday.
    let weekday = (days + 4).rem_euclid(7) as usize;

    // Days to civil date, by Howard Hinnant's algorithm: eras of 400 years
    // that start on March 1st, so that a leap day is the last of its year.
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era =
        (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let shifted_month = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * shifted_month + 2) / 5 + 1;
    let month = if shifted_month < 10 {
        shifted_month + 3
    } else {
        shifted_month - 9
    };
    let year = year_of_era + era * 400 + i64::from(month <= 2);

    format!(
        "{}, {day:02} {} {year:04} {:02}:{:02}:{:02} GMT",
        DAYS[weekday],
        MONTHS[month as usize - 1],
        second_of_day / 3_600,
        second_of_day / 60 % 60,
        second_of_day % 60,
    )
}

pub(crate) fn now() -> String {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64);
    http_date(now)
}

#[cfg(test)]
mod tests {
    use super::http_date;

    #[test]
    fn known_dates() {
        assert_eq!(http_date(0), "Thu, 01 Jan 1970 00:00:00 GMT");
        assert_eq!(http_date(784_111_777), "Sun, 06 Nov 1994 08:49:37 GMT");
        assert_eq!(http_date(951_782_400), "Tue, 29 Feb 2000 00:00:00 GMT");
        assert_eq!(http_date(951_868_799), "Tue, 29 Feb 2000 23:59:59 GMT");
        assert_eq!(http_date(951_868_800), "Wed, 01 Mar 2000 00:00:00 GMT");
        assert_eq!(http_date(1_794_700_800), "Sun, 15 Nov 2026 00:00:00 GMT");
        assert_eq!(http_date(4_107_542_399), "Sun, 28 Feb 2100 23:59:59 GMT");
        assert_eq!(http_date(4_107_542_400), "Mon, 01 Mar 2100 00:00:00 GMT");
        assert_eq!(http_date(-1), "Wed, 31 Dec 1969 23:59:59 GMT");
    }
}
