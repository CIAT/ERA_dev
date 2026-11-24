# Decode only fields whose source schema identifies calendar dates. Never apply
# this to year labels, days-after-planting offsets or arbitrary numeric fields.
era_decode_legacy_dates <- function(x) {
  raw <- as.character(x)
  token <- trimws(raw)
  date <- rep(as.Date(NA), length(x))
  status <- rep("unsupported", length(x))
  missing <- is.na(token) | token %in% c("", "NA", "Not in template", "-9999")
  status[missing] <- "missing"
  if (inherits(x, "Date")) {
    date <- x
    status[!missing] <- "date"
  } else {
    if (!(is.character(x) || is.numeric(x) || is.factor(x) ||
          (is.logical(x) && all(is.na(x))))) stop("Unsupported calendar-date type")
    serial <- !missing & grepl("^[0-9]+([.]0+)?$", token)
    # Serial 60 is Excel's nonexistent 1900-02-29. Earlier values require a
    # different leap-year convention; leave them for explicit source review.
    number <- suppressWarnings(as.numeric(token))
    valid_serial <- serial & number >= 61 & number < 2958101
    date[valid_serial] <- as.Date(number[valid_serial], origin = "1899-12-30")
    status[serial] <- "unsupported_serial"
    status[valid_serial] <- "excel_serial"
    for (spec in list(c("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$", "%d/%m/%Y", "dmy"),
                      c("^[0-9]{4}-[0-9]{2}-[0-9]{2}$", "%Y-%m-%d", "iso"))) {
      idx <- which(!missing & grepl(spec[1], token))
      if (!length(idx)) next
      parts <- strsplit(token[idx], if (spec[3] == "dmy") "/" else "-", fixed = TRUE)
      canonical <- vapply(parts, function(p) {
        p <- as.integer(p)
        if (spec[3] == "dmy") sprintf("%04d-%02d-%02d", p[3], p[2], p[1]) else
          sprintf("%04d-%02d-%02d", p[1], p[2], p[3])
      }, character(1))
      parsed <- as.Date(canonical, format = "%Y-%m-%d")
      valid <- !is.na(parsed) & format(parsed, "%Y-%m-%d") == canonical
      date[idx[valid]] <- parsed[valid]
      status[idx] <- "invalid_calendar_date"
      status[idx[valid]] <- spec[3]
    }
    short_year <- !missing & grepl("^[0-9]{1,2}/[0-9]{1,2}/[0-9]{2}$", token)
    status[short_year] <- "ambiguous_two_digit_year"
  }
  # Preserve already typed dates, including unresolved sentinel-like years.
  # Report these separately; do not invent a calendar year or silently adopt
  # a universal meaning for 9999 (the documented time-label sentinel is 99999).
  unresolved <- !is.na(date) & as.integer(format(date, "%Y")) >= 9999
  status[unresolved] <- "unresolved_year"
  data.frame(date = date, raw = raw, status = status, stringsAsFactors = FALSE)
}

# Date JSON carries calendar days. Some existing IE Date values have fractional
# days; retain those in RData and compare the same day-only ISO representation.
era_date_iso <- function(x) {
  stopifnot(inherits(x, "Date"))
  result <- sprintf("%04d-%s", as.integer(format(x, "%Y")), format(x, "%m-%d"))
  result[is.na(x)] <- NA_character_
  result
}

era_bind_date_safe <- function(ie, mh, table) {
  date_fields <- union(names(ie)[vapply(ie, inherits, logical(1), "Date")],
                       names(mh)[vapply(mh, inherits, logical(1), "Date")])
  for (field in date_fields) {
    if (!inherits(ie[[field]], "Date") || !inherits(mh[[field]], "Date")) {
      stop(sprintf("Calendar-date type mismatch in %s.%s; decode before binding", table, field))
    }
  }
  result <- data.table::rbindlist(list(ie, mh), use.names = TRUE)
  for (field in date_fields) {
    if (!identical(result[[field]], c(ie[[field]], mh[[field]]))) {
      stop(sprintf("Calendar dates changed while binding %s.%s", table, field))
    }
  }
  result
}
