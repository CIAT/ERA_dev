# Source era_dates.R first. These checks perform no remote writes.
era_sha256 <- function(path) digest::digest(file = path, algo = 'sha256')
era_receipt_path <- function(path) sub('[.]json$', '-validation.json', path)
era_date_fields <- list(
  Data.Out = c('PD.Plant.Start', 'PD.Plant.End', 'PD.Harvest.Start',
               'PD.Harvest.End', 'ED.Sample.Start', 'ED.Sample.End'),
  Fert.Method = 'F.Date', Res.Method = 'M.Date',
  PD.Out = c('PD.Date.Start', 'PD.Date.End'))

era_date_holds <- function(data, audit) {
  holds <- data.table::as.data.table(data.table::copy(audit))
  holds[, date := as.character(date)]
  holds <- holds[!status %in% c('date', 'excel_serial', 'dmy', 'iso')]
  for (table in names(data)) for (field in names(data[[table]])) {
    x <- data[[table]][[field]]
    if (!inherits(x, 'Date')) next
    year <- as.integer(format(x, '%Y'))
    rows <- which(!is.na(year) & (year < 1900 | year >= 9999))
    if (!length(rows)) next
    extra <- data.table::data.table(table = table, field = field,
      merged_row = rows, B.Code = data[[table]]$B.Code[rows],
      raw = era_date_iso(x[rows]), date = era_date_iso(x[rows]), status = 'unresolved_year')
    holds <- data.table::rbindlist(list(holds, extra), fill = TRUE)
  }
  unique(holds)
}

era_check_calendar_dates <- function(data, holds, today = Sys.Date()) {
  for (table in names(era_date_fields)) {
    if (is.null(data[[table]])) stop('Missing calendar table: ', table)
    for (field in era_date_fields[[table]]) {
      x <- data[[table]][[field]]
      if (is.null(x)) {
        # Sparse JSON may omit a wholly missing Date column. Producer round-trip
        # validation checks the original vector before issuing its receipt.
        next
      }
      if (inherits(x, 'Date')) {
        if (!typeof(x) %in% c('double', 'integer')) stop('Mixed Date storage: ', table, '.', field)
        text <- era_date_iso(x)
      } else {
        if (!is.character(x) && !(is.logical(x) && all(is.na(x))))
          stop('Non-ISO calendar field: ', table, '.', field)
        text <- as.character(x)
      }
      rows <- which(!is.na(text))
      if (!length(rows)) next
      values <- text[rows]
      parsed <- as.Date(values, format = '%Y-%m-%d')
      valid <- grepl('^[0-9]{4}-[0-9]{2}-[0-9]{2}$', values) &
        !is.na(parsed) & era_date_iso(parsed) == values
      if (any(!valid)) stop('Invalid ISO date: ', table, '.', field)
      year <- as.integer(substr(values, 1, 4))
      if (any(parsed > today & year < 9999))
        stop('Future calendar date (possible epoch shift): ', table, '.', field)
      exceptional <- which(year < 1900 | year >= 9999)
      for (i in exceptional) {
        match <- holds$table == table & holds$field == field &
          holds$merged_row == rows[i] & holds$B.Code == data[[table]]$B.Code[rows[i]] &
          holds$date == values[i] & holds$status == 'unresolved_year'
        if (!any(match, na.rm = TRUE)) stop('Unrecorded year hold: ', table, '.', field)
      }
    }
  }
  invisible(TRUE)
}

era_write_date_receipt <- function(json_path, data, audit, source_files) {
  audit_path <- sub('[.]json$', '-date-audit.csv', json_path)
  holds_path <- sub('[.]json$', '-date-holds.csv', json_path)
  holds <- era_date_holds(data, audit)
  era_check_calendar_dates(data, holds)
  restored <- jsonlite::fromJSON(json_path)
  for (table in names(data)) {
    if (nrow(restored[[table]]) != nrow(data[[table]])) stop('JSON row count changed: ', table)
    for (field in names(data[[table]])) {
      x <- data[[table]][[field]]
      if (!inherits(x, 'Date')) next
      actual <- restored[[table]][[field]]
      if (is.null(actual) && all(is.na(x))) actual <- rep(NA_character_, length(x))
      if (!identical(era_date_iso(x), as.character(actual)))
        stop('JSON date round-trip failed: ', table, '.', field)
    }
  }
  era_check_calendar_dates(restored, holds)
  data.table::fwrite(audit, audit_path)
  data.table::fwrite(holds, holds_path)
  pin <- function(f) list(name = basename(f), sha256 = era_sha256(f))
  rdata_path <- sub('[.]json$', '.RData', json_path)
  receipt <- list(rdata = if (file.exists(rdata_path)) pin(rdata_path) else NULL, schema = 'era.agronomy-date-validation/v1',
    json = pin(json_path), audit = pin(audit_path), holds = pin(holds_path),
    sources = lapply(source_files, pin),
    checked = list(calendar_dates = TRUE, date_roundtrip = TRUE),
    created_utc = format(Sys.time(), tz = 'UTC', usetz = TRUE))
  jsonlite::write_json(receipt, era_receipt_path(json_path), auto_unbox = TRUE, pretty = TRUE)
  invisible(receipt)
}

era_validate_agronomic_json <- function(path) {
  receipt_path <- era_receipt_path(path)
  if (!file.exists(receipt_path)) stop('Missing date-validation receipt: ', path)
  receipt <- jsonlite::read_json(receipt_path, simplifyVector = FALSE)
  if (!identical(receipt$schema, 'era.agronomy-date-validation/v1') ||
      !isTRUE(receipt$checked$calendar_dates) || !isTRUE(receipt$checked$date_roundtrip))
    stop('Invalid date-validation receipt: ', path)
  for (entry in c('json', 'audit', 'holds')) {
    pin <- receipt[[entry]]
    if (is.null(pin$name) || basename(pin$name) != pin$name) stop('Invalid receipt member')
    f <- file.path(dirname(path), pin$name)
    if (!file.exists(f) || !identical(era_sha256(f), pin$sha256))
      stop('Validation hash mismatch: ', f)
  }
  if (basename(path) != receipt$json$name) stop('Receipt describes another JSON file')
  holds <- data.table::fread(file.path(dirname(path), receipt$holds$name), colClasses = 'character')
  era_check_calendar_dates(jsonlite::fromJSON(path), holds)
  invisible(c(path, receipt_path, file.path(dirname(path), c(receipt$audit$name, receipt$holds$name))))
}

era_validate_agronomy_bundle <- function(path) {
  members <- utils::untar(path, list = TRUE)
  if (!length(members) || anyDuplicated(members) ||
      any(basename(members) != members | members %in% c('.', '..')))
    stop('Unsafe or duplicate archive members')
  scratch <- tempfile('era-bundle-check-'); dir.create(scratch)
  on.exit(unlink(scratch, recursive = TRUE), add = TRUE)
  utils::untar(path, exdir = scratch)
  json <- members[grepl('^agronomic_.*[.]json$', members) & !grepl('-validation[.]json$', members)]
  if (length(json) != 1L) stop('Bundle must contain exactly one agronomic JSON')
  era_validate_agronomic_json(file.path(scratch, json))
  version <- file.path(scratch, 'era_agronomy_version.csv')
  if (!file.exists(version)) stop('Missing package member manifest')
  manifest <- data.table::fread(version, colClasses = 'character')
  if (!all(c('versions', 'sha256') %in% names(manifest)) ||
      anyDuplicated(manifest$versions) || !setequal(manifest$versions, members))
    stop('Package member manifest mismatch')
  for (i in which(manifest$versions != basename(version))) {
    if (!identical(era_sha256(file.path(scratch, manifest$versions[i])), manifest$sha256[i]))
      stop('Package member hash mismatch: ', manifest$versions[i])
  }
  invisible(TRUE)
}

era_validate_uploads <- function(files) {
  for (path in files) {
    name <- basename(path)
    if (grepl('^agronomic_.*[.]json$', name) && !grepl('-validation[.]json$', name))
      era_validate_agronomic_json(path)
    if (grepl('^agronomic_.*[.]RData$', name)) {
      json <- sub('[.]RData$', '.json', path)
      era_validate_agronomic_json(json)
      receipt <- jsonlite::read_json(era_receipt_path(json))
      if (is.null(receipt$rdata) || !identical(receipt$rdata$name, name) ||
          !identical(receipt$rdata$sha256, era_sha256(path))) stop('RData validation hash mismatch: ', path)
    }
    if (grepl('^era_agronomy_bundle.*[.]tar[.]gz$', name)) era_validate_agronomy_bundle(path)
  }
  invisible(TRUE)
}

era_upload_files_checked <- function(files, ..., upload = upload_files_to_s3) {
  # Validate the entire batch before invoking an uploader even once.
  era_validate_uploads(files)
  upload(files = files, ...)
}

era_package_agronomy <- function(inputs, output_dir, release_id = as.character(Sys.Date())) {
  if (!setequal(names(inputs), c('comparisons', 'json', 'codes')) ||
      any(lengths(inputs) != 1L) || !all(file.exists(unlist(inputs)))) stop('Pin three existing package inputs')
  if (!grepl('^[A-Za-z0-9-]+$', release_id)) stop('Invalid release ID')
  payload <- unique(c(era_validate_agronomic_json(inputs$json), inputs$comparisons, inputs$codes))
  if (anyDuplicated(basename(payload))) stop('Duplicate package member name')
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  dated <- file.path(output_dir, paste0('era_agronomy_bundle_', release_id, '.tar.gz'))
  alias <- file.path(output_dir, 'era_agronomy_bundle.tar.gz')
  if (file.exists(dated)) stop('Versioned bundle already exists: ', dated)
  scratch <- tempfile('era-package-'); dir.create(scratch)
  on.exit(unlink(scratch, recursive = TRUE), add = TRUE)
  if (!all(file.copy(payload, scratch))) stop('Failed to stage package inputs')
  manifest <- data.table::data.table(versions = c(basename(payload), 'era_agronomy_version.csv'),
    sha256 = c(vapply(payload, era_sha256, character(1)), NA_character_))
  data.table::fwrite(manifest, file.path(scratch, 'era_agronomy_version.csv'))
  temp_archive <- tempfile(tmpdir = normalizePath(output_dir), fileext = '.tar.gz')
  on.exit(unlink(temp_archive), add = TRUE)
  old <- setwd(scratch)
  tryCatch(utils::tar(temp_archive, files = manifest$versions, compression = 'gzip', tar = 'internal'),
           finally = setwd(old))
  era_validate_agronomy_bundle(temp_archive)
  if (!file.copy(temp_archive, dated) || !file.copy(temp_archive, alias, overwrite = TRUE)) stop('Package save failed')
  c(dated, alias)
}
