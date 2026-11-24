library(testthat)
library(data.table)
source('R/utils/era_dates.R')
source('R/utils/era_release_validation.R')
fixture <- function(held = FALSE) {
  root <- tempfile('era-release-test-'); dir.create(root)
  data <- lapply(era_date_fields, function(fields) {
    tab <- data.table(B.Code = c('IE1', 'MH1'))
    for (field in fields) set(tab, j = field, value = as.Date(c('2019-12-21', '2005-07-17')))
    tab
  })
  if (held) data$Data.Out$PD.Plant.Start[2] <- as.Date('9999-01-01')
  audit <- data.table(table = 'Data.Out', field = 'PD.Plant.Start', source_row = 1L,
    merged_row = 2L, B.Code = 'MH1', raw = '38550', date = as.Date('2005-07-17'), status = 'excel_serial')
  path <- file.path(root, 'agronomic_test.json')
  jsonlite::write_json(data, path, dataframe = 'rows', Date = 'ISO8601', auto_unbox = TRUE)
  era_merge <- data; save(era_merge, file = sub('[.]json$', '.RData', path))
  source <- file.path(root, 'input.txt'); writeLines('pinned source', source)
  era_write_date_receipt(path, data, audit, source)
  list(root = root, data = data, audit = audit, path = path, source = source)
}

test_that('valid release requires exact JSON, audit and holds hashes', {
  f <- fixture(); expect_silent(era_validate_agronomic_json(f$path))
  writeLines('changed', sub('[.]json$', '-date-audit.csv', f$path))
  expect_error(era_validate_agronomic_json(f$path), 'hash mismatch')
})
test_that('shifted dates and lost IE dates cannot receive receipts', {
  f <- fixture(); shifted <- copy(f$data)
  shifted$Data.Out$PD.Plant.Start <- shifted$Data.Out$PD.Plant.Start + 25569
  expect_error(era_write_date_receipt(f$path, shifted, f$audit, f$source), 'Future calendar')
  lost <- copy(f$data); lost$Data.Out$PD.Plant.Start[1] <- as.Date(NA)
  jsonlite::write_json(lost, f$path, dataframe = 'rows', Date = 'ISO8601', auto_unbox = TRUE)
  expect_error(era_write_date_receipt(f$path, f$data, f$audit, f$source), 'round-trip')
})
test_that('a changed JSON and malformed calendar dates are rejected', {
  f <- fixture(); writeLines('{}', f$path)
  expect_error(era_validate_agronomic_json(f$path), 'hash mismatch')
  x <- f$data; x$Data.Out$PD.Plant.Start <- c('2011-02-30', '38550')
  expect_error(era_check_calendar_dates(x, data.frame()), 'Invalid ISO')
  x$Data.Out$PD.Plant.Start <- c(15330, 12981)
  expect_error(era_check_calendar_dates(x, data.frame()), 'Non-ISO')
})
test_that('unresolved years require exact row-linked holds and remain unchanged', {
  f <- fixture(held = TRUE)
  expect_silent(era_validate_agronomic_json(f$path))
  h <- fread(sub('[.]json$', '-date-holds.csv', f$path), colClasses = 'character')
  expect_identical(h$date, '9999-01-01')
  expect_error(era_check_calendar_dates(f$data, h[0]), 'Unrecorded year hold')
  h$merged_row <- '1'
  expect_error(era_check_calendar_dates(f$data, h), 'Unrecorded year hold')
})
test_that('a bad batch invokes no uploader even with good files first', {
  f <- fixture(); bad <- file.path(f$root, 'agronomic_unvalidated.json'); file.copy(f$path, bad)
  calls <- 0L
  uploader <- function(files, ...) { calls <<- calls + 1L; files }
  expect_error(era_upload_files_checked(c(f$path, bad), upload = uploader), 'Missing date-validation')
  expect_identical(calls, 0L)
  expect_identical(era_upload_files_checked(f$path, upload = uploader), f$path)
  expect_identical(calls, 1L)
})
test_that('package membership is exact, dated outputs immutable and archives checked', {
  f <- fixture(); comparisons <- file.path(f$root, 'era_compiled-test.parquet')
  codes <- file.path(f$root, 'era_master_codes-test.json')
  writeBin(as.raw(1:10), comparisons); writeLines('{}', codes)
  inputs <- list(json = f$path, comparisons = comparisons, codes = codes)
  out <- file.path(f$root, 'packaged')
  paths <- era_package_agronomy(inputs, out, 'test')
  expect_length(paths, 2L)
  expect_identical(era_sha256(paths[1]), era_sha256(paths[2]))
  expect_silent(era_validate_agronomy_bundle(paths[1]))
  expect_error(era_package_agronomy(inputs, out, 'test'), 'already exists')
  scratch <- file.path(f$root, 'tampered'); dir.create(scratch); untar(paths[1], exdir = scratch)
  writeLines('versions,sha256\nwrong.csv,wrong', file.path(scratch, 'era_agronomy_version.csv'))
  bad <- file.path(out, 'era_agronomy_bundle_tampered.tar.gz')
  old <- setwd(scratch); tar(bad, files = list.files('.'), compression = 'gzip', tar = 'internal'); setwd(old)
  expect_error(era_validate_agronomy_bundle(bad), 'manifest mismatch')
})

test_that('standalone RData must match the same validated producer output', {
  f <- fixture(); path <- sub('[.]json$', '.RData', f$path)
  expect_silent(era_validate_uploads(path))
  writeLines('unrelated replacement', path)
  expect_error(era_validate_uploads(path), 'RData validation hash mismatch')
})
