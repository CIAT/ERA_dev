# Offline integration of the actual package script; uploader is mocked.
# Usage: Rscript tests/replay_issue94_packaging.R validated.json comparisons.parquet codes.json livestock.RData empty-output-dir
library(data.table)
args <- commandArgs(TRUE)
if (length(args) != 5L) stop("Expected validated JSON, comparisons, codes, livestock RData and empty output directory")
paths <- normalizePath(args[1:4], mustWork = TRUE)
root <- normalizePath('.')
scratch <- args[5]
if (dir.exists(scratch) && length(list.files(scratch, all.files=TRUE, no..=TRUE))) stop("Output must be empty")
dir.create(scratch, recursive=TRUE, showWarnings=FALSE)
scratch <- normalizePath(scratch)
data_dir <- file.path(scratch, 'data'); dir.create(data_dir)
stopifnot(file.copy(paths[2], data_dir),
          file.copy(paths[3], file.path(data_dir, 'era_master_codes-test.json')),
          file.copy(paths[4], data_dir))
json <- paths[1]
stopifnot(length(json)==1L)
env <- new.env(); env$project_dir <- root
env$era_dirs <- list(era_masterdata_dir=data_dir,era_packaged_dir=file.path(scratch,'packaged'),era_packaged_s3='s3://test-not-used')
dir.create(env$era_dirs$era_packaged_dir)
writeLines('deliberately invalid old archive',file.path(env$era_dirs$era_packaged_dir,'era_agronomy_bundle_old.tar.gz'))
env$versions <- data.frame(versions='stale-input-must-not-leak')
env$era_agronomy_inputs <- list(json=json,comparisons=list.files(data_dir,'[.]parquet$',full.names=TRUE),codes=file.path(data_dir,'era_master_codes-test.json'))
uploaded <- character()
env$upload_files_to_s3 <- function(files, ...) {uploaded <<- files;invisible(TRUE)}
sys.source(file.path(root,'R/export/package_datasets.R'),envir=env)
stopifnot(length(uploaded)==4L,!any(grepl('_old',uploaded)))
for (file in uploaded[grepl('agronomy',uploaded)]) env$era_validate_agronomy_bundle(file)
ls_members <- untar(uploaded[grepl('livestock',uploaded)][1],list=TRUE)
stopifnot(!any(grepl('agronomic_',ls_members)), 'era_livestock_version.csv' %in% ls_members)
writeLines(uploaded,file.path(scratch,'mock-uploaded.txt'))
cat('PASS: actual packaging script builds both packages and uploads only four fresh validated archives; old decoy excluded.\n')
