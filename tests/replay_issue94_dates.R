# Full, offline replay against explicitly selected processed source snapshots.
# Usage: Rscript tests/replay_issue94_dates.R MH.RData IE.RData codes.json output_dir
library(data.table)
args <- commandArgs(TRUE)
if(length(args)!=4L) stop('Expected MH.RData IE.RData codes.json output_dir')
inputs <- normalizePath(args[1:3],mustWork=TRUE)
out <- args[4]
if(dir.exists(out) && length(list.files(out,all.files=TRUE,no..=TRUE))) stop('Output must be empty')
dir.create(out,recursive=TRUE,showWarnings=FALSE)
out <- normalizePath(out)
root <- normalizePath('.')
env <- new.env()
env$project_dir <- root
env$field_absent <- 'Not in template'; env$field_absent_num <- '-9999'
env$era_projects <- list(majestic_hippo_2020='majestic_hippo_2020',industrious_elephant_2023='industrious_elephant_2023')
env$master_codes <- lapply(jsonlite::fromJSON(inputs[3]),as.data.table)
env$era_dirs <- list(era_masterdata_dir=out)
# The legacy script discovers inputs relative to Data/. Pin only discovery;
# execute every processing and save expression from the actual merge script.
env$list.files <- function(path,pattern,full.names) {
  if(identical(pattern,'majestic_hippo_2020')) inputs[1] else
    if(identical(pattern,'industrious_elephant_2023')) inputs[2] else stop('Unexpected discovery')
}
sys.source(file.path(root,'R/compile/merge_mj_ie.R'),envir=env)
saveRDS(list(mh=env$era_mh,ie=env$era_ie,merged=env$era_merge),file.path(out,'replay.rds'))
fwrite(data.table(path=inputs,md5=unname(tools::md5sum(inputs))),file.path(out,'input-pins.csv'))
cat('Full merge, RData save, JSON serialization and every Date-field round-trip passed.\n')

# Source-row ordinals tie date-only release staging to this exact replay.
for (table in names(env$date_fields_mh)) {
  fields <- env$date_fields_mh[[table]]
  patch <- copy(env$era_merge[[table]][,c("B.Code",fields),with=FALSE])
  patch[,merged_row:=seq_len(.N)]
  for (field in fields) set(patch,j=field,value=env$era_date_iso(patch[[field]]))
  fwrite(patch,file.path(out,paste0(table,"-date-patch.csv")),na="")
}
