# ERA Data Packaging and Upload Script ####
# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)' 
#
# This script automates the process of preparing, archiving, and uploading the latest ERA agronomy and livestock datasets.
# 
# ## Main steps:
# 1. Loads necessary packages using `pacman::p_load`.
# 2. Locates the most recent agronomy and livestock data files (compiled comparisons, data models, and master codebooks).
# 3. For agronomy and livestock data:
#     - Creates a `.tar.gz` archive of the relevant files.
#     - Uploads the archive to an S3 bucket using `upload_files_to_s3`.
# 4. Additionally, for livestock:
#     - Reads and converts a `.RData` data model into JSON.
#     - Filters the compiled dataset to only include `"Animal"` entries and re-saves it.
# 
# Note:
# - S3 uploads are set to `"public-read"` mode.
# - A stub is included to archive and clean up older S3 files if needed.
# 

# Use p_load to install if not present and load the packages
pacman::p_load(s3fs,zip,arrow,miceadds,paws,jsonlite)

# 1) Agronomy ####
  ## Combine most recent data for ERA agronomy 
file_list<-list()

(file_list$era_comparisons<-tail(list.files(era_dirs$era_masterdata_dir,"era_compiled.*parquet",full.names = T),1))
(file_list$era_agronomy_dm<-tail(list.files(era_dirs$era_masterdata_dir,"agronomic.*json",full.names = T),1))
(file_list$era_master_codes<-tail(list.files(era_dirs$era_masterdata_dir,"era_master_codes.*json",full.names = T),1))

versions<-data.frame(versions=unlist(lapply(file_list,basename)))
file_list$version<-file.path(era_dirs$era_masterdata_dir,"era_agronomy_version.csv")
fwrite(versions,file_list$version)

# Create .tar.gz
file_archive<-file.path(era_dirs$era_packaged_dir,paste0("era_agronomy_bundle.tar.gz"))

# Set working directory temporarily to the files' location
old_wd <- setwd(dirname(file_list[[1]]))

# Create archive with files (basename only, no paths)
tar(
  tarfile = file_archive,
  files = basename(unlist(file_list)),
  compression = "gzip",
  tar = "internal"
)
# Reset working directory
setwd(old_wd)

# Remove version file
unlink(file_list$version)

 

# 2) Livestock ####
(file_list$era_livestock_dm<-tail(list.files(era_dirs$era_masterdata_dir,"skinny_cow.*RData",full.names = T),1))
livestock_dm<-load.Rdata2(basename(file_list$era_livestock_dm),dirname(file_list$era_livestock_dm))
file_list$era_livestock_dm<-gsub(".RData",".json",file_list$era_livestock_dm)
jsonlite::write_json(livestock_dm,file_list$era_livestock_dm)

  ## Combine most recent data for livestock
(file_list$era_comparisons<-tail(list.files(era_dirs$era_masterdata_dir,"era_compiled.*parquet",full.names = T),1))
(file_list$era_master_codes<-tail(list.files(era_dirs$era_masterdata_dir,"era_master_codes.*json",full.names = T),1))

# To do: Subset compiled data to livestock
compiled_livestock<-arrow::read_parquet(file_list$era_comparisons)
compiled_livestock<-compiled_livestock[Product.Type=="Animal"]

x<-(file_list$era_comparisons)
x<-gsub("era_compiled-","era_compiled_ls-",x)

arrow::write_parquet(compiled_livestock,x)

file_list$era_comparisons<-x

versions<-data.frame(versions=unlist(lapply(file_list,basename)))
file_list$version<-file.path(era_dirs$era_masterdata_dir,"era_agronomy_version.csv")
fwrite(versions,file_list$version)

# Create .tar.gz
file_archive<-file.path(era_dirs$era_packaged_dir,paste0("era_livestock_bundle.tar.gz"))

# Set working directory temporarily to the files' location
old_wd <- setwd(dirname(file_list[[1]]))

# Create archive with files (basename only, no paths)
tar(
  tarfile = file_archive,
  files = basename(unlist(file_list)),
  compression = "gzip",
  tar = "internal"
)
# Reset working directory
setwd(old_wd)

# 3) Upload ####

s3_bucket<-era_dirs$era_packaged_s3
folder_local<-era_dirs$era_packaged_dir

(files<-list.files(folder_local,full.names = T,recursive=F,include.dirs = F))

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read")

if(F){
  # Files no-longer present locally
  files_s3<-s3$dir_ls(s3_bucket)
  files_s3<-files_s3[files_s3!="s3://digital-atlas/era/data/archive"]
  files_2archive_from<-files_s3[!basename(files_s3) %in% basename(files)]
  files_2archive_to<-gsub("/packaged/","/packaged/archive/",files_2archive_from)
  
  s3fs::s3_file_copy(
    path = files_2archive_from,
    new_path = files_2archive_to
  )
  
  s3fs::s3_file_delete(
    path = files_2archive_from
  )
  
  s3$dir_ls(file.path(s3_bucket,"archive/"))
}

## Combine most recent data for ERA livestock ####
file_list<-list()