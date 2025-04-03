# Use p_load to install if not present and load the packages
pacman::p_load(s3fs,zip,arrow,miceadds,paws,jsonlite)


# Combine most recent data for ERA agronomy ####
file_list<-list()

(file_list$era_comparisons<-tail(list.files(era_dirs$era_masterdata_dir,"era_compiled.*parquet",full.names = T),1))
(file_list$era_agronomy_dm<-tail(list.files(era_dirs$era_masterdata_dir,"agronomic.*json",full.names = T),1))
(file_list$era_master_codes<-tail(list.files(era_dirs$era_masterdata_dir,"era_master_codes.*json",full.names = T),1))

# Create .tar.gz
file_archive<-file.path(era_dirs$era_packaged_dir,paste0("era_agronomy_bundle-",Sys.Date(),".tar.gz"))

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

# Upload ####

s3_bucket<-era_dirs$era_packaged_s3
folder_local<-era_dirs$era_packaged_dir

(files<-list.files(folder_local,full.names = T,recursive=F,include.dirs = F))

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F,
                   mode="public-read")

if(F){
  # Files no-longer present locally
  files_s3<-s3$dir_ls(s3_bucket)
  files_s3<-files_s3[files_s3!="s3://digital-atlas/era/data/archive"]
  files_2archive_from<-files_s3[!basename(files_s3) %in% basename(files)]
  files_2archive_to<-gsub("/data/","/data/archive/",files_2archive_from)
  
  s3fs::s3_file_copy(
    path = files_2archive_from,
    new_path = files_2archive_to
  )
  
  s3fs::s3_file_delete(
    path = files_2archive_from
  )
  
  s3$dir_ls(file.path(s3_bucket,"archive"))
}



# Combine most recent data for ERA climate ####
file_list<-list()
