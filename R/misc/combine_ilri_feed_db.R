# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(data.table,install=TRUE)

update<-F

# Data is downloaded from: https://feedsdatabase.ilri.org/
db_file<-gsub(era_dirs$ilri_feed_db_S3,era_dirs$ilri_feed_db_dir,era_dirs$ilri_feed_db_file)

files<-list.files(era_dirs$ilri_feed_db_dir,".csv",full.names = T)
files<-files[basename(files)!=basename(db_file)]

ilri_feed_db<-rbindlist(lapply(1:length(files),FUN=function(i){
  file<-files[i]
  data<-fread(file)
  data[,feed_type:=gsub(".csv","",basename(file))]
  data
}),fill=T)

fwrite(ilri_feed_db,file=db_file)
