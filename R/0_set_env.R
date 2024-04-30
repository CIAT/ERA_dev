project_dir<-getwd()

# Set era s3 dir
era_s3<-"s3://digital-atlas/era"

# CGlabs server
era_dir<-"/home/jovyan/common_data/era"
setwd(era_dir)

# Working locally
era_dir<-"C:/rprojects/common_data/era"
if(!dir.exists(era_dir)){
  dir.create(era_dir,recursive=T)
}
setwd(era_dir)
