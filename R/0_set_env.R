project_dir<-getwd()

# CGlabs server
era_dir<-"/home/jovyan/common_data/era"
setwd(era_dir)

# Working locally
era_dir<-"C:/rprojects/common_data/era"
if(!dir.exists(era_dir)){
  dir.create(era_dir,recursive=T)
}
setwd(era_dir)
