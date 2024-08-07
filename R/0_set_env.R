# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(s3fs,paws.storage,rnaturalearth,terra,remotes,data.table)

if(!require(ERAg)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAg")
  library(ERAg)
}

if(!require(ERAgON)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAgON")
  library(ERAgON)
}

if(!require(openalexR)){
  devtools::install_github("https://github.com/ropensci/openalexR")
  library(openalexR)
}

# Use the isciences version and not the CRAN version of exactextractr
if(!require("exactextractr")){
  remotes::install_github("isciences/exactextractr")
}


source("https://raw.githubusercontent.com/CIAT/ERA_dev/main/R/functions.R")

# 1) Set directories ####
  if(!exists("project_dir")){
    project_dir<-getwd()
  }

  # 1.1) Set era s3 dir #####
  era_s3<-"s3://digital-atlas/era"
  era_s3_http<-"https://digital-atlas.s3.amazonaws.com/era"
  
  # If the project directory does not exist you will need to provide credentials to create it
  if(F){
    if(length(s3fs::s3_dir_ls(era_s3))==0){
      s3fs::s3_dir_create(era_s3)
    }
  }
  
  s3<-s3fs::S3FileSystem$new(anonymous = T)
  
  # 1.2) Set local working directory #####
  # CGlabs server
  CGlabs<-F
  if(project_dir=="/home/jovyan/rstudio/ERA_dev"){
    era_dir<-"/home/jovyan/common_data/era"
    CGlabs<-T
  }
  
  # Working locally
  if(project_dir=="C:/rprojects/ERA_dev"){
    era_dir<-"C:/rprojects/common_data/era"
  }
  
  Aflabs<-F
  if(project_dir=="/home/psteward/rprojects/ERA_dev"){
    Aflabs<-T
    era_dir<-"/cluster01/workspace/era"
  }
  
  
  if(!dir.exists(era_dir)){
    dir.create(era_dir,recursive=T)
  }
  setwd(era_dir)
  
  # 1.3) ERA projects #####
  era_projects<-list(v1.0_2018="v1.0_2018",
                     majestic_hippo_2020="majestic_hippo_2020",
                     skinny_cow_2022="skinny_cow_2022",
                     industrious_elephant_2023="industrious_elephant_2023",
                     livestock_2024="livestock_2024")
  
  # 1.4) Create ERA output dirs #####
  era_dirs<-list()
  
  # era master datasets
  era_dirs$era_masterdata_dir<-file.path(era_dir,"data")
  era_dirs$era_masterdata_s3<-file.path(era_s3,"data")
  
  # errordir
  era_dirs$era_error_dir<-file.path(era_dirs$era_masterdata_dir,"errors")
  
  # search history
  era_dirs$era_search_prj<-file.path(project_dir,"data/search_history")
  era_dirs$era_search_dir<-file.path(era_dir,"search_history")
  era_dirs$era_search_s3<-file.path(era_s3,"search_history")
  
  # data entry folders
  era_dirs$era_dataentry_prj<-file.path(project_dir,"data_entry")
  era_dirs$era_dataentry_dir<-file.path(era_dir,"data_entry")
  era_dirs$era_dataentry_s3<-file.path(era_s3,"data_entry")
  
  # extracted geodata folders
  era_dirs$era_geodata_dir<-file.path(era_dir,"geodata")
  era_dirs$era_geodata_s3<-file.path(era_s3,"geodata")
  
  # ancillary data folders
  era_dirs$ancillary_dir<-file.path(era_dir,"ancillary_datasets")
  era_dirs$ancillary_s3<-file.path(era_s3,"ancillary_datasets")
  
  era_dirs$dem_dir<-file.path(era_dirs$ancillary_dir,"dem_download")
  era_dirs$dem_s3<-file.path(era_dirs$ancillary_s3,"dem_download")
  
  era_dirs$soilgrid_dir<-file.path(era_dirs$ancillary_dir,"soilgrids_download")
  era_dirs$soilgrid_s3<-file.path(era_dirs$ancillary_s3,"soilgrids_download")
  
  era_dirs$power_dir<-file.path(era_dirs$ancillary_dir,"power_download")
  era_dirs$power_S3<-file.path(era_dirs$ancillary_s3,"power_download")
  era_dirs$power_S3_file<-file.path(era_dirs$power_S3,"power_download.zip")
  
  era_dirs$chirps_dir<-file.path(era_dirs$ancillary_dir,"chirps_download")
  era_dirs$chirps_S3<-file.path(era_dirs$ancillary_s3,"chirps_download")
  era_dirs$chirps_S3_file<-file.path(era_dirs$chirps_S3,"chirps_download.zip")
  
  # create folders if they do not exist
  for(i in grep("_dir",names(era_dirs))){
    dir_focus<-era_dirs[[i]]
    if(!dir.exists(dir_focus)){
      dir.create(dir_focus,recursive = T)
    }
  }

  # 1.5) Set urls #####
  era_vocab_url<-"https://github.com/peetmate/era_codes/raw/main/era_master_sheet.xlsx"
  # 1.4) Set directories of external datasets (e.g. chirps)
  if(CGlabs){
    chirps_dir<-"/home/jovyan/common_data/chirps_wrld"
    chirts_dir<-"/home/jovyan/common_data/chirts"
  }
  
  if(Aflabs){
    chirps_dir<-"/cluster01/workspace/common/climate/chirps/global_daily/tifs/p05"
    chirts_dir<-NA
  }
# 2) Download core datasets ####
  # 2.1) ERA master datasets #####
  update<-F
    # List files in the specified S3 bucket and prefix
    files_s3<-s3$dir_ls(era_dirs$era_masterdata_s3)
    files_local<-gsub(era_dirs$era_masterdata_s3,era_dirs$era_masterdata_dir,files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }
    
  # 2.2) ERA geodata #####
    update<-F
    # List files in the specified S3 bucket and prefix
    files_s3<-s3$dir_ls(era_dirs$era_geodata_s3)
    files_s3<-files_s3[!grepl(".csv|ESA-CCI",files_s3)]
    files_local<-gsub(era_dirs$era_geodata_s3,era_dirs$era_geodata_dir,files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }

  # 2.3) ERA master_codes #####
    era_vocab_local<-file.path(project_dir,"data/vocab/era_master_sheet.xlsx")
    
    update<-T
    if(update){
      download.file(era_vocab_url, era_vocab_local, mode = "wb")  # Download and write in binary mode
    }  
    
# 3) Create table of unique locations (for use with geodata functions) ####
    data<-arrow::read_parquet(file.path(era_dirs$era_masterdata_dir,"era.compiled.parquet"))
    era_locations<-list(unique(data[!(is.na(Latitude)|is.na(Longitude)|Buffer==0),list(Site.Key,Latitude,Longitude,Buffer,Country)]))
    
    # Add in other era extractions
    data<-miceadds::load.Rdata2(path=era_dirs$era_masterdata_dir,filename= list.files(era_dirs$era_masterdata_dir,"skinny_cow"))
    data<-data$Site.Out[,list(Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Site.Buffer.Manual,Country)]
    setnames(data,c("Site.LatD","Site.LonD","Site.Buffer.Manual"),c("Latitude","Longitude","Buffer"))
    data<-data[!(is.na(Latitude)|is.na(Longitude))]
    data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
    # Assign 5km buffer to missing buffer values
    data<-data[!(is.na(Buffer)|Buffer==0)]
    # Create key value
    data[,Site.Key:=paste0(sprintf("%07.4f",Latitude)," ",sprintf("%07.4f",Longitude)," B",Buffer)]
    # Remove unecessary cols
    data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
    
    era_locations[[2]]<-data
    
    data<-miceadds::load.Rdata2(path=era_dirs$era_masterdata_dir,filename= tail(list.files(era_dirs$era_masterdata_dir,"industrious_elephant"),1))
    data<-data$Site.Out[,list(Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Buffer.Manual,Country)]
    setnames(data,c("Site.LatD","Site.LonD","Buffer.Manual"),c("Latitude","Longitude","Buffer"))
    data<-data[!(is.na(Latitude)|is.na(Longitude))]
    data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
    # Assign 5km buffer to missing buffer values
    data<-data[!(is.na(Buffer)|Buffer==0)]
    # Create key value
    data[,Site.Key:=paste0(sprintf("%07.4f",Latitude)," ",sprintf("%07.4f",Longitude)," B",Buffer)]
    # Remove unecessary cols
    data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
    
    era_locations[[3]]<-data  
    
    era_locations<-rbindlist(era_locations,use.names = T)
    era_locations<-unique(era_locations[,Latitude:=round(Latitude,4)][,Longitude:=round(Longitude,4)])
    
    # Check for any duplicates
    era_locations[,N:=.N,by=Site.Key][N>1][order(Site.Key)]
    era_locations[,N:=NULL]
    
    # 3.1) Create spatvect of site buffers #####

    # Buffer points - projected - geographic
    era_locations_vect_g<-ERAg::Pbuffer(Data = era_locations,ID = "Site.Key" ,Projected=F)

    # 3.2) Get a vector of Africa #####
  
    # Get the vector data for Africa
    africa_vector <- rnaturalearth::ne_countries(continent = "Africa", returnclass = "sf")
    
    # Convert to terra SpatVector
    africa_vect <- terra::vect(africa_vector)
    
    # Create empty rast with extent of africa
    africa_rast <- terra::rast()
    ext(africa_rast) <- terra::ext(africa_rast)
    
# 4) Set time origin ####
    time_origin<-as.Date("1900-01-01")
    
    
    