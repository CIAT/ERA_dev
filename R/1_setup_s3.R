# First run R/0_set_env.R
# 0.1) load packages #####
if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(s3fs,zip,arrow,miceadds,paws,jsonlite)


# 1.1) (Legacy) upload era master files to s3 #####
s3_bucket<-era_dirs$era_masterdata_s3
local_dir<-era_dirs$era_masterdata_dir
# Inital set-up from old file system (delete when development is stable)
if(F){
  folder<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Compendium Master Database"
  
  files<-list.files(folder,full.names = T)
  files <- files[!file.info(files)$isdir]
  files<-grep("csv$|RData$",files,value=T)
  
  # Get most recent versions of data
  files1<-grep("V1.0",files,value=T)
  files1<-grep(".csv",files1,value = T)
  data<-fread(files1)
  arrow::write_parquet(data,file.path(folder,paste0("era_data_",era_projects$v1.0_2018,".parquet")))
  
  files1<-grep("V1.1",files,value=T)
  files1<-grep(".csv",files1,value = T)
  data<-fread(files1)
  arrow::write_parquet(data,file.path(folder,paste0("era_data_",era_projects$v1.1_2020,".parquet")))
  
  files1<-grep("V1.1 Tab",files,value=T)
  files1new<-file.path(folder,paste0("era_data_",era_projects$v1.1_2020,"_full.RData"))
  file.copy(files1,files1new,overwrite = T)
  
  files2<-grep("era_skinny",files,value=T)
  files2<-grep(".RData",files2,value = T)
  files2new<-file.path(folder,paste0("era_data_",era_projects$skinny_cow_2022,"_full.RData"))
  file.copy(files2,files2new,overwrite=T)
  
  
  folder1<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/ERA Current Version"
  files<-list.files(folder1,full.names = T)
  version<-names(read.delim(grep("Version.txt",files,value = T)))
  files<-grep("Wide",files,value = T)
  data<-data.table(miceadds::load.Rdata2(filename=basename(files),path=folder1))
  files3new<-file.path(folder1,"era.compiled.parquet")
  arrow::write_parquet(data,files3new)
  
  files<-c(list.files(folder,"parquet$",full.names = T),files1new,files2new,files3new)
}

  # 1.1.1) Compiled datasets
  files<-list.files(local_dir,"compiled",full.names = T)
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=F,
                     mode="public-read")
  
  # Code to remove old versions
  if(F){
  s3_files<-s3$dir_ls(s3_bucket)
  s3_files<-grep("compiled",s3_files,value=T)
  s3_files<-grep("skinny_cow",s3_files,value=T)
  s3_files<-s3_files[!grepl("_10_09",s3_files)]
  s3$file_delete(s3_files)
  }

# 1.2) upload data to s3 #####
  # 1.2.1) 2023 industrious elephant ######
    # 1.2.1.1) upload excels ########
    # where is the working folder for the ERA data extractions (internal team directory)
    folder_local<-"/Users/pstewarda/Library/CloudStorage/OneDrive-CGIAR/ERA/Data Entry/Data Entry 2023/Data/"
    project<-era_projects$industrious_elephant_2023
    folder<-file.path(era_dirs$era_dataentry_dir,
                      project,
                      "excel_files")
    
    if(!dir.exists(folder)){
      dir.create(folder,recursive = T)
    }
    
    # this is the target folder on the S3 bucket and generalized structured file system
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,
                         project,
                         "excel_files")
    
    # List excel files to be zipped
    files<-list.files(folder_local,full.names = T,recursive=T)
    files<- grep("xlsm$",files,value=T)
    files<-files[!grepl("~",files)]
    files<- grep("/Quality Controlled/|/Extracted/",files,value=T)
    
    # zip all the excels and upload to the s3
    output_zip_file <- file.path(folder,paste0(project,".zip"))
    
    zip::zipr(zipfile = output_zip_file, files =files)
    
    upload_files_to_s3(files = output_zip_file,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read")
    
    # 1.2.1.2) upload pdfs #####
    
    # 1.2.1.3) upload imported data #####
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$industrious_elephant_2023,full.names = T,recursive=T)
    files<-grep(".RData",files,value=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    # 1.2.1.4) comparisons #######
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$industrious_elephant_2023,full.names = T,recursive=T)
    files<-grep("comparisons",files,value=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    # 1.2.1.5) training materials #######
    folder_local<-file.path(era_dirs$era_dataentry_dir,era_projects$industrious_elephant_2023,"training_materials")
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,era_projects$industrious_elephant_2023,"training_materials")
    
    files<-list.files(folder_local,full.names = T,recursive=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
  # 1.2.2) 2022 skinny cow ######
    # 1.2.2.1) upload excels #######
    folder_local<-file.path("/Users/pstewarda/Library/CloudStorage/OneDrive-CGIAR/ERA/Data Entry/Data Entry 2022/Data",c("Extracted","Quality Controlled"))
    
    project<-era_projects$skinny_cow_2022
    folder<-file.path(era_dirs$era_dataentry_dir,
                      project,
                      "excel_files")
    
    if(!dir.exists(folder)){
      dir.create(folder,recursive = T)
    }
    
    # this is the target folder on the S3 bucket and generalized structured file system
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,
                         project,
                         "excel_files")
    
    # List excel files to be zipped
    
    files<-list.files(folder_local,full.names = T,recursive=T)
    files<-grep("csv$|RData$|zip$|xlsx$|xlsm$",files,value=T)
    files<- grep("xlsm$",files,value=T)
    files<-files[!grepl("~",files)]
    
    # zip all the excels and upload to the s3
    output_zip_file <- file.path(folder,paste0(project,".zip"))
    zip::zipr(zipfile = output_zip_file, files =files)
    
    upload_files_to_s3(files = output_zip_file,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read")
    
    # 1.2.2.2) upload imported data #######
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$skinny_cow_2022,full.names = T,recursive=T)
    files<-grep(".RData",files,value=T)
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")

    # 1.2.2.3) comparisons #######
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$skinny_cow_2022,full.names = T,recursive=T)
    files<-grep("comparisons",files,value=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    # 1.2.2.3) upload pdfs #######
  # 1.2.3) 2020 majestic_hippo ######
    # 1.2.3.1) upload pdfs #######
    local_folder<-"C://Users//PSteward//OneDrive - CGIAR//ERA//Bibliography//Reconstructed Search History for Classification Models//ERA Bibliography 2013-18//2013-18 ENL.Data//PDF"
    files<-list.files(pdf_folder,".pdf",recursive = T,full.names = T)
    
    # pdf folders
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,era_projects$majestic_hippo_2020,"pdfs")
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="private")
    
    # 1.2.3.2) upload excels #######
    # where is the working folder for the ERA data extractions (internal team directory)
    folder_local<-"/Users/pstewarda/Library/CloudStorage/OneDrive-CGIAR/ERA/Data Entry/Data Entry 2020/Quality controlled"
    project<-era_projects$majestic_hippo_2020
    folder<-file.path(era_dirs$era_dataentry_dir,
                      project,
                      "excel_files")
    
    if(!dir.exists(folder)){
      dir.create(folder,recursive = T)
    }
    
    # this is the target folder on the S3 bucket and generalized structured file system
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,
                         project,
                         "excel_files")
    
    # List excel files to be zipped
    files<-list.files(folder_local,full.names = T,recursive=T)
    
    # zip all the excels and upload to the s3
    output_zip_file <- file.path(folder,paste0(project,".zip"))
    
    zip::zipr(zipfile = output_zip_file, files =files)
    
    upload_files_to_s3(files = output_zip_file,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read")
    # 1.2.3.3) legacy harmonization files #######
    local_folder<-file.path("data_entry",era_projects$majestic_hippo_2020,"legacy_files")
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,era_projects$majestic_hippo_2020,"legacy_harmonization_files")
    
    files<-list.files(local_folder,recursive = T,full.names = T)
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    # 1.2.3.4) upload imported data #######
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$majestic_hippo_2020,full.names = T,recursive=T)
    files<-grep(".RData",files,value=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    # 1.2.3.5) comparisons #######
    folder_local<-era_dirs$era_masterdata_dir
    s3_bucket<-era_dirs$era_masterdata_s3
    
    files<-list.files(folder_local,era_projects$majestic_hippo_2020,full.names = T,recursive=T)
    files<-grep("comparisons",files,value=T)
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
  # 1.2.4) 2024 courageous_camel ######
    # 1.2.4.1) upload search data #######
folder<-file.path(era_dirs$era_search_dir,era_projects$courageous_camel_2024)
s3_bucket<-file.path(era_dirs$era_search_s3,era_projects$courageous_camel_2024)

files<-list.files(folder,full.names = T,recursive=T)

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F,
                   mode="public-read")


    # 1.2.4.2) upload excels ########
    # where is the working folder for the ERA data extractions (internal team directory)
    folder_local<-"/Users/pstewarda/Library/CloudStorage/GoogleDrive-peetmate@gmail.com/.shortcut-targets-by-id/1onn-IqY6kuHSboqNSZgEzggmKIv576BB/Data Entry 2024/"
    project<-era_projects$courageous_camel_2024
    folder<-file.path(era_dirs$era_dataentry_dir,
                      project,
                      "excel_files")
    
    if(!dir.exists(folder)){
      dir.create(folder,recursive = T)
    }
    
    # this is the target folder on the S3 bucket and generalized structured file system
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,
                         project,
                         "excel_files")
    
    # List excel files to be zipped
    files<-list.files(folder_local,full.names = T,recursive=T)
    files<- grep("xlsm$",files,value=T)
    files<-files[!grepl("~",files)]
    files<- grep("/Quality Controlled/|/Extracted/",files,value=T)
    
    # zip all the excels and upload to the s3
    output_zip_file <- file.path(folder,paste0(project,".zip"))
    
    zip::zipr(zipfile = output_zip_file, files =files)
    
    upload_files_to_s3(files = output_zip_file,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read")
  # 1.2.5) compiled dataset
    s3_bucket<-era_dirs$era_masterdata_s3
    folder_local<-era_dirs$era_masterdata_dir
    
    files<-list.files(folder_local,"era_compiled",full.names = T,recursive=T)
    files<-grep("comparisons",files,value=T)
    
    # PLEASE SELECT MOST RECENT VERSION ONLY
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")

# 1.4) Upload environmental data ####
  # 1.4.1) Aspect, slope, elevation ######
  s3_bucket<-era_dirs$era_geodata_s3
  
  # Inital set-up from old file system:  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Physical"
  if(F){
  files<- list.files(data_dir,".csv$",full.names = T)
  
  # Create parquet version
  data<-data.table::fread(files)
  arrow::write_parquet(data,gsub(basename(files),"era_site_topography.parquet",files))
  
  files<- list.files(data_dir,full.names = T)
  }else{
    files<-file.path(era_dirs$era_geodata_dir,"era_site_topography.parquet")
  }
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # 1.4.2) Climate (old file system) ######
  # Inital set-up from old file system:  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Physical"
  if(F){
  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Large Files"
  s3_bucket<-era_dirs$era_geodata_s3
  files<- list.files(data_dir,".RData$",full.names = T)
  
  files<-grep("CHIRPS|AgMERRA|ERA_SOS|POWER.CHIRPS|POWER",files,value=T)
  
  # CHIRPS
  files1<-grep("CHIRPS.RData",files,value=T)
  data<-data.table(miceadds::load.Rdata2(filename=basename(files1),path=data_dir))
  num.cols<-c("Temp.Mean","Temp.Min","Temp.Max","Solar.Rad","WindSpeed","RH.Max","Rain")
  data[, (num.cols) := lapply(.SD, round, digits = 1), .SDcols = num.cols]
  data[,DayCount:=as.integer(DayCount)][,Buffer:=as.integer(Buffer)]
  setnames(data,"Site.Key","Site.ID")
  data<-data[order(Site.ID,Latitude,Longitude,Buffer,DayCount)]
  arrow::write_parquet(data,gsub(".RData",".parquet",files1))
  
  # POWER
  files1<-grep("POWER",files,value=T)
  data<-data.table(miceadds::load.Rdata2(filename=basename(files1),path=data_dir))
  num.cols<-c("SRad","Specific.Humid","Temp.Min","Temp.Mean","Temp.Max","WindSpeed","Humid","Pressure","Rain",'Pressure.Adjusted')
  data[, (num.cols) := lapply(.SD, round, digits = 1), .SDcols = num.cols]
  data[,Buffer:=as.integer(Buffer)][,Altitude:=as.integer(round(Altitude,0))]
  data<-data[,list(Site.Key,Latitude,Longitude,Altitude,Buffer,NCells,DayCount,Year,Day,Date,Temp.Min,Temp.Mean,Temp.Max,Rain,Specific.Humid,Humid,
             WindSpeed,Pressure,Pressure.Adjusted,SRad,ETo)]
  data<-data[order(Site.Key,Latitude,Longitude,Buffer,Altitude,DayCount)]
  arrow::write_parquet(data,gsub(".RData",".parquet",files1))
  
  # POWERCHIPS
  files1<-grep("POWER.CHIRP",files,value=T)
  data<-data.table(miceadds::load.Rdata2(filename=basename(files1),path=data_dir))
  num.cols<-c("SRad","Specific.Humid","Temp.Min","Temp.Mean","Temp.Max","WindSpeed","Humid","Pressure","Rain",'Pressure.Adjusted')
  data[, (num.cols) := lapply(.SD, round, digits = 1), .SDcols = num.cols]
  data[,Buffer:=as.integer(Buffer)][,Altitude:=as.integer(round(Altitude,0))]
  data<-data[,list(Site.Key,Latitude,Longitude,Altitude,Buffer,NCells,DayCount,Year,Day,Date,Temp.Min,Temp.Mean,Temp.Max,Rain,Specific.Humid,Humid,
                   WindSpeed,Pressure,Pressure.Adjusted,SRad,ETo)]
  data<-data[order(Site.Key,Latitude,Longitude,Buffer,Altitude,DayCount)]
  arrow::write_parquet(data,gsub(".RData",".parquet",files1))
  
  # AgMERRA
  files1<-grep("AgMERRA",files,value=T)
  data<-data.table(miceadds::load.Rdata2(filename=basename(files1),path=data_dir))
  num.cols<-c("Temp.Min","Temp.Mean","Temp.Max","Solar.Rad","WindSpeed","RH.Max","Rain")
  data[, (num.cols) := lapply(.SD, round, digits = 1), .SDcols = num.cols]
  data[,Buffer:=as.integer(Buffer)]
  data<-data[order(Site.Key,Latitude,Longitude,Buffer,DayCount)]
  arrow::write_parquet(data,gsub(".RData",".parquet",files1))
  
  # ERA_SOS
  files1<-grep("ERA_SOS",files,value=T)
  
  files<- c(files1,list.files(data_dir,"parquet",full.names = T))
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  }

  # 1.4.3) Soils ######
  s3_bucket<-era_dirs$era_geodata_s3
  
  # Inital set-up from old file system:  
  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Physical"
  if(F){
    data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Soils"
    files<- list.files(data_dir,"19.csv$",full.names = T)
  
    # Create parquet version
    data<-data.table::fread(files)
    arrow::write_parquet(data,gsub(basename(files),"era_site_soilgrids19.parquet",files))
    
    files<- list.files(data_dir,full.names = T)
  }else{
    files<-list.files(era_dirs$era_geodata_dir,"soil_af_isda",full.names = T)
  }
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read")
  
  # 1.4.4) Legacy - Other linked datasets  ######
  if(F){
  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Other Linked Datasets"
  s3_bucket<-era_dirs$era_geodata_s3
  files<- list.files(data_dir,".csv$",full.names = T)
  
  # Select most recent data
  dates<-as.Date(unlist(tstrsplit(basename(files)," ",keep=4)))
  files<-files[!is.na(dates)]
  dates<-dates[!is.na(dates)]
  files<-files[which(dates==max(dates))]
  
  # Create parquet version
  data<-data.table::fread(files)
  arrow::write_parquet(data,gsub(basename(files),"era_site_others.parquet",files))
  
  files<- c(files,list.files(data_dir,".parquet",full.names = T))
  
  upload_files_to_s3(files = files,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read") 
  }
  # 1.4.5) LULC  ######
  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Landscape"
  s3_bucket<-era_dirs$era_geodata_s3
  files<- list.files(data_dir,".csv$",full.names = T)
  
  # Create parquet version
  data<-data.table::fread(files)
  file<-gsub(basename(files),"era_site_landcover.parquet",files)
  arrow::write_parquet(data,file)
  
  upload_files_to_s3(files = file,
                     selected_bucket=s3_bucket,
                     max_attempts = 3,
                     overwrite=T,
                     mode="public-read") 
  
  # 1.4.6) POWER #####
    # 1.4.6.1) Raw Data ######
    # Upload POWER data from local folder to S3
    s3_bucket<-era_dirs$power_S3
    
    if(F){
      # Inital set-up from old file system:  data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Physical"
      data_dir<-"C:/Users/PSteward/OneDrive - CGIAR/ERA/ERA/Data/Climate/Climate Past/POWER/Downloads"
    }else{
      data_dir<-era_dirs$power_dir
    }
    
    files<- list.files(data_dir,".csv$",full.names = T)
    
    # zip the files
    zip_file<-file.path(data_dir,"power_download.zip")
    zip(zipfile = zip_file,files = files)
  
    upload_files_to_s3(files = zip_file,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read") 
    
    # 1.4.6.2) Processed Data ######
    s3_bucket<-era_dirs$era_geodata_s3
    data_dir<-era_dirs$era_geodata_dir
    
    files<- list.files(data_dir,"POWER",full.names = T)
    files<-files[!grepl("POWER.CHIRPS",files)]
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read") 

  # 1.4.7) CHIRPS #####
    s3_bucket<-era_dirs$era_geodata_s3
    
    files<-list.files(era_dirs$era_geodata_dir,"CHIRPS_",full.names = T)
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
  # 1.4.8) Feedipedia
    s3_bucket<-era_dirs$ancillary_s3
    files<-list.files(era_dirs$ancillary_dir,"feedipedia.parquet",full.names = T,recursive=T)
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=F,
                       mode="public-read")
    
    

  # 1.4.8) AEZ ####
    s3_bucket<-era_dirs$era_geodata_s3
    (files<- list.files(era_dirs$era_geodata_dir,"aez_",full.names = T))
    
    upload_files_to_s3(files = files,
                       selected_bucket=s3_bucket,
                       max_attempts = 3,
                       overwrite=T,
                       mode="public-read") 
    