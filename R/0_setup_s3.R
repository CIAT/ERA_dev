# 0.1) Load packages #####
if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(s3fs,zip,arrow,miceadds)


# 0.2) Create function to upload files S3 bucket #####
upload_files_to_s3 <- function(files,s3_file_names=NULL, folder=NULL, selected_bucket, new_only=F, max_attempts = 3, overwrite=F,mode="private") {
  
  # Create the s3 directory if it does not already exist
  if(!s3_dir_exists(selected_bucket)){
    s3_dir_create(selected_bucket)
  }
  
  # List files if a folder location is provided
  if(!is.null(folder)){
    files <- list.files(folder, full.names = T)
  }
  
  if(overwrite==F){
    # List files in the s3 bucket
    files_s3 <- basename(s3_dir_ls(selected_bucket))
    # Remove any files that already exist in the s3 bucket
    files <- files[!basename(files) %in% files_s3]
  }
  
  for (i in seq_along(files)) {
    cat('\r', paste("File:", i, "/", length(files))," | ",basename(files[i]),"                                                 ")
    flush.console()
    
    if(is.null(s3_file_names)){
      s3_file_path <- paste0(selected_bucket, "/", basename(files[i]))
    }else{
      if(length(s3_file_names)!=length(files)){stop("s3 filenames provided different length to local files")}
      s3_file_path <- paste0(selected_bucket, "/", s3_file_names[i])
    }
    
    tryCatch({
      attempt <- 1
      while(attempt <= max_attempts) {
        s3_file_upload(files[i], s3_file_path, overwrite = overwrite)
        # Check if upload successful
        file_check <- s3_file_exists(s3_file_path)
        
        if(mode!="private"){
          s3_file_chmod(path=s3_file_path,mode=mode)
        }
        
        if (file_check) break # Exit the loop if upload is successful
        
        if (attempt == max_attempts && !file_check) {
          stop("File did not upload successfully after ", max_attempts, " attempts.")
        }
        attempt <- attempt + 1
      }
    }, error = function(e) {
      cat("Error during file upload:", e$message, "\n")
    })
  }
}

# 1.1) Upload era master files to s3 #####
s3_bucket<-era_dirs$era_masterdata_s3

# Inital set-up from old file system:
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

# List files in new data structure
files<-list.files(era_dirs$era_masterdata_dir,full.names = T)
files <- files[!file.info(files)$isdir]

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=F,
                   mode="public-read")

# 1.2) Upload 2023 extraction files to s3 #####
# where is the working folder for the ERA data extractions (internal team directory)
folder_local<-"G:/.shortcut-targets-by-id/1WRc7ooeLNhQTTAx_4WGTCOBg2skSzg4C/Data Entry 2023"

folder<-file.path(era_dirs$era_dataentry_dir,
                  era_projects$industrious_elephant_2023,
                  "excel_files")

# this is the target folder on the S3 bucket and generalized structured file system
s3_bucket<-file.path(era_dirs$era_dataentry_s3,
                     era_projects$industrious_elephant_2023,
                     "excel_files")

# List excel files to be zipped
files<-list.files(folder_local,full.names = T,recursive=T)
files<-grep("csv$|RData$|zip$|xlsx$|xlsm$",files,value=T)
files<- grep("xlsm$",files,value=T)
files<-files[!grepl("~",files)]
files<- grep("/QCed/|/Extracted/",files,value=T)

# zip all the excels and upload to the s3
output_zip_file <- file.path(folder,"industrious_elephant_2023.zip")

zip::zipr(zipfile = output_zip_file, files =files)

upload_files_to_s3(files = output_zip_file,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read")

# 1.3) Upload livestock 2024 search to s3 #####
folder<-"data_entry/data_entry_2024/search_history/livestock_2024"

s3_bucket<-file.path(era_dirs$era_search_s3,era_projects$livestock_2024)

files<-list.files(folder,full.names = T,recursive=T)
files<-files[!grepl("zip$",files)]

# Specify the output zip file path
output_zip_file <- file.path(folder,paste0(basename(folder),".zip"))

# Create the zip archive
zip::zipr(zipfile = output_zip_file, files = files)

files<-c(output_zip_file,files)

files<-grep("zip$|openalex",files,value=T)

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
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
  
  # 1.4.4) Other linked datasets  ######
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
    
