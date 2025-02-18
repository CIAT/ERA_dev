

data<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"POWER.parquet"))

# 1) Get aridity index ####
# https://www.nature.com/articles/s41597-022-01493-1
file<-"ai_v3_yr.tif"
if(update==T|!grepl(file,list.files(era_dirs$aez_dir))){
  aez_file <- file.path(era_dirs$aez_dir,"Global-AI_ET0_v3_annual.zip")
  
  if(!file.exists(aez_file)){
    
    url<-"https://figshare.com/ndownloader/files/34377245"
    
    # Perform the API request and save the file
    cat("downloading file (~760mb) - this may take some time")
    response <- httr::GET(url = url, httr::write_disk(aez_file, overwrite = TRUE))
    
    # Check if the download was successful
    if (httr::status_code(response) == 200) {
      print(paste0("File  downloaded successfully."))
    }else{
      print(paste("Failed to download file Status code:", httr::status_code(response)))
    }
    
    # List all files in the ZIP archive
    zip_contents <- unzip(aez_file, list = TRUE)
    
    # Filter for files containing "ai_v3"
    filtered_files <- zip_contents$Name[grepl("ai_v3", zip_contents$Name)]
    
    unzip(aez_file,files=filtered_files,exdir=era_dirs$aez_dir,junkpaths = T)
  
    unlink(list.files(era_dirs$aez_dir,"zip",full.names = T))
    
  }
}

ai<-terra::rast(file.path(era_dirs$aez_dir,file))

# 2) Get temperature #####
  file<-"wc2.1_30s_prec.zip"
  
  if(update==T|!grepl(file,list.files(era_dirs$aez_dir))){
    aez_file <- file.path(era_dirs$aez_dir,"wc2.1_30s_prec.zip")
    
    if(!file.exists(aez_file)){
      
      url<-"https://geodata.ucdavis.edu/climate/worldclim/2_1/base/wc2.1_30s_prec.zip"
      
      # Perform the API request and save the file
      cat("downloading file (~978 mb) - this may take some time")
      response <- httr::GET(url = url, httr::write_disk(aez_file, overwrite = TRUE))
      
      # Check if the download was successful
      if (httr::status_code(response) == 200) {
        print(paste0("File  downloaded successfully."))
      }else{
        print(paste("Failed to download file Status code:", httr::status_code(response)))
      }
      
      # List all files in the ZIP archive
      zip_contents <- unzip(aez_file, list = TRUE)
      
      # Filter for files containing "ai_v3"
      filtered_files <- zip_contents$Name[grepl("ai_v3", zip_contents$Name)]
      
      unzip(aez_file,files=filtered_files,exdir=era_dirs$aez_dir,junkpaths = T)
      
      unlink(list.files(era_dirs$aez_dir,"zip",full.names = T))
      
    }
  }
    
  prec<-terra::rast(file.path(era_dirs$aez_dir,file))

# 3) Align datasets ####
  
# 4) Calculate indices ####

  