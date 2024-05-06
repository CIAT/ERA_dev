# 0.1) Load packages #####
if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(s3fs,zip)


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

# Set s3 era directory ####
era_s3<-"s3://digital-atlas/era"

if(length(s3fs::s3_dir_ls(era_s3))==0){
  s3fs::s3_dir_create(era_s3)
}


# 1.1) Upload era master files to s3 #####
folder<-"data"
s3_bucket<-paste0(era_s3,"/",folder)

files<-list.files(folder,full.names = T,recursive=T)
files <- files[!file.info(files)$isdir]
files<-grep("csv$|RData$|zip$|xlsx$",files,value=T)

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read")

# 1.2) Upload 2023 extraction files to s3 #####
# where is the working folder for the ERA data extractions (internal team directory)
folder_local<-"G:/.shortcut-targets-by-id/1WRc7ooeLNhQTTAx_4WGTCOBg2skSzg4C/Data Entry 2023"

# this is the target folder on the S3 bucket and generalized structured file system
folder<-"data_entry/industrious_elephant_2023/excel_files"
s3_bucket<-paste0(era_s3,"/",folder)

# List excel files to be zipped
files<-list.files(folder_local,full.names = T,recursive=T)
files <- files[!file.info(files)$isdir]
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
s3_bucket<-paste0(era_s3,"/",folder)


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
