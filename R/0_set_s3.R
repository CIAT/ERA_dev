require(s3fs)

# Create function to upload files S3 bucket ####
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


# Upload era master files to s3
folder<-"data"
s3_bucket<-"s3://digital-atlas/era/data"

files<-list.files(folder,full.names = T,recursive=T)
files <- files[!file.info(files)$isdir]
files<-grep("csv$|RData$|zip$|xlsx$",files,value=T)

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read")

# Upload 2023 extraction files to s3
folder<-"data_entry/data_entry_2023"
s3_bucket<-"s3://digital-atlas/era/data_entry/data_entry_2023"

files<-list.files(folder,full.names = T,recursive=T)
files <- files[!file.info(files)$isdir]
files<-grep("csv$|RData$|zip$|xlsx$|xlsm$",files,value=T)

upload_files_to_s3(files = files,
                   selected_bucket=s3_bucket,
                   max_attempts = 3,
                   overwrite=T,
                   mode="public-read")
