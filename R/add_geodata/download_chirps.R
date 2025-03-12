# First run R/0_set_env.R

# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####
pacman::p_load(data.table,pbapply,future,future.apply,R.utils,progressr,RCurl)
curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection

# 0.2) Set parallel workers ####
worker_n<-10

# 1) Create functions ####

  remove_trailing_slash <- function(path) {
    if (stringr::str_ends(path, "/")) {
      path <- stringr::str_sub(path, 1, -2)
    }
    return(path)
  }
  
  create_chirps_paths<-function(base_url,format,startyear,endyear,variable_dir,variable,dataset){
    base_url<-remove_trailing_slash(path=base_url)
    if(grepl("tif",format)){
      file_paths <- data.table(date=seq.Date(from = as.Date(paste0(startyear, "-01-01")),
                                            to = as.Date(paste0(endyear, "-12-31")),
                                            by = "day")
                              )[,year:=format(date,"%Y")]
      
      if(!is.na(variable_dir)){
        file_paths<-file_paths[,file_path:=file.path(base_url,variable_dir,year,paste0(variable,".",gsub("-",".",date),".",format))
                                  ][,file_path]
      }else{
        file_paths<-file_paths[,file_path:=file.path(base_url,year,paste0(variable,".",gsub("-",".",date),".",format))
                   ][,file_path]
      }
      
    }
    
    if(grepl(".nc",format)){
        if(is.na(variable_dir)){
          file_paths<-file.path(base_url,paste0(variable,".",startyear:endyear,".",format))
        }else{
          file_paths<-file.path(base_url,variable_dir,paste0(variable,".",startyear:endyear,".",format))
        }
    }
    
    if(exists("file_paths")){
      return(file_paths)
    }else{
      return(NA)
    }
  }
  
  download_chirps <- function(chirps_index, var, reg, res, type, startyear, endyear, save_dir, worker_n,year_folder) {
    # Filter files based on parameters
    files <- unlist(chirps_index[variable == var & region == reg & resolution == res & format == type, file_paths])
    files <- files[grepl(paste(startyear:endyear, collapse = "|"), files)]
    
    if(year_folder & type=="tif"){
      local_path <- file.path(save_dir,basename(dirname(files)),basename(files))
    }else{
      local_path <- file.path(save_dir, basename(files))
    }
    
    # Create directories
    dirs<-unique(dirname(local_path))
    for(DIR in dirs){
    if(!dir.exist(dirname(DIR))){
      dir.create(dirname(DIR),recursive = T)
    }
    }
    
    # Create a data frame with URLs and local paths
    files_df <- data.frame(
      url = files,
      local_path = file.path(save_dir, basename(files)),
      stringsAsFactors = FALSE
    )
    
    # Check if files already exist
    files_df$f_exists <- file.exists(files_df$local_path)
    files_df[!files_df$f_exists, "f_exists"] <- file.exists(gsub(".tif.gz", ".tif", files_df$local_path))
    
    # Filter out existing files
    files_df <- files_df[!files_df$f_exists, ]
    
    # Function to attempt download with retries
    attempt_download <- function(url, local_path) {

      attempts <- 3
      success <- FALSE
      for (i in 1:attempts) {
        try({
          download.file(url, local_path)
          success <- TRUE
          break
        }, silent = TRUE)
        if (!success && grepl(".tif.gz$", url)) {
          url <- gsub(".tif.gz$", ".tif", url)
        }
      }
      return(success)
    }
    
    # Perform downloads with lapply or future_lapply
    failed_downloads <- NULL
    if (worker_n == 1) {
      failed_downloads <- lapply(1:nrow(files_df), function(i) {
        cat("Downloading file", i, "/", nrow(files_df), "\n")
        success <- attempt_download(files_df$url[i], files_df$local_path[i])
        if (!success) return(files_df[i, ])
        return(NULL)
      })
    } else {
      future::plan(multisession, workers = worker_n)
      failed_downloads <- future.apply::future_lapply(1:nrow(files_df), function(i) {
        success <- attempt_download(files_df$url[i], files_df$local_path[i])
        if (!success) return(files_df[i, ])
        return(NULL)
      })
      future::plan(sequential)
    }
    
    # Combine and return failed downloads
    failed_downloads <- do.call(rbind, failed_downloads)
    return(failed_downloads)
  }
  
# 2) Create index of file download paths ####
  chirps_index<-rbind(
  data.table(dataset="chirps",
             region="africa",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirps",
             region="africa",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="days_p25.nc",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="days_p05.nc",
             timestep="daily",
             resolution=5)
  )
  

  chirps_index<-rbindlist(lapply(1:nrow(chirps_index),FUN=function(i){
    x<-data.frame(chirps_index[i])
    file_paths<-create_chirps_paths(base_url=x$base_url,
                                    format=x$format,
                                    startyear=x$startyear,
                                    endyear=x$endyear,
                                    variable_dir=x$variable_dir,
                                    variable=x$variable,
                                    dataset=x$dataset)
    
    x$file_paths<-list(file_paths)
    return(x)
  }))


  chirps_index[grepl("chirps",variable),variable:="prec"
               ][,variable:=tolower(variable)
                 ][,variable_dir:=NULL
                   ][grepl("tif",format),format:="tif"
                     ][grepl("nc",format),format:="nc"
                       ][,base_url:=NULL]
  
# 3) Download data
  download_chirps(chirps_index,
                  var="prec",
                  reg="global",
                  res=5,
                  type="tif",
                  startyear = 1981,
                  endyear = 2023,
                  save_dir = era_dirs$chirps_dir,
                  year_folder=F)
  
# 4) Unzip gz files ####
  files<-list.files(era_dirs$chirps_dir,".gz$",full.names = T)
  
  # Setup parallel plan
  future::plan(multisession, workers = worker_n)
  handlers("progress")
  # Track progress
  with_progress({
    p <- progressor(along = files)
    results <- future_lapply(seq_along(files), function(i) {
      gunzip(files[i], remove = TRUE)
      p(message = sprintf("Processed file %d/%d", i, length(files)))
    })
  })
  # Reset to sequential if needed
  future::plan(sequential)  
  
  # 4.1) Check files are complete ####
  files<-list.files(era_dirs$chirps_dir,".tif$",full.names = T)
  dates<-gsub(".tif","",basename(files))
  file_prefix<-"chirps-v2.0."
  dates<-gsub(file_prefix,"",dates)
  dates<-as.Date(dates,"%Y.%m.%d")
  
  # Create sequence of all days in chirps timeseries download range
  dates_seq <- seq.Date(from = as.Date("1981-01-01"), by = "day", to = as.Date("2023-12-31"))
  
  # Check all days are included
  dates_seq[!dates_seq %in% dates]
  