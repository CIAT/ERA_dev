# First run R/0_set_env.R

# 1) Set-up workspace ####
  # 1.1) Load packages and source functions #####
pacman::p_load(data.table,pbapply,future,future.apply,R.utils,progressr,RCurl,exactextractr,sf)
curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection

source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/download_chirps.R")
source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/process_chirps.R")


  # 1.2) Set parallel workers ####
worker_n<-10

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

# 3) Download data ####
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

# 5) Process data ####

  ## 5.1) Restructure CHIRPS ####
  
    ### 5.1.1) Create folders ####
    save_dir<-file.path(era_dirs$chirps_dir,"restructured")
    if(!dir.exists(save_dir)){
      dir.create(save_dir)
    }
  
    ### 5.1.2) Create continental bounding boxes ####
    # Define bounding boxes as named vectors
    bbox_africa <- c(lon_min = -25, lon_max = 63, lat_min = -36, lat_max = 38)
    bbox_sam    <- c(lon_min = -120, lon_max = -30, lat_min = -36, lat_max = 38)
    bbox_asia   <- c(lon_min = 80, lon_max = 155, lat_min = -36, lat_max = 38)
    
    # Combine into a list if needed
    bounding_boxes <- list(
      Africa = bbox_africa,
      SouthCentralAmerica = bbox_sam,
      SouthSoutheastAsia = bbox_asia
    )
  
    ### 5.1.3) Restructure data ####
    
    for(i in 1:length(bounding_boxes)){
      cat("Transposing ",names(bounding_boxes)[i],", bounding box ",i,"/",length(bounding_boxes),"\n")
      transpose_chirps(data_dir=era_dirs$chirps_dir,
                       save_dir=file.path(save_dir,names(bounding_boxes)[i]),
                       crop_box=bounding_boxes[[i]],
                       overwrite=F)
    }
  
  ## 5.2) Extract CHIRPS ####
  
  terra::gdalCache(size=20000)
    
  files<-list.files(era_dirs$era_geodata_dir,"chirps.*parquet",full.names = T)
  (files<-files[!grepl("annual|ltavg",files)])
  (files<-tail(files,1)  )
  
  if(length(files)==1){
    chirps<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"chirps.parquet"))
  }else{
    chirps<-NULL
  }
  
  chirps_new<-extract_chirps(site_vect=era_locations_vect_g,
                 id_field="Site.Key",
                 data_dir=era_dirs$chirps_dir,
                 existing_data=chirps, 
                 round_digits=2,
                 add_daycount = TRUE,
                 time_origin=time_origin,
                 max_cells_in_memory = 3e+08,
                 overwrite=F)
  
  if(!is.null(chirps_new)){
    arrow::write_parquet(chirps_new,file.path(era_dirs$era_geodata_dir,paste0("chirps_",Sys.Date(),".parquet")))
  }
  
  
  ### 5.2.1) Calculate Annual & LT Data ####  
    if(!is.null(chirps_new)){
      
  # Load CHIRPS
  chirps_new[,Year:=format(date[1],"%Y"),by=date]
  
  # Remove incomplete years
  chirps_new[,n:=.N,by=.(Site.Key,Year)][!n<365][,n:=NULL][,Year:=NULL]
  
  # Calculate annual data 
  chirps_annual<-chirps_new[,list(Total.Rain=sum(Rain)),by=c("Site.Key","Year")]
  
  # Calculate LT Data
  chirps_ltavg<-chirps_annual[,list(Total.Rain.mean=mean(Total.Rain),Total.Rain.sd=sd(Total.Rain)),by=c("Site.Key")]
  
  # Round results
  round_digits<-2
  chirps_annual[,Total.Rain:=round(Total.Rain,round_digits)]
  chirps_ltavg[,Total.Rain:=round(Total.Rain.mean,round_digits)
  ][,Total.Rain.sd:=round(Total.Rain.sd,round_digits)]
  
  # Save results
  arrow::write_parquet(chirps_annual,file.path(era_dirs$era_geodata_dir,paste0("chirps_annual_",Sys.Date(),".parquet")))
  arrow::write_parquet(chirps_ltavg,file.path(era_dirs$era_geodata_dir,paste0("chirps_ltavg_",Sys.Date(),".parquet")))
  
  }
  