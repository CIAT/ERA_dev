# This script downloads soilgrids and isda data using geodata and then extracts information for unique locations in ERA. 
# Run ERA_dev/R/0_set_env before executing this script

# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####

p_load(terra,sf,data.table,remotes,exactextractr,future.apply,progressr)

# Make sure you install geodata from github not cran, a number of bugs are fixed in the git not pushed to CRAN
# remotes::install_github("rspatial/geodata", force = TRUE)
library(geodata)
# geodata:::.soil_grids_url # check cec is a var option

# 1) Prepare ERA data ####
SS<-era_locations[Buffer<50000]
pbuf_g<-era_locations_vect_g[era_locations[,Buffer<50000]]

# 2) Download soilgrids data ####

 ## 2.1) Create function to download data in parallel####
  # Function to process each row with tryCatch and suppress messages
  download_soil_file <- function(var, depth, path,dataset) {
    attempts <- 0
    success <- FALSE
    max_attempts <- 3
    
    while (attempts < max_attempts && !success) {
      attempts <- attempts + 1
      tryCatch({
        # Suppress messages from the function call
        suppressMessages({
          if(dataset=="isda"){
          geodata::soil_af_isda(var = var, depth = depth, path = path)
          geodata::soil_af_isda(var = var, depth = depth, path = path, error = TRUE)
          }
          if(dataset=="soilgrids"){
            soil_rast1<-geodata::soil_world(var = var, depth = depth, stat="mean",path=path)
            # Files do not exist
           # soil_rast2<-geodata::soil_world(var = var, depth = depth,stat="uncertainty",path=path)
          }
          
        })
        success <- TRUE  # Set success flag to TRUE if no error occurs
      }, error = function(e) {
        if (attempts >= max_attempts) {
          message("Failed after ", max_attempts, " attempts for var ", var," depth ",depth)
        }
      })
    }
  }
  
  ## 2.2) Download isda ####
  soil_files <- data.table(
    var = c("Al", "bdr", "clay", "C.tot", "Ca", "db.od", "eCEC.f", "Fe", "K", "Mg", "N.tot", "OC", "P", "pH.H2O", "sand", "silt", "S", "texture", "wpg2", "Zn"),
    description = c(
      "extractable aluminum", "bed rock depth", "clay content", "total carbon",
      "extractable calcium", "bulk density", "effective cation exchange capacity",
      "extractable iron", "extractable potassium", "extractable magnesium",
      "total organic nitrogen", "organic carbon", "phosphorus content", "pH (H2O)",
      "aand content", "ailt content", "extractable sulphur", "texture class", "stone content", "extractable zinc"
    ),
    unit = c(
      "mg kg-1", "cm", "%", "kg-1", "mg kg-1", "kg m-3", "cmol(+) kg-1",
      "mg kg-1", "mg kg-1", "mg kg-1", "g kg-1", "g kg-1", "mg kg-1", "-", 
      "%", "%", "mg kg-1", "-", "%", "mg kg-1"
    )
  )
  
  meta_data<-copy(soil_files)
  meta_data[,var:=tolower(var)]
  fwrite(meta_data,file.path(era_dirs$era_geodata_dir,"isda_metadata.csv"))
  
  soil_files<-rbind(copy(soil_files)[,depth:=20],copy(soil_files)[,depth:=50])[,source:="https://rdrr.io/github/rspatial/geodata/man/soil_af_isda.html"]
  
  # Parallel processing with progressr
  n_workers<-5
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("txtprogressbar")
  
  # Set up parallel processing plan
  future::plan(multisession, workers = n_workers)  # Adjust 'workers' based on available cores
  
  with_progress({
    # Create progress bar
    progress <- progressr::progressor(along = 1:nrow(soil_files))
    
    future.apply::future_lapply(1:nrow(soil_files), function(i) {
      # Call the processing function
      download_soil_file(var=soil_files$var[i], depth=soil_files$depth[i], path=era_dirs$soilgrid_dir,dataset="isda")
      # Update progress
      progress()
    })
  })
  
  plan(sequential)
  
  # Check files can be read
  files<-list.files(file.path(era_dirs$soilgrid_dir,"soil_af_isda"), ".tif$", recursive = TRUE, full.names = TRUE)
  result <- sapply(files, 
                   FUN = function(file) {
                     tryCatch({
                       # Attempt to load the file with `rast()` and perform the operation
                       rast(file) + 0
                       TRUE  # Return TRUE if successful
                     }, error = function(e) {
                       FALSE  # Return FALSE if there is an error
                     })
                   })
  (bad_files<-files[!result])
  unlink(bad_files,recursive = T)
  
  if(length(bad_files)>0){
    for(i in 1:nrow(soil_files)){
        geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir)
      # Note some error files do not exist, so do not worry about download failures regarding these
        geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir,error=T)
    }
  }
  
  ## 2.2) Download soilgrids ####
  
  # Note geodata function documentation appears to be incorrect or files missing from server
  # https://geodata.ucdavis.edu/geodata/soil/soilgrids/
  # There are no uncertainty, cec or ocs files available as of 2025-14-03
  if(F){
  soil_files <-  data.table(
    var = c("bdod", "cec", "cfvo", "nitrogen", "phh2o", "sand", "silt", "clay", "soc", "ocd", "ocs"),
    description = c(
      "Bulk density of the fine earth fraction",
      "Cation Exchange Capacity of the soil",
      "Vol. fraction of coarse fragments (> 2 mm)",
      "Total nitrogen (N)",
      "pH (H2O)",
      "Sand (> 0.05 mm) in fine earth",
      "Silt (0.002-0.05 mm) in fine earth",
      "Clay (< 0.002 mm) in fine earth",
      "Soil organic carbon in fine earth",
      "Organic carbon density",
      "Organic carbon stocks"
    ),
    unit = c("kg dm-3", "cmol(+) kg-1", "%", "g kg-1", "-", "%", "%", "%", "g kg-1", "kg m-3", "kg m-2"),
    stringsAsFactors = FALSE
  )
  
  
  fwrite(soil_files,file.path(era_dirs$era_geodata_dir,"soilgrids_metadata.csv"))
  
  # Consider using expand_grid in future
  soil_files<-rbind(copy(soil_files)[,depth:=5],
                         copy(soil_files)[,depth:=15],
                         copy(soil_files)[,depth:=30],
                         copy(soil_files)[,depth:=60],
                         copy(soil_files)[,depth:=100])
  
  # These do not exist
  soil_files<-soil_files[!var %in% c("cec","ocs")]
  
  n_workers<-10
  
  if(n_workers>1){
  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("txtprogressbar")
  
  # Set up parallel processing plan
  future::plan(multisession, workers = n_workers)  # Adjust 'workers' based on available cores
  
  # Parallel processing with progressr
  with_progress({
    # Create progress bar
    progress <- progressr::progressor(along = 1:nrow(soil_files))
    
    future.apply::future_lapply(1:nrow(soil_files), function(i) {
      # Call the processing function
      download_soil_file(var=soil_files$var[i],
                         depth=soil_files$depth[i], 
                         path=era_dirs$soilgrid_dir,
                         dataset = "soilgrids")
      # Update progress
      progress()
    })
  })
  
  plan(sequential)
  }else{
    
    lapply(1:nrow(soil_files), FUN=function(i) {
      # Call the processing function
      download_soil_file(var=soil_files$var[i], depth=soil_files$depth[i], path=era_dirs$soilgrid_dir,dataset = "soilgrids")
    }) 
  }
  
  # Check files can be read
  files<-list.files(file.path(era_dirs$soilgrid_dir,"soil_world"), ".tif$", recursive = TRUE, full.names = TRUE)
  result <- sapply(files, 
                   FUN = function(file) {
                     tryCatch({
                       # Attempt to load the file with `rast()` and perform the operation
                       rast(file) + 0
                       TRUE  # Return TRUE if successful
                     }, error = function(e) {
                       FALSE  # Return FALSE if there is an error
                     })
                   })
  (bad_files<-files[!result])
  unlink(bad_files,recursive = T)
  
  if(length(bad_files)>0){
    for(i in 1:nrow(soil_files)){
        geodata::soil_world(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir,stat="mean")
      # Note some error files do not exist, so do not worry about download failures regarding these
        geodata::soil_world(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir,stat="uncertainty")
    }
  }
  }
  
# 3) Extract soil grids data for era buffers ####
params<-data.table(
  save_file=c(
    file.path(era_dirs$era_geodata_dir,paste0("isda_",Sys.Date(),".parquet")),
    file.path(era_dirs$era_geodata_dir,paste0("soilgrids2.0_",Sys.Date(),".parquet"))
  ),
  data_dir=c(
    file.path(era_dirs$soilgrid_dir,"soil_af_isda"),
    soilgrids_dir<-file.path(era_dirs$soilgrid_dir,"soil_world")
    ),
  dataset=c("isda","soilgrids")
)

# Only extract isda for now
params<-params[1]

# Check countries in africa match african_countries
unique(pbuf_g$Country)[!unique(pbuf_g$Country) %in% african_countries]
african_countries<-c(african_countries,"Cameroon..Chad")

for(i in 1:nrow(params)){
  # Filter out sites for which data have already been extracted
  soil_file<-params$save_file[i]
  dataset<-params$dataset[i]
  
  site_vect<-pbuf_g
  
  if(dataset=="isda"){
    site_vect[site_vect$Country %in% african_countries,]
  }
  
  # Stack all the rasters in the soil directory and fast extract data by era site buffers
  data_dir<-params$data_dir[i]
  data<-terra::rast(list.files(data_dir,".tif$",full.names = T))
  data_ex<- exactextractr::exact_extract(data, sf::st_as_sf(site_vect), fun = "mean", append_cols = c("Site.Key"))
  colnames(data_ex)<-gsub("mean.","",colnames(data_ex),fixed=T)
  data_ex$stat<-"mean"
  
  data_ex_median<- exactextractr::exact_extract(data, sf::st_as_sf(site_vect), fun = "median", append_cols = c("Site.Key"))
  colnames(data_ex_median)<-gsub("median.","",colnames(data_ex_median),fixed=T)
  data_ex_median$stat<-"median"
  
  data_ex<-unique(data.table(rbind(data_ex,data_ex_median)))
  data_ex_m<-data.table(melt(data_ex,id.vars=c("Site.Key","stat")))
  
  if(dataset=="isda"){
  data_ex_m[grep("error",variable),error:="error"
            ][!grepl("error",variable),error:="value"
              ][,variable:=gsub("-error","",variable)
              ][,variable:=gsub("c.tot.","c.tot_",variable)
                ][,depth:=unlist(tstrsplit(variable,"_",keep=2))
                  ][,variable:=unlist(tstrsplit(variable,"_",keep=1))]
    
    data_ex_m<-data.table(dcast(data_ex_m,Site.Key+stat+variable+depth~error,value.var = "value"))
  }
  
  if(dataset=="soilgrids"){
    data_ex_m[,depth:=unlist(tstrsplit(variable,"_",keep=2))
              ][,variable:=unlist(tstrsplit(variable,"_",keep=1))
                ][,error:=NA]
  }
  
  data_ex_m[,error:=round(error,2)][,value:=round(value,2)]
  
  (check<-data_ex_m[,.N,by=Site.Key][,unique(N)])
  if(length(check)>1){
    stop("Uneven data availability between sites, i = ",i)
  }
  
  arrow::write_parquet(data_ex_m,soil_file)
}
