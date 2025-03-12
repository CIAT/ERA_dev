# First run R/0_set_env.R & R/add_geodata/download_chirps.R

# 0) Set-up workspace ####
  ## 0.1) Load packages and create functions #####
  pacman::p_load(data.table,R.utils,terra,arrow,exactextractr,sf)

#' Transpose CHIRPS Raster Data to Parquet
#'
#' This function reads CHIRPS raster data (`.tif` files), crops it to a specified bounding box, 
#' processes it into a long-format data frame, and saves it as a Parquet file per year.
#'
#' @param data_dir Character. Path to the directory containing `.tif` raster files.
#' @param save_dir Character. Path to the directory where the processed Parquet files will be saved.
#' @param crop_box Named numeric vector. Bounding box for cropping in the format 
#'   `c(lon_min = -25, lon_max = 63, lat_min = -36, lat_max = 38)`. Default is global bounds.
#' @param overwrite Logical. If `TRUE`, existing Parquet files will be overwritten. Default is `FALSE`.
#'
#' @import data.table
#' @import terra
#' @import arrow
#'
#' @return Saves yearly Parquet files in `save_dir`. No value is returned.
#' @export
transpose_chirps <- function(data_dir, 
                             save_dir, 
                             round_digits=NULL,
                             crop_box = c(lon_min = -25, lon_max = 63, lat_min = -36, lat_max = 38), 
                             overwrite = FALSE) {
  
  # Check if the save directory exists
  if (!dir.exists(save_dir)) {
    dir.create(save_dir,recursive = T)
  }
  
  # List all .tif files in data_dir and extract file names
  files <- data.table(filename = list.files(path = data_dir, pattern = "\\.tif$"))
  
  # Extract dates from filenames and convert to Date format
  files[, date := gsub("\\.tif$", "", filename)
  ][, date := substr(date[1], nchar(date[1]) - 9, nchar(date[1])), by = date
  ][, date := as.Date(date, format = "%Y.%m.%d")
  ][, year := format(date, "%Y")]
  
  # Loop over each year in the dataset
  for (i in seq(min(files$year), max(files$year))) {
    
    cat("Processing year", i, "\n")
    save_file <- file.path(save_dir, paste0(i, ".parquet"))
    
    # Process only if file does not exist or overwrite is enabled
    if (!file.exists(save_file) || overwrite) {
      
      # Select file names corresponding to the current year
      X <- files$filename[files$year == i]
      
      # Load and crop raster data
      rast_dat <- terra::rast(file.path(data_dir, X))
      rast_dat <- terra::crop(rast_dat, ext(crop_box))
      
      # Round raster values (if round_digits is present)
      if(!is.null(round_digits)){
        rast_dat <- round(rast_dat, round_digits)
      }
      
      # Convert raster to long-format data table
      rast_vals <- as.data.frame(rast_dat, xy = TRUE)
      rast_vals <- data.table(rast_vals)
      rast_vals <- melt(rast_vals, id.vars = c("x", "y"), variable.name = "date")
      
      # Process date values
      rast_vals[, date := as.character(date)
      ][, date := substr(date[1], nchar(date[1]) - 9, nchar(date[1])), by = date
      ][, date := as.Date(date[1], format = "%Y.%m.%d"), by = date]
      
      # Replace negative values with NA
      rast_vals[value < 0, value := NA]
      
      # Save as Parquet file
      arrow::write_parquet(rast_vals, save_file)
    }
  }
}

extract_chirps<-function(site_vect,
                         id_field,
                         data_dir,
                         save_dir,
                         round_digits,
                         add_date     = TRUE,
                         add_daycount = TRUE,
                         time_origin,
                         max_cells_in_memory,
                         overwrite){
  
  # Check if the save directory exists
  if (!dir.exists(save_dir)) {
    dir.create(save_dir,recursive = T)
  }
  
  sites<-unique(data.frame(site.vect)[,id_field])
  
  save_file<-file.path(save_dir,"chirps.parquet")
  
  if(file.exists(save_file)){
  results_old<-data.table(arrow::read_parquet(save_file))
    if(!overwrite){
      sites<-sites[!sites %in% results[,unique(id_field),with=F]]
      site_vect<-site_vect[site_vect[,id_field] %in% sites]
    }else{
      results_old<-NULL
    }
  }else{
    results_old<-NULL
  }
  
  if(length(site_vect)>0){
  
  # List all .tif files in data_dir and extract file names
  files <- data.table(filename = list.files(path = data_dir, pattern = "\\.tif$"))
  
  # Extract dates from filenames and convert to Date format
  files[, date := gsub("\\.tif$", "", filename)
  ][, date := substr(date[1], nchar(date[1]) - 9, nchar(date[1])), by = date
  ][, date := as.Date(date, format = "%Y.%m.%d")
  ][, year := format(date, "%Y")]
  
  rast_dat<-terra::rast(files)
  
  rast_vals<-data.table(exactextractr::exact_extract(rast_dat, 
                               sf::st_as_sf(site_vect), 
                                max_cells_in_memory = max_cells_in_memory))
  
  
  # Add id_field
  rast_vals[,id_field:=site_vect[,id_field]]
  
  # Transpose
  rast_vals<-melt(rast_vals,id.vars=id_field)
  
  rast_vals[, date := as.character(date)
  ][, date := substr(date[1], nchar(date[1]) - 9, nchar(date[1])), by = date
  ][, date := as.Date(date[1], format = "%Y.%m.%d"), by = date]
  
  if(!is.null(round_digits)){
    rast_vals[,value:=round(value,round_digits)]
  }
  
  # If requested, add a DayCount column (days since 'time_origin').
  if (add_daycount) {
    data[, day_count := as.integer(floor(unclass(date[1] - time_origin))), by = date]
  }
  
  }
  
  
}
  
  ## 0.2) Set parallel workers ####
  worker_n<-10
  
  ## 0.3) Create folders ####
  save_dir<-file.path(era_dirs$chirps_dir,"restructured")
  if(!dir.exists(save_dir)){
    dir.create(save_dir)
  }
  
  ## 0.4) Crete continental bounding boxes ####
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
  
# 1) Restructure CHIRPS ####
  for(i in 1:length(bounding_boxes)){
  cat("Transposing ",names(bounding_boxes)[i],", bounding box ",i,"/",length(bounding_boxes),"\n")
  transpose_chirps(data_dir=era_dirs$chirps_dir,
                   save_dir=file.path(save_dir,names(bounding_boxes)[i]),
                   crop_box=bounding_boxes[[i]],
                   overwrite=F)
  }

# 2) Extract CHIRPS ####
  
  
 i<-1  

  
  
  ExtractFolder<- function (FileDirectory,
                            MaxChunkSize, 
                            ExtractBy, 
                            ID.Target, 
                            ID.Target.Name = "variable", 
                            ID.ExtractBy, 
                            ID.ExtractBy.Name = NA, 
                            Function = mean,
                            value.name = "value", 
                            NAValue = NA, 
                            Round = NA) 
  {
    if (length(ID.ExtractBy) != length(ExtractBy)) {
      stop("Error: length(ID.ExtractBy) != length(ExtractBy)")
    }
    Files <- list.files(FileDirectory, full.names = T)
    Files2 <- gsub("[.]tif", "", list.files(FileDirectory, full.names = F))
    Files2 <- stringr::str_replace_all(Files2, "[[:punct:]]", 
                                       ".")
    if (length(ID.Target) != length(Files)) {
      stop("Error: length(ID.Target) != length(list.files(FileDirectory,full.names = T))")
    }
    if (sum(!grepl("[.]tif", Files)) > 0) {
      stop("Error: Non .tif files in FileDirectory")
    }
    File.Size <- mean(file.size(Files)/10^9)
    Chunks <- split(Files, ceiling(seq_along(1:length(Files))/MaxChunkSize))
    ExtractBy <- terra::project(ExtractBy, terra::crs(terra::rast(Files[1])))
    Extracted <- rbindlist(pblapply(Chunks, FUN = function(Chunk) {
      Data <- terra::rast(Chunk)
      if (!is.na(NAValue)) {
        Data <- terra::classify(Data, matrix(c(NAValue, as.numeric(NA)), 
                                             ncol = 2))
      }
      if (!suppressWarnings(is.na(Function))) {
        Data <- data.table(terra::extract(Data, ExtractBy, 
                                          fun = Function, na.rm = T))
      }
      else {
        Data <- data.table(terra::extract(Data, ExtractBy))
      }
      melt(Data, id.vars = "ID")
    }))
    if (!is.na(Round)) {
      Extracted[, `:=`(value, round(value, Round))]
    }
    if (value.name != "value") {
      setnames(Extracted, "value", value.name)
    }
    ID.Target <- data.table(ID.Target = ID.Target, variable = Files2)
    Extracted <- merge(Extracted, ID.Target, by = "variable", 
                       all.x = T)[, `:=`(variable, NULL)]
    if (!is.na(ID.Target.Name)) {
      setnames(Extracted, "ID.Target", ID.Target.Name)
    }
    ID.ExtractBy <- data.table(ID.ExtractBy = ID.ExtractBy)[, 
                                                            `:=`(ID, 1:.N)]
    Extracted <- merge(Extracted, ID.ExtractBy, by = "ID", all.x = T)[, 
                                                                      `:=`(ID, NULL)]
    if (!is.na(ID.ExtractBy.Name)) {
      setnames(Extracted, "ID.ExtractBy", ID.ExtractBy.Name)
    }
    return(Extracted)
  }

# Slower more reliable method using terra::extract
# If parallel can be added then speeds can be increased
# see https://github.com/rspatial/terra/issues/36 using pack and unpack functions will make serializable SpatRaster and SpatVector objects

FileDirectory<-"D:/datasets/chirps/raw"

SS<-unique(Meta.Table[!(is.na(Latitude)|is.na(Longitude)|is.na(Buffer)|Buffer>50000),list(Latitude,Longitude,Buffer,Site.Key)])
ExtractBy<-vect(Pbuffer(Data=SS,ID="Site.Key",Projected = T))

# Generate names for .tif files in CHIRPS directory, in this case the files represent dates
Dates<-list.files(FileDirectory)
Dates<-as.Date(gsub("[.]tif","",gsub("chirps-v2[.]0[.]","",Dates)),format="%Y.%m.%d")

CHIRPS<-ERAgON::ExtractFolder(FileDirectory=FileDirectory,
                              MaxChunkSize=100,
                              ExtractBy=ExtractBy,
                              ID.Target=Dates,
                              ID.Target.Name="Date",
                              ID.ExtractBy=SS[,Site.Key],
                              ID.ExtractBy.Name="Site.Key",
                              Function = mean,
                              value.name="Rain",
                              NAValue=-9999,
                              Round=4)

CHIRPS[,DayCount:=as.integer(floor(unclass(Date-as.Date(M.ORIGIN)))),by=Date]


save(CHIRPS,file="Large Files/CHIRPS.RData")


# 3) Calculate Annual & LT Data ####  
# Load CHIRPS
CHIRPS<-miceadds::load.Rdata2("CHIRPS.RData",path="Large Files")

CHIRPS[,Year:=format(Date,"%Y"),by=Date]

# Remove incomplete years
if(length(CHIRPS[Site.Key==CHIRPS$Site.Key[1],list(N=.N),by=Year][N<365,Year])>0){
  CHIRPS<-CHIRPS[!Year==CHIRPS[Site.Key==CHIRPS$Site.Key[1],list(N=.N),by=Year][N<365,Year]]
}

# Calculate annual data 
CHIRPS.Annual<-CHIRPS[,list(Total.Rain=sum(Rain)),by=c("Site.Key","Year")]

# Calculate LT Data
CHIRPS.LT<-CHIRPS.Annual[,list(Total.Rain=mean(Total.Rain),Total.Rain.sd=sd(Total.Rain)),by=c("Site.Key")]

# Round results
CHIRPS.Annual[,3:ncol(CHIRPS.Annual)]<-round(CHIRPS.Annual[,-c(1:2)],3)
CHIRPS.LT[,2:ncol(CHIRPS.LT)]<-round(CHIRPS.LT[,-c(1)],3)

# Save results
save(CHIRPS.Annual,file=paste0(ClimatePast,"CHIRPS/CHIRPS Annual.RData"))
save(CHIRPS.LT,file=paste0(ClimatePast,"CHIRPS/CHIRPS.LT.RData"))
