# This script downloads soilgrids data for unique locations in ERA. 
# Run ERA_dev/R/0_set_env before executing this script

# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####
p_load(terra,sf,data.table,geodata,exactextractr,future.apply,progressr)

# 1) Prepare ERA data ####
SS<-era_locations[Buffer<50000]
pbuf_g<-era_locations_vect_g[era_locations[,Buffer<50000]]

# 2) Download soilgrids data ####

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

soil_files<-rbind(copy(soil_files)[,depth:=20],copy(soil_files)[,depth:=50])[,source:="https://rdrr.io/github/rspatial/geodata/man/soil_af_isda.html"]

fwrite(soil_files,file.path(era_dirs$era_geodata_dir,"soil_af_isda_metadata.csv"))

# Download data in parallel
n_workers<-5
# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("txtprogressbar")

# Set up parallel processing plan
future::plan(multisession, workers = n_workers)  # Adjust 'workers' based on available cores

# Function to process each row with tryCatch and suppress messages
process_soil_file <- function(i, soil_files, era_dirs) {
  attempts <- 0
  success <- FALSE
  max_attempts <- 3
  
  while (attempts < max_attempts && !success) {
    attempts <- attempts + 1
    tryCatch({
      # Suppress messages from the function call
      suppressMessages({
        geodata::soil_af_isda(var = soil_files$var[i], depth = soil_files$depth[i], path = era_dirs$soilgrid_dir)
        geodata::soil_af_isda(var = soil_files$var[i], depth = soil_files$depth[i], path = era_dirs$soilgrid_dir, error = TRUE)
      })
      success <- TRUE  # Set success flag to TRUE if no error occurs
    }, error = function(e) {
      if (attempts >= max_attempts) {
        message("Failed after ", max_attempts, " attempts for row ", i)
      }
    })
  }
}

# Parallel processing with progressr
with_progress({
  # Create progress bar
  progress <- progressr::progressor(along = 1:nrow(soil_files))
  
  future.apply::future_lapply(1:nrow(soil_files), function(i) {
    # Call the processing function
    process_soil_file(i, soil_files, era_dirs)
    # Update progress
    progress()
  })
})

# Non parallel version of the above in case you are experiencing issues
if(F){
for(i in 1:nrow(soil_files)){
    geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir)
    geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir,error=T)
}
}

# 3) Extract soil grids data for era buffers ####
overwrite<-F # Re-extract all data that exists for era sites?
soil_file<-file.path(era_dirs$era_geodata_dir,"era_site_soil_af_isda.parquet")
isda_dir<-file.path(era_dirs$soilgrid_dir,"soil_af_isda")

# Filter out sites for which data have already been extracted
if(file.exists(soil_file) & overwrite==F){
  existing_data<-arrow::read_parquet(soil_file)
  pbuf_g<-pbuf_g[!pbuf_g$Site.Key %in% existing_data[,unique(Site.Key)]]
}

# Stack all the rasters in the soil directory and fast extract data by era site buffers
data<-terra::rast(list.files(isda_dir,".tif$",full.names = T))
data_ex<- exactextractr::exact_extract(data, sf::st_as_sf(pbuf_g), fun = "mean", append_cols = c("Site.Key"))
colnames(data_ex)<-gsub("mean.","",colnames(data_ex))
data_ex$stat<-"mean"

data_ex_median<- exactextractr::exact_extract(data, sf::st_as_sf(pbuf_g), fun = "median", append_cols = c("Site.Key"))
colnames(data_ex_median)<-gsub("median.","",colnames(data_ex_median))
data_ex_median$stat<-"median"

data_ex<-unique(data.table(rbind(data_ex,data_ex_median)))
data_ex_m<-melt(data_ex,id.vars=c("Site.Key","stat"))
data_ex_m[grep("error",variable),error:="error"
          ][!grepl("error",variable),error:="value"
            ][,variable:=gsub("-error","",variable)
            ][,variable:=gsub("c.tot.","c.tot_",variable)
              ][,depth:=unlist(tstrsplit(variable,"_",keep=2))
                ][,variable:=unlist(tstrsplit(variable,"_",keep=1))]

data_ex_m<-dcast(data_ex_m,Site.Key+stat+variable+depth~error,value.var = "value")

if(file.exists(soil_file) & overwrite==F){
  data_ex_m<-unique(rbind(existing_data,data_ex_m))
}

data_ex_m[,error:=round(error,2)][,value:=round(value,2)]

arrow::write_parquet(data_ex_m,soil_file)

