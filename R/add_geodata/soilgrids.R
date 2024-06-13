# This script downloads soilgrids data for unique locations in ERA. 
# Run ERA_dev/R/0_set_env before executing this script

# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####
packages<-c("terra","sf","data.table","geodata","exact_extract")
p_load(char=packages)

get_soil_grids <- function(vector_box, variable, depth) {
  base_url <- "https://maps.isric.org/mapserv?map=/map/soilgrids_global_250m.map&SERVICE=WCS&VERSION=2.0.1&REQUEST=GetCoverage"
  
  bbox <- ext(vector_box)
  params <- list(
    FORMAT = "image/tiff",
    COVERAGEID = paste(variable, depth, sep = "_"),
    SUBSET = c(paste0("x(", bbox[1], ",", bbox[2], ")"), paste0("y(", bbox[3], ",", bbox[4], ")"))
  )
  
  response <- GET(base_url, query = params)
  
  if (response$status_code == 200) {
    temp_file <- tempfile(fileext = ".tif")
    writeBin(content(response, "raw"), temp_file)
    soil_data <- rast(temp_file)
    crs(soil_data) <- crs(vector_box)
    return(soil_data)
  } else {
    stop("Failed to download soil grids data.")
  }
}

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

soil_files<-rbind(soil_files[,depth:=20],soil_properties[,depth:=50])[,source:="https://rdrr.io/github/rspatial/geodata/man/soil_af_isda.html"]

fwrite(soil_files,file.path(era_dirs$era_geodata_dir,"soil_af_isda_metadata.csv"))

for(i in 1:nrow(soil_files)){
    geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir)
    geodata::soil_af_isda(var=soil_files$var[i],depth = soil_files$depth[i],path=era_dirs$soilgrid_dir,error=T)
  }

# 3) Extract soil grids data for era buffers ####
soil_file<-file.path(era_dirs$era_geodata_dir,"era_site_soil_af_isda.parquet")
isda_dir<-file.path(era_dirs$soilgrid_dir,"soil_af_isda")

data<-terra::rast(list.files(isda_dir,".tif$",full.names = T))
data_ex<- exactextractr::exact_extract(data, sf::st_as_sf(pbuf_g), fun = "mean", append_cols = c("Site.Key"))
data_ex_median<- exactextractr::exact_extract(data, sf::st_as_sf(pbuf_g), fun = "median", append_cols = c("Site.Key"))



