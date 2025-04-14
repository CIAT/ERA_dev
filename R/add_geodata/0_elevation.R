# Elevation Data Download & Processing Pipeline
#
# Purpose:
# This script sets up the R workspace and automates the retrieval and processing
# of elevation (DEM) data for specified geographic locations. It downloads elevation
# statistics and topographic information using parallel processing and saves the outputs
# in both Parquet and CSV formats.
#
# Key Actions:
# 1. Load required packages and functions, and adjust global options (e.g., timeout, globals size).
# 2. Filter the input location data (era_locations) based on buffer size criteria.
# 3. Retrieve elevation data using the get_elevation function with parallel processing.
# 4. Save the elevation results in Parquet and CSV formats for further analysis.
#
# Inputs:
# - era_locations: Data frame with location data including latitude, longitude, Site.Key,
#   and Buffer.
# - era_dirs: List containing directory paths for saving DEM and geodata files.
# - get_elevation function from a remote source.
#
# Outputs:
# - Elevation statistics and topographic data, saved as a Parquet file and a CSV file.
#
# Note: Ensure the 'era_dirs' and 'era_locations' objects are defined and that the
# environment is initialized by first running R/0_set_env.R.

# 1) Set-up workspace ####
  ## 1.1) Load packages and create functions #####
  pacman::p_load(data.table,pbapply,future,future.apply,elevatr,RCurl)
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/get_elevation.R")
  
  ## 1.2) Increase globals size ####
  options(future.globals.maxSize = 2 * 1024^3)
# 2) Download data ####
  
  results<-get_elevation(df=era_locations[order(Buffer)][Buffer<50000],
                      out_dir=era_dirs$dem_dir,
                      z = 12,
                      overwrite = FALSE,
                      max_tries = 3,
                      workers = 15,
                      use_progressr = TRUE,
                      calc_topo=T,
                      lat_col = "Latitude",
                      lon_col = "Longitude",
                      id_col = "Site.Key",
                      max_buffer=50000,
                      buffer_col = "Buffer")
  
  arrow::write_parquet(results$stats,file.path(era_dirs$era_geodata_dir,paste0("elevation_",Sys.Date(),".parquet")))
  fwrite(results$stats,file=file.path(era_dirs$era_geodata_dir,paste0("elevation_",Sys.Date(),".csv")))

  