# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)
#
# Description:
# This script downloads, processes, and cleans SoilGrids 2.0 data for ERA locations outside Africa.
# It involves filtering the site locations, downloading specific soil variables at specified depths,
# handling errors in the download process, and saving the cleaned and merged data as a Parquet file.
# Parallelization and progress tracking are used for efficiency.
#
# Requirements:
  # Please run script R/0_set_env.R before proceeded with this code

# 0) Set-up workspace ####
  ## 0.1) Load required packages and source helper functions ####
  p_load(soilDB, terra, data.table, future.apply, progressr)
  
  # Source function for downloading SoilGrids data
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/download_soilgrids2.R")
  
  ## 0.2) Set the number of parallel workers ####
  worker_n <- 15
  
  ## 0.3) Subset ERA sites to non-African countries and save temporary site vector ####
  
  # Copy existing location vector
  site_vect <- era_locations_vect_g
  
  # Remove African countries and any sites with suspicious country names (containing "..")
  site_vect <- site_vect[!site_vect$Country %in% african_countries][!grepl("[.][.]", site_vect$Country)]
  
  # Filter to sites with buffer <= 50km
  site_vect <- site_vect[site_vect$Buffer <= 50000, ]
  
  # Save temporary shapefile of filtered sites
  temp_site_vect <- file.path(era_dirs$soilgrid_dir, "site_vect_temp.shp")
  terra::writeVector(site_vect, temp_site_vect, overwrite = TRUE)
  
  ## 0.4) Set or create download directory for SoilGrids data ####
  dl_dir <- file.path(era_dirs$soilgrid_dir, "soilgrids2.0_download")
  if (!dir.exists(dl_dir)) {
    dir.create(dl_dir)
  }
  
# 1) Download SoilGrids data ####
  ## 1.1) Define metadata table for variables of interest ####
  meta_data <- data.table(
    Name = c("bdod", "cec", "cfvo", "clay", "nitrogen", "phh2o", "sand", "silt", "soc", 
             "ocd", "ocs", "wv0010", "wv0033", "wv1500"),
    Description = c(
      "Bulk density of the fine earth fraction",
      "Cation Exchange Capacity of the soil",
      "Volumetric fraction of coarse fragments (> 2 mm)",
      "Proportion of clay particles (< 0.002 mm) in the fine earth fraction",
      "Total nitrogen (N)",
      "Soil pH",
      "Proportion of sand particles (> 0.05 mm) in the fine earth fraction",
      "Proportion of silt particles (>= 0.002 mm and <= 0.05 mm) in the fine earth fraction",
      "Soil organic carbon content in the fine earth fraction",
      "Organic carbon density",
      "Organic carbon stocks (0-30cm depth interval only)",
      "Volumetric Water Content at 10kPa",
      "Volumetric Water Content at 33kPa",
      "Volumetric Water Content at 1500kPa"
    ),
    `Mapped units` = c(
      "cg/cm^3", "mmol(c)/kg", "cm^3/dm^3 (vol per mil)", "g/kg", "cg/kg", "pH*10", "g/kg", "g/kg", "dg/kg", "hg/m^3",
      "t/ha", "0.1 v% or 1 mm/m", "0.1 v% or 1 mm/m", "0.1 v% or 1 mm/m"
    ),
    `Conversion factor` = c(100, 10, 10, 10, 100, 10, 10, 10, 10, 10, 10, 10, 10, 10),
    `Conventional units` = c(
      "kg/dm^3", "cmol(c)/kg", "cm^3/100cm^3 (vol%)", "g/100g (%)", "g/kg", "pH", "g/100g (%)", "g/100g (%)",
      "g/kg", "kg/m^3", "kg/m^2", "volume (%)", "volume (%)", "volume (%)"
    )
  )
  
  meta_data$source <- "SoilGrids 2.0"
  meta_data$resolution <- "250m"
  
  # Select variables to download (omit problematic ones)
  variables <- meta_data[!Name %in% c("ocs", "wv0010", "wv0033", "wv1500", "ocd"), Name]
  # Depth layers to download
  depths <- c("0-5", "5-15", "15-30", "30-60", "60-100")
  # Only download mean values
  stats <- c("mean")
  
  fwrite(meta_data,file.path(era_dirs$era_geodata_dir,"soilgrids2.0_metadata.csv"))
  
  ## 1.2) Download SoilGrids data ####
  sg2_data <- download_soilgrids_data(site_vect = temp_site_vect,
                                      variables = variables, 
                                      depth = depths, 
                                      stats = stats,
                                      dl_dir = dl_dir,
                                      worker_n = worker_n)
  
  # Remove temporary shapefile
  unlink(temp_site_vect)
  
  ## 1.3) Error checking & fixes ####
    ### 1.3.1) Import, check, and merge downloaded data ####
    soilgrids_import <- function(dl_dir) {
      files <- list.files(dl_dir, ".csv$", full.names = TRUE)
      
      # Check all downloaded files and remove empty ones
      check <- lapply(seq_along(files), FUN = function(i) {
        cat(i, "/", length(files), "      \r")
        file <- files[i]
        data <- fread(file)
        if (nrow(data) == 0) {
          warning(paste(basename(file), "has no rows. Deleting empty file."))
          unlink(file)
          return(list(data = NULL, error = basename(file)))
        } else {
          data[, file := basename(file)]
          return(list(data = data, error = NULL))
        }
      })
      
      errors <- rbindlist(lapply(check, "[[", "error"))
      results <- rbindlist(lapply(check, "[[", "data"))
      
      # Fix variable names based on file name and expected depth order
      results[, vname := gsub(".csv", "", unlist(tstrsplit(file[1], "_", keep = 2))), by = file]
      
      # Map variable names to correct depth layers
      results[variable == "lyr.1", variable := paste0(vname, "_mean_0.5cm")
      ][variable == "lyr.2", variable := paste0(vname, "_mean_5.15cm")
      ][variable == "lyr.3", variable := paste0(vname, "_mean_15.30cm")
      ][variable == "lyr.4", variable := paste0(vname, "_mean_30.60cm")
      ][variable == "lyr.5", variable := paste0(vname, "_mean_60.100cm")
      ][variable == "lyr.6", variable := paste0(vname, "_mean_100.200cm")
      ][, vname := NULL]
      
      # Extract site key, depth, stat, and variable columns
      results[, Site.Key := unlist(tstrsplit(file[1], "_", keep = 1)), by = file
      ][, depth := unlist(tstrsplit(variable[1], "_", keep = 3)), by = variable
      ][, depth := gsub("[.]", "-", depth[1]), by = depth
      ][, file := NULL
      ][, stat := unlist(tstrsplit(variable[1], "_", keep = 2)), by = variable
      ][, variable := unlist(tstrsplit(variable[1], "_", keep = 1)), by = variable]
      
      return(list(results = results, errors = errors))
    }
    
    # Import and check downloads
    results <- soilgrids_import(dl_dir)
    errors <- results$errors
    results <- results$results
    
    ### 1.3.2) Rebuffer problem sites and retry download ####
    
    # Identify sites missing data
    (missing <- site_vect$Site.Key[!site_vect$Site.Key %in% results$Site.Key])
    
    # Modify buffers for missing sites to prevent errors (increase to 250m)
    era_loc_mod <- era_locations[Site.Key %in% missing]
    era_loc_mod[Buffer <= 200, Buffer := 250]
    
    # Buffer problem sites
    site_vect <- ERAg::Pbuffer(Data = era_loc_mod, ID = "Site.Key", Projected = FALSE)
    site_vect$Country <- era_loc_mod$Country
    
    # Save temporary shapefile of modified sites
    terra::writeVector(site_vect, temp_site_vect, overwrite = TRUE)
    
      #### 1.3.2.1) Re-download problem sites ####
worker_n <- 1  # Use single thread to avoid further errors
sg2_data <- download_soilgrids_data(site_vect = temp_site_vect,
                                    variables = variables, 
                                    depth = depths, 
                                    stats = stats,
                                    dl_dir = dl_dir,
                                    worker_n = if (length(site_vect) < worker_n) { length(site_vect) } else { worker_n })

# Re-import merged data
results <- soilgrids_import(dl_dir)
errors <- results$errors
results <- results$results

# Check again for missing sites
(missing <- site_vect$Site.Key[!site_vect$Site.Key %in% unique(results$Site.Key)])

# 2) Save final merged dataset ####
save_file <- file.path(era_dirs$era_geodata_dir, paste0("soilgrids2.0_", Sys.Date(), ".parquet"))
arrow::write_parquet(results, save_file)
