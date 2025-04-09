# First run R/0_set_env.R 

##########################################################
# ERA Other Data Extraction Script
#
# Description:
#   This script extracts and summarizes raster data for 
#   specific ERA sites using a compendium dataset and a 
#   directory of raster files. The extraction is performed 
#   for each ERA site (and its buffer) defined in the input 
#   locations. The result is a summary table (saved in Parquet 
#   format) that includes the proportion of coverage for each 
#   raster value at each site.
#
# Inputs:
#   - Vector of locations (e.g., 'era_locations_vect_g')
#   - Directory of raster files
#
# Output:
#   - A parquet file containing the summary table for each ERA 
#     site and its associated buffer.
#
# Dependencies:
#   - pacman, data.table, exactextractr, terra, sp, pbapply, arrow
#
##########################################################

# 1) Load required packages using pacman ####
if(!require("pacman", character.only = TRUE)){
  install.packages("pacman", dependencies = TRUE)
}
p_load(data.table, exactextractr, terra, sp, pbapply, arrow)

# 2) Prepare Input Data ####

# Convert ERA locations vector (assumed to be loaded into 'era_locations_vect_g')
# into a Spatial object required for extraction.
locations <- as(era_locations_vect_g, "Spatial")

# List all raster files (.tif or .asc) in the specified directory.
# The directory 'era_dirs$aez_dir' should be defined in your environment.
files <- list.files(era_dirs$aez_dir, ".tif$|asc$", full.names = TRUE)

# 3) Define Helper Functions ####

  ## 3.1) Function: get_mode ####
  # Computes the mode (most frequently occurring value) of a vector.
  # If there are multiple modes, they are concatenated using a specified delimiter.
  get_mode <- function(x, delim = ";") {
    freq_table <- table(x)            # Frequency count for each unique value
    max_freq <- max(freq_table)         # Maximum frequency
    modes <- names(freq_table[freq_table == max_freq])  # Extract mode(s)
    paste(modes, collapse = delim)      # Return concatenated modes
  }
  
  ## 3.2) Function: map_vals ####
  # Maps a concatenated string of values to their categorical descriptions.
  # Splits the input string by a delimiter, matches each value to a mapping table,
  # and concatenates the resulting categories.
  map_vals <- function(value, mappings, delim = ",") {
    vals <- unlist(strsplit(value, delim))  # Split the input string into individual values
    # Match the split values to the mapping table and concatenate the 'category' field.
    category <- mappings[match(vals, value), paste(category, collapse = delim)]
    return(category)
  }

# 4) Process Each Raster File ####

# Loop over each raster file and process extraction.
result <- lapply(1:length(files), FUN = function(i) {
  file <- files[i]
  cat("Processing file ", i, "/", length(files), basename(file), "\n")
  
  # Load the current raster file
  dat_rast <- terra::rast(file)
  
  # Extract raster values for each feature in 'locations'
  # exact_extract returns a list with one data.frame per polygon.
  results <- exactextractr::exact_extract(dat_rast, locations)
  
  # Combine the list of extraction results into a single data.table.
  # For each polygon, replicate its attributes to align with the extracted values.
  results <- rbindlist(pblapply(1:length(results), FUN = function(i) {
    x <- results[[i]]
    x <- cbind(x, data.frame(locations)[i, ][rep(1, nrow(x)), ])
    return(x)
  }))
  
  # Summarize the extracted data:
  #   - Group by raster value and location attributes (Latitude, Longitude, Site.Key, Buffer)
  #   - Sum the 'coverage_fraction' to get total coverage per group.
  results <- results[, .(coverage = sum(coverage_fraction, na.rm = TRUE)), 
                     by = .(value, Latitude, Longitude, Site.Key, Buffer)]
  
  # Calculate total coverage per location (for normalization).
  results[, coverage_tot := sum(coverage), by = .(Latitude, Longitude, Buffer, Site.Key)]
  
  # Compute the proportion of coverage for each group (rounded to 2 decimals).
  results[, prop := round(coverage / coverage_tot, 2)]
  # Remove intermediate coverage columns as they are no longer needed.
  results[, c("coverage", "coverage_tot") := NULL]
  
  # For each location, determine the group(s) with the maximum coverage proportion.
  results <- results[, prop_max := prop == max(prop), 
                     by = .(Latitude, Longitude, Site.Key, Buffer)]
  results <- results[prop_max == TRUE][, prop_max := NULL]
  
  # Collapse multiple 'value' entries into a single comma-separated string per group.
  results <- results[, .(value = paste(value, collapse = ",")), 
                     by = .(Latitude, Longitude, Site.Key, Buffer, prop)]
  
  # Add the dataset identifier (name of the raster file).
  results[, dataset := basename(file)]
  
  # Apply categorical mapping using the raster's attribute levels.
  # The mapping table is extracted from the raster's 'levels' attribute.
  mappings <- data.table(levels(dat_rast)[[1]])
  results[, value_cat := map_vals(value[1], mappings, delim = ","), by = value]
  
  return(results)
})

# Combine results from all files into a single data.table.
result <- rbindlist(result)

# 5) Save the summarized data to a parquet file ####
arrow::write_parquet(result, file.path(era_dirs$era_geodata_dir, paste0("aez_", Sys.Date(), ".parquet")))

