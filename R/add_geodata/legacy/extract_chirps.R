# First run R/0_set_env.R and R/add_geodata/download_chirps_chirts.R
# Note: This updated script was lost by SCiO in CGlabs but now uses an exactextractr‐based approach.

# 0) Set-up workspace ####

# 0.1) Load packages and create functions #####
# Define the required packages and load them using pacman::p_load.
packages <- c("terra", "data.table", "arrow", "pbapply", "future", "future.apply", "sf", "exactextractr")
pacman::p_load(char = packages)

# 0.2) Set workers for parallel ####
worker_n <- parallel::detectCores() 

# 1) Prepare ERA data ####

# Subset ERA locations with a buffer less than 50,000 (the unit depends on your CRS)
SS <- era_locations[Buffer < 50000]

# Subset the spatial vector of ERA locations based on the same buffer criteria.
# Convert the subset to an sf object for compatibility with exact_extract.
sites_vect <- era_locations_vect_g[era_locations[, Buffer < 50000]]
sites_vect <- sf::st_as_sf(sites_vect)

# 2) Index CHIRPS files ####

# List all .tif files from the 'chirps_dir' directory (recursively) and store in a data.table.
file_index_chirps <- data.table(file_path = list.files(chirps_dir, ".tif$", full.names = TRUE, recursive = TRUE))

# Process the file_index_chirps to extract file names, dates, file sizes, years, and add metadata:
file_index_chirps[, file_name := basename(file_path)]                      # Get the file name from the path.
file_index_chirps[, date := gsub(".tif", "", gsub("chirps-v2.0.", "", file_name))]  # Remove prefix and extension to isolate the date string.
file_index_chirps[, date := as.Date(date, format = "%Y.%m.%d")]              # Convert the string to a Date object.
file_index_chirps[, file_size := (file.info(file_path)$size / 10^6)]           # Calculate file size in MB.
file_index_chirps[, year := format(date, "%Y")]                              # Extract the year from the date.
file_index_chirps[, var := "prec"]                                           # Add a column for variable name (precipitation).
file_index_chirps[, dataset := "chirps_v2.0"]                                # Add a column for the dataset identifier.

# Check overall file size (for informational purposes)
(file_size <- file_index[, sum(file_size[1])])
# Optionally, print total dataset size in GB and size for the year 2000.
if (TRUE) {
  file_index[, sum(file_size / 1000)]
  file_index[year == 2000, sum(file_size) / 1000]
}

# 3) Extract sites ####

# Use the file_index_chirps as the primary index for processing.
file_index <- file_index_chirps

# Define parallel processing parameters:
files_per_worker <- 10    # Number of files to process per worker.
# The following line appears to compute a rough estimate (not stored) of total workload:
worker_n * file_size * files_per_worker / 1000

# Create an 'index' column to group files into chunks of 10 (i.e. one group per worker task).
file_index[, index := ceiling(.I / files_per_worker)]  # .I represents the row number in data.table.

# Set up a multisession future plan for parallel processing.
future::plan("multisession", workers = worker_n)

# Set up progress reporting using the progressr package.
progressr::handlers(global = TRUE)
progressr::handlers("progress")

# Use progressr to wrap the extraction process.
p <- progressr::with_progress({
  # Create a progressor that will iterate over the number of chunks.
  progress <- progressr::progressor(along = 1:max(file_index$index))
  
  # Process each chunk in parallel.
  data_ex <- future.apply::future_lapply(1:max(file_index$index), FUN = function(i) {
    # Update progress with the current block number.
    progress(sprintf("Block %d/%d", i, max(file_index$index)))
    # Retrieve the file paths for the current chunk.
    files <- unlist(file_index[file_index$index == i, "file_path"])
    # Read the raster files using terra::rast.
    data <- terra::rast(files)
    # Extract values from the raster for each ERA site polygon using exact_extract.
    # The 'include_cols' argument pulls the 'Site.Key' from the spatial object.
    data_ex <- data.table::rbindlist(exactextractr::exact_extract(data, sites_vect, fun = NULL, include_cols = "Site.Key"))
    return(data_ex)
  })
})

# Reset the future plan to sequential processing.
future::plan(sequential)

# Melt and average the extracted data from each chunk:
# Iterate over the list 'data_ex' using pbapply for progress reporting.
data_ex_melt <- rbindlist(pbapply::pblapply(1:length(data_ex), FUN = function(i) {
  dat <- data_ex[[i]]
  # Melt the data.table from wide to long format using 'Site.Key' and 'coverage_fraction' as id variables.
  dat <- melt(dat, id.vars = c("Site.Key", "coverage_fraction"))
  # Calculate weighted mean, maximum, and minimum for each Site.Key and variable.
  dat <- dat[, list(
    mean = weighted.mean(value, coverage_fraction, na.rm = TRUE),
    max  = max(value, na.rm = TRUE),
    min  = min(value, na.rm = TRUE)
  ), by = .(Site.Key, variable)]
  return(dat)
}))

# Post-process the melted data:
# Round the computed statistics to one decimal place,
# extract the date from the variable name,
# and then remove the now redundant 'variable' column.
data_ex_melt[, mean := round(mean, 1)
][, max := round(max, 1)
][, min := round(min, 1)
][, date := as.Date(gsub("chirps-v2.0.", "", variable), format = "%Y.%m.%d")
][, variable := NULL]


# 3.1) Save results ######
# Write the final processed data to a Parquet file with the current date in the filename.
arrow::write_parquet(data_ex_melt, file.path(era_dirs$era_geodata_dir, paste0("CHIRPS_", Sys.Date(), ".parquet")))

# 4) Calculate annual and long-term averages ####
# NEED TO PICK UP FROM HERE!!!##### 

data_ex_melt[,Year:=format(Date,"%Y"),by=Date]

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
