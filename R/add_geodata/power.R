# This script downloads NASA POWER data for unique locations in ERA.
# It depends on these scripts:
#   - Run ERA_dev/R/0_set_env.R first.
#   - Run ERA_dev/R/add_geodata/elevation.R to calculate altitude data.
#
# The script proceeds through these major steps:
#   0) Workspace set-up (loading packages, subsetting sites, merging altitude, etc.)
#   1) Downloading POWER data using the NASA POWER API.
#   2) Processing and aggregating the downloaded data (annual averages, long-term averages).

# 0) Set-up workspace ####

  ## 0.1) Load packages & functions #####
  # Load required packages using pacman::p_load for concise syntax.
  p_load(terra, sf, data.table, RCurl, future, future.apply, progressr, pbapply)
  
  # Source external R scripts for PET calculations and POWER data functions.
  # These functions are hosted on GitHub and must be accessible.
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/PETcalc.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/download_power.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/process_power.R")
  
  ## 0.2) Subset ERA sites according to buffer distance #####
  # Limit to sites with a buffer distance below 50,000 units.
  SS <- era_locations[Buffer < 50000]
  # Subset the spatial vector of ERA sites to those with buffer < 50000.
  pbuf_g <- era_locations_vect_g[era_locations_vect_g$Buffer < 50000, ]
  
  ## 0.3) Merge altitude data #####
  # Read in elevation data from a CSV file located in the ERA geodata directory.
  era_elevation <- fread(file.path(era_dirs$era_geodata_dir, "elevation.csv"))
  # Extract mean elevation for each site (Site.Key) where the variable is "elevation".
  altitude_data <- era_elevation[variable == "elevation" & stat == "mean", .(Site.Key, value)]
  # Rename the 'value' column to 'Altitude' to match expected naming conventions.
  setnames(altitude_data, "value", "Altitude")
  
  # Merge the altitude data with the site buffer spatial object based on 'Site.Key'.
  n_before<-length(pbuf_g)
  pbuf_g <- merge(pbuf_g, altitude_data, by = "Site.Key")
  n_after<-length(pbug_g)
  
  if(n_after<n_before){
    warning("Some sites losts because they lack altitude data.")
    }
  
  ## 0.3) Create POWER parameter object #####
  # Define a named character vector mapping human-readable parameter names to the NASA POWER API codes.
  # Note: There is a correction for PS to PSC (see comments below regarding Pressure).
  parameters <- c(
    SRad              = "ALLSKY_SFC_SW_DWN",   # Insolation Incident on a Horizontal Surface - MJ/m^2/day
    Rain              = "PRECTOT",             # Precipitation - mm day-1
    Pressure.Corrected= "PSC",                 # Corrected Atmospheric Pressure (Adjusted For Site Elevation) - kPa
    Pressure          = "PS",                  # Surface Pressure - kPa
    Specific.Humid    = "QV2M",                # Specific Humidity at 2 Meters - kg kg-1
    Humid             = "RH2M",                # Relative Humidity at 2 Meters - %
    Temp.Mean         = "T2M",                 # Temperature at 2 Meters - C
    Temp.Max          = "T2M_MAX",             # Maximum Temperature at 2 Meters - C
    Temp.Min          = "T2M_MIN",             # Minimum Temperature at 2 Meters - C
    WindSpeed         = "WS2M"                 # Wind Speed at 2 Meters - m/s
  )
  
  ## 0.5) Set POWER data start and end dates #####
  # Define the temporal window for data download.
  date_start <- "1984-01-01"
  date_end   <- "2024-12-31"
  
  ## 0.6) Set workers for parallel downloads ####
  # Define the number of parallel workers for the download operation.
  worker_n <- 10
  
# 1) Download POWER ####

  ## 1.1) Run download function ####
  # Increase the RCurl timeout option to allow for slow connections.
  curlSetOpt(timeout = 190)
  
  # Set the overwrite flag. Set to FALSE to avoid re-downloading files that already exist.
  overwrite <- FALSE
  
  # Call the download_power function to fetch data from the NASA POWER API.
  # This function uses parallel processing and progress reporting.
  dl_errors <- download_power(
    site_vect      = pbuf_g,           # Spatial object containing site information
    date_start     = date_start,       # Start date for the download period
    date_end       = date_end,         # End date for the download period
    altitude_field = "Altitude",       # Field in pbuf_g containing altitude data
    save_dir       = era_dirs$power_dir, # Directory where CSV files will be saved
    parameters     = parameters,       # Parameter codes for the API
    user_name      = "CIAT",           # Username for the NASA POWER API
    id             = "Site.Key",       # Field used as the site identifier
    verbose        = FALSE,            # Set to TRUE for detailed output during download
    attempts       = 3,                # Number of retry attempts for failed downloads
    worker_n       = worker_n,         # Number of parallel workers
    overwrite      = overwrite         # Flag indicating whether to overwrite existing files
  )
  
  # Check for incomplete downloads:
  # List all CSV files in the download directory and calculate file sizes (in KB).
  files <- list.files(era_dirs$power_dir, ".csv", full.names = TRUE)
  file_sizes <- file.info(files)$size / 10^3
  hist(file_sizes)  # Display a histogram to inspect file sizes (e.g., files should be >900kb).
  
  ## 1.2) Recompile Data ####
  # List CSV files again (ensuring file names end with .csv exactly).
  files <- list.files(era_dirs$power_dir, ".csv$", full.names = TRUE)
  
  # Process the downloaded CSV files into a consolidated dataset.
  # The process_power function aggregates and processes the POWER data.
  power_data <- process_power(
    files         = files,
    parameters    = parameters,
    rename        = names(parameters),  # Rename columns using the names defined in parameters
    id_field      = "Site.Key",
    altitude_field= "Altitude",
    add_date      = TRUE,
    add_daycount  = TRUE,
    time_origin   = time_origin,  # Must be defined in your environment
    add_pet       = TRUE,
    altitude_data = altitude_data,
    worker_n      = worker_n
  )
  
  # Save the processed data to a Parquet file, with today's date appended.
  save_file <- file.path(era_dirs$era_geodata_dir, paste0("POWER_", Sys.Date(), ".parquet"))
  arrow::write_parquet(power_data, save_file)
  
# 2) Calculate annual averages ####

  ## 2.1.1) Remove incomplete years ####
  # For each Site.Key and Year, count the number of observations.
  # Remove groups with fewer than 365 records (i.e., incomplete years).
  power_data <- power_data[, N := .N, by = list(Site.Key, Year)][!N < 365][, N := NULL]
  
  ## 2.1.2) Calculate annual data ####
  # For each Site.Key and Year, compute annual aggregates:
  # - Total rainfall, total PET (ETo), mean humidity, temperature statistics, etc.
  power_data_annual <- power_data[, list(
    Total.Rain      = as.integer(sum(Rain)),
    Total.ETo       = as.integer(sum(ETo)),
    S.Humid.Mean    = round(mean(Specific.Humid), 2),
    Humid.Mean      = round(mean(Humid), 2),
    Temp.Mean.Mean  = round(mean(Temp.Mean), 2),
    Mean.N.30.Days  = as.integer(sum(Temp.Mean > 30)),
    Mean.N.35.Days  = as.integer(sum(Temp.Mean > 35)),
    Temp.Max.Mean   = round(mean(Temp.Max), 2),
    Temp.Max        = max(Temp.Max),
    Max.N.40.Days   = as.integer(sum(Temp.Max > 40)),
    Temp.Min.Mean   = round(mean(Temp.Min), 2),
    Temp.Min        = min(Temp.Min)
  ), by = .(Site.Key, Year)]
  
  ## 2.1.3) Calculate LT (Long-Term) Data ####
  # Compute long-term averages and standard deviations by Site.Key across years.
  power_data_lt <- power_data_annual[, list(
    Total.Rain.Mean    = mean(Total.Rain),
    Total.ETo.Mean     = mean(Total.ETo),
    S.Humid.Mean       = mean(S.Humid.Mean),
    Humid.Mean         = mean(Humid.Mean),
    Temp.Mean.Mean     = mean(Temp.Mean.Mean),
    Mean.N.30.Days     = mean(Mean.N.30.Days),
    Mean.N.35.Days     = mean(Mean.N.35.Days),
    Temp.Max.Mean      = mean(Temp.Max.Mean),
    Temp.Max           = mean(Temp.Max),
    Max.N.40.Days      = mean(Max.N.40.Days),
    Temp.Min.Mean      = mean(Temp.Min.Mean),
    Temp.Min           = mean(Temp.Min),
    Total.Rain.sd      = sd(Total.Rain),
    Total.ETo.sd       = sd(Total.ETo),
    S.Humid.Mean.sd    = sd(S.Humid.Mean),
    Humid.Mean.sd      = sd(Humid.Mean),
    Temp.Mean.Mean.sd  = sd(Temp.Mean.Mean),
    Mean.N.30.Days.sd  = sd(Mean.N.30.Days),
    Temp.Max.Mean.sd   = sd(Temp.Max.Mean),
    Temp.Max.sd        = sd(Temp.Max),
    Max.N.40.Days.sd   = sd(Max.N.40.Days),
    Temp.Min.Mean.sd   = sd(Temp.Min.Mean),
    Temp.Min.sd        = sd(Temp.Min)
  ), by = Site.Key]
  
  ## 2.1.4) Round numeric columns ####
  # Define the number of decimal places to round to.
  n_dp <- 2
  
  # Identify numeric columns and round them in the long-term dataset.
  numeric_cols <- names(power_data_lt)[sapply(power_data_lt, is.numeric)]
  power_data_lt[, (numeric_cols) := lapply(.SD, round, n_dp), .SDcols = numeric_cols]
  
  # Similarly, round numeric columns in the annual dataset.
  numeric_cols <- names(power_data_annual)[sapply(power_data_annual, is.numeric)]
  power_data_annual[, (numeric_cols) := lapply(.SD, round, n_dp), .SDcols = numeric_cols]
  
  ## 2.1.5) Save annual & long-term datasets ####
  # Write the annual and long-term aggregated data to separate Parquet files.
  arrow::write_parquet(power_data_annual, gsub("POWER_", "POWER_annual_", save_file))
  arrow::write_parquet(power_data_lt, gsub("POWER_", "POWER_ltavg_", save_file))