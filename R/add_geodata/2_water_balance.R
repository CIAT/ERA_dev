# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)
#
# Description:
# This script calculates daily water balance for ERA sites using two different soil datasets (ISDA soils and SoilGrids2.0).
# The pipeline includes:
# - Reading and processing soil horizon data from ISDA and SoilGrids2.0 sources.
# - Reading daily weather data (POWER & CHIRPS datasets) and merging them.
# - Running a full water balance calculation for each soil dataset.
# - Saving the results.
# - Optionally validating the outputs.

# Prerequisites:
# Before running, ensure the following scripts have been executed:
# 1. R/0_set_env.R
# 2. R/add_geodata/chirps.R
# 3. R/add_geodata/power.R
# 4. R/add_geodata/soilgrids.R (ISDA soils data)
# 5. R/add_geodata/soilgrids2.0.R (SoilGrids2.0 soils data)

# 0) Set-up workspace ####

## 0.1) Load required packages & functions ####
pacman::p_load(data.table, future, future.apply, progressr, pbapply, parallel, arrow)

# Source water balance function
source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/water_balance.R")

## 0.2) Set number of parallel workers ####
worker_n <- 4

# 1) Read and process soil horizon data ####
## 1.1) ISDA soils ####

# Read ISDA horizon data
files <- list.files(era_dirs$era_geodata_dir, "isda.*parquet", full.names = TRUE)
files<-files[!grepl("watbal",files)]
(files <- tail(files, 1))
horizon_data <- data.table(arrow::read_parquet(files))

# Check data availability is equal between sites (differences could mean duplications)
(check<-horizon_data[,.N,by=Site.Key][,unique(N)])
if(length(check)>1){
  stop("Uneven data availability between sites in 1.1) ISDA Soils")
}

# Read ISDA metadata
horizon_metadata <- unique(fread(file.path(era_dirs$era_geodata_dir, "isda_metadata.csv")))

# Variable conversion mapping: from ISDA variable names to ERA standards
vars <- data.table(
  from = c("depth", "sand.tot.psa", "silt.tot.psa", "clay.tot.psa", "oc", "db.od", "ecec.f", "ph.h2o"),
  to = c("depth", "SNDPPT", "SLTPPT", "CLYPPT", "ORCDRC", "BLD", "CEC", "PHIHOX"),
  conversion = c(NA, 10, 10, 10, 1, 1, 1, 10)
)

# Filter, reshape, and format ISDA data
horizon_data <- horizon_data[variable %in% vars[, from] & stat == "mean"]
horizon_data <- dcast(horizon_data, Site.Key + depth ~ variable, value.var = "value")
horizon_data[, depth := gsub("0-20cm", 10, depth)][, depth := gsub("20-50cm", 35, depth)][, depth := as.numeric(depth)]

# Rename and convert units
for (i in seq_len(nrow(vars))) {
  old_col <- vars$from[i]
  new_col <- vars$to[i]
  conv <- vars$conversion[i]
  setnames(horizon_data, old = old_col, new = new_col)
  if (!is.na(conv)) horizon_data[[new_col]] <- horizon_data[[new_col]] * conv
}

# Remove rows missing clay data (essential for water balance)
horizon_data <- horizon_data[!is.na(CLYPPT)]

# Ensure data.table format
setDT(horizon_data)
horizon_isda <- copy(horizon_data)

## 1.2) SoilGrids2.0 soils ####

# Read latest SoilGrids2.0 file
files <- list.files(era_dirs$era_geodata_dir, "soilgrids2[.]0_.*parquet", full.names = TRUE)
files<-files[!grepl("watbal",files)]
(files <- tail(files, 1))
horizon_data <- data.table(arrow::read_parquet(files))

# Read SoilGrids metadata
horizon_metadata <- unique(fread(file.path(era_dirs$era_geodata_dir, "soilgrids2.0_metadata.csv")))

# Variable conversion mapping
vars <- data.table(
  from = c("sand", "silt", "clay", "soc", "bdod", "cec", "phh2o"),
  to = c("SNDPPT", "SLTPPT", "CLYPPT", "ORCDRC", "BLD", "CEC", "PHIHOX"),
  conversion = c(10, 10, 10, 1, 1, 1, 10)
)

# Calculate mean value per site, depth, variable
horizon_data <- horizon_data[, .(value = mean(value, na.rm = TRUE)), by = .(Site.Key, depth, variable, stat)]

# Check data availability is equal between sites (differences could mean duplications)
(check<-horizon_data[,.N,by=Site.Key][,unique(N)])
if(length(check)>1){
  stop("Uneven data availability between sites in 1.2) SoilGrid2.0")
}

# Filter and reshape
horizon_data <- horizon_data[variable %in% vars[, from] & stat == "mean"]
horizon_data <- dcast(horizon_data, Site.Key + depth ~ variable, value.var = "value")

# Format depth intervals (midpoints)
horizon_data[, depth := gsub("0-5cm", 2.5, depth)
][, depth := gsub("5-15cm", 10, depth)
][, depth := gsub("15-30cm", 22.5, depth)
][, depth := gsub("30-60cm", 45, depth)
][, depth := gsub("60-100cm", 80, depth)
][, depth := as.numeric(depth)]

# Rename columns and convert units
for (i in seq_len(nrow(vars))) {
  print(i)
  old_col <- vars$from[i]
  new_col <- vars$to[i]
  conv <- vars$conversion[i]
  setnames(horizon_data, old = old_col, new = new_col)
  if (!is.na(conv)) horizon_data[[new_col]] <- horizon_data[[new_col]] * conv
}

# Remove incomplete rows
horizon_data <- horizon_data[!is.na(CLYPPT)]
setDT(horizon_data)
horizon_soilgrids2 <- copy(horizon_data)

## 1.3) Check for missing sites ####
soil_sites<-unique(c(horizon_isda[,Site.Key],horizon_soilgrids2[,Site.Key]))

# Mauritius and Cabo Verde are not covered
era_locations[!Site.Key %in% soil_sites & Buffer<50000 & ! Country %in% c("Mauritius","Cabo Verde"),]


# 2) Read daily weather data (POWER + CHIRPS) ####

# POWER data
files <- list.files(era_dirs$era_geodata_dir, "power.*parquet", full.names = TRUE, ignore.case = TRUE)
files <- files[!grepl("annual|ltavg", files)]
(files<-tail(files,1))
power <- arrow::read_parquet(files)

# Check for duplicate entries in power
(check<-unique(power[,.N,by=.(Site.Key,DayCount)][N>1][,.(Site.Key,N)]))
if(nrow(check)>1){
  stop("2) Duplicates in power data")
}

# CHIRPS data
files <- list.files(era_dirs$era_geodata_dir, "chirps.*parquet", full.names = TRUE, ignore.case = TRUE)
files <- files[!grepl("annual|ltavg", files)]
(files<-tail(files,1))
chirps <- arrow::read_parquet(files)

# Replace POWER rainfall with CHIRPS rainfall
setnames(chirps, c("Rain", "day_count"), c("Rain_chirps", "DayCount"), skip_absent = TRUE)

# Check for duplicate entries in chirps
(check<-unique(chirps[,.N,by=.(Site.Key,DayCount)][N>1][,.(Site.Key,N)]))
if(nrow(check)>1){
  stop("2) Duplicates in chirps data")
}

weather_data <- merge(power,chirps, by = c("Site.Key", "DayCount"), all.x = TRUE, sort = FALSE)


if(nrow(power) != nrow(weather_data)){
  stop(paste0("2) nrow power = ",nrow(power),", nrow weather data = ",nrow(weather_data)))
}

weather_data[, Rain := Rain_chirps][, Rain_chirps := NULL]
power <- NULL
chirps <- NULL

# Standardize field names
from <- c("Site.Key", "Date", "Temp.Min", "Temp.Max", "Temp.Mean", "Rain", "SRad")
to <- c("Site.Key", "DATE", "TMIN", "TMAX", "TMEAN", "RAIN", "SRAD")
setnames(weather_data, from, to)
weather_data <- weather_data[, ..to]

# Remove incomplete records
weather_data <- weather_data[!is.na(TMIN) & !is.na(RAIN)]

# 3) Run water balance pipeline ####
options(future.globals.maxSize = 15 * 1024^3)  # Increase memory limit (15 GiB)

## 3.1) ISDA soils ####
watbal_result_isda <- run_full_water_balance(
  horizon_data = copy(horizon_isda),
  weather_data = copy(weather_data),
  worker_n = 5,
  root_depth = 60,
  min_depth = 45,
  max_depth = 100,
  id_field = "Site.Key"
)

## Round results to save space
watbal_result_isda[,ETMAX:=round(ETMAX,2)
                   ][,AVAIL:=round(AVAIL,2)
                     ][,ETMAX:=round(ETMAX,2)
                       ][,ERATIO:=round(ERATIO,2)
                         ][,DEMAND:=round(DEMAND,2)
                           ][,RUNOFF:=round(RUNOFF,2)
                             ][,scp:=round(scp,2)
                               ][,ssat:=round(ssat,2)
                                 ][,TMIN:=round(TMIN,1)
                                   ][,TMAX:=round(TMAX,1)
                                     ][,TMEAN:=round(TMEAN,1)
                                       ][,SRAD:=round(SRAD,1)
                                         ][,RAIN:=round(RAIN,1)]

(check<-unique(watbal_result_isda[,.(N=.N),by=Site.Key][N>min(N),.(Site.Key,N)]))
if(nrow(check)>0){
  stop("Potential duplicates in watbal_result_isda")
}

## Save ISDA result
arrow::write_parquet(watbal_result_isda, file.path(era_dirs$era_geodata_dir, paste0("watbal-isda_", Sys.Date(), ".parquet")))

## 3.2) SoilGrids2.0 soils ####
watbal_result_soilgrids2 <- run_full_water_balance(
  horizon_data = copy(horizon_soilgrids2),
  weather_data = copy(weather_data),
  worker_n = 5,
  root_depth = 60,
  min_depth = 45,
  max_depth = 100,
  id_field = "Site.Key"
)

## Round results to save space
watbal_result_soilgrids2[,ETMAX:=round(ETMAX,2)
][,AVAIL:=round(AVAIL,2)
][,ETMAX:=round(ETMAX,2)
][,ERATIO:=round(ERATIO,2)
][,DEMAND:=round(DEMAND,2)
][,RUNOFF:=round(RUNOFF,2)
][,scp:=round(scp,2)
][,ssat:=round(ssat,2)
][,TMIN:=round(TMIN,1)
][,TMAX:=round(TMAX,1)
][,TMEAN:=round(TMEAN,1)
][,SRAD:=round(SRAD,1)
][,RAIN:=round(RAIN,1)]

(check<-watbal_result_soilgrids2[,.(N=.N),by=Site.Key][,(unique(N))])
if(length(check)>1){
  stop("Potential duplicates in watbal_result_soilgrids2")
}

## Save SoilGrids2.0 result
arrow::write_parquet(watbal_result_soilgrids2, file.path(era_dirs$era_geodata_dir, paste0("watbal-soilgrids2.0_", Sys.Date(), ".parquet")))

# 4) Validate water balance results ####

# Example validation (adjust 'watbal_result' and 'site' as needed)
# val_out <- validate_watbal(watbal_result, site = site, make_plots = TRUE)
# 
# # Show summary stats
# print(val_out$summary_stats)
# 
# # Inspect any problematic AVAIL or LOGGING rows:
# print(val_out$invalid_avail)
# print(val_out$invalid_logging)
# print(watbal_result[, unique(LOGGING)])
# 
# # Display diagnostic plots:
# print(val_out$plots$avail_plot)
# print(val_out$plots$rain_runoff_plot)