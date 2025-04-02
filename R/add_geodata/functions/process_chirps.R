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

#' Extract CHIRPS Rainfall Data at Specified Sites
#'
#' Extracts rainfall data from CHIRPS raster (.tif) files at specific geographic locations, summarizing data to weighted mean rainfall per site. Results are saved as a Parquet file.
#'
#' @param site_vect Spatial object (sf or Spatial*) containing site locations.
#' @param id_field Character. Name of the field in `site_vect` containing unique site identifiers.
#' @param data_dir Character. Directory containing input CHIRPS raster (.tif) files.
#' @param save_dir Character. Directory where output Parquet file (`chirps.parquet`) will be saved.
#' @param round_digits Numeric or NULL. Number of digits to round rainfall values. Default is NULL (no rounding).
#' @param add_daycount Logical. If TRUE, adds a 'day_count' column representing days since `time_origin`.
#' @param time_origin Date. Origin date used for calculating day count.
#' @param max_cells_in_memory Numeric. Maximum number of raster cells processed in memory at once. Default is 3e+07.
#' @param delete_temp Logical. If TRUE, deletes temporary files after processing. Default is TRUE.
#' @param overwrite Logical. If TRUE, existing Parquet output will be overwritten.
#'
#' @import data.table
#' @import terra
#' @import arrow
#' @import sf
#' @import exactextractr
#'
#' @return Saves extracted rainfall data to a Parquet file in `save_dir`. No value returned.
#' @export
extract_chirps <- function(site_vect,
                           id_field,
                           data_dir,
                           existing_data=NULL,
                           round_digits = NULL,
                           add_daycount = TRUE,
                           time_origin,
                           max_cells_in_memory = 3e+07,
                           delete_temp = TRUE,
                           overwrite = FALSE) {
  
  sites <- data.frame(site_vect)[, id_field]

  # Load existing data if present and handle overwrite logic
  if (!is.null(existing_data)) {
    if (!overwrite) {
      site_logical<-!sites %in% existing_data[[id_field]]
      site_vect <- site_vect[site_logical, ]
    } else {
      existing_data <- NULL
    }
  } else {
    existing_data <- NULL
  }
  
  if (length(site_vect) > 0) {
    temp_dir <- file.path(data_dir, "temp")
    if (!dir.exists(temp_dir)) {
      dir.create(temp_dir)
    }
    
    # Identify all raster files in data directory
    files <- data.table(filename = list.files(path = data_dir, pattern = "\\.tif$", full.names = TRUE))
    
    # Parse dates from filenames
    files[, date := gsub("\\.tif$", "", basename(filename))
    ][, date := substr(date[1], nchar(date[1]) - 9, nchar(date[1])), by = date
    ][, date := as.Date(date, format = "%Y.%m.%d")
    ][, year := format(date, "%Y")]
    
    # Process data by year
    results <- lapply(seq(min(files$year), max(files$year)), function(i) {
      
      cat("Processing year", i, "\n")
      save_year_file <- file.path(temp_dir, paste0(i, ".parquet"))
      
      if (!file.exists(save_year_file)) {
        files_year <- files$filename[files$year == i]
        rast_dat <- terra::rast(files_year)
        
        # Extract rainfall using exactextractr
        rast_vals <- exactextractr::exact_extract(rast_dat, sf::st_as_sf(site_vect), max_cells_in_memory = max_cells_in_memory)
        
        rast_vals <- rbindlist(lapply(seq_along(rast_vals), function(j) {
          x <- rast_vals[[j]]
          x$id <- sites[j]
          return(x)
        }))
        
        # Reshape data into long format
        rast_vals <- data.table(melt(rast_vals, id.vars = c("coverage_fraction", "id"), variable.name = "date"))
        rast_vals[value == -9999, value := NA]
        
        # Calculate weighted mean rainfall
        rast_vals <- rast_vals[, .(Rain = weighted.mean(value, coverage_fraction, na.rm = TRUE)), by = .(id, date)]
        
        # Optional rounding
        if (!is.null(round_digits)) {
          rast_vals[, Rain := round(Rain, round_digits)]
        }
        
        # Convert date strings to Date objects
        rast_vals[, date := as.Date(substr(as.character(date), nchar(as.character(date)) - 9, nchar(as.character(date))), format = "%Y.%m.%d")]
        
        # Add day count column if requested
        if (add_daycount) {
          rast_vals[, day_count := as.integer(date - time_origin)]
        }
        
        # Save intermediate yearly data
        arrow::write_parquet(rast_vals, save_year_file)
        
        rm(rast_vals)
        gc()
      } 
     
      return(i)
    })
    
    files<-list.files(temp_dir,"parquet",full.names = T)
    results <- rbindlist(lapply(files,FUN=function(file){arrow::read_parquet(file)}))
    setnames(results, "id", id_field)
    
    # Combine with previous results if needed
    if (!is.null(existing_data)) {
      results <- rbind(existing_data, results)
    }
    
    # Delete temporary files if requested
    if (delete_temp) {
      unlink(temp_dir, recursive = TRUE)
    }
    
    return(results)
    
  }else{
    warning("No new sites discovered")
    return(NULL)
  }
}
