#' @import data.table
#' @import future.apply
#' @import progressr
#' @import terra
#' @import reshape2
NULL

#' Attempt to Fetch SoilGrids Data with Retries
#'
#' This helper function attempts to download SoilGrids data for a given site and soil variable using
#' the \code{fetchSoilGrids} function. It makes up to 3 attempts to retrieve and process the data. If all attempts
#' fail, a warning is printed and the function returns \code{NULL}.
#'
#' @param site A spatial object representing the site extent. Must contain a \code{Site.Key} property.
#' @param var Character. The soil property name to download.
#' @param depths Character vector. Depth intervals (e.g., \code{c("0-5", "5-15")}).
#' @param stats Character. Summary statistic to retrieve (e.g., \code{"mean"}).
#' @param dl_dir Character. Directory path where CSV files are saved.
#' @param file Character. Full file path for the output CSV.
#'
#' @return A \code{data.frame} containing the downloaded and processed soil data, or \code{NULL} if the data
#' could not be downloaded after 3 attempts.
#'
#' @examples
#' \dontrun{
#'   # Assume 'site' is already loaded as a terra object with a Site.Key attribute.
#'   res <- attempt_fetch(site, "bdod", c("0-5", "5-15"), "mean", "./downloads", "site1_bdod.csv")
#' }
attempt_fetch <- function(site, var, depths, stats, dl_dir, file) {
  attempts <- 3
  for (attempt in 1:attempts) {
    result <- tryCatch({
      # Download SoilGrids data for the given site and variable
      soil_data <- fetchSoilGrids(
        x = site,
        grid = TRUE,
        variables = var,             # Soil property of interest
        depth_intervals = depths,    # Depth intervals to retrieve
        target_resolution = c(250, 250), # Resolution in meters
        summary_type = stats         # Summary statistic (e.g., "mean")
      )
      # Project the data to WGS84 (EPSG:4326)
      soil_data <- terra::project(soil_data, "EPSG:4326")
      # Mask the data to the site extent
      soil_data <- terra::mask(soil_data, site)
      # Convert to data.frame and reshape using melt
      soil_data <- data.frame(soil_data)
      soil_data <- suppressMessages(suppressWarnings(melt(soil_data)))
      # Save the processed data as CSV
      fwrite(soil_data, file)
      soil_data  # Return the downloaded data
    }, error = function(e) {
      warning(sprintf("Attempt %d failed for site %s, variable %s: %s", 
                      attempt, site$Site.Key, var, e$message))
      Sys.sleep(1)  # Pause for 1 second before retrying
      NULL
    })
    if (!is.null(result)) return(result)
  }
  warning(sprintf("Failed to download data for site %s, variable %s after %d attempts",
                  site$Site.Key, var, attempts))
  return(NULL)
}

#' Download SoilGrids Data for Multiple Sites with Parallel Processing
#'
#' This function downloads SoilGrids data for multiple sites and soil variables. It accepts a vector of file paths
#' (each corresponding to a spatial site file), loads each site within the worker (using \code{terra::vect()}),
#' and processes the download for each soil variable. Parallel processing is enabled via \code{future.apply} and
#' progress is reported using \code{progressr}.
#'
#' For each site file in \code{site_vect} and each soil variable in \code{variables}, the function first checks whether
#' a CSV file already exists for the given combination. If not, it attempts to download the data using the \code{attempt_fetch}
#' helper function. The downloaded data is projected to WGS84 (EPSG:4326), masked to the site extent, and then reshaped
#' into a \code{data.frame} before being saved.
#'
#' @param site_vect A character vector of file paths to spatial site objects (e.g., shapefiles). Each file must contain
#' a \code{Site.Key} attribute once loaded.
#' @param variables A character vector of soil property names (e.g., \code{c("bdod", "cec")}).
#' @param depths A character vector of depth intervals (e.g., \code{c("0-5", "5-15")}).
#' @param stats Character. The summary statistic to retrieve (e.g., \code{"mean"}).
#' @param dl_dir Character. Directory path where CSV files will be saved.
#' @param worker_n Integer. The number of parallel workers to use. If \code{worker_n > 1}, parallel processing is enabled.
#'
#' @return A \code{data.table} containing the combined soil data for all sites and variables.
#'
#' @examples
#' \dontrun{
#'   # Define parameters
#'   site_vect <- c("path/to/site1.shp", "path/to/site2.shp")  # Paths to spatial files
#'   variables <- c("bdod", "cec")
#'   depths <- c("0-5", "5-15")
#'   stats <- "mean"
#'   dl_dir <- "./downloads"
#'
#'   # Download the data using 4 parallel workers
#'   sg2_data <- download_soilgrids_data(site_vect, variables, depths, stats, dl_dir, worker_n = 4)
#' }
download_soilgrids_data <- function(site_vect, variables, depths, stats, dl_dir, worker_n = 1) {
  # Set up a text progress bar handler
  handlers("txtprogressbar")
  
  n_sites<-length(terra::vect(site_vect))
  
  # Internal function to process a single site.
  # The site is loaded from its file path inside this function to avoid passing terra objects to workers.
  process_site <- function(i) {
    # Load the spatial site from its file path
    sites<-terra::vect(site_vect)
    site <-sites [i]
    site_data <- rbindlist(lapply(seq_along(variables), function(j) {
      var <- variables[j]
      site_key<-site$Site.Key
      cat(sprintf("Downloading site %d/%d %s | variable %d/%d %s\r",
                  i, length(sites), site_key, j, length(variables), var))
      file <- file.path(dl_dir, paste0(site_key, "_", var, ".csv"))
      
      if (!file.exists(file)) {
        # Attempt to fetch the data using the helper function with retry logic
        res <- attempt_fetch(site, var, depths, stats, dl_dir, file)
        return(res)
      } else {
        # If the file exists, read the data from disk
        return(fread(file))
      }
    }))
    return(site_data)
  }
  
  # Process each site sequentially or in parallel based on worker_n
  if (worker_n > 1) {
    plan(multisession, workers = worker_n)
    sg2_data <- progressr::with_progress({
      p <- progressr::progressor(steps = n_sites)
      future.apply::future_lapply(1:n_sites, function(i) {
        process_site(i)
      },future.seed = TRUE)
      p()
    })
  } else {
    sg2_data <- lapply(1:n_sites, function(i) {
      process_site(i)
    })
  }
  
  sg2_data <- rbindlist(sg2_data, fill = TRUE)
  return(sg2_data)
}

# ------------------------------------------------------------------------------
# Example usage:
# ------------------------------------------------------------------------------
# Uncomment and modify the lines below as needed:
# site_vect <- c("path/to/site1.shp", "path/to/site2.shp")  # Paths to spatial files with a Site.Key attribute
# variables <- c("bdod", "cec", "cfvo")  # Soil properties to download
# depths <- c("0-5", "5-15")  # Depth intervals
# stats <- "mean"  # Summary statistic to retrieve
# dl_dir <- "./downloads"  # Directory to save CSV files
#
# sg2_data <- download_soilgrids_data(site_vect, variables, depths, stats, dl_dir, worker_n = 4)
# print(sg2_data)