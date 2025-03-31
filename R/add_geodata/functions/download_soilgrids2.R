#' Attempt to Fetch SoilGrids Data with Retries
#'
#' Helper function to download SoilGrids data for a single site and soil variable.
#' It attempts up to 3 retries in case of failures, handles projection, masking, reshaping,
#' and saves the processed output as a CSV.
#' 
#' Internally, it calls [soilDB::fetchSoilGrids()] for querying ISRIC's global soil data.
#'
#' See the `soilDB` package for details:  
#' <https://cran.r-project.org/package=soilDB>  
#' Direct function reference: <https://rdrr.io/pkg/soilDB/man/fetchSoilGrids.html>
#'
#' See the [SoilGrids REST API](https://soilgrids.org/) for more information about available variables, depths and resolutions.
#'
#' @param site A \code{SpatVector} object representing the site extent. Must contain a \code{Site.Key} attribute.
#' @param var Character. Soil property name to download (e.g., \code{"bdod"}).
#' @param depths Character vector. Depth intervals (e.g., \code{c("0-5", "5-15")}).
#' @param stats Character. Summary statistic to retrieve (e.g., \code{"mean"}).
#' @param dl_dir Character. Directory path where CSV files are saved.
#' @param file Character. Full file path for the output CSV.
#' @param target_resolution Default: c(250, 250) (250m x 250m pixels).
#'
#' @import data.table
#' @importFrom soilDB fetchSoilGrids
#' @import terra
#' @import reshape2
#'
#' @return A \code{data.frame} containing the downloaded and processed soil data, or \code{NULL} if the data
#' could not be downloaded after 3 attempts.
#'
#' @examples
#' \dontrun{
#'   # Example usage:
#'   site <- terra::vect("path/to/site1.shp")
#'   res <- attempt_fetch(site, "bdod", c("0-5", "5-15"), "mean", "./downloads", "site1_bdod.csv")
#' }
attempt_fetch <- function(site, var, depths, stats, dl_dir, file,target_resolution=c(250,250)) {
  attempts <- 3
  for (attempt in 1:attempts) {
    result <- tryCatch({
      # Download SoilGrids data
      soil_data <- soilDB::fetchSoilGrids(
        x = site,
        grid = TRUE,
        variables = var,
        depth_intervals = depths,
        target_resolution = target_resolution,
        summary_type = stats
      )
      # Project to WGS84
      soil_data <- terra::project(soil_data, "EPSG:4326")
      # Mask to site extent
      soil_data <- terra::mask(soil_data, site)
      # Convert to data.frame and melt
      soil_data <- data.frame(soil_data)
      soil_data <- suppressMessages(suppressWarnings(reshape2::melt(soil_data)))
      # Save CSV
      data.table::fwrite(soil_data, file)
      soil_data
    }, error = function(e) {
      warning(sprintf("Attempt %d failed for site %s, variable %s: %s", 
                      attempt, site$Site.Key, var, e$message))
      Sys.sleep(1)
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
#' Downloads SoilGrids 2.0 data for multiple spatial sites and soil variables.
#' Parallel processing is supported via \code{future.apply}, with progress tracked by \code{progressr}.
#' The function processes each site file individually, handles retries, reshapes output, and saves CSVs.
#'
#' @param site_vect Character vector. File paths to spatial site objects (e.g., shapefiles). Each must contain
#' a \code{Site.Key} attribute once loaded.
#' @param variables Character vector. Soil property names (e.g., \code{c("bdod", "cec")}).
#' @param depths Character vector. Depth intervals (e.g., \code{c("0-5", "5-15")}).
#' @param stats Character. Summary statistic to retrieve (e.g., \code{"mean"}).
#' @param dl_dir Character. Directory where CSV files will be saved.
#' @param worker_n Integer. Number of parallel workers. Set to \code{1} to disable parallelization.
#' @param add_id Logical. Whether to force add \code{Site.Key} column to output (default = \code{FALSE}).
#'
#' @import terra
#' @import data.table
#' @import progressr
#' @import future
#' @import future.apply
#'
#' @return A \code{data.table} containing combined soil data for all sites and variables.
#'
#' @examples
#' \dontrun{
#'   site_vect <- c("path/to/site1.shp", "path/to/site2.shp")
#'   variables <- c("bdod", "cec")
#'   depths <- c("0-5", "5-15")
#'   stats <- "mean"
#'   dl_dir <- "./downloads"
#'
#'   sg2_data <- download_soilgrids_data(site_vect, variables, depths, stats, dl_dir, worker_n = 4)
#' }
download_soilgrids_data <- function(site_vect, variables, depths, stats, dl_dir, worker_n = 1, add_id = FALSE) {
  
  n_sites <- length(terra::vect(site_vect))
  
  # Process individual site
  process_site <- function(i, site_vect, variables, depths, dl_dir, stats, worker_n, add_id) {
    sites <- terra::vect(site_vect)
    site <- sites[i]
    site_data <- rbindlist(lapply(seq_along(variables), function(j) {
      var <- variables[j]
      site_key <- site$Site.Key
      if (worker_n == 1) {
        cat(sprintf("Downloading site %d/%d %s | variable %d/%d %s                \r",
                    i, length(sites), site_key, j, length(variables), var))
      }
      file <- file.path(dl_dir, paste0(site_key, "_", var, ".csv"))
      if (!file.exists(file)) {
        result <- attempt_fetch(site, var, depths, stats, dl_dir, file)
      } else {
        result <- fread(file)
      }
      if (nrow(result) == 0) {
        warning(paste("\nDownloaded soilgrids data has zero rows", site_key, var))
      }
      if (any(grepl("lyr.1", unique(result$variable)))) {
        warning(paste("\nDownloaded soilgrids data has non-informative variable name", site_key, var))
      }
      if (add_id) {
        result$Site.Key <- site_key
      }
      return(result)
    }))
    return(site_data)
  }
  
  # Parallel or sequential
  if (worker_n > 1) {
    progressr::handlers(global = TRUE)
    progressr::handlers("progress")
    future::plan(multisession, workers = worker_n)
    
    sg2_data <- progressr::with_progress({
      p <- progressr::progressor(along = seq_along(1:n_sites))
      future.apply::future_lapply(1:n_sites, function(i) {
        p(sprintf("Processing site %d/%d", i, n_sites))
        res <- process_site(i, site_vect, variables, depths, dl_dir, stats, worker_n, add_id)
        return(res)
      }, future.seed = TRUE)
    })
  } else {
    sg2_data <- lapply(1:n_sites, function(i) {
      cat("Processing site", i, "/", n_sites, "\r")
      process_site(i, site_vect, variables, depths, dl_dir, stats, worker_n, add_id)
    })
  }
  
  sg2_data <- rbindlist(sg2_data, fill = TRUE)
  return(sg2_data)
}