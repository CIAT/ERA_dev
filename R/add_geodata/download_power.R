#' Download Daily Weather Data from NASA POWER API with Retry and Parallel Processing
#'
#' This function downloads daily weather data from the NASA POWER API for a set of sites.
#' Each site is represented by a spatial object (e.g. a \code{terra::SpatVector}) from which the
#' spatial extent is extracted. The extent is rounded to 5 decimal places, and then the function
#' determines the nearest grid cell boundaries based on predefined latitude (0.5° increments) and
#' longitude (5/8° increments) sequences. For each grid cell within the extent, the function constructs
#' a URL to download data in CSV format from the POWER API.
#'
#' The URL includes parameters for start and end dates, grid cell latitude and longitude, community ("ag"),
#' a comma‐separated list of parameter codes, output format ("csv"), a user name, a header flag,
#' time standard ("lst"), and site elevation. The downloaded files are saved in \code{save_dir} with filenames
#' of the form "POWER <id>-<cell_index>.csv". If a download fails, it is retried up to \code{attempts} times.
#'
#' Parallel processing is supported using \code{future.apply::future_lapply} with \code{progressr} for a progress bar;
#' if \code{worker_n == 1}, processing is sequential with progress reported via \code{cat()}.
#'
#' @param site_vect A list (or vector) of spatial objects (e.g. \code{terra::SpatVector}s) representing sites.
#' @param date_start A Date (or string convertible to Date) indicating the start date for data retrieval.
#' @param date_end A Date (or string convertible to Date) indicating the end date for data retrieval.
#' @param altitude Numeric. The site elevation (in meters) to be passed to the API.
#' @param save_dir Character. Directory where the downloaded CSV files will be saved.
#' @param parameters A character vector of parameter codes (e.g. \code{"T2M"}, \code{"PRECTOTCORR"}) to request.
#' @param id_field Character. The name of the field in each spatial object that contains the site ID (default "Site.Key").
#' @param user_name Character. The username to pass to the API (default "ICRAF2").
#' @param verbose Logical. If TRUE, prints progress messages (default FALSE).
#' @param overwrite Logical. If TRUE, forces re-download even if the file exists (default FALSE).
#' @param attempts Integer. Number of retry attempts for each grid cell download if an error occurs (default 3).
#' @param worker_n Integer. Number of parallel workers; if 1, processing is sequential.
#'
#' @return A list of results (one per site) as returned by the inner download function.
#'
#' @importFrom terra ext
#' @importFrom terra values
#' @importFrom future plan
#' @importFrom future.apply future_lapply
#' @importFrom progressr with_progress progressor
#' @importFrom sf st_as_sf
#'
#' @examples
#' \dontrun{
#'   # Assume you have a list of spatial objects 'site_list' (each with attribute "Site.Key")
#'   results <- download_POWER_data_parallel(
#'     site_vect = site_list,
#'     date_start = "2020-01-01",
#'     date_end = "2020-12-31",
#'     altitude = 150,
#'     save_dir = "power_data",
#'     parameters = c("T2M", "PRECTOTCORR"),
#'     id_field = "Site.Key",
#'     user_name = "ICRAF2",
#'     verbose = TRUE,
#'     overwrite = FALSE,
#'     attempts = 3,
#'     worker_n = 4
#'   )
#' }
#'
#' @export
download_power <- function(site_vect, 
                           date_start, 
                           date_end, 
                           save_dir, 
                           parameters,
                           altitude_field="Altitude",
                           id_field = "Site.Key", 
                           user_name = "",
                           verbose = FALSE, 
                           overwrite = FALSE, 
                           attempts = 3, 
                           worker_n = 1) {
  
  # Inner function: Downloads POWER data for a single spatial object (site)
  download_power_for_site <- function(vect_data,
                                      date_start, 
                                      date_end, 
                                      save_dir, 
                                      parameters,
                                      altitude_field,
                                      id_field, 
                                      user_name,
                                      verbose, 
                                      overwrite, 
                                      attempts) {
    # Extract site ID from the spatial object using the specified field
    id <- values(vect_data[, id_field])
    
    altitude<-values(vect_data[, altitude_field])
    
    # Get the spatial extent and round to 5 decimals
    vect_ext <- round(terra::ext(vect_data), 5)
    base_url <- "https://power.larc.nasa.gov/api/temporal/daily/"
    
    # Convert date strings to Date objects
    date_start <- as.Date(date_start)
    date_end <- as.Date(date_end)
    options(warn = -1)
    
    # Extract extent boundaries
    lonmin <- vect_ext[1]
    lonmax <- vect_ext[2]
    latmin <- vect_ext[3]
    latmax <- vect_ext[4]
    
    # Define grid sequences for snapping boundaries
    lat_vals <- seq(-89.75, 89.75, 0.5)
    lon_vals <- seq(-179.75, 179.75, 5/8)
    
    # Snap boundaries to nearest grid cell values
    lonmax <- lon_vals[which.min(abs(lon_vals - lonmax))]
    lonmin <- lon_vals[which.min(abs(lon_vals - lonmin))]
    latmax <- lat_vals[which.min(abs(lat_vals - latmax))]
    latmin <- lat_vals[which.min(abs(lat_vals - latmin))]
    
    # Create grid from the unique boundary values
    n_cells <- expand.grid(lat = unique(c(latmin, latmax)), lon = unique(c(lonmin, lonmax)))
    
    # Loop over grid cells for the site
    for (j in 1:nrow(n_cells)) {
      file_name <- file.path(save_dir, paste0("POWER ", id, "-", j, ".csv"))
      # Download if file does not exist or if overwrite is TRUE
      if (!file.exists(file_name) || overwrite) {
        # Construct the URL using grid cell coordinates
        URL <- paste0(base_url, 
                      "point?start=", format(date_start, "%Y%m%d"), 
                      "&end=", format(date_end, "%Y%m%d"), 
                      "&latitude=", n_cells[j, "lat"], 
                      "&longitude=", n_cells[j, "lon"], 
                      "&community=ag", 
                      "&parameters=", paste(parameters, collapse = "%2C"), 
                      "&format=csv", 
                      "&user=", user_name, 
                      "&header=true&time-standard=lst", 
                      "&site-elevation=", altitude)
        # If verbose, print progress
        if (verbose) {
          cat(sprintf("\rPOWER: Downloading site %s | Cell %d/%d", id, j, nrow(n_cells)))
          flush.console()
        }
        
        # Try downloading up to 'attempts' times
        success <- FALSE
        for (i in seq_len(attempts)) {
          tryCatch({
            download.file(URL, file_name, method = "libcurl", quiet = !verbose, mode = "w", cacheOK = TRUE)
            success <- TRUE
            break
          }, error = function(e) {
            if (verbose) cat(sprintf("\nAttempt %d failed for %s: %s", i, file_name, e$message))
          })
        }
        if (!success) {
          print(paste(file_name, "did not work out after", attempts, "attempts"))
        }
      }
    }
    options(warn = 0)
  }
  
  # Wrapper: Process each site in site_vect, either sequentially or in parallel.
  if (worker_n == 1) {
    results <- vector("list", length(site_vect))
    for (i in seq_along(site_vect)) {
      cat(sprintf("\rProcessing site %d/%d", i, length(site_vect)))
      flush.console()
      results[[i]] <- download_power_for_site(vect_data=site_vect[i],
                                              date_start=date_start, 
                                              date_end=date_end, 
                                              save_dir=save_dir, 
                                              parameters=parameters,
                                              altitude_field=altitude_field,
                                              id_field=id_field, 
                                              user_name=user_name,
                                              verbose=verbose, 
                                              overwrite=overwrite, 
                                              attempts=attempts)
    }
    cat("\n")
  } else {
    future::plan(future::multisession, workers = worker_n)
    results <- progressr::with_progress({
      p <- progressr::progressor(steps = length(site_vect))
      future.apply::future_lapply(seq_along(site_vect), function(i) {
        out <- download_power_for_site(site_vect[i],
                                       date_start=date_start, 
                                       date_end=date_end, 
                                       save_dir=save_dir, 
                                       parameters=parameters,
                                       altitude_field=altitude_field,
                                       id_field=id_field, 
                                       user_name=user_name,
                                       verbose=verbose, 
                                       overwrite=overwrite, 
                                       attempts=attempts)
        p(sprintf("Processed site %d/%d", i, length(site_vect)))
        out
      }, future.seed = TRUE)
    })
    future::plan(future::sequential)
  }
  
  return(results)
}

