# First run R/0_set_env.R

# 1) Set-up workspace ####
  ## 1.1) Load packages and create functions #####
  pacman::p_load(data.table,pbapply,future,future.apply,elevatr,RCurl)
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  
  ## 1.2) Create functions ####
  #' Download DEMs via elevatr for Buffered Points, Compute Topography, and Summarize Stats
  #'
  #' This function reads a data.frame (or data.table) containing latitude, longitude,
  #' a unique ID, and a buffer radius (in meters). For each unique row, it creates a point,
  #' buffers it (after projecting to EPSG:3857 so that the buffer is measured in meters) and then transforms
  #' back to WGS84. If \code{calc_topo} is TRUE, an extra margin is automatically added based on the zoom level
  #' (z) to ensure sufficient extra pixels for robust slope and aspect calculations. The function then downloads DEM data
  #' for the buffered area using \code{elevatr::get_elev_raster} and saves the DEM as \code{<ID>.tif}. If \code{calc_topo} is TRUE,
  #' slope and aspect are computed (saved as \code{<ID>_slope.tif} and \code{<ID>_aspect.tif}). For each DEM (and if computed, for slope and aspect),
  #' summary statistics (mean, mode, max, min, and standard deviation) are calculated. All of these summary statistics are compiled
  #' into a summary CSV (saved as "summary_stats.csv") in the output directory. Finally, the function binds all CSV files in \code{out_dir}
  #' (excluding any file named "all_sites.csv") into a single file called "all_sites.csv". If a file already exists and \code{overwrite} is FALSE,
  #' the download and statistics computation are skipped. The function will retry each download up to \code{max_tries} times in case of errors, and
  #' supports both parallel processing (via \code{future.apply} and \code{progressr}) and sequential processing (with a \code{cat()} progress update).
  #'
  #' @param df A data.frame or data.table with at least four columns.
  #' @param out_dir Character. Directory to save downloaded DEM files and CSV summary files.
  #' @param z Integer. Zoom level for \code{elevatr::get_elev_raster}. The z parameter controls the DEM resolution.
  #'   Under a simple heuristic, at z = 6 the pixel size is ~180 m at the equator; each increment in z halves the pixel size
  #'   (e.g., z = 10 gives ~11.25 m per pixel).
  #' @param overwrite Logical. If FALSE (default), skip download if file exists and passes an integrity check.
  #'   If TRUE, re-download and re-calculate everything.
  #' @param max_tries Integer. Maximum number of attempts per feature if an error occurs (default = 3).
  #' @param workers Integer. Number of parallel workers. If 1, runs sequentially with a \code{cat()} progress update.
  #' @param use_progressr Logical. If TRUE (default), shows a progress bar when using parallel processing.
  #' @param lat_col Character. Name of the latitude column in df (default "lat").
  #' @param lon_col Character. Name of the longitude column in df (default "lon").
  #' @param id_col Character. Name of the unique identifier column in df (default "id").
  #' @param buffer_col Character. Name of the buffer radius column in df (default "buffer").
  #' @param calc_topo Logical. If TRUE, compute slope and aspect from the downloaded DEM.
  #'
  #' @return A data.frame summarizing for each feature:
  #'   \itemize{
  #'     \item \code{id}: The unique identifier.
  #'     \item \code{file}: The DEM file path.
  #'     \item \code{status}: "OK", "SKIP", or "ERROR".
  #'     \item \code{message}: Details on the outcome.
  #'   }
  #'
  #' In addition, a file named "summary_stats.csv" is written to \code{out_dir} containing the summary statistics,
  #' and all CSV files in \code{out_dir} (except "all_sites.csv") are bound together and saved as "all_sites.csv".
  #'
  #' @importFrom sf st_as_sf st_transform st_buffer
  #' @importFrom elevatr get_elev_raster
  #' @importFrom terra writeRaster rast terrain values
  #' @importFrom future plan
  #' @importFrom future.apply future_lapply
  #' @importFrom progressr with_progress progressor
  #' @export
  download_elevatr_dem <- function(df,
                                   out_dir,
                                   z = 7,
                                   overwrite = FALSE,
                                   max_tries = 3,
                                   workers = 1,
                                   use_progressr = TRUE,
                                   lat_col = "lat",
                                   lon_col = "lon",
                                   id_col = "id",
                                   buffer_col = "buffer",
                                   calc_topo = FALSE) {
    # Check required packages
    if (!requireNamespace("sf", quietly = TRUE)) stop("Package 'sf' is required.")
    if (!requireNamespace("elevatr", quietly = TRUE)) stop("Package 'elevatr' is required.")
    if (!requireNamespace("terra", quietly = TRUE)) stop("Package 'terra' is required.")
    if (workers > 1 && (!requireNamespace("future", quietly = TRUE) ||
                        !requireNamespace("future.apply", quietly = TRUE))) {
      stop("For parallel processing, please install 'future' and 'future.apply'.")
    }
    if (use_progressr && !requireNamespace("progressr", quietly = TRUE)) {
      message("Package 'progressr' not installed; disabling progress bar.")
      use_progressr <- FALSE
    }
    
    # If df is a data.table, convert it to a data.frame
    if (inherits(df, "data.table")) df <- as.data.frame(df)
    
    # Ensure required columns exist in df
    req_cols <- c(lat_col, lon_col, id_col, buffer_col)
    if (!all(req_cols %in% names(df))) {
      stop("Input data must contain the columns: ", paste(req_cols, collapse = ", "))
    }
    
    # Remove duplicates based on lat, lon, and buffer
    df_unique <- unique(df[, req_cols, drop = FALSE])
    
    # Extract vectors for each row
    lats  <- as.numeric(df_unique[[lat_col]])
    lons  <- as.numeric(df_unique[[lon_col]])
    ids   <- as.character(df_unique[[id_col]])
    buffs <- as.numeric(df_unique[[buffer_col]])
    
    n <- nrow(df_unique)
    
    # Create output directory if it doesn't exist
    if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)
    
    # Compute approximate pixel size at the equator based on z.
    # Assumption: at z = 6, pixel size is ~180 m; each increment halves the pixel size.
    approx_pixel_size <- 180 / (2^(z - 6))
    extra_margin <- if (calc_topo) { 3 * approx_pixel_size } else { 0 }
    
    # Helper function to compute summary statistics from a SpatRaster
    calc_stats <- function(r) {
      vals <- terra::values(r, mat = FALSE)
      vals <- vals[!is.na(vals)]
      m <- mean(vals)
      # Compute mode by rounding to one decimal place:
      rnd <- round(vals, 1)
      ux <- unique(rnd)
      md <- ux[which.max(tabulate(match(rnd, ux)))]
      mx <- max(vals)
      mn <- min(vals)
      sd_val <- sd(vals)
      c(mean = m, mode = md, max = mx, min = mn, sd = sd_val)
    }
    
    # Function to process one row:
    process_one <- function(i) {
      this_id <- ids[i]
      effective_buffer <- buffs[i] + extra_margin
      # Create a point using sf
      pt <- sf::st_as_sf(data.frame(lon = lons[i], lat = lats[i]), coords = c("lon", "lat"), crs = 4326)
      # Project to EPSG:3857 for buffering (buffer in meters)
      pt_proj <- sf::st_transform(pt, 3857)
      buf_proj <- sf::st_buffer(pt_proj, dist = effective_buffer)
      buf <- sf::st_transform(buf_proj, 4326)
      
      # Define output filename (e.g., "<id>.tif")
      out_file <- file.path(out_dir, paste0(this_id, ".tif"))
      
      # If file exists and not overwriting, skip download and compute stats
      if (file.exists(out_file) && !overwrite) {
        dem <- terra::rast(out_file)
        dem_stats <- calc_stats(dem)
        result <- list(id = this_id, file = out_file, status = "SKIP", message = "File exists", 
                       dem_stats = dem_stats)
        if (calc_topo) {
          slope_file <- file.path(out_dir, paste0(this_id, "_slope.tif"))
          aspect_file <- file.path(out_dir, paste0(this_id, "_aspect.tif"))
          if (file.exists(slope_file) && file.exists(aspect_file)) {
            slope <- terra::rast(slope_file)
            aspect <- terra::rast(aspect_file)
            result$slope_stats <- calc_stats(slope)
            result$aspect_stats <- calc_stats(aspect)
          }
        }
        return(result)
      }
      
      attempt <- 0
      success <- FALSE
      last_error <- ""
      dem_rast <- NULL
      
      while (attempt < max_tries && !success) {
        attempt <- attempt + 1
        if (file.exists(out_file)) file.remove(out_file)
        
        tryCatch({
          dem_rast <- elevatr::get_elev_raster(locations = buf, z = z, clip = "locations")
          terra::writeRaster(dem_rast, out_file, overwrite = TRUE)
          # Check integrity by trying to load it back
          test <- terra::rast(out_file)
          success <- TRUE
        }, error = function(e) {
          last_error <<- e$message
        })
      }
      
      if (!success) {
        return(list(id = this_id, file = out_file, status = "ERROR", message = last_error))
      }
      
      # Compute summary statistics for the DEM
      dem <- terra::rast(out_file)
      dem_stats <- calc_stats(dem)
      
      # If calc_topo is TRUE, compute slope and aspect and their stats.
      if (calc_topo) {
        slope <- terra::terrain(dem, v = "slope", unit = "degrees")
        aspect <- terra::terrain(dem, v = "aspect", unit = "degrees")
        slope_file <- file.path(out_dir, paste0(this_id, "_slope.tif"))
        aspect_file <- file.path(out_dir, paste0(this_id, "_aspect.tif"))
        tryCatch({
          terra::writeRaster(slope, slope_file, overwrite = TRUE)
          terra::writeRaster(aspect, aspect_file, overwrite = TRUE)
        }, error = function(e) {
          last_error <<- paste("Topography calculation error:", e$message)
          success <<- FALSE
        })
        slope_stats <- calc_stats(slope)
        aspect_stats <- calc_stats(aspect)
      }
      
      msg <- paste("Downloaded after", attempt, "tries")
      if (calc_topo) msg <- paste(msg, "with slope and aspect computed")
      result <- list(id = this_id, file = out_file, status = "OK", message = msg, dem_stats = dem_stats)
      if (calc_topo) {
        result$slope_stats <- slope_stats
        result$aspect_stats <- aspect_stats
      }
      return(result)
    }
    
    # Run processing either sequentially or in parallel
    results_list <- NULL
    if (workers == 1) {
      results_list <- vector("list", n)
      for (i in seq_len(n)) {
        results_list[[i]] <- process_one(i)
        cat(sprintf("\rProcessed %d/%d", i, n))
        flush.console()
      }
      cat("\n")
    } else {
      future::plan(future::multisession, workers = workers)
      if (use_progressr) {
        results_list <- progressr::with_progress({
          p <- progressr::progressor(steps = n)
          future.apply::future_lapply(seq_len(n), function(i) {
            res <- process_one(i)
            p(sprintf("Processed %d/%d", i, n))
            res
          })
        })
      } else {
        results_list <- future.apply::future_lapply(seq_len(n), process_one)
      }
      future::plan(future::sequential)
    }
    
    # Combine results into a data.frame
    res_df <- do.call(rbind, lapply(results_list, function(x) {
      data.frame(id = x$id, file = x$file, status = x$status, message = x$message, stringsAsFactors = FALSE)
    }))
    
    # Write the summary CSV file (summary_stats.csv)
    summary_file <- file.path(out_dir, "summary_stats.csv")
    write.csv(res_df, summary_file, row.names = FALSE)
    
    # Bind all CSV files in out_dir (excluding "all_sites.csv") into one CSV "all_sites.csv"
    csv_files <- list.files(out_dir, pattern = "\\.csv$", full.names = TRUE)
    csv_files <- csv_files[!grepl("all_sites\\.csv", csv_files)]
    if (length(csv_files) > 0) {
      all_sites <- do.call(rbind, lapply(csv_files, function(f) read.csv(f, stringsAsFactors = FALSE)))
      all_sites_file <- file.path(out_dir, "all_sites.csv")
      write.csv(all_sites, all_sites_file, row.names = FALSE)
    }
    
    return(res_df)
  }
# 2) Download data ####
  options(future.globals.maxSize = 2 * 1024^3)
  
  download_elevatr_dem(df=era_locations[order(Buffer)],
                      out_dir=era_dirs$dem_dir,
                      z = 10,
                      overwrite = TRUE,
                      max_tries = 3,
                      workers = 15,
                      use_progressr = TRUE,
                      calc_topo=T,
                      lat_col = "Latitude",
                      lon_col = "Longitude",
                      id_col = "Site.Key",
                      buffer_col = "Buffer")


  
  