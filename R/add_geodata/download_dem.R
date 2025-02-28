# First run R/0_set_env.R

# 1) Set-up workspace ####
  ## 1.1) Load packages and create functions #####
  pacman::p_load(data.table,pbapply,future,future.apply,elevatr,RCurl)
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  
  ## 1.2) Create functions ####
  #' Download DEMs via elevatr for Buffered Points (Simplified)
  #'
  #' This function reads a data.frame (or data.table) containing at least four columns:
  #' latitude, longitude, a unique ID, and a buffer radius (in meters). For each row,
  #' it creates a point and applies a buffer. If calc_topo is TRUE, an extra margin is
  #' automatically added to the user-specified buffer. The extra margin is computed using the
  #' heuristic that at z = 6 the pixel size is approximately 180 m at the equator and each increment
  #' in z halves the pixel size (e.g., z = 10 gives ~11.25 m per pixel). This padded buffer is then
  #' used to download a DEM using elevatr::get_elev_raster (with clip = "bbox"). The DEM is saved
  #' as <ID>.tif in the specified output directory.
  #'
  #' @param df A data.frame or data.table with columns for latitude, longitude, unique ID, and buffer.
  #' @param out_dir Character. Directory to save downloaded DEM files.
  #' @param z Integer. Zoom level for elevatr::get_elev_raster. Specifies the zoom level of the elevation data retrieved from AWS Terrain Tiles. Higher values provide finer spatial resolution, with approximate pixel sizes at the equator as follows:
  #'   z = 5  ~4–5 km per pixel, z = 10 ~150 m per pixel, z = 14 ~9.5 m per pixel. Increasing z improves detail but significantly increases data size. 
  #' @param src  A character indicating which API to use. Currently supports "aws" and "gl3", "gl1", "alos", or "srtm15plus" from the OpenTopography 
  #'   API global datasets. "aws" is the default.
  #' @param overwrite Logical. If FALSE (default), skip download if the DEM file already exists.
  #' @param max_tries Integer. Maximum number of attempts per site if an error occurs (default = 3).
  #' @param workers Integer. Number of parallel workers. If 1, runs sequentially with cat() progress.
  #' @param use_progressr Logical. If TRUE (default), shows a progress bar in parallel mode.
  #' @param lat_col Character. Name of the latitude column in df (default "lat").
  #' @param lon_col Character. Name of the longitude column in df (default "lon").
  #' @param id_col Character. Name of the unique identifier column in df (default "id").
  #' @param buffer_col Character. Name of the buffer radius column in df (default "buffer").
  #' @param calc_topo Logical. If TRUE, add extra margin to the buffer; otherwise, use the provided buffer.
  #'
  #' @return A data.frame summarizing each site (ID, DEM file path, and status).
  #'
  #' @importFrom terra vect project buffer ext writeRaster rast
  #' @importFrom elevatr get_elev_raster
  #' @importFrom future plan
  #' @importFrom future.apply future_lapply
  #' @importFrom progressr with_progress progressor
  #' @importFrom sf st_as_sf st_bbox
  #' @export
  download_dem <- function(df,
                          out_dir,
                          z = 11,
                          src="aws",
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
if (!requireNamespace("terra", quietly = TRUE)) stop("Package 'terra' is required.")
if (!requireNamespace("elevatr", quietly = TRUE)) stop("Package 'elevatr' is required.")
if (workers > 1 && (!requireNamespace("future", quietly = TRUE) ||
                !requireNamespace("future.apply", quietly = TRUE))) {
stop("For parallel processing, please install 'future' and 'future.apply'.")
}
if (use_progressr && !requireNamespace("progressr", quietly = TRUE)) {
message("Package 'progressr' not installed; disabling progress bar.")
use_progressr <- FALSE
}
if (inherits(df, "data.table")) df <- as.data.frame(df)
req_cols <- c(lat_col, lon_col, id_col, buffer_col)
if (!all(req_cols %in% names(df))) stop("Missing required columns: ", paste(req_cols, collapse=", "))

# Remove duplicates and extract vectors
df_unique <- unique(df[, req_cols, drop = FALSE])
lats  <- as.numeric(df_unique[[lat_col]])
lons  <- as.numeric(df_unique[[lon_col]])
ids   <- as.character(df_unique[[id_col]])
buffs <- as.numeric(df_unique[[buffer_col]])
n <- nrow(df_unique)

# Helper function to calculate stats
stats_fun<-function(df,vals,variable){
rbind(
data.table(df,variable=variable,stat="mean",value=mean(vals,na.rm=T)),
data.table(df,variable=variable,stat="sd",value=sd(vals,na.rm = T)),
data.table(df,variable=variable,stat="median",value=median(vals,na.rm=T)),
data.table(df,variable=variable,stat="max",value=max(vals,na.rm = T)),
data.table(df,variable=variable,stat="min",value=min(vals,na.rm=T))
)
}

if (!dir.exists(out_dir)) dir.create(out_dir, recursive = TRUE)

# Create a table that links z values to resolutions for src = aws
z_tab <- data.table(
z_value = 1:14,
res = c(2.748776e-01, 1.545563e-01, 8.405647e-02, 4.340482e-02, 2.190285e-02, 
      1.097753e-02, 5.492062e-03, 2.746444e-03, 1.373274e-03, 6.866434e-04, 
      3.433225e-04, 1.716611e-04, 8.583057e-05, 4.291529e-05),
res_m = c(31116.1, 17495.8, 9515.2, 4913.4, 2479.4, 1242.7, 621.7, 310.9, 155.5, 
        77.7, 38.9, 19.4, 9.7, 4.9)
)

cat("src = ",src,". Using z = ",z,", approximately", z_tab[z_tab$z_value == z, ]$res_m,"m at the equator.\n")

# Compute extra margin if calc_topo is TRUE.
approx_pixel_size <- z_tab[z_tab$z_value == z, ]$res_m
extra_margin <- if (calc_topo) 3 * approx_pixel_size else 0

# Core processing function for one site
process_one <- function(i) {
  
  stat_tab<-list()
  
  this_id <- ids[i]
  padded_buff <- buffs[i] + extra_margin
  # Create point using sf (for bounding box creation)
  pt <- terra::vect(data.frame(lon = lons[i], lat = lats[i]), geom = c("lon", "lat"), crs = "EPSG:4326")
  
  # Create buffer
  buf <- terra::buffer(pt, width = buffs[i])
  buf_pad <- terra::buffer(pt, width = padded_buff)
  
  # Define output file name
  out_file <- file.path(out_dir, paste0(this_id, ".tif"))
  stats_file <- file.path(out_dir,paste0(this_id, "_stats.csv"))
  
  if (file.exists(out_file) && !overwrite) {
  return(list(id = this_id, file = out_file, status = "SKIP"))
  }
  
  attempt <- 0
  success <- FALSE
  last_error <- ""
  while (attempt < max_tries && !success) {
  attempt <- attempt + 1
  if (file.exists(out_file)) file.remove(out_file)
  
  tryCatch({
    dem_rast <- suppressWarnings(suppressMessages(
      elevatr::get_elev_raster(locations = sf::st_as_sf(buf_pad), z = z, clip = "locations", src = "aws",verbose=F)
    ))
    dem <- terra::rast(dem_rast)
    
    if (calc_topo) {
      slope <-suppressMessages(terra::terrain(dem, v = "slope", unit = "degrees"))
      slope <- terra::mask(slope, buf)
      aspect <- suppressMessages(terra::terrain(dem, v = "aspect", unit = "degrees"))
      aspect <- terra::mask(aspect, buf)
      dem <- terra::mask(dem, buf)
      
      slope_file <- file.path(out_dir, paste0(this_id, "_slope.tif"))
      aspect_file <- file.path(out_dir, paste0(this_id, "_aspect.tif"))
      tryCatch({
        terra::writeRaster(slope, slope_file, overwrite = TRUE)
        terra::writeRaster(aspect, aspect_file, overwrite = TRUE)
        
        stat_tab<-c(stat_tab,list(stats_fun(df=df[i,],vals=values(slope),variable="slope")))
        stat_tab<-c(stat_tab,list(stats_fun(df=df[i,],vals=values(aspect),variable="aspect")))
        
      }, error = function(e) {
        last_error <<- paste("Topography calculation error:", e$message)
        success <<- FALSE
      })
    }
    
    stat_tab<-c(stat_tab,list(stats_fun(df=df[i,],vals=values(dem),variable="elevation")))
    
    terra::writeRaster(dem, out_file, overwrite = TRUE)
    data.table::fwrite(rbindlist(stat_tab),file=stats_file)
    test <- terra::rast(out_file)
    success <- TRUE
    
  }, error = function(e) {
    last_error <<- e$message
  })
  }
  
  if (!success) {
  return(list(id = this_id, file = NA, status = "ERROR", error = last_error))
  }
  
  return(list(id = this_id, file = out_file, status = "OK"))
}

# Process sites sequentially or in parallel
results_list <- vector("list", n)
if (workers == 1) {
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
  }, future.seed = TRUE)
})
} else {
results_list <- future.apply::future_lapply(seq_len(n), process_one, future.seed = TRUE)
}
future::plan(future::sequential)
}

# Combine results into a data.table
res_df <- rbindlist(lapply(results_list, as.data.table), fill = TRUE)
stats_tab<-rbindlist(lapply(list.files(out_dir,".csv$",full.names = T),fread))
result<-list(results=res_df,stats=stats_tab)
return(result)
}

  ## 1.3) Increase globals size ####
  options(future.globals.maxSize = 2 * 1024^3)
# 2) Download data ####
  
  results<-download_dem(df=era_locations[order(Buffer)],
                      out_dir=era_dirs$dem_dir,
                      z = 12,
                      overwrite = FALSE,
                      max_tries = 3,
                      workers = 15,
                      use_progressr = TRUE,
                      calc_topo=T,
                      lat_col = "Latitude",
                      lon_col = "Longitude",
                      id_col = "Site.Key",
                      buffer_col = "Buffer")

  