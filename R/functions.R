#' Process Power CSV Files
#'
#' This function processes multiple power CSV files, averaging data for sites with multiple cells, 
#' and optionally adds additional fields such as date, day count, and potential evapotranspiration (PET).
#'
#' @param files A character vector of file paths to the CSV files.
#' @param parameters A character vector of parameter names to rename.
#' @param rename A character vector of new names for the parameters.
#' @param id_field A character string specifying the ID field name.
#' @param add_date Logical; whether to add a date field. Default is \code{TRUE}.
#' @param add_daycount Logical; whether to add a day count field. Default is \code{TRUE}.
#' @param time_origin A Date object specifying the origin date for day count calculation.
#' @param add_pet Logical; whether to add PET (Potential Evapotranspiration). Default is \code{TRUE}.
#' @param altitude_data A data.table containing altitude information for the sites.
#' @return A data.table with processed power data.
#' @examples
#' \dontrun{
#' 
#' See https://github.com/CIAT/ERA_dev/blob/main/R/add_geodata/power.R
#' 
#' files <- list.files(path = "path/to/csv/files", full.names = TRUE)
#'   parameters<-c(
#'   SRad="ALLSKY_SFC_SW_DWN", # Insolation Incident on a Horizontal Surface - MJ/m^2/day
#'   Rain="PRECTOT", # Precipitation - mm day-1
#'   Pressure.Corrected="PSC", # Corrected Atmospheric Pressure (Adjusted For Site Elevation) *(see Site Elevation section below) - kPa - Throws an error!
#'   Pressure="PS", # Surface Pressure - kPa
#'   Specific.Humid="QV2M", # Specific Humidity at 2 Meters - kg kg-1
#'   Humid="RH2M", # Relative Humidity at 2 Meters - %
#'   Temp.Mean="T2M", # Temperature at 2 Meters - C
#'   Temp.Max="T2M_MAX", # Maximum Temperature at 2 Meters - C
#'   Temp.Min="T2M_MIN", # Minimum Temperature at 2 Meters - C
#'   WindSpeed="WS2M" # Wind Speed at 2 Meters - m/s
#'   )
#' id_field <- "Site.Key"
#' time_origin <- as.Date("1900-01-01")
#' 
#' era_elevation<-data.table(arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"era_site_topography.parquet")))
#' altitude_data<-era_elevation[,list(Site.Key,Altitude.mean)]
#' setnames(altitude_data,"Altitude.mean","Altitude")
#' processed_data <- process_power(files, parameters=parameters, rename=names(parameters), id_field, add_date=TRUE, add_daycount=TRUE, time_origin, add_pet=TRUE, altitude_data)
#' }
#' @export
process_power <- function(files,
                          parameters,
                          rename,
                          id_field,
                          add_date = TRUE,
                          add_daycount = TRUE,
                          time_origin,
                          add_pet = TRUE,
                          altitude_data) {
  cat("Loading power csv files", "\n")
  
  # Load data from all files and combine into one data.table
  data <- rbindlist(pbapply::pblapply(1:length(files), FUN = function(i) {
    file <- files[i]
    id <- basename(file)
    data <- data.table(id = id, data.table::fread(file))
  }), use.names = TRUE, fill = TRUE)
  
  # Process ID field
  data[, id := gsub(" -", " m", basename(id[1])), by = id]
  data[, id := gsub("m", "-", unlist(data.table::tstrsplit(gsub("POWER ", "", id[1]), "-", keep = 1))), by = id]
  
  # Identify parameter columns
  param_cols <- colnames(data)[-(1:3)]
  data[, N := .N, by = list(id, YEAR, DOY)]
  
  cat("Averaging data for sites with multiple cells, this may take some time.", "\n")
  
  # Calculate means and round them for each group
  means <- data[N > 1, lapply(.SD, function(x) mean(x, na.rm = TRUE)), 
                .SDcols = param_cols, by = .(id, YEAR, DOY)]
  
  means[, `:=`((param_cols), round(.SD, 2)), .SDcols = param_cols]
  
  data <- rbind(data[N == 1][, N := NULL], means)
  
  # Extract latitude and longitude from ID
  data[, Latitude := as.numeric(unlist(tstrsplit(id[1], " ", keep = 1))), by = id]
  data[, Longitude := as.numeric(unlist(tstrsplit(id[1], " ", keep = 1))), by = id]
  
  # Rename columns
  setnames(data,
           c("id", "DOY", "YEAR", "PRECTOTCORR"),
           c(id_field, "Day", "Year", "PRECTOT"),
           skip_absent = TRUE)
  
  # Merge with altitude data
  data <- merge(data, altitude_data, by = id_field, all.x = TRUE)
  
  # Calculate PET if required
  if (add_pet) {
    data[, ETo := ERAgON::PETcalc(Tmin = T2M_MIN,
                                  Tmax = T2M_MAX,
                                  SRad = ALLSKY_SFC_SW_DWN,
                                  Wind = WS2M,
                                  Rain = PRECTOT,
                                  Pressure = PSC,
                                  Humid = RH2M,
                                  YearDay = Day,
                                  Latitude = Latitude,
                                  Altitude = Altitude)[, 1]]
  }
  
  # Rename parameters
  setnames(data, parameters, rename, skip_absent = TRUE)
  
  # Add date field if required
  if (add_date) {
    data[, Date := as.Date(paste0(Year[1], "-", Day[1]), format = "%Y-%j"), by = list(Day, Year)]
  }
  
  # Add day count field if required
  if (add_daycount) {
    data[, DayCount := as.integer(floor(unclass(Date[1] - time_origin))), by = Date]
  }
  
  return(data)
}
#' Download POWER Data
#'
#' This function downloads POWER data from NASA for specified locations and dates, and saves the data as CSV files.
#'
#' @param vect_data A spatial object containing the locations for which to download data.
#' @param date_start The start date for the data download (format: "YYYY-MM-DD").
#' @param date_end The end date for the data download (format: "YYYY-MM-DD").
#' @param altitude The altitude of the locations.
#' @param save_dir The directory where the downloaded CSV files will be saved.
#' @param parameters A character vector of parameters to download.
#' @param id An identifier for the locations.
#' @param verbose Logical; whether to print progress messages. Default is \code{FALSE}.
#' @param overwrite Logical; whether to overwrite existing files. Default is \code{FALSE}.
#' @return None. This function saves the downloaded data as CSV files in the specified directory.
#' @examples
#' \dontrun{
#' See https://github.com/CIAT/ERA_dev/blob/main/R/add_geodata/power.R
#' era_locations<-unique(ERAg::ERA.Compiled[,list(Site.Key,Latitude,Longitude,Buffer)])
#' pbuf_g<-ERAg::Pbuffer(Data = era_locations,ID = "Site.Key" ,Projected=F)
#'
#' era_elevation <- data.table(arrow::read_parquet(file.path(era_dirs$era_geodata_dir, "era_site_topography.parquet")))
#' pbuf_g <- merge(pbuf_g, era_elevation[, list(Site.Key, Altitude.mean)])
#'
#'for(i in 1:length(pbuf_g)){
#'  
#'  cat("\r", "POWER: Downloading site", i,"/",length(pbuf_g),"\n")
#'  
#'  save_dir<-era_dirs$power_dir
#'  
#'  download_power(vect_data=pbuf_g[i],
#'                 date_start = date_start,
#'                 date_end=date_end,
#'                 altitude=pbuf_g[i]$Altitude.mean,
#'                 save_dir=era_dirs$power_dir,
#'                 parameters = parameters,
#'                 id=pbuf_g[i]$Site.Key,
#'                 verbose=F,
#'                 overwrite=update_missing_times)
#'}
#' }
#' @export
download_power <- function(vect_data, date_start, date_end, altitude, save_dir, parameters, id, verbose = FALSE, overwrite = FALSE) {
  
  vect_ext <- round(terra::ext(vect_data), 5)
  base_url <- "https://power.larc.nasa.gov/api/temporal/daily/"
  
  date_start <- as.Date(date_start)
  date_end <- as.Date(date_end)
  options(warn = -1)
  
  lonmin <- vect_ext[1]
  lonmax <- vect_ext[2]
  latmin <- vect_ext[3]
  latmax <- vect_ext[4]
  
  lat_vals <- seq(-89.75, 89.75, 0.5)
  lon_vals <- seq(-179.75, 179.75, 5/8)
  
  lonmax <- lon_vals[which.min(abs(lon_vals - lonmax))]
  lonmin <- lon_vals[which.min(abs(lon_vals - lonmin))]
  latmax <- lat_vals[which.min(abs(lat_vals - latmax))]
  latmin <- lat_vals[which.min(abs(lat_vals - latmin))]
  n_cells <- expand.grid(lat = unique(c(latmin, latmax)), lon = unique(c(lonmin, lonmax)))
  
  for (j in 1:nrow(n_cells)) {
    file_name <- file.path(save_dir, paste0("POWER ", id, "-", j, ".csv"))
    if (!file.exists(file_name) || overwrite) {
      URL <- paste0(base_url, 
                    "point?start=", format(date_start, "%Y%m%d"), 
                    "&end=", format(date_end, "%Y%m%d"), 
                    "&latitude=", n_cells[j, "lat"], 
                    "&longitude=", n_cells[j, "lon"], 
                    "&community=ag", 
                    "&parameters=", paste(parameters, collapse = "%2C"), 
                    "&format=csv", 
                    "&user=ICRAF2", 
                    "&header=true&time-standard=lst", 
                    "&site-elevation=", altitude)
      
      if (verbose) {
        cat("\r", "POWER: Downloading site", id, "| Cell", j, "/", nrow(n_cells))
        flush.console()
      }
      
      tryCatch(download.file(URL, file_name, method = "libcurl", 
                             quiet = !verbose, mode = "w", cacheOK = TRUE), 
               error = function(e) print(paste(file_name, "did not work out")))
    }
  }
  options(warn = 0)
}
#' Make S3 Bucket Public
#'
#' This function sets the policy of an S3 bucket to allow public read access to specified folders or items.
#' It also backs up the past and current policies to the bucket for reference.
#' The function is vectorized where possible and can handle multiple paths simultaneously. 
#' Due to the overhead of multiple writes and creating S3 instances, it is recommended to 
#' pass a list of URIs to the function rather than using it within a loop when adding multiple
#' folders.
#'
#' @param s3_uri A character string or list of strings specifying the S3 bucket and path (e.g., "s3://bucket-name/folder-path").
#' @param bucket A character string specifying the S3 bucket.
#' @param directory A logical value indicating if "/*" should be appended to the end of the path for globbing. Default is TRUE.
#' @return The new bucket policy. This function modifies the S3 bucket policy.
#' @importFrom jsonlite parseJSON toJSON write_json
#' @importFrom paws.storage s3
#' @examples
#' \dontrun{
#' make_s3_public("s3://your-bucket-name/your-folder-path", "your-bucket-name")
#' make_s3_public(c("your-bucket-name/your-folder-path1", "your-bucket-name/your-folder-path2"), "your-bucket-name")
#' }
#' @export
makeObjectPublic <- function(s3_uri, bucket = "digital-atlas", directory = TRUE) {
  s3_inst <- paws.storage::s3()
  if(gsub('/|s3:|\\*', "", s3_uri) == bucket) {
    stop("Setting full bucket to public is not allowed using this function to prevential accidential changes.")
  }
  policy <- s3_inst$get_bucket_policy(Bucket = bucket)$Policy
  policy_ls <- jsonlite::parse_json(policy)
  tmp <- tempdir()
  tmp_dir <- file.path(tmp, "s3_policy")
  if (!dir.exists(tmp_dir)) dir.create(tmp_dir, recursive = T)
  on.exit(unlink(tmp_dir, recursive = T))
  jsonlite::write_json(policy_ls, file.path(tmp_dir, 'previous_policy.json'),
                       pretty = T, auto_unbox = T)
  s3_inst$put_object(Bucket = bucket, 
                     Key = '.bucket_policy/previous_policy.json',
                     Body = file.path(tmp_dir, 'previous_policy.json'))
  s3_uri_clean <- gsub("s3://", "", s3_uri)
  dir_wildcard  <- ifelse(directory, "/*", "")
  s3_uri_clean <- paste0(s3_uri_clean, dir_wildcard)
  s3_arn <- paste0("arn:aws:s3:::", s3_uri_clean)
  s3_path <- gsub(paste0(bucket, "/"), "", s3_uri_clean)
  policy_ls$Statement <- lapply(policy_ls$Statement, function(statement) {
    switch(statement$Sid,
           "AllowPublicGet" = {
             statement$Resource <- unique(c(statement$Resource, s3_arn))
           },
           "AllowPublicList" = {
             statement$Condition$StringLike$`s3:prefix` <- unique(
               c(statement$Condition$StringLike$`s3:prefix`, s3_path)
             )
           }
    )
    return(statement)
  })
  new_policy <- jsonlite::toJSON(policy_ls, pretty = T, auto_unbox = T)
  s3_inst$put_bucket_policy(Bucket = bucket, Policy = new_policy)
  jsonlite::write_json(policy_ls, file.path(tmp_dir, 'current_policy.json'), 
                       pretty = T, auto_unbox = T)
  s3_inst$put_object(Bucket = bucket, 
                     Key = '.bucket_policy/current_policy.json',
                     Body = file.path(tmp_dir, 'current_policy.json'))
  return(new_policy)
}
#' Upload Files to S3
#'
#' This function uploads local files to an S3 bucket, optionally setting the access mode.
#'
#' @param files A character vector of local file paths to be uploaded.
#' @param s3_file_names A character vector of filenames to use in S3 (optional).
#' @param folder A character string specifying a local folder to list files from (optional).
#' @param selected_bucket A character string specifying the S3 bucket.
#' @param new_only Logical; whether to upload only new files. Default is \code{FALSE}.
#' @param max_attempts Integer; maximum number of attempts for each file upload. Default is 3.
#' @param overwrite Logical; whether to overwrite existing files. Default is \code{FALSE}.
#' @param mode A character string specifying the access mode ("private", "public-read"). Default is "private".
#' @param directory A logical value indicating if "/*" should be appended to the end of the path for globbing. Default is TRUE.
#' @return None. This function uploads files to an S3 bucket.
#' @importFrom paws s3
#' @importFrom utils flush.console
#' @examples
#' \dontrun{
#' upload_files_to_s3(files = c("file1.txt", "file2.txt"), selected_bucket = "s3://your-bucket-name")
#' }
#' @export
upload_files_to_s3 <- function(files,
                               s3_file_names = NULL, 
                               folder = NULL, 
                               selected_bucket, 
                               new_only = FALSE, 
                               max_attempts = 3, 
                               overwrite = FALSE,
                               mode = "private",
                               directory=T) {
  s3 <- paws::s3()
  
  # Create the s3 directory if it does not already exist
  if (!s3_dir_exists(selected_bucket)) {
    s3_dir_create(selected_bucket)
  }
  
  # List files if a folder location is provided
  if (!is.null(folder)) {
    files <- list.files(folder, full.names = TRUE)
  }
  
  if (!overwrite) {
    # List files in the s3 bucket
    files_s3 <- basename(s3_dir_ls(selected_bucket))
    # Remove any files that already exist in the s3 bucket
    files <- files[!basename(files) %in% files_s3]
  }
  
  for (i in seq_along(files)) {
    cat('\r', paste("File:", i, "/", length(files)), " | ", basename(files[i]), "                                                 ")
    flush.console()
    
    if (is.null(s3_file_names)) {
      s3_file_path <- paste0(selected_bucket, "/", basename(files[i]))
    } else {
      if (length(s3_file_names) != length(files)) {
        stop("s3 filenames provided different length to local files")
      }
      s3_file_path <- paste0(selected_bucket, "/", s3_file_names[i])
    }
    
    tryCatch({
      attempt <- 1
      while (attempt <= max_attempts) {
        s3_file_upload(files[i], s3_file_path, overwrite = overwrite)
        # Check if upload successful
        file_check <- s3_file_exists(s3_file_path)
        
        if (mode != "private") {
          s3_file_chmod(path = s3_file_path, mode = mode)
        }
        
        if (file_check) break # Exit the loop if upload is successful
        
        if (attempt == max_attempts && !file_check) {
          stop("File did not upload successfully after ", max_attempts, " attempts.")
        }
        attempt <- attempt + 1
      }
    }, error = function(e) {
      cat("\nError during file upload:", e$message, "\n")
    })
  }
  
  if (mode == "public-read") {
    makeObjectPublic(selected_bucket,directory=directory)
  }
}
#' Wait If Not
#'
#' This function checks a condition and, if it is not true, prints a message and enters an infinite loop.
#'
#' @param cond A logical condition to be checked.
#'
#' @return This function does not return any value. If the condition is not true, it prints a message and enters an infinite loop.
#'
#' @examples
#' \dontrun{
#' waitifnot(1 == 2)
#' }
waitifnot <- function(cond) {
  if (!cond) {
    message(deparse(substitute(cond)), " is not TRUE")
    while (TRUE) {}
  }
}
#' Replace Zeros with NAs
#'
#' This function replaces all occurrences of zero in the input data with NA.
#'
#' @param data A numeric vector or data frame in which zeros should be replaced with NAs.
#'
#' @return A numeric vector or data frame with zeros replaced by NAs.
#'
#' @examples
#' # Replace zeros with NAs in a numeric vector
#' replace_zero_with_NA(c(1, 0, 2, 0, 3))
#'
#' # Replace zeros with NAs in a data frame
#' df <- data.frame(a = c(1, 0, 2), b = c(0, 3, 0))
#' replace_zero_with_NA(df)
