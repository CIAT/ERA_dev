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
      cat("Error during file upload:", e$message, "\n")
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
replace_zero_with_NA <- function(data) {
  data[data == 0] <- NA
  return(data)
}
#' Error Tracker
#'
#' This function tracks errors, saves them to a CSV file, and merges new errors with existing ones.
#'
#' @param errors A data frame containing error information.
#' @param filename A string representing the name of the file to save the errors to (without extension).
#' @param error_dir A string representing the directory where the error file should be saved.
#' @param error_list A list of existing error data frames. Defaults to `NULL`.
#'
#' @return A list of error data frames, updated with the new errors.
#'
#' @examples
#' \dontrun{
#' errors <- data.frame(error_message = c("Error 1", "Error 2"), stringsAsFactors = FALSE)
#' error_tracker(errors, "errors_file", "/path/to/error_dir")
#' }
#' @importFrom data.table fread fwrite
error_tracker <- function(errors, filename, error_dir, error_list = NULL) {
  if (is.null(error_list)) {
    error_list <- list()
  }
  
  error_file <- file.path(error_dir, paste0(filename, ".csv"))
  
  if (nrow(errors) > 0) {
    if (file.exists(error_file)) {
      error_tracking <- unique(fread(error_file))
      error_tracking[, addressed_by_whom := as.character(addressed_by_whom)]
      
      if ("value" %in% colnames(error_tracking)) {
        error_tracking[, value := as.character(value)]
      }
      
      if ("notes" %in% colnames(error_tracking)) {
        error_tracking[, notes := as.character(notes)]
      } else {
        error_tracking[, notes := ""]
      }
      
      errors <- merge(errors, error_tracking, all.x = TRUE, by = colnames(errors))
      errors[is.na(issue_addressed), issue_addressed := FALSE
      ][is.na(addressed_by_whom), addressed_by_whom := ""
      ][is.na(notes), notes := ""]
    } else {
      errors[, issue_addressed := FALSE
      ][, addressed_by_whom := ""
      ][, notes := ""]
    }
    error_list[[filename]] <- errors
    fwrite(errors, error_file)
  } else {
    unlink(error_file)
  }
  
  return(error_list)
}
#' Convert "NA" Strings to NA Values
#'
#' This function converts character strings "NA" to actual NA values in a data.table for all character columns using .SD for efficiency.
#'
#' @param dt A data.table in which "NA" strings should be converted to NA values.
#'
#' @return A data.table with "NA" strings converted to NA values in all character columns.
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(a = c("NA", "1", "2"), b = c("3", "NA", "4"))
#' convert_NA_strings_SD(dt)
#' }
#' @import data.table
convert_NA_strings_SD <- function(dt) {
  # Ensure the input is a data.table
  if (!is.data.table(dt)) {
    stop("Input must be a data.table")
  }
  
  # Identify which columns are character type
  char_cols <- names(dt)[sapply(dt, is.character)]
  
  # Perform the operation on all character columns at once
  dt[, (char_cols) := lapply(.SD, function(x) fifelse(x == "NA", NA_character_, x)), .SDcols = char_cols]
  
  # Return the modified data.table
  return(dt)
}
# 'harmonizer' function:
# This function harmonizes data fields by replacing old values with new values based on master codes.
# It supports both primary and alternate harmonization rules if provided.
#
# Args:
#   data: DataFrame - The data.table to be harmonized.
#   master_codes: List - A list containing the 'lookup_levels' DataFrame with old to new value mappings.
#   h_table: String - The name of the table in 'lookup_levels' used for the primary harmonization.
#   h_field: String - The name of the field in 'lookup_levels' used for the primary harmonization.
#   h_table_alt: String - Optional. The name of the alternate table for harmonization if different from primary.
#   h_field_alt: String - Optional. The name of the alternate field for harmonization if different from primary.
#
# Returns:
#   A list containing:
#   - 'data': DataFrame with the harmonized data.
#   - 'h_tasks': DataFrame listing any non-matched harmonization tasks by B.Code and value.
harmonizer <- function(data, master_codes, master_tab="lookup_levels",h_table, h_field, h_table_alt=NA, h_field_alt=NA,ignore_vals=NULL) {
  data<-data.table(data)
  
  # subset master_codes to relevant tab
  m_codes<-master_codes[[master_tab]]
  
  # Selecting relevant columns for output
  h_cols <- c("B.Code", h_field)
  
  # Harmonize old names to new names
  if (is.na(h_field_alt)) {
    # Retrieve mappings for primary fields if no alternate field is provided
    h_tab <- m_codes[Table == h_table & Field == h_field, list(Values_New, Values_Old)]
  } else {
    if (is.na(h_table_alt)) {
      # Warning if alternate field is provided without an alternate table
      warning("If h_field_alt is provided, h_table_alt should also be provided, currently using h_table which may result in non-matches.")
      h_table_alt <- h_table
    }
    # Retrieve mappings for alternate fields
    h_tab <- m_codes[Table == h_table_alt & Field == h_field_alt, list(Values_New, Values_Old)]

    # Split Values_Old by ";" and unnest the list into separate rows
    h_tab<-rbindlist(lapply(1:nrow(h_tab),FUN=function(i){
      data.table(Values_New=h_tab$Values_New[i],Values_Old=unlist(strsplit(h_tab$Values_Old[i],";")))
    }))
    
  }
  
  # Matching old values to new values and updating data
  N <- match(unlist(data[, ..h_field]), h_tab[, Values_Old])
  data <- data[!is.na(N), (h_field) := h_tab[N[!is.na(N)], Values_New]]
  
  # Check for any non-matches after harmonization
  N <- match(unlist(data[, ..h_field]), h_tab[, Values_New])
  if(!is.null(ignore_vals)){
    if(!is.na(ignore_vals)){
      h_tasks <- unique(data[is.na(N) & !grepl(paste(ignore_vals,collapse="|"),unlist(data[, ..h_field]),ignore.case = T), ..h_cols])
    }else{
      h_tasks <- unique(data[is.na(N), ..h_cols])
    }
  }else{
    h_tasks <- unique(data[is.na(N), ..h_cols])
  }
  colnames(h_tasks)[2] <- "value"
  h_tasks <- h_tasks[, .(B.Code = paste(B.Code, collapse = "/")), by = list(value)][!is.na(value)]
  
  # Adding metadata columns to the tasks output for further tracking
  h_tasks[, table := h_table][, field := h_field][,table_alt:=h_table_alt][,field_alt:=h_field_alt][,master_tab:=master_tab]
  
  return(list(data = data, h_tasks = h_tasks))
}
#' Harmonizer Wrapper
#'
#' This function wraps the harmonization process, applying the harmonizer function to the provided data according to the specified parameters and master codes.
#'
#' @param data A data frame or data table to be harmonized.
#' @param h_params A data frame containing harmonization parameters, including `h_table`, `h_field`, `h_table_alt`, `h_field_alt`, and optionally `master_tab` and `ignore_vals`.
#' @param master_codes A data frame or data table containing master codes used for harmonization.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{data}{The harmonized data.}
#'   \item{h_tasks}{A data table of harmonization tasks.}
#' }
#'
#' @examples
#' \dontrun{
#' data <- data.frame(id = 1:3, value = c("A", "B", "C"))
#' h_params <- data.frame(h_table = "table1", h_field = "field1", 
#'                        h_table_alt = "table2", h_field_alt = "field2")
#' master_codes <- data.frame(code = c("A", "B", "C"), value = c(1, 2, 3))
#' harmonizer_wrap(data, h_params, master_codes)
#' }
harmonizer_wrap <- function(data, h_params, master_codes) {
  h_tasks <- list()
  
  if (!any(grepl("master_tab", colnames(h_params)))) {
    h_params$master_tab <- "lookup_levels"
  }
  
  if (!any(grepl("ignore_vals", colnames(h_params)))) {
    h_params$ignore_vals <- NA
  }
  
  for (i in 1:nrow(h_params)) {
    results <- harmonizer(
      data = data,
      master_codes = master_codes,
      master_tab = h_params$master_tab[i],
      h_table = h_params$h_table[i], 
      h_field = h_params$h_field[i],
      h_table_alt = h_params$h_table_alt[i],
      h_field_alt = h_params$h_field_alt[i],
      ignore_vals = h_params$ignore_vals[i]
    )
    
    data <- results$data
    h_tasks[[i]] <- results$h_tasks
  }
  
  return(list(data = data, h_tasks = rbindlist(h_tasks)))
}
#' Value Checker
#'
#' This function checks if values in a specified field of the data exist in the master codes table. It can handle both exact and approximate matching.
#'
#' @param data A data frame or data table to be checked.
#' @param tabname A string representing the name of the table in the data.
#' @param master_codes A list of data frames or data tables containing master codes for different tabs.
#' @param master_tab A string representing the tab name in the master codes list.
#' @param h_field A string representing the name of the field to be checked.
#' @param h_field_alt A string representing an alternate field to be used if `h_field` is not found. Defaults to `NA`.
#' @param exact A logical value indicating whether to perform exact matching (`TRUE`) or approximate matching (`FALSE`). Defaults to `TRUE`.
#' @param ignore_vals A vector of values to be ignored during the check. Defaults to `NULL`.
#'
#' @return If `exact` is `TRUE`, returns a data table of unmatched values. If `exact` is `FALSE`, returns a list containing a data table of unmatched values and the modified data with matched values.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), h_field = c("val1", "val2", "val3"))
#' master_codes <- list(tab1 = data.frame(h_field = c("val1", "val4"), h_field_alt = c("alt1", "alt4")))
#' val_checker(data, "example_table", master_codes, "tab1", "h_field")
#' }
#' @import data.table
val_checker <- function(data, tabname, master_codes, master_tab, h_field, h_field_alt = NA, exact = TRUE, ignore_vals = NULL) {
  
  n_col <- c("B.Code", h_field)
  h_tasks <- data.table(data)[, ..n_col]
  setnames(h_tasks, h_field, "value")
  h_tasks <- h_tasks[, .(B.Code = paste(B.Code, collapse = "/")), by = list(value)][!is.na(value)]
  h_tasks[, field := h_field][, field_alt := h_field_alt][, table := tabname][, master_tab := master_tab]
  
  # Subset master_codes to relevant tab
  m_codes <- master_codes[[master_tab]]
  
  # Harmonize old names to new names
  if (is.na(h_field_alt)) {
    # Retrieve mappings for primary fields if no alternate field is provided
    mc_vals <- unlist(m_codes[, ..h_field])
  } else {
    # Retrieve mappings for alternate fields
    mc_vals <- unlist(m_codes[, ..h_field_alt])
  }
  
  # Check if values are in master codes
  if (exact) {
    h_tasks <- h_tasks[, check := FALSE][value %in% mc_vals, check := TRUE][check == FALSE][, check := NULL]
    return(h_tasks)
  } else {
    matched <- as.character(mc_vals)[match(tolower(h_tasks$value), tolower(mc_vals))]
    h_tasks <- h_tasks[is.na(matched)]
    setnames(data, h_field, "value")
    matched <- as.character(mc_vals)[match(tolower(data$value), tolower(mc_vals))]
    data[!is.na(matched), value := matched[!is.na(matched)]]
    setnames(data, "value", h_field)
    if (!is.null(ignore_vals)) {
      h_tasks <- h_tasks[!grepl(paste0(ignore_vals, collapse = "|"), value, ignore.case = TRUE)]
    }
    return(list(h_task = h_tasks, data = data))
  }
}
#' Find and Report Non-Numeric Values
#'
#' This function identifies non-numeric values in specified numeric columns of a data frame or data table.
#'
#' @param data A data frame or data table to be checked.
#' @param numeric_cols A vector of column names that are expected to contain numeric values.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of non-numeric values, including the table name and field where they were found.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), num_col1 = c("1", "two", "3"), num_col2 = c("4", "five", "6"))
#' find_non_numeric(data, c("num_col1", "num_col2"), "example_table")
#' }
#' @import data.table
find_non_numeric <- function(data, numeric_cols, tabname) {
  results <- rbindlist(lapply(1:length(numeric_cols), FUN = function(i) {
    n_col <- numeric_cols[i]
    vals <- unlist(data[, ..n_col])
    vals_u <- unique(vals)
    vals_u <- vals_u[!is.na(vals_u)]
    NAs <- vals_u[is.na(as.numeric(vals_u))]
    n_col2 <- c("B.Code", n_col)
    result <- unique(data[vals %in% NAs, ..n_col2])[, table := tabname][, field := n_col]
    setnames(result, n_col, "value")
    result
  }))
  
  return(results)
}
#' Check Site and Time Fields
#'
#' This function checks if the `Site.ID` and `Time` fields in the data match the corresponding entries in the site and time tables.
#'
#' @param data A data frame or data table to be checked.
#' @param tabname A string representing the name of the table in the data.
#' @param ignore_values A vector of values to be ignored during the check.
#' @param site_data A data frame or data table containing valid `Site.ID` entries.
#' @param time_data A data frame or data table containing valid `Time` entries.
#' @param do_site A logical value indicating whether to check `Site.ID`. Defaults to `TRUE`.
#' @param do_time A logical value indicating whether to check `Time`. Defaults to `TRUE`.
#'
#' @return A data table of errors found, including unmatched `Site.ID` and `Time` values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), Site.ID = c("S1", "S2", "S3"), Time = c("T1", "T2", "T3"))
#' site_data <- data.frame(Site.ID = c("S1", "S2"), B.Code = c("A1", "B2"))
#' time_data <- data.frame(Time = c("T1", "T2"), B.Code = c("A1", "B2"))
#' check_site_time(data, "example_table", c("ignore1", "ignore2"), site_data, time_data)
#' }
#' @import data.table
check_site_time <- function(data, tabname, ignore_values, site_data, time_data, do_site = TRUE, do_time = TRUE) {
  errors <- list()
  
  if ("Site.ID" %in% colnames(data) & do_site) {
    site_data <- site_data[, check := TRUE][, list(Site.ID, B.Code, check)]
    # Non-match in Site.ID
    errors1 <- unique(data[!grepl(paste0(ignore_values, collapse = "|"), Site.ID, ignore.case = TRUE)
    ][!is.na(Site.ID), list(B.Code, Site.ID)])
    errors1 <- merge(errors1, site_data, all.x = TRUE)[is.na(check)
    ][, check := NULL
    ][, table := tabname
    ][, field := "Site.ID"
    ][, issue := "A Site.ID used does not match the Site tab."]
    setnames(errors1, "Site.ID", "value")
    errors$errors1 <- errors1
  }
  
  if ("Time" %in% colnames(data) & do_time) {
    # Non-match in Time
    time_data <- time_data[, check := TRUE][, list(Time, B.Code, check)]
    errors2 <- unique(data[!grepl(paste0(ignore_values, collapse = "|"), Time, ignore.case = TRUE)
    ][!is.na(Time), list(B.Code, Time)])
    errors2 <- merge(errors2, time_data, all.x = TRUE)[is.na(check)
    ][, check := NULL
    ][, table := tabname
    ][, field := "Time"
    ][, issue := "A Time used does not match the Time tab."]
    setnames(errors2, "Time", "value")
    errors$errors2 <- errors2
  }
  
  if (length(errors) > 0) {
    errors <- rbindlist(errors)
  } else {
    NULL
  }
  
  return(errors)
}
#' Check Dates
#'
#' This function checks if the dates in the specified date columns are within the valid date range.
#'
#' @param data A data frame or data table containing the data to be checked.
#' @param date_cols A vector of column names that are expected to contain date values.
#' @param valid_start A Date object representing the start of the valid date range.
#' @param valid_end A Date object representing the end of the valid date range.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including dates not within the valid date range, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), date1 = as.Date(c("2021-01-01", "2022-02-02", "2023-03-03")))
#' check_dates(data, c("date1"), as.Date("2020-01-01"), as.Date("2022-12-31"), "example_table")
#' }
#' @import data.table
check_dates <- function(data, date_cols, valid_start, valid_end, tabname) {
  
  results <- rbindlist(lapply(1:length(date_cols), FUN = function(i) {
    n_col <- c("B.Code", date_cols[i])
    data_ss <- data[, ..n_col]
    colnames(data_ss)[2] <- "value"
    
    data_ss <- data_ss[!is.na(value)]
    
    data_ss <- data_ss[, problem := TRUE
    ][value >= valid_start & value <= valid_end, problem := FALSE
    ][, list(value = paste(value, collapse = "/")), by = list(B.Code, problem)
    ][, field := date_cols[i]]
    data_ss
  }))
  
  results <- results[problem == TRUE
  ][, table := tabname
  ][, problem := NULL
  ][, issue := paste("Date not between", valid_start, "&", valid_end)]
  
  return(results)
}
#' Detect Extreme Values
#'
#' This function detects extreme values in a specified field of the data that are outside a given range.
#'
#' @param data A data frame or data table containing the data to be checked.
#' @param field A string representing the name of the field to be checked for extreme values.
#' @param lower_bound A numeric value representing the lower bound of the acceptable range.
#' @param upper_bound A numeric value representing the upper bound of the acceptable range.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of extreme values found, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), value_field = c(1, 100, 50))
#' detect_extremes(data, "value_field", 0, 60, "example_table")
#' }
#' @import data.table
detect_extremes <- function(data, field, lower_bound, upper_bound, tabname) {
  # Dynamically construct the filtering condition
  condition <- paste0(field, " > ", upper_bound, " | ", field, " < ", lower_bound)
  
  # Evaluate the condition within the data.table context
  errors <- data[eval(parse(text = condition))
  ][, list(value = paste0(unique(get(field)), collapse = "/")), by = list(B.Code)
  ][, table := tabname
  ][, field := field
  ][, issue := paste0("Extreme values detected in field (outside range: ", lower_bound, "-", upper_bound, ")")]
  
  return(errors)
}
#' Check Units
#'
#' This function checks if units are missing for fields where amounts are present in the data.
#'
#' @param data A data frame or data table to be checked.
#' @param unit_pairs A data frame containing pairs of unit and variable fields along with their name fields.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including instances where amounts are present but units are missing, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), amount = c(10, NA, 30), unit = c(NA, "kg", NA), name_field = c("item1", "item2", "item3"))
#' unit_pairs <- data.frame(unit = c("unit"), var = c("amount"), name_field = c("name_field"))
#' check_units(data, unit_pairs, "example_table")
#' }
#' @import data.table
check_units <- function(data, unit_pairs, tabname) {
  results <- rbindlist(lapply(1:nrow(unit_pairs), FUN = function(i) {
    Unit <- unit_pairs$unit[i]
    Var <- unit_pairs$var[i]
    Name_Field <<- unit_pairs$name_field[i]
    
    # Dynamically construct the filtering condition
    condition <- paste0("is.na(", Unit, ") & !is.na(", Var, ")")
    condition2 <- paste0(Name_Field, "[!is.na(", Name_Field, ")]")
    
    # Evaluate the condition within the data.table context
    errors <- data[eval(parse(text = condition))
    ][, list(value = paste(eval(parse(text = condition2)), collapse = "/")), by = B.Code
    ][, table := tabname
    ][, field := Name_Field
    ][, issue := paste0("Amount is present, but unit is missing for ", Var, ".")
    ][order(B.Code)]
    errors
  }))
  
  return(results)
}
#' Check High and Low Values
#'
#' This function checks if the low values are greater than the high values for specified columns in the data.
#'
#' @param data A data frame or data table to be checked.
#' @param hilo_pairs A data frame containing pairs of high and low columns along with their name fields.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including instances where low values are greater than high values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), low_value = c(10, 30, 50), high_value = c(20, 25, 45), name_field = c("item1", "item2", "item3"))
#' hilo_pairs <- data.frame(low_col = c("low_value"), high_col = c("high_value"), name_field = c("name_field"))
#' check_hilow(data, hilo_pairs, "example_table")
#' }
#' @import data.table
check_hilow <- function(data, hilo_pairs, tabname) {
  results <- rbindlist(lapply(1:nrow(hilo_pairs), FUN = function(i) {
    low_col <- hilo_pairs$low_col[i]
    high_col <- hilo_pairs$high_col[i]
    Name_Field <- hilo_pairs$name_field[i]
    
    # Dynamically construct the filtering condition
    condition <- paste0(low_col, " > ", high_col)
    condition2 <- paste0(Name_Field, "[!is.na(", Name_Field, ")]")
    
    # Evaluate the condition within the data.table context
    errors <- data[eval(parse(text = condition))
    ][, list(value = paste(eval(parse(text = condition2)), collapse = "/")), by = B.Code
    ][, table := tabname
    ][, field := Name_Field
    ][, issue := paste0(low_col, " is greater than ", high_col, ".")
    ][order(B.Code)]
    errors
  }))
  
  return(results)
}
#' Validate Data
#'
#' This function performs various validation checks on the data, including checking for unique values, compulsory fields, non-numeric values in numeric fields, date ranges, extreme values, missing units, and high/low pair issues.
#'
#' @param data A data frame or data table to be checked.
#' @param numeric_cols A vector of column names that are expected to contain numeric values. Defaults to `NULL`.
#' @param numeric_ignore_vals A vector of values to be ignored in numeric columns. Defaults to `NULL`.
#' @param date_cols A vector of column names that are expected to contain date values. Defaults to `NULL`.
#' @param zero_cols A vector of column names where zeros should be replaced with `NA`. Defaults to `NULL`.
#' @param unique_cols A vector of column names that should contain unique values. Defaults to `NULL`.
#' @param compulsory_cols A named vector where names are columns and values are associated compulsory fields. Defaults to `NULL`.
#' @param extreme_cols A named list where names are columns and values are vectors with lower and upper bounds. Defaults to `NULL`.
#' @param unit_pairs A data frame containing pairs of unit and variable fields along with their name fields. Defaults to `NULL`.
#' @param hilo_pairs A data frame containing pairs of high and low columns along with their name fields. Defaults to `NULL`.
#' @param tabname A string representing the name of the table in the data.
#' @param valid_start A Date object representing the start of the valid date range.
#' @param valid_end A Date object representing the end of the valid date range.
#' @param site_data A data frame or data table containing valid `Site.ID` entries. Defaults to `NULL`.
#' @param time_data A data frame or data table containing valid `Time` entries. Defaults to `NULL`.
#' @param ignore_values A vector of values to be ignored during the checks. Defaults to `NULL`.
#' @param trim_ws A logical value indicating whether to trim whitespace from character columns. Defaults to `FALSE`.
#' @param do_site A logical value indicating whether to check `Site.ID`. Defaults to `TRUE`.
#' @param do_time A logical value indicating whether to check `Time`. Defaults to `TRUE`.
#' @param convert_NA_strings A logical value indicating whether to convert "NA" strings to actual `NA` values. Defaults to `FALSE`.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{data}{The validated data.}
#'   \item{errors}{A data table of validation errors found.}
#' }
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), numeric_col = c("10", "20", "non-numeric"))
#' validator(data, numeric_cols = c("numeric_col"), tabname = "example_table", valid_start = as.Date("2020-01-01"), valid_end = as.Date("2022-12-31"))
#' }
#' @import data.table
validator <- function(data,
                      numeric_cols = NULL,
                      numeric_ignore_vals = NULL,
                      date_cols = NULL,
                      zero_cols = NULL,
                      unique_cols = NULL,
                      compulsory_cols = NULL,
                      extreme_cols = NULL,
                      unit_pairs = NULL,
                      hilo_pairs = NULL,
                      tabname,
                      valid_start,
                      valid_end,
                      site_data = NULL,
                      time_data = NULL,
                      ignore_values = NULL,
                      trim_ws = FALSE,
                      do_site = TRUE,
                      do_time = TRUE,
                      convert_NA_strings = FALSE) {
  
  errors <- list()
  n <- 1
  
  if (convert_NA_strings) {
    data <- convert_NA_strings_SD(data)
  }
  
  # zero cols
  if (!is.null(zero_cols)) {
    data <- data[, (zero_cols) := lapply(.SD, replace_zero_with_NA), .SDcols = zero_cols]
  }
  
  if (!is.null(unique_cols)) {
    errors1 <- rbindlist(lapply(1:length(unique_cols), FUN = function(i) {
      field <- unique_cols[i]
      n_col <- c("B.Code", field)
      dat <- data[, ..n_col]
      colnames(dat)[2] <- "value"
      errors <- dat[, list(N = .N), by = list(B.Code, value)
      ][N > 1
      ][, N := NULL
      ][, list(value = paste(value, collapse = "/")), by = B.Code
      ][, table := tabname
      ][, field := field
      ][, issue := "Duplicate value in unique field."
      ][order(B.Code)]
      errors
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  if (!is.null(compulsory_cols)) {
    errors1 <- rbindlist(lapply(1:length(compulsory_cols), FUN = function(i) {
      field <- compulsory_cols[i]
      assoc_field <- names(compulsory_cols)[i]
      n_col <- c("B.Code", field, assoc_field)
      dat <- data[, ..n_col]
      colnames(dat)[2:3] <- c("focus", "value")
      errors <- dat[is.na(focus),
      ][, list(value = paste(unique(value), collapse = "/")), by = B.Code
      ][, table := tabname
      ][, field := assoc_field
      ][, issue := paste0("Missing value in compulsory field ", compulsory_cols[i], ".")
      ][order(B.Code)]
      errors
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  if (!is.null(date_cols)) {
    numeric_cols <- unique(c(numeric_cols, date_cols))
  }
  
  # Substitute , for . in numeric columns
  if (!is.null(numeric_cols)) {
    data[, (numeric_cols) := lapply(.SD, function(x) gsub(",|·", ".", x)), .SDcols = numeric_cols]
    # Remove spaces in numeric columns
    data[, (numeric_cols) := lapply(.SD, function(x) gsub(" ", "", x)), .SDcols = numeric_cols]
    # Replace − with -
    data[, (numeric_cols) := lapply(.SD, function(x) gsub("−", "-", x)), .SDcols = numeric_cols]
    
    # Look for instances where a non-numeric value is present in a numeric field
    errors1 <- find_non_numeric(data = data, numeric_cols = numeric_cols, tabname = tabname)
    errors1 <- errors1[, list(value = paste(value, collapse = "/")), by = list(B.Code, table, field)
    ][, issue := "Non-numeric value in numeric field."]
    errors[[n]] <- errors1
    n <- n + 1
    # Convert numeric fields to being numeric
    data[, (numeric_cols) := lapply(.SD, function(x) as.numeric(x)), .SDcols = numeric_cols]
  }
  
  # Detect extremes
  if (!is.null(extreme_cols)) {
    errors1 <- rbindlist(lapply(1:length(extreme_cols), FUN = function(i) {
      detect_extremes(data = data,
                      field = names(extreme_cols)[i],
                      lower_bound = extreme_cols[[i]][1],
                      upper_bound = extreme_cols[[i]][2],
                      tabname = tabname)
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Trim white space
  if (trim_ws) {
    char_cols <- sapply(data, is.character)
    data[, (names(data)[char_cols]) := lapply(.SD, trimws), .SDcols = char_cols]
  }
  
  # Convert date cols to date format
  if (!is.null(date_cols)) {
    # Convert Excel date numbers to R dates
    data[, (date_cols) := lapply(.SD, function(x) as.Date(x, origin = "1899-12-30")), .SDcols = date_cols]
    
    # Look for dates outside of a specified range
    errors[[n]] <- check_dates(data = data,
                               date_cols = date_cols,
                               valid_start = valid_start,
                               valid_end = valid_end,
                               tabname = tabname)[, value := as.character(value)]
    n <- n + 1
  }
  
  # Check for missing units
  if (!is.null(unit_pairs)) {
    errors1 <- check_units(data, unit_pairs = unit_pairs, tabname = tabname)
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Check for high/low pair issues
  if (!is.null(hilo_pairs)) {
    errors1 <- check_hilow(data, hilo_pairs = hilo_pairs, tabname = tabname)
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Check for non-matches between site.id and time fields and their parent tables
  if (any(c("Site.ID", "Time") %in% colnames(data)) & tabname != "Site.Out") {
    if (is.null(ignore_values)) {
      ignore_values <- c("All Times", "Unspecified", "Not specified", "All Sites")
    }
    errors1 <- check_site_time(data = data,
                               ignore_values = ignore_values,
                               tabname = tabname,
                               site_data = site_data,
                               time_data = time_data,
                               do_site = do_site,
                               do_time = do_time)
    
    if (length(errors1) > 0) {
      errors1 <- errors1[, list(value = paste0(value, collapse = "/")), by = list(B.Code, table, field, issue)]
      errors[[n]] <- errors1
    }
  }
  
  errors <- rbindlist(errors, use.names = TRUE)
  
  return(list(data = data, errors = errors))
}
#' Check Key Field between Parent and Child Tables
#'
#' This function checks for mismatches in a key field between parent and child tables.
#'
#' @param parent A data frame or data table representing the parent table.
#' @param child A data frame or data table representing the child table.
#' @param tabname A string representing the name of the child table.
#' @param keyfield A string representing the name of the key field to be checked.
#' @param collapse_on_code A logical value indicating whether to collapse mismatched values on the B.Code. Defaults to `TRUE`.
#' @param tabname_parent A string representing the name of the parent table. Defaults to `NULL`.
#'
#' @return A data table of mismatched key field values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' parent <- data.table(B.Code = c("A1", "A2"), key_field = c("key1", "key2"))
#' child <- data.table(B.Code = c("A1", "A3"), key_field = c("key1", "key3"))
#' check_key(parent, child, "child_table", "key_field", TRUE, "parent_table")
#' }
#' @import data.table
check_key <- function(parent, child, tabname, keyfield, collapse_on_code = TRUE, tabname_parent = NULL) {
  n_col <- c("B.Code", keyfield)
  
  mergetab <- unique(merge(child[, ..n_col], parent[, ..n_col][, check := TRUE], all.x = TRUE)[is.na(check)][, check := NULL])
  setnames(mergetab, keyfield, "value")
  
  if (collapse_on_code) {
    mergetab <- mergetab[, list(value = paste(unique(value), collapse = "/")), by = list(B.Code)]
  } else {
    mergetab <- mergetab[, list(B.Code, value)]
  }
  
  mergetab[, table := tabname
  ][, field := keyfield
  ][, issue := "Mismatch in field value between parent and child tables."]
  
  if (!is.null(tabname_parent)) {
    mergetab[, parent_table := tabname_parent]
  }
  
  return(mergetab)
}
#' Check Coordinates
#'
#' This function checks if the coordinates (longitude and latitude) in the data are within the boundaries of the specified countries.
#'
#' @param data A data frame or data table containing the coordinates and country ISO codes. The data should have columns `Site.LonD` for longitude, `Site.LatD` for latitude, and `ISO.3166.1.alpha.3` for country ISO codes.
#'
#' @return A logical vector indicating whether each coordinate is within the boundaries of the specified country. `NA` is returned if the country code is not found.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(Site.LonD = c(10, 20, 30), Site.LatD = c(50, 60, 70), ISO.3166.1.alpha.3 = c("USA", "CAN", "MEX"))
#' check_coordinates(data)
#' }
#' @importFrom rnaturalearth ne_countries
#' @importFrom sf st_transform st_make_valid st_as_sf st_within
#' @importFrom dplyr filter
check_coordinates <- function(data) {
  # Load country boundaries
  countries <- rnaturalearth::ne_countries(scale = "large", returnclass = "sf")
  
  # Ensure CRS (Coordinate Reference System) is set to WGS84 (EPSG:4326)
  countries <- st_transform(countries, crs = 4326)
  
  # Make geometries valid
  countries <- sf::st_make_valid(countries)  
  
  # Convert data to sf object
  data_sf <- sf::st_as_sf(data, coords = c("Site.LonD", "Site.LatD"), crs = 4326, remove = FALSE)
  
  # Initialize result vector
  is_within_country <- logical(nrow(data))
  
  # Iterate over unique ISO codes in the data
  for (iso3 in unique(data$ISO.3166.1.alpha.3)) {
    # Filter country boundaries for the current ISO code
    country_boundary <- countries %>% filter(iso_a3 == iso3)
    
    if (nrow(country_boundary) > 0) {
      # Check if points are within the country boundary
      points_in_country <- st_within(data_sf, country_boundary, sparse = FALSE)
      
      # Store results
      is_within_country[data$ISO.3166.1.alpha.3 == iso3] <- apply(points_in_country, 1, any)[data$ISO.3166.1.alpha.3 == iso3]
    } else {
      # Assign NA if country code not found
      is_within_country[data$ISO.3166.1.alpha.3 == iso3] <- NA
    }
  }
  
  return(is_within_country)
}
