#' Remove Trailing Slash from Path
#'
#' Removes a trailing slash ('/') from a given file or URL path, if present.
#'
#' @param path Character. Path string to clean.
#'
#' @importFrom stringr str_ends str_sub
#'
#' @return Character. Path without a trailing slash.
#' @export
remove_trailing_slash <- function(path) {
  if (stringr::str_ends(path, "/")) {
    path <- stringr::str_sub(path, 1, -2)
  }
  return(path)
}

#' Create CHIRPS File Paths
#'
#' Generates complete URLs for downloading CHIRPS data files based on specified parameters.
#'
#' @param base_url Character. Base URL of the CHIRPS dataset.
#' @param format Character. File format ('tif', 'tif.gz', 'nc').
#' @param startyear Numeric. Start year for data retrieval.
#' @param endyear Numeric. End year for data retrieval.
#' @param variable_dir Character or NA. Subdirectory for the variable, if applicable.
#' @param variable Character. Variable name (e.g., 'chirps').
#' @param dataset Character. Dataset identifier (currently unused).
#'
#' @import data.table
#'
#' @return Character vector of file URLs.
#' @export
create_chirps_paths <- function(base_url, format, startyear, endyear, variable_dir, variable, dataset) {
  
  # Ensure base_url has no trailing slash
  base_url <- remove_trailing_slash(path = base_url)
  
  # Generate daily file paths for TIF formats
  if (grepl("tif", format)) {
    file_paths <- data.table(date = seq.Date(from = as.Date(paste0(startyear, "-01-01")),
                                             to = as.Date(paste0(endyear, "-12-31")),
                                             by = "day"))[, year := format(date, "%Y")]
    
    if (!is.na(variable_dir)) {
      file_paths <- file_paths[, file_path := file.path(base_url, variable_dir, year,
                                                        paste0(variable, ".", gsub("-", ".", date), ".", format))
      ][, file_path]
    } else {
      file_paths <- file_paths[, file_path := file.path(base_url, year,
                                                        paste0(variable, ".", gsub("-", ".", date), ".", format))
      ][, file_path]
    }
  }
  
  # Generate yearly file paths for NetCDF formats
  if (grepl(".nc", format)) {
    if (is.na(variable_dir)) {
      file_paths <- file.path(base_url, paste0(variable, ".", startyear:endyear, ".", format))
    } else {
      file_paths <- file.path(base_url, variable_dir, paste0(variable, ".", startyear:endyear, ".", format))
    }
  }
  
  # Return paths or NA if not generated
  if (exists("file_paths")) {
    return(file_paths)
  } else {
    return(NA)
  }
}

#' Download CHIRPS Data
#'
#' Downloads CHIRPS data files filtered by specified criteria, with optional parallel processing and error handling.
#'
#' @param chirps_index Data.table. Index table containing available CHIRPS file URLs.
#' @param var Character. Variable to filter from index.
#' @param reg Character. Region to filter from index.
#' @param res Character. Spatial resolution.
#' @param type Character. File format (e.g., 'tif', 'nc').
#' @param startyear Numeric. Start year for downloading data.
#' @param endyear Numeric. End year for downloading data.
#' @param save_dir Character. Local directory for saving downloaded files.
#' @param worker_n Numeric. Number of parallel workers (1 for sequential download).
#' @param year_folder Logical. Whether to create separate year-based folders.
#'
#' @import data.table
#' @import future
#' @import future.apply
#'
#' @return Data.frame listing any failed downloads.
#' @export
download_chirps <- function(chirps_index, var, reg, res, type, startyear, endyear, save_dir, worker_n, year_folder) {
  # Filter URLs based on input parameters
  files <- unlist(chirps_index[variable == var & region == reg & resolution == res & format == type, file_paths])
  files <- files[grepl(paste(startyear:endyear, collapse = "|"), files)]
  
  # Define local save paths
  if (year_folder & type == "tif") {
    local_path <- file.path(save_dir, basename(dirname(files)), basename(files))
  } else {
    local_path <- file.path(save_dir, basename(files))
  }
  
  # Create necessary directories
  dirs <- unique(dirname(local_path))
  for (DIR in dirs) {
    if (!dir.exists(DIR)) {
      dir.create(DIR, recursive = TRUE)
    }
  }
  
  # Dataframe of URLs and local file paths
  files_df <- data.frame(
    url = files,
    local_path = local_path,
    stringsAsFactors = FALSE
  )
  
  # Check if files already exist
  files_df$f_exists <- file.exists(files_df$local_path)
  files_df[!files_df$f_exists, "f_exists"] <- file.exists(gsub(".tif.gz$", ".tif", files_df$local_path[!files_df$f_exists]))
  
  # Filter files needing download
  files_df <- files_df[!files_df$f_exists, ]
  
  # Helper function to download with retries
  attempt_download <- function(url, local_path) {
    attempts <- 3
    success <- FALSE
    for (i in 1:attempts) {
      try({
        download.file(url, local_path)
        success <- TRUE
        break
      }, silent = TRUE)
      if (!success && grepl(".tif.gz$", url)) {
        url <- gsub(".tif.gz$", ".tif", url)
      }
    }
    return(success)
  }
  
  # Perform downloads sequentially or in parallel
  if (worker_n == 1) {
    failed_downloads <- lapply(1:nrow(files_df), function(i) {
      cat("Downloading file", i, "/", nrow(files_df), "\n")
      success <- attempt_download(files_df$url[i], files_df$local_path[i])
      if (!success) return(files_df[i, ]) else return(NULL)
    })
  } else {
    future::plan(multisession, workers = worker_n)
    failed_downloads <- future.apply::future_lapply(1:nrow(files_df), function(i) {
      success <- attempt_download(files_df$url[i], files_df$local_path[i])
      if (!success) return(files_df[i, ]) else return(NULL)
    })
    future::plan(sequential)
  }
  
  # Combine and report failed downloads
  failed_downloads <- do.call(rbind, failed_downloads)
  return(failed_downloads)
}
