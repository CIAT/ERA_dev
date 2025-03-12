#' Process NASA POWER CSV Files
#'
#' This function processes a list of NASA POWER CSV files, combining them into a single
#' \code{data.table}, averaging values for sites with multiple grid cells, optionally computing
#' reference evapotranspiration (PET) via \code{\link{PETcalc}}, and adding date/day‐count fields.
#' It also merges in site altitude and allows renaming selected parameter columns.
#'
#' @param files Character vector of file paths to NASA POWER CSV files.
#' @param parameters Character vector of parameter names (columns) in the CSV files to be renamed.
#' @param rename Character vector of new names corresponding to \code{parameters}. Must be the
#'   same length as \code{parameters}.
#' @param id_field Character string specifying the name of the ID column in the output. Typically
#'   matches a column in \code{altitude_data} for merging.
#' @param add_date Logical. If \code{TRUE}, a \code{Date} column is added based on \code{Year} and
#'   \code{Day} (DOY). Defaults to \code{TRUE}.
#' @param add_daycount Logical. If \code{TRUE}, a \code{DayCount} column is added to indicate the
#'   number of days since \code{time_origin}. Defaults to \code{TRUE}.
#' @param time_origin A Date (or string convertible to Date) used as the origin for day counting.
#'   Only relevant if \code{add_daycount = TRUE}.
#' @param add_pet Logical. If \code{TRUE}, calculates reference evapotranspiration (PET) using
#'   \code{\link{PETcalc}} and adds it as a column \code{ETo}. Defaults to \code{TRUE}.
#' @param altitude_data A \code{data.frame} or \code{data.table} containing altitude information
#'   for each site. Must include a column with the same name as \code{id_field}.
#'
#' @details
#' \enumerate{
#'   \item Reads each file in \code{files} into a \code{data.table} along with its file basename
#'         (as \code{id}).
#'   \item Adjusts \code{id} to remove the prefix \code{"POWER "} and extract part of the filename
#'         as the site ID, also forcing multiple grid cells for the same site/date to merge.
#'   \item Averages parameters for sites that have more than one grid cell on a given \code{YEAR}‐\code{DOY}.
#'   \item Extracts approximate \code{Latitude} and \code{Longitude} from the \code{id} string.
#'   \item Merges in altitude data from \code{altitude_data}.
#'   \item Optionally calculates PET (via \code{\link{PETcalc}}) if \code{add_pet} is \code{TRUE}.
#'   \item Renames columns defined in \code{parameters} to the strings in \code{rename}.
#'   \item Optionally adds a \code{Date} column and/or a \code{DayCount} column.
#' }
#'
#' @return A \code{data.table} with combined and processed NASA POWER data, including any new
#'   fields for PET, \code{Date}, and \code{DayCount}.
#'
#' @import data.table
#' @import pbapply
#' @export
#'
#' @examples
#' \dontrun{
#' processed_data <- process_power(
#'   files       = list.files(path = "path_to_power_files", pattern = "\\.csv$", full.names = TRUE),
#'   parameters  = c("T2M_MIN", "T2M_MAX", "PRECTOT", "ALLSKY_SFC_SW_DWN"),
#'   rename      = c("Tmin", "Tmax", "Rain", "SolarRad"),
#'   id_field    = "SiteID",
#'   add_date    = TRUE,
#'   add_daycount= TRUE,
#'   time_origin = as.Date("1970-01-01"),
#'   add_pet     = TRUE,
#'   altitude_data = your_altitude_df
#' )
#' }
process_power <- function(files,
                          parameters,
                          rename,
                          id_field,
                          altitude_field,
                          add_date     = TRUE,
                          add_daycount = TRUE,
                          time_origin,
                          add_pet      = TRUE,
                          worker_n=1,
                          altitude_data) {
  
  # Inform the user that CSV files are being read
  cat("Loading power csv files", "\n")
  
  # Load data from all specified files, each stacked into one data.table.
  # Also attach an 'id' column using the base file name for later identification.
  data <- data.table::rbindlist(
    pbapply::pblapply(seq_along(files), function(i) {
      file <- files[i]
      id   <- basename(file)
      # Read the CSV into a data.table, associating this file's basename as 'id'.
      data.table::data.table(id = id, data.table::fread(file))
    }),
    use.names = TRUE, fill = TRUE
  )
  
  # Process the 'id' field:
  # 1) Replace ' -' with ' m' in the basename
  # 2) Remove the prefix 'POWER ' from the id
  # 3) Extract the site ID portion (split by '-') and re-insert '-'
  data[, id := gsub(" -", " m", basename(id[1])), by = id]
  data[, id := gsub(
    "m",
    "-",
    unlist(data.table::tstrsplit(gsub("POWER ", "", id[1]), "-", keep = 1))
  ), by = id]
  
  # Identify the parameter columns. Exclude the first three columns (id, plus presumably YEAR/DOY).
  param_cols <- colnames(data)[-(1:3)]
  
  # N counts how many rows per (id, YEAR, DOY) group. This will identify multiple cells for the same site/date.
  data[, N := .N, by = list(id, YEAR, DOY)]
  
  cat("Averaging data for sites with multiple cells, this may take some time.", "\n")
  
  # For sites with more than one cell in a given (id, YEAR, DOY), compute the mean for each parameter.
  data.table::setDTthreads(worker_n) 
  means <- data[N > 1, lapply(.SD, mean, na.rm = TRUE),
                .SDcols = param_cols,
                by = .(id, YEAR, DOY)]
  data.table::setDTthreads(1) 
  
  # Round these averaged values to 2 decimal places.
  means[, (param_cols) := lapply(.SD, round, digits = 2), .SDcols = param_cols]
  
  # Combine (rbind) rows for single-cell sites with the newly computed multi-cell means,
  # removing the 'N' column from single-cell rows.
  data <- data.table::rbindlist(
    list(data[N == 1][, N := NULL], means),
    use.names = TRUE, fill = TRUE
  )
  
  # Extract approximate Latitude and Longitude from the first part of the 'id' string.
  data[, Latitude  := as.numeric(unlist(data.table::tstrsplit(id[1], " ", keep = 1))), by = id]
  data[, Longitude := as.numeric(unlist(data.table::tstrsplit(id[1], " ", keep = 1))), by = id]
  
  # Rename columns: map 'id' to the user-specified id_field, 'DOY' to 'Day', 'YEAR' to 'Year',
  # and 'PRECTOTCORR' to 'PRECTOT'. Skip any that don't exist in the table.
  data.table::setnames(
    data,
    old = c("id", "DOY", "YEAR", "PRECTOTCORR"),
    new = c(id_field, "Day", "Year", "PRECTOT"),
    skip_absent = TRUE
  )
  
  # If requested, check that all required columns for PET calculation exist.
  if (add_pet) {
    required_pet_cols <- c("T2M_MIN", "T2M_MAX", "ALLSKY_SFC_SW_DWN", "WS2M", 
                           "PRECTOT", "PSC", "RH2M")
    missing_cols <- setdiff(required_pet_cols, names(data))
    if (length(missing_cols) > 0) {
      stop("When add_pet = TRUE, the following required columns are missing: ",
           paste(missing_cols, collapse = ", "))
    }
  
  # Merge the altitude data into the main table by the user-specified id_field.
  # 'all.x' ensures all rows in 'data' are preserved even if altitude_data is incomplete.
  setnames(altitude_data,altitude_field,"Altitude")
  data <- merge(data, altitude_data, by = id_field, all.x = TRUE)
  
    # If all are present, calculate PET
    data[, ETo := PETcalc(
      Tmin     = T2M_MIN,
      Tmax     = T2M_MAX,
      SRad     = ALLSKY_SFC_SW_DWN,
      Wind     = WS2M,
      Rain     = PRECTOT,
      Pressure = PSC,
      Humid    = RH2M,
      YearDay  = Day,
      Latitude = Latitude,
      Altitude = Altitude
    )[, 1]]
  }
  
  # Rename the parameter columns as specified by the user.
  # 'parameters' are old names, 'rename' are the new names.
  data.table::setnames(data, old = parameters, new = rename, skip_absent = TRUE)
  
  # If requested, add a Date column based on 'Year' and 'Day' (DOY).
  if (add_date) {
    data[, Date := as.Date(paste0(Year[1], "-", Day[1]), format = "%Y-%j"), 
         by = list(Day, Year)]
    
    # If requested, add a DayCount column (days since 'time_origin').
    if (add_daycount) {
      data[, DayCount := as.integer(floor(unclass(Date[1] - time_origin))), by = Date]
    }
    
  }
  

  return(data)
}
