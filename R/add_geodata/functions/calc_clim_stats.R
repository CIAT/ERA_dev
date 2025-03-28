#' Calculate Climate Statistics (Modular Wrapper)
#'
#' This function computes climate statistics on a per–site, per–observation basis.
#' It is designed to be fully modular so that each type of climate statistic is computed only if the corresponding parameters are supplied (i.e. non-NULL).
#'
#' The five optional statistic groups are:
#' \itemize{
#'   \item \strong{GDD statistics} – growing degree days; requires daily maximum and minimum temperatures and the crop thresholds (e.g. Tlow, Topt.low, Thigh, Topt.high) from the site record.
#'   \item \strong{Rainfall statistics} – rainfall totals, water balance, and sequence counts; requires daily rainfall and reference evapotranspiration (ETo).
#'   \item \strong{Temperature statistics} – summary and threshold‐based statistics for temperature; requires daily maximum, minimum, and mean temperatures.
#'   \item \strong{ERatio statistics} – statistics based on the ratio of actual to potential evapotranspiration; requires the daily ERATIO field.
#'   \item \strong{Waterlogging statistics} – counts and summary measures of waterlogging; requires daily LOGGING values and soil saturation (ssat).
#' }
#'
#' \strong{Minimum data requirements:} The site-level dataset (\code{data}) must include at least a \code{PlantingDate} field (in "yyyy-mm-dd" format or as Date objects) and the unique site identifier. These field names, along with the season length field, are provided in the \code{data_params} list. For each activated statistic, the required climate data fields must be present in the \code{climate} data.table (e.g. \code{Temp.Max} and \code{Temp.Min} for GDD).
#'
#' @param data A data.frame or data.table of site/product records. Must include the site identifier and planting start date, as well as a season length field.
#' @param climate A daily climate data.table containing a \code{Date} column and the necessary climate fields (e.g. \code{Temp.Max}, \code{Temp.Min}, \code{Rain}, etc.).
#' @param data_params A list of parameters specifying the site-level field names. This list must include:
#'   \itemize{
#'     \item \code{id_col}: The name of the unique site identifier column (e.g., "Site.Key").
#'     \item \code{append_cols}: The name of fields from the data object to appeneded to output tables.
#'     \item \code{plant_start_col}: The name of the planting start date field (e.g., "PlantingDate").
#'     \item \code{season_length_col}: The name of the season length field (e.g., "SeasonLength.Data").
#'   }
#' @param gdd_params (Optional) A list of parameters for GDD calculations. If non-NULL, the list must contain:
#'   \itemize{
#'     \item \code{t_max_col}: Name of the daily maximum temperature column in \code{climate} (e.g., "Temp.Max").
#'     \item \code{t_min_col}: Name of the daily minimum temperature column (e.g., "Temp.Min").
#'     \item \code{t_low_col}: Name of the base temperature field in \code{data} (e.g., "Tlow").
#'     \item \code{t_opt_low_col}: Name of the lower optimal temperature field (e.g., "Topt.low").
#'     \item \code{t_high_col}: Name of the high threshold temperature field (e.g., "Thigh").
#'     \item \code{t_opt_high_col}: Name of the upper optimal temperature field (e.g., "Topt.high").
#'     \item \code{sum_daily}: Logical; if TRUE, sum the daily GDD values in each GDD category to provide seasonal totals.
#'     \item \code{round_digits}: Number of decimal places for rounding the GDD values.
#'   }
#'   If \code{gdd_params} is \code{NULL}, GDD statistics will be skipped.
#' @param rainfall_params (Optional) A list of parameters for rainfall statistics. The list must include:
#'   \itemize{
#'     \item \code{rain_col}: Name of the daily rainfall column in \code{climate} (e.g., "Rain").
#'     \item \code{eto_col}: Name of the daily reference evapotranspiration column (e.g., "ETo").
#'     \item \code{threshold_dt}: A data.table specifying rainfall thresholds and directions.
#'     \item \code{r_seq_len}: An integer vector of sequence lengths (in days).
#'   }
#'   If \code{rainfall_params} is \code{NULL}, rainfall statistics will be skipped.
#' @param temp_params (Optional) A list of parameters for temperature statistics. The list must include:
#'   \itemize{
#'     \item \code{t_max_col}: Name of the daily maximum temperature column.
#'     \item \code{t_min_col}: Name of the daily minimum temperature column.
#'     \item \code{t_mean_col}: Name of the daily mean temperature column.
#'     \item \code{threshold_dt}: A data.table specifying temperature thresholds and directions.
#'     \item \code{t_seq_len}: An integer vector of sequence lengths (in days).
#'   }
#'   If \code{temp_params} is \code{NULL}, temperature statistics will be skipped.
#' @param eratio_params (Optional) A list of parameters for ERatio statistics. The list must include:
#'   \itemize{
#'     \item \code{eratio_col}: Name of the daily ERATIO column in \code{climate}.
#'     \item \code{thresholds}: A numeric vector of ERatio thresholds.
#'     \item \code{r_seq_len}: An integer vector of sequence lengths.
#'   }
#'   If \code{eratio_params} is \code{NULL}, ERatio statistics will be skipped.
#' @param logging_params (Optional) A list of parameters for waterlogging statistics. The list must include:
#'   \itemize{
#'     \item \code{logging_col}: Name of the daily LOGGING column.
#'     \item \code{ssat_col}: Name of the soil saturation column (ssat).
#'     \item \code{r_seq_len}: An integer vector of sequence lengths.
#'   }
#'   If \code{logging_params} is \code{NULL}, waterlogging statistics will be skipped.
#' @param verbose Logical. If TRUE (default), print messages showing which statistics are activated.
#' @return A list containing the calculated statistics for each site and observation. The list is organized by site, and within each site, the results for each observation are stored in a list.
#' @import data.table
#' @examples
#' \dontrun{
#' # Example: Calculate only GDD and rainfall statistics.
#' library(data.table)
#'
#' # Dummy site-level dataset.
#' site_data <- data.table(
#'   site_id = c("A", "A"),
#'   PlantingDate = as.Date(c("2020-04-01", "2020-04-15")),
#'   SeasonLength.Data = c(90, 95),
#'   Tlow = c(15, 15),
#'   Topt.low = c(18, 18),
#'   Thigh = c(30, 30),
#'   Topt.high = c(25, 25),
#'   EU = c("X", "X"),
#'   M.Year = c(2020, 2020),
#'   M.Season = c(1, 1)
#' )
#'
#' # Dummy climate dataset.
#' climate_data <- data.table(
#'   site_id = rep("A", 120),
#'   Date = seq(as.Date("2020-03-01"), by = "day", length.out = 120),
#'   Temp.Max = runif(120, 25, 35),
#'   Temp.Min = runif(120, 15, 25),
#'   Rain = runif(120, 0, 5),
#'   ETo = runif(120, 0, 5)
#' )
#'
#' # Define data_params and parameter lists for activated statistics.
#' data_params <- list(
#'   id_col = "site_id",
#'   plant_start_col = "PlantingDate",
#'   season_length_col = "SeasonLength.Data"
#' )
#'
#' gdd_params <- list(
#'   t_max_col = "Temp.Max",
#'   t_min_col = "Temp.Min",
#'   t_low_col = "Tlow",
#'   t_opt_low_col = "Topt.low",
#'   t_high_col = "Thigh",
#'   t_opt_high_col = "Topt.high",
#'   sum_daily = TRUE,
#'   round_digits = 2
#' )
#'
#' rainfall_params <- list(
#'   rain_col = "Rain",
#'   eto_col = "ETo",
#'   threshold_dt = data.table(threshold = c(1, 2), direction = c("lower", "higher")),
#'   r_seq_len = c(5, 10)
#' )
#'
#' # Call the wrapper with only GDD and rainfall activated.
#' results <- calc_clim_stats(
#'   data = site_data,
#'   climate = climate_data,
#'   data_params = data_params,
#'   gdd_params = gdd_params,
#'   rainfall_params = rainfall_params,
#'   temp_params = NULL,
#'   eratio_params = NULL,
#'   logging_params = NULL,
#'   verbose = TRUE
#' )
#' }
calc_clim_stats <- function(data,
                            climate,
                            data_params,
                            gdd_params = NULL,
                            rainfall_params = NULL,
                            temp_params = NULL,
                            eratio_params = NULL,
                            logging_params = NULL,
                            verbose = TRUE) {
  # data_params must include:
  #   id_col: name of the unique site identifier (e.g., "site_id")
  #   plant_start_col: name of the planting start date field (e.g., "PlantingDate")
  #   season_length_col: name of the season length field (e.g., "SeasonLength.Data")
  required_data <- c("id_col", "plant_start_col", "season_length_col","append_cols")
  missing_data <- setdiff(required_data, names(data_params))
  if (length(missing_data) > 0) {
    stop("Missing fields in data_params: ", paste(missing_data, collapse = ", "))
  }
  
  # Check that the required columns exist in the site-level data.
  if (!(data_params$plant_start_col %in% names(data))) {
    stop(paste("The 'data' dataset must include a planting start column named", data_params$plant_start_col))
  }
  if (!(data_params$season_length_col %in% names(data))) {
    stop(paste("The 'data' dataset must include a season length column named", data_params$season_length_col))
  }
  if (!(data_params$id_col %in% names(data))) {
    stop(paste("The 'data' dataset must include the id column:", data_params$id_col))
  }
  if (!("Date" %in% names(climate))) {
    stop("The 'climate' dataset must include a 'Date' column.")
  }
  
  # Validate parameter lists for each activated calculation.
  if (!is.null(gdd_params)) {
    required <- c("t_max_col", "t_min_col", "t_low_col", "t_opt_low_col", "t_high_col", "t_opt_high_col", "round_digits", "sum_daily")
    missing_params <- setdiff(required, names(gdd_params))
    if (length(missing_params) > 0) {
      stop("Missing parameters in gdd_params: ", paste(missing_params, collapse = ", "))
    }
  }
  if (!is.null(rainfall_params)) {
    required <- c("rain_col", "eto_col", "threshold_dt", "r_seq_len")
    missing_params <- setdiff(required, names(rainfall_params))
    if (length(missing_params) > 0) {
      stop("Missing parameters in rainfall_params: ", paste(missing_params, collapse = ", "))
    }
  }
  if (!is.null(temp_params)) {
    required <- c("t_max_col", "t_min_col", "t_mean_col", "threshold_dt", "t_seq_len")
    missing_params <- setdiff(required, names(temp_params))
    if (length(missing_params) > 0) {
      stop("Missing parameters in temp_params: ", paste(missing_params, collapse = ", "))
    }
  }
  if (!is.null(eratio_params)) {
    required <- c("eratio_col", "thresholds", "r_seq_len")
    missing_params <- setdiff(required, names(eratio_params))
    if (length(missing_params) > 0) {
      stop("Missing parameters in eratio_params: ", paste(missing_params, collapse = ", "))
    }
  }
  if (!is.null(logging_params)) {
    required <- c("logging_col", "ssat_col", "r_seq_len")
    missing_params <- setdiff(required, names(logging_params))
    if (length(missing_params) > 0) {
      stop("Missing parameters in logging_params: ", paste(missing_params, collapse = ", "))
    }
  }
  
  if (verbose) {
    message("Calculating the following statistics:")
    if (!is.null(gdd_params)) message("- GDD statistics")
    if (!is.null(rainfall_params)) message("- Rainfall statistics")
    if (!is.null(temp_params)) message("- Temperature statistics")
    if (!is.null(eratio_params)) message("- ERatio statistics")
    if (!is.null(logging_params)) message("- Waterlogging statistics")
  }
  
  sites <- unique(data[[data_params$id_col]])
  results_list <- list()
  
  data[,index:=1:.N]
  
  for (j in 1:length(sites)) {
    site<-sites[j]
    site_data <- data[data[[data_params$id_col]] == site, ]
    site_climate <- climate[climate[[data_params$id_col]] == site, ]
    
    if (nrow(site_data) == 0) next
    if (nrow(site_climate) == 0) {
      warning("No climate data found for site: ", site, ". Skipping.")
      next
    }
    
    obs_results <- list()
    for (i in 1:nrow(site_data)) {
      
      obs <- site_data[i, ]
      index <- site_data[i,index]
      if(!is.null(data_params$append_cols)){
        a_cols<-site_data[i,data_params$append_cols,with=F]
      }
      
      if(verbose){
        cat("Processing site j =",j,"/",length(sites),", row index =",index,"( i = ",i,")         \r")
      }
      
      
      if (!inherits(obs[[data_params$plant_start_col]], "Date")) {
        obs[[data_params$plant_start_col]] <- as.Date(obs[[data_params$plant_start_col]])
      }
      
      if (!(data_params$season_length_col %in% names(obs))) {
        warning("Observation missing season length for site ", site, " observation ", i, ". Skipping observation.")
        next
      }
      window_start <- obs[[data_params$plant_start_col]]
      window_end <- obs[[data_params$plant_start_col]] + obs[[data_params$season_length_col]]
      
      obs_climate <- site_climate[site_climate$Date >= window_start & site_climate$Date <= window_end, ]
      if (nrow(obs_climate) == 0) {
        warning("No climate data in the window for site ", site, " observation ", i, ". Skipping observation.")
        next
      }
      
      obs_stat <- list()
      
      # --- GDD Statistics ---
      if (!is.null(gdd_params)) {
        required_site_cols <- c(gdd_params$t_low_col, gdd_params$t_opt_low_col, gdd_params$t_high_col, gdd_params$t_opt_high_col)
        missing_site <- setdiff(required_site_cols, names(obs))
        if (length(missing_site) > 0) {
          warning("Missing site columns for GDD calculation for site ", site, " observation ", i, ". Skipping GDD.")
        } else if (!all(c(gdd_params$t_max_col, gdd_params$t_min_col) %in% names(obs_climate))) {
          warning("Missing climate columns for GDD calculation for site ", site, " observation ", i, ". Skipping GDD.")
        } else {
          gdd_stat <- calc_gdd(
            t_max = obs_climate[[gdd_params$t_max_col]],
            t_min = obs_climate[[gdd_params$t_min_col]],
            t_low = obs[[gdd_params$t_low_col]],
            t_opt_low = obs[[gdd_params$t_opt_low_col]],
            t_high = obs[[gdd_params$t_high_col]],
            t_opt_high = obs[[gdd_params$t_opt_high_col]],
            round_digits = gdd_params$round_digits,
            sum_daily=gdd_params$sum_daily
          )
          gdd_stat$row_index<-index
          if(!is.null(data_params$append_cols)){
            gdd_stat<-cbind(gdd_stat,a_cols)
          }
          obs_stat$gdd <- gdd_stat
        }
      }
      
      # --- Rainfall Statistics ---
      if (!is.null(rainfall_params)) {
        if (!all(c(rainfall_params$rain_col, rainfall_params$eto_col) %in% names(obs_climate))) {
          warning("Missing climate columns for Rainfall calculation for site ", site, " observation ", i, ". Skipping Rainfall.")
        } else {
          rain_stat <- calc_rainfall_statistics(
            rain = obs_climate[[rainfall_params$rain_col]],
            eto = obs_climate[[rainfall_params$eto_col]],
            threshold_dt = rainfall_params$threshold_dt,
            r_seq_len = rainfall_params$r_seq_len
          )
          rain_stat$row_index<-index
          if(!is.null(data_params$append_cols)){
            rain_stat<-cbind(rain_stat,a_cols)
          }
          obs_stat$rainfall <- rain_stat
        }
      }
      
      # --- Temperature Statistics ---
      if (!is.null(temp_params)) {
        if (!all(c(temp_params$t_max_col, temp_params$t_min_col, temp_params$t_mean_col) %in% names(obs_climate))) {
          warning("Missing climate columns for Temperature calculation for site ", site, " observation ", i, ". Skipping Temperature.")
        } else {
          temp_stat <- calc_temperature_statistics(
            t_max = obs_climate[[temp_params$t_max_col]],
            t_min = obs_climate[[temp_params$t_min_col]],
            t_mean = obs_climate[[temp_params$t_mean_col]],
            threshold_dt = temp_params$threshold_dt,
            t_seq_len = temp_params$t_seq_len
          )
          temp_stat$row_index<-index
          if(!is.null(data_params$append_cols)){
            temp_stat<-cbind(temp_stat,a_cols)
          }         
          obs_stat$temperature <- temp_stat
        }
      }
      
      # --- ERatio Statistics ---
      if (!is.null(eratio_params)) {
        if (!(eratio_params$eratio_col %in% names(obs_climate))) {
          warning("Missing climate column for ERatio calculation for site ", site, " observation ", i, ". Skipping ERatio.")
        } else {
          eratio_stat <- calc_eratio_statistics(
            eratio = obs_climate[[eratio_params$eratio_col]],
            thresholds = eratio_params$thresholds,
            r_seq_len = eratio_params$r_seq_len
          )
          eratio_stat$row_index<-index
          if(!is.null(data_params$append_cols)){
            eratio_stat<-cbind(eratio_stat,a_cols)
          }         
          obs_stat$eratio <- eratio_stat
        }
      }
      
      # --- Waterlogging Statistics ---
      if (!is.null(logging_params)) {
        if (!all(c(logging_params$logging_col, logging_params$ssat_col) %in% names(obs_climate))) {
          warning("Missing climate columns for Logging calculation for site ", site, " observation ", i, ". Skipping Logging.")
        } else {
          logging_stat <- calc_logging_statistics(
            logging = obs_climate[[logging_params$logging_col]],
            ssat = obs_climate[[logging_params$ssat_col]][1],
            r_seq_len = logging_params$r_seq_len
          )
          logging_stat$row_index<-index
          if(!is.null(data_params$append_cols)){
            logging_stat<-cbind(logging_stat,a_cols)
          }         
          obs_stat$logging <- logging_stat
        }
      }
      
      obs_results[[index]] <- obs_stat
    }
    
    obs_consolidated<-list()
    if (!is.null(gdd_params)) {
      stat<-rbindlist(lapply(obs_results,"[[","gdd"))
      if(nrow(stat)>0){stat$id<-site}
      obs_consolidated[["gdd"]]<-stat
    }
    
    if (!is.null(temp_params)) {
      stat<-rbindlist(lapply(obs_results,"[[","temperature"))
      if(nrow(stat)>0){stat$id<-site}
      obs_consolidated[["temperature"]]<-stat
    }
    
    if (!is.null(rainfall_params)) {
      stat<-rbindlist(lapply(obs_results,"[[","rainfall"))
      if(nrow(stat)>0){stat$id<-site}
      obs_consolidated[["rainfall"]]<-stat
    }
    
    if (!is.null(eratio_params)) {
      stat<-rbindlist(lapply(obs_results,"[[","eratio"))
      if(nrow(stat)>0){stat$id<-site}
      obs_consolidated[["eratio"]]<-stat
    }
    
    if (!is.null(logging_params)) {
      stat<-rbindlist(lapply(obs_results,"[[","logging"))
      if(nrow(stat)>0){stat$id<-site}
      obs_consolidated[["logging"]]<-stat
    }
    
    results_list[[site]] <- obs_consolidated
  }
  
  results_consolidated<-list()
  
  window<-paste0(data_params$plant_start_col,"-",data_params$season_length_col)
  
  if (!is.null(gdd_params)) {
    stat<-rbindlist(lapply(results_list,"[[","gdd"))
    stat$window<-window
    setnames(stat,"id",data_params$id_col)
    results_consolidated[["gdd"]]<-stat
  }
  
  if (!is.null(temp_params)) {
    stat<-rbindlist(lapply(results_list,"[[","temperature"))
    stat$window<-window
    setnames(stat,"id",data_params$id_col)
    results_consolidated[["temperature"]]<-stat
  }
  
  if (!is.null(rainfall_params)) {
    stat<-rbindlist(lapply(results_list,"[[","rainfall"))
    stat$window<-window
    setnames(stat,"id",data_params$id_col)
    results_consolidated[["rainfall"]]<-stat
  }
  
  if (!is.null(eratio_params)) {
    stat<-rbindlist(lapply(results_list,"[[","eratio"))
    stat$window<-window
    setnames(stat,"id",data_params$id_col)
    results_consolidated[["eratio"]]<-stat
  }
  
  if (!is.null(logging_params)) {
    stat<-rbindlist(lapply(results_list,"[[","logging"))
    stat$window<-window
    setnames(stat,"id",data_params$id_col)
    results_consolidated[["logging"]]<-stat
  }
  
  if (verbose) {
    message("Finished calculating modular climate statistics.")
  }
  
  return(results_consolidated)
}
