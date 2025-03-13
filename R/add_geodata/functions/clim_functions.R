###############################################################################
#' Adjust Hourly Temperatures Based on Thresholds
#'
#' This function adjusts a numeric vector of hourly temperatures using the
#' provided base and optimal thresholds. Values below the base are set to 0,
#' values between the base and optimal have the base subtracted, and values
#' above the optimal are capped at the difference (optimal - base).
#'
#' @param x Numeric vector of hourly temperatures.
#' @param base_temp Numeric base temperature.
#' @param optimal_temp Numeric optimal temperature.
#' @return A numeric vector of adjusted temperature values.
#' @importFrom base as.numeric
#' @examples
#' # Example usage:
#' temps <- c(16, 18, 20, 22, 24)
#' adjust_temperature(temps, base_temp = 18, optimal_temp = 22)
#' @export
adjust_temperature <- function(x, base_temp, optimal_temp) {
  if (!is.numeric(x)) {
    stop("`x` must be a numeric vector.")
  }
  if (!is.numeric(base_temp) || !is.numeric(optimal_temp)) {
    stop("`base_temp` and `optimal_temp` must be numeric values.")
  }
  # For temperatures below the base, no degree day is accumulated.
  x[x < base_temp] <- 0
  # For temperatures between base and optimal, subtract the base.
  x[x >= base_temp & x <= optimal_temp] <- x[x >= base_temp & x <= optimal_temp] - base_temp
  # For temperatures above optimal, cap the value.
  x[x > optimal_temp] <- optimal_temp - base_temp
  return(x)
}

###############################################################################
#' Calculate Daily Growing Degree Days (GDD) via Hourly Interpolation
#'
#' This function estimates daily GDD by interpolating hourly temperatures from
#' daily maximum and minimum values using a sine curve. The interpolated hourly
#' values are then adjusted using the provided base and optimal thresholds.
#'
#' @param temp_df A data frame containing columns `t_max` and `t_min` plus additional
#'   columns representing a sine curve for each hour.
#' @param base_temp Numeric base temperature for the calculation.
#' @param optimal_temp Numeric optimal temperature for the calculation.
#' @param sine_curve Numeric vector representing the 24-hour sine curve.
#' @param round_digits Integer number of decimal places for rounding.
#' @return Numeric vector of daily GDD values.
#' @importFrom base apply sapply round
#' @examples
#' # Example usage:
#' t_max <- c(30, 32)
#' t_min <- c(15, 16)
#' sine_curve <- sin(seq(1.5 * pi, 3.5 * pi, length.out = 24))
#' temp_df <- data.frame(t_max = t_max, t_min = t_min,
#'   matrix(rep(sine_curve, each = 2), ncol = 24))
#' calc_daily_gdd(temp_df, base_temp = 18, optimal_temp = 22, sine_curve = sine_curve)
#' @export
calc_daily_gdd <- function(temp_df, base_temp, optimal_temp, sine_curve, round_digits = 2) {
  if (!is.data.frame(temp_df)) {
    stop("`temp_df` must be a data frame.")
  }
  if (!all(c("t_max", "t_min") %in% names(temp_df))) {
    stop("`temp_df` must contain columns 't_max' and 't_min'.")
  }
  if (!is.numeric(sine_curve) || length(sine_curve) != 24) {
    stop("`sine_curve` must be a numeric vector of length 24.")
  }
  if (!is.numeric(base_temp) || !is.numeric(optimal_temp)) {
    stop("`base_temp` and `optimal_temp` must be numeric values.")
  }

  # Interpolate hourly temperatures using the sine curve.
  temp_df[, 3:26] <- temp_df[, 3:26] * (temp_df$t_max - temp_df$t_min) / (max(sine_curve) - min(sine_curve))

  # Adjust the hourly estimates so that the maximum equals t_max.
  temp_df[, 3:26] <- temp_df[, 3:26] + (temp_df$t_max - apply(temp_df[, 3:26], 1, max))

  # Adjust the interpolated hourly temperatures using the thresholds.
  temp_df[, 3:26] <- sapply(temp_df[, 3:26], adjust_temperature,
                            base_temp = base_temp, optimal_temp = optimal_temp)

  # Compute the daily GDD as the mean of the adjusted hourly values.
  daily_gdd <- round(apply(temp_df[, 3:26], 1, mean), round_digits)
  return(daily_gdd)
}

###############################################################################
#' Count Consecutive Days Meeting a Condition
#'
#' This function uses run-length encoding to count consecutive days in a numeric
#' vector where a specified condition is met. The condition is defined based on a
#' threshold and a direction ("lower" for values below the threshold, "higher" for values above).
#'
#' @param data Numeric vector representing daily values.
#' @param threshold Numeric threshold.
#' @param fun Function to apply to the run lengths (e.g., \code{max}, \code{sum}).
#' @param direction Character, either "lower" or "higher".
#' @return A single numeric value (the result of applying \code{fun} to the run lengths).
#' @importFrom base rle as.character
#' @examples
#' # Example usage:
#' temps <- c(18, 19, 21, 17, 16, 15, 22)
#' count_consecutive_helper(temps, threshold = 20, fun = max, direction = "lower")
#' @export
count_consecutive_helper <- function(data, threshold = 0, fun = max, direction = "lower") {
  if (!is.numeric(data)) {
    stop("`data` must be a numeric vector.")
  }
  if (!is.numeric(threshold)) {
    stop("`threshold` must be a numeric value.")
  }
  if (!direction %in% c("lower", "higher")) {
    stop("`direction` must be either 'lower' or 'higher'.")
  }

  if (direction == "lower") {
    data[data < threshold] <- 9999
  } else {
    data[data > threshold] <- 9999
  }
  rle_result <- rle(as.character(data))
  lengths <- rle_result$lengths[rle_result$values == "9999"]
  if (length(lengths) > 0) return(fun(lengths)) else return(0)
}

###############################################################################
#' Estimate Planting Date from Rainfall Data
#'
#' This function calculates an estimated planting date by scanning through sequential
#' rainfall windows. For each window, it computes a rolling sum and returns the first day
#' when the sum exceeds the specified threshold.
#'
#' @param rain Numeric vector of daily rainfall (mm). Should not contain NA values.
#' @param date Vector of dates corresponding to the rainfall data. Must be in "yyyy-mm-dd"
#'   format or Date objects.
#' @param widths Integer vector defining the rolling window widths (in days) for each window.
#' @param rain_window_threshold Numeric vector of rainfall thresholds for each window.
#' @param rain_windows Integer vector defining the total days for each window.
#' @return A Date object representing the estimated planting date, or NA if no window meets the threshold.
#' @importFrom zoo rollapply as.zoo
#' @examples
#' # Example usage:
#' rain <- rep(1, 50)
#' date <- as.Date("2020-04-01") + 0:49
#' widths <- c(3, 3, 2, 2)
#' thresholds <- c(3, 3, 2, 2)
#' windows <- c(10, 10, 10, 10)
#' calc_estimated_rain_date(rain, date, widths, thresholds, windows)
#' @export
calc_estimated_rain_date <- function(rain, date, widths, rain_window_threshold, rain_windows) {
  if (!is.numeric(rain)) {
    stop("`rain` must be a numeric vector.")
  }
  if (length(rain) != length(date)) {
    stop("`rain` and `date` must be of the same length.")
  }
  if (!inherits(date, "Date")) {
    warning("`date` is not of class Date. Attempting to convert using as.Date(...). Ensure format is yyyy-mm-dd.")
    date <- as.Date(date)
    if (any(is.na(date))) {
      stop("Conversion of `date` to Date resulted in NA. Please check the date format.")
    }
  }
  if (any(is.na(rain))) {
    warning("`rain` contains NA values. They will be removed before calculation.")
    valid_idx <- !is.na(rain)
    rain <- rain[valid_idx]
    date <- date[valid_idx]
  }

  base_date <- date[1]

  for (i in seq_along(rain_windows)) {
    if (i == 1) {
      window_indices <- 1:rain_windows[1]
    } else {
      start_index <- sum(rain_windows[1:(i - 1)]) + 1
      end_index <- sum(rain_windows[1:i])
      window_indices <- start_index:end_index
    }

    if (length(rain) < max(window_indices)) {
      warning("Not enough rain data to cover window ", i)
      next
    }

    roll_sums <- zoo::rollapply(zoo::as.zoo(rain[window_indices]),
                                width = widths[i], sum)
    exceed_idx <- which(roll_sums > rain_window_threshold[i])
    if (length(exceed_idx) > 0) {
      estimated_date <- base_date + exceed_idx[1]
      return(estimated_date)
    }
  }

  warning("No window met the rainfall threshold. Returning NA for estimated planting date.")
  return(NA)
}

###############################################################################
#' Calculate Growing Degree Days (GDD)
#'
#' This function calculates four measures of growing degree days (GDD) based on
#' daily maximum and minimum temperatures. It interpolates hourly temperatures using
#' a sine curve and applies threshold adjustments to compute:
#' \itemize{
#'   \item \code{gdd_low}: below optimal,
#'   \item \code{gdd_opt}: within optimal range,
#'   \item \code{gdd_high}: above optimal (but below high threshold),
#'   \item \code{gdd_max}: above high threshold.
#' }
#'
#' @param t_max Numeric vector of daily maximum temperatures (°C).
#' @param t_min Numeric vector of daily minimum temperatures (°C).
#' @param t_low Numeric base temperature.
#' @param t_opt_low Numeric lower optimal temperature.
#' @param t_high Numeric high threshold temperature.
#' @param t_opt_high Numeric upper optimal temperature.
#' @param round_digits Integer number of decimal places for rounding.
#' @return A data.table with columns: \code{gdd_low}, \code{gdd_opt}, \code{gdd_high}, and \code{gdd_max}.
#' @import data.table
#' @importFrom base rep seq
#' @examples
#' # Example usage:
#' t_max <- c(30, 32, 31)
#' t_min <- c(15, 16, 15)
#' calc_gdd(t_max, t_min, t_low = 18, t_opt_low = 20, t_high = 25, t_opt_high = 23)
#' @export
calc_gdd <- function(t_max, t_min, t_low, t_opt_low, t_high, t_opt_high, round_digits = 2) {
  if (!is.numeric(t_max) || !is.numeric(t_min)) {
    stop("`t_max` and `t_min` must be numeric vectors.")
  }
  if (length(t_max) != length(t_min)) {
    stop("`t_max` and `t_min` must be of equal length.")
  }

  sine_curve <- sin(seq(1.5 * pi, 3.5 * pi, length.out = 24))

  temp_df <- data.frame(t_max = t_max, t_min = t_min,
                        matrix(rep(sine_curve, each = length(t_min)), ncol = 24))

  gdd_low  <- calc_daily_gdd(temp_df, base_temp = t_low, optimal_temp = t_opt_low,
                             sine_curve = sine_curve, round_digits = round_digits)
  gdd_opt  <- calc_daily_gdd(temp_df, base_temp = t_opt_low, optimal_temp = t_opt_high,
                             sine_curve = sine_curve, round_digits = round_digits)
  gdd_high <- calc_daily_gdd(temp_df, base_temp = t_opt_high, optimal_temp = t_high,
                             sine_curve = sine_curve, round_digits = round_digits)
  gdd_max  <- calc_daily_gdd(temp_df, base_temp = t_high, optimal_temp = 1e6,
                             sine_curve = sine_curve, round_digits = round_digits)

  return(data.table(gdd_low = gdd_low, gdd_opt = gdd_opt, gdd_high = gdd_high, gdd_max = gdd_max))
}

###############################################################################
#' Calculate Rainfall and Evapotranspiration Statistics
#'
#' This function computes summary statistics for daily rainfall and reference
#' evapotranspiration (ETo). It returns overall totals (e.g., total rainfall,
#' water balance) and, for each defined threshold, counts the number of days and
#' sequences of days that exceed (or fall below) the threshold.
#'
#' @param rain Numeric vector of daily rainfall (mm).
#' @param eto Numeric vector of daily reference evapotranspiration (mm).
#' @param threshold_dt A data.table with columns \code{threshold} (numeric) and
#'   \code{direction} (character: "higher" or "lower").
#' @param r_seq_len Integer vector of sequence lengths (days).
#' @return A data.table of rainfall statistics.
#' @import data.table
#' @examples
#' # Example usage:
#' rain <- c(0.5, 2, 0, 3, 1, 0, 2)
#' eto <- c(0.2, 0.5, 0.1, 0.8, 0.3, 0.1, 0.6)
#' thresh_dt <- data.table(threshold = c(1, 2), direction = c("lower", "higher"))
#' calc_rainfall_statistics(rain, eto, thresh_dt, r_seq_len = c(2, 3))
#' @export
calc_rainfall_statistics <- function(rain, eto, threshold_dt, r_seq_len) {
  if (!is.numeric(rain) || !is.numeric(eto)) {
    stop("`rain` and `eto` must be numeric vectors.")
  }
  if (!("threshold" %in% names(threshold_dt)) || !("direction" %in% names(threshold_dt))) {
    stop("`threshold_dt` must contain columns 'threshold' and 'direction'.")
  }

  stats_list <- lapply(1:nrow(threshold_dt), function(i) {
    thresh <- threshold_dt[i, threshold]
    dir <- threshold_dt[i, direction]
    if (dir == "higher") {
      col_prefix <- paste0("rain_g_", thresh)
      seq_counts <- sapply(r_seq_len, function(j) {
        count_consecutive_helper(rain, threshold = thresh, fun = function(x) sum(x > j), direction = "higher")
      })
      temp_dt <- data.table(
        days = round(sum(rain > thresh), 2),
        days_pr = round(sum(rain > thresh) / length(rain), 2),
        max_seq = count_consecutive_helper(rain, threshold = thresh, fun = max, direction = "higher")
      )
      seq_dt <- data.table(t(seq_counts))
      setnames(seq_dt, paste0("n_seq_d", r_seq_len))
      temp_dt <- cbind(temp_dt, seq_dt)
      setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
      return(temp_dt)
    } else {
      col_prefix <- paste0("rain_l_", thresh)
      seq_counts <- sapply(r_seq_len, function(j) {
        count_consecutive_helper(rain, threshold = thresh, fun = function(x) sum(x > j), direction = "lower")
      })
      temp_dt <- data.table(
        days = round(sum(rain < thresh), 2),
        days_pr = round(sum(rain < thresh) / length(rain), 2),
        max_seq = count_consecutive_helper(rain, threshold = thresh, fun = max, direction = "lower")
      )
      seq_dt <- data.table(t(seq_counts))
      setnames(seq_dt, paste0("n_seq_d", r_seq_len))
      temp_dt <- cbind(temp_dt, seq_dt)
      setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
      return(temp_dt)
    }
  })

  stats_dt <- do.call("cbind", stats_list)
  overall_dt <- data.table(
    rain_sum = sum(rain),
    eto_sum = sum(eto),
    eto_na = sum(is.na(eto)),
    w_balance = sum(rain) - sum(eto),
    w_balance_negdays = sum((rain - eto) < 0)
  )
  result_dt <- cbind(overall_dt, stats_dt)
  result_dt[, lapply(.SD, as.numeric)]
}

###############################################################################
#' Calculate ERatio (Actual/Potential ET) Statistics
#'
#' This function computes summary statistics for the daily ratio of actual to
#' potential evapotranspiration (ERatio). For each specified threshold, it calculates
#' the number of days and the maximum length of consecutive days where ERatio falls below that threshold.
#'
#' @param eratio Numeric vector of daily ERatio values.
#' @param thresholds Numeric vector of threshold values.
#' @param r_seq_len Integer vector of sequence lengths (days).
#' @return A data.table of ERatio statistics.
#' @import data.table
#' @examples
#' # Example usage:
#' eratio <- c(0.6, 0.4, 0.5, 0.3, 0.7)
#' calc_eratio_statistics(eratio, thresholds = c(0.5, 0.4), r_seq_len = c(2, 3))
#' @export
calc_eratio_statistics <- function(eratio, thresholds, r_seq_len) {
  if (!is.numeric(eratio)) {
    stop("`eratio` must be a numeric vector.")
  }
  if (!is.numeric(thresholds)) {
    stop("`thresholds` must be a numeric vector.")
  }

  stats_list <- lapply(1:length(thresholds), function(i) {
    thresh <- thresholds[i]
    col_prefix <- paste0("eratio_l_", thresh)
    seq_counts <- sapply(r_seq_len, function(j) {
      count_consecutive_helper(eratio, threshold = thresh, fun = function(x) sum(x > j))
    })
    temp_dt <- data.table(
      days = round(sum(eratio < thresh), 2),
      days_pr = round(sum(eratio < thresh) / length(eratio), 2),
      max_seq = count_consecutive_helper(eratio, threshold = thresh, fun = max)
    )
    seq_dt <- data.table(t(seq_counts))
    setnames(seq_dt, paste0("n_seq_d", r_seq_len))
    temp_dt <- cbind(temp_dt, seq_dt)
    setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
    return(temp_dt)
  })

  stats_dt <- do.call("cbind", stats_list)
  overall_dt <- data.table(
    eratio_mean = mean(eratio, na.rm = TRUE),
    eratio_median = as.numeric(median(eratio, na.rm = TRUE)),
    eratio_min = min(eratio, na.rm = TRUE)
  )
  result_dt <- cbind(overall_dt, stats_dt)
  result_dt[, lapply(.SD, as.numeric)]
}

###############################################################################
#' Calculate Waterlogging (Logging) Statistics
#'
#' Computes summary statistics for daily waterlogging data given a soil saturation value.
#' It calculates totals, means, and counts the number of days (and sequences) where
#' waterlogging exceeds defined thresholds.
#'
#' @param logging Numeric vector of daily waterlogging values (mm).
#' @param ssat Numeric soil saturation value (mm).
#' @param r_seq_len Integer vector of sequence lengths (days).
#' @return A data.table of waterlogging statistics.
#' @import data.table
#' @examples
#' # Example usage:
#' logging <- c(0, 5, 10, 15, 5, 0, 20)
#' calc_logging_statistics(logging, ssat = 20, r_seq_len = c(2, 3))
#' @export
calc_logging_statistics <- function(logging, ssat, r_seq_len) {
  if (!is.numeric(logging)) {
    stop("`logging` must be a numeric vector.")
  }
  if (!is.numeric(ssat) || length(ssat) != 1) {
    stop("`ssat` must be a single numeric value.")
  }

  thresholds <- c(0, ssat * 0.5, ssat * 0.9)
  threshold_names <- c("0", "ssat_0.5", "ssat_0.9")

  stats_list <- lapply(1:length(thresholds), function(i) {
    thresh <- thresholds[i]
    col_prefix <- paste0("logging_g_", threshold_names[i])
    seq_counts <- sapply(r_seq_len, function(j) {
      count_consecutive_helper(logging, threshold = thresh, fun = function(x) sum(x > j))
    })
    temp_dt <- data.table(
      days = round(sum(logging > thresh, na.rm = TRUE), 2),
      days_pr = round(sum(logging > thresh, na.rm = TRUE) / length(logging), 2),
      max_seq = count_consecutive_helper(logging, threshold = thresh, fun = max)
    )
    seq_dt <- data.table(t(seq_counts))
    setnames(seq_dt, paste0("n_seq_d", r_seq_len))
    temp_dt <- cbind(temp_dt, seq_dt)
    setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
    return(temp_dt)
  })

  stats_dt <- do.call("cbind", stats_list)
  overall_dt <- data.table(
    logging_sum = sum(logging, na.rm = TRUE),
    logging_mean = mean(logging, na.rm = TRUE),
    logging_median = median(logging, na.rm = TRUE),
    logging_present_mean = if (any(logging > 0, na.rm = TRUE)) mean(logging[logging > 0], na.rm = TRUE) else 0
  )
  result_dt <- cbind(overall_dt, stats_dt)
  result_dt[, lapply(.SD, as.numeric)]
}

###############################################################################
#' Calculate Temperature Statistics
#'
#' Computes summary statistics for daily maximum, minimum, and mean temperatures.
#' It also counts the number of days (and sequences of days) where temperatures exceed
#' or fall below specified thresholds.
#'
#' @param t_max Numeric vector of daily maximum temperatures (°C).
#' @param t_min Numeric vector of daily minimum temperatures (°C).
#' @param t_mean Numeric vector of daily mean temperatures (°C).
#' @param threshold_dt A data.table with columns \code{threshold} (numeric) and
#'   \code{direction} (character: "higher" or "lower").
#' @param t_seq_len Integer vector of sequence lengths (days).
#' @return A data.table of temperature statistics.
#' @import data.table
#' @examples
#' # Example usage:
#' t_max <- c(30, 32, 31)
#' t_min <- c(15, 16, 15)
#' t_mean <- (t_max + t_min) / 2
#' thresh_dt <- data.table(threshold = c(20, 30), direction = c("lower", "higher"))
#' calc_temperature_statistics(t_max, t_min, t_mean, thresh_dt, t_seq_len = c(2, 3))
#' @export
calc_temperature_statistics <- function(t_max, t_min, t_mean, threshold_dt, t_seq_len) {
  if (!is.numeric(t_max) || !is.numeric(t_min) || !is.numeric(t_mean)) {
    stop("`t_max`, `t_min`, and `t_mean` must be numeric vectors.")
  }
  if (length(t_max) != length(t_min) || length(t_max) != length(t_mean)) {
    stop("`t_max`, `t_min`, and `t_mean` must be of the same length.")
  }
  if (!("threshold" %in% names(threshold_dt)) || !("direction" %in% names(threshold_dt))) {
    stop("`threshold_dt` must have columns 'threshold' and 'direction'.")
  }

  stats_list <- lapply(1:nrow(threshold_dt), function(i) {
    thresh <- threshold_dt[i, threshold]
    dir <- threshold_dt[i, direction]
    if (dir == "higher") {
      col_prefix <- paste0("tmax_tg_", thresh)
      seq_counts <- sapply(t_seq_len, function(j) {
        count_consecutive_helper(t_max, threshold = thresh, fun = function(x) sum(x > j), direction = "higher")
      })
      temp_dt <- data.table(
        days = round(sum(t_max > thresh), 2),
        days_pr = round(sum(t_max > thresh) / length(t_max), 2),
        max_rseq = count_consecutive_helper(t_max, threshold = thresh, fun = max, direction = "higher")
      )
      seq_dt <- data.table(t(seq_counts))
      setnames(seq_dt, paste0("n_seq_d", t_seq_len))
      temp_dt <- cbind(temp_dt, seq_dt)
      setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
      return(temp_dt)
    } else {
      col_prefix <- paste0("tmin_tl_", thresh)
      seq_counts <- sapply(t_seq_len, function(j) {
        count_consecutive_helper(t_min, threshold = thresh, fun = function(x) sum(x > j), direction = "lower")
      })
      temp_dt <- data.table(
        days = round(sum(t_min < thresh), 2),
        max_rseq = count_consecutive_helper(t_min, threshold = thresh, fun = max, direction = "lower")
      )
      seq_dt <- data.table(t(seq_counts))
      setnames(seq_dt, paste0("n_seq_d", t_seq_len))
      temp_dt <- cbind(temp_dt, seq_dt)
      setnames(temp_dt, names(temp_dt), paste0(col_prefix, ".", names(temp_dt)))
      return(temp_dt)
    }
  })

  stats_dt <- do.call("cbind", stats_list)
  summary_dt <- data.table(
    tmin_min = min(t_min),
    tmin_mean = mean(t_min),
    tmin_var = var(t_min),
    tmin_sd = sd(t_min),
    tmin_range = diff(range(t_min)),

    tmax_max = max(t_max),
    tmax_mean = mean(t_max),
    tmax_var = var(t_max),
    tmax_sd = sd(t_max),
    tmax_range = diff(range(t_max)),

    tmean_max = max(t_mean),
    tmean_min = min(t_mean),
    tmean_mean = mean(t_mean),
    tmean_var = var(t_mean),
    tmean_sd = sd(t_mean),
    tmean_range = diff(range(t_mean))
  )

  result_dt <- cbind(stats_dt, summary_dt)
  result_dt[, lapply(.SD, as.numeric)]
}

###############################################################################
#' Calculate the Circular Median of Dates
#'
#' Converts a vector of dates to a circular (annual) scale and computes a central
#' tendency (by default, the median) of the day-of-year. The result is returned as a
#' three-digit Julian day.
#'
#' @param dates Vector of dates in "yyyy-mm-dd" format or as Date objects.
#' @param fun Function to compute central tendency (default is \code{median}).
#' @return A character string representing the circular median as a three-digit Julian day.
#' @import circular
#' @examples
#' # Example usage:
#' dates <- as.Date(c("2020-04-15", "2020-04-20", "2020-04-25"))
#' calc_circular_median(dates)
#' @export
calc_circular_median <- function(dates, fun = median) {
  if (!inherits(dates, "Date")) {
    warning("`dates` is not of class Date. Attempting to convert using as.Date(...).")
    dates <- as.Date(dates)
    if (any(is.na(dates))) {
      stop("Conversion of `dates` to Date resulted in NA. Please check the date format.")
    }
  }

  day_of_year <- as.numeric(format(dates, "%j"))
  angles <- day_of_year * 360 / 365 * pi / 180
  circular_angles <- circular::circular(angles)

  central_angle <- fun(circular_angles)
  central_day <- round(as.numeric(central_angle) * 180 / pi * 365 / 360, 0)

  if (length(dates) > 1) {
    if (central_day <= 0) {
      central_day <- 365 + central_day
    }
    return(sprintf("%03d", central_day))
  } else {
    return(sprintf("%03d", as.numeric(format(dates, "%j"))))
  }
}
