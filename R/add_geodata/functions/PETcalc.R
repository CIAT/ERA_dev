#' Calculate Reference Evapotranspiration (ETo) and Water Balance
#'
#' This function computes the reference evapotranspiration (ETo) using daily
#' minimum and maximum temperatures, solar radiation, wind speed, atmospheric pressure,
#' relative humidity, and day of year. It also computes a water balance (precipitation minus ETo)
#' if precipitation data are provided. The function uses standard formulas including:
#'
#' \itemize{
#'   \item Calculation of the mean temperature (Tmean).
#'   \item Computation of the slope of the saturation vapor pressure curve (\eqn{\Delta}).
#'   \item Calculation of the psychrometric constant (\eqn{\gamma}).
#'   \item Derivation of radiation and wind components of evapotranspiration.
#'   \item Estimation of extraterrestrial radiation (Ra) based on the day of year and latitude.
#'   \item Calculation of net shortwave (Rns) and net longwave (Rnl) radiation to determine net radiation (Rn).
#'   \item Conversion of net radiation into equivalent evaporation (Rng) and subsequent calculation
#'         of the radiation and wind components of ETo.
#' }
#'
#' The final ETo (in mm/day) is computed as the sum of the wind and radiation components,
#' and rounded to two decimal places. If precipitation data (Rain) are provided, the water
#' balance is computed as Rain minus ETo.
#'
#' @param Tmin Numeric. Daily minimum temperature (°C).
#' @param Tmax Numeric. Daily maximum temperature (°C).
#' @param SRad Numeric. Incoming solar radiation (MJ m\eqn{^{-2}} day\eqn{^{-1}}).
#' @param Wind Numeric. Wind speed (m s\eqn{^{-1}}).
#' @param Pressure Numeric. Atmospheric pressure (kPa).
#' @param Humid Numeric. Relative humidity (%). If all values are NA, then \code{ea} is set equal to \code{eTmin}.
#' @param Rain Numeric. Precipitation (mm). If provided (not all NA), water balance is computed as Rain - ETo.
#' @param YearDay Numeric. Day of year (1-365).
#' @param Latitude Numeric. Latitude of the location (in decimal degrees).
#' @param Altitude Numeric. Altitude of the location (in meters).
#'
#' @return A data.frame with one row and the following columns:
#'   \item{ETo}{Reference evapotranspiration (mm/day).}
#'   \item{WBalance}{Water balance (Rain - ETo) in mm/day, if precipitation is provided; otherwise, not returned.}
#'
#' @examples
#' # Calculate ETo for a day with:
#' # Tmin = 15, Tmax = 25, solar radiation = 20 MJ/m²/day,
#' # wind speed = 2 m/s, pressure = 101.3 kPa, relative humidity = 70%,
#' # precipitation = 5 mm, day 200, latitude = 35°, altitude = 150 m.
#' PETcalc(15, 25, 20, 2, 101.3, 70, 5, 200, 35, 150)
#'
#' # If no precipitation data are provided (all NA), only ETo is returned:
#' PETcalc(15, 25, 20, 2, 101.3, 70, NA, 200, 35, 150)
#'
#' @export
PETcalc <- function(Tmin, Tmax, SRad, Wind, Pressure, Humid, Rain, YearDay, Latitude, Altitude) {
  # Calculate the mean temperature
  Tmean <- (Tmin + Tmax) / 2
  
  # Solar radiation (MJ/m²/day)
  Rs <- SRad
  
  # Wind speed (m/s)
  WS <- Wind
  
  # Calculate the slope of the saturation vapor pressure curve (delta)
  delta <- (4098 * (0.6108 * exp((17.27 * Tmean) / (Tmean + 237.3)))) / (Tmean + 237.3)^2
  
  # Use provided pressure (kPa)
  gamma <- 0.000665 * Pressure
  
  # Calculate partitioning factors for evaporation components
  DT <- delta / (delta + gamma * (1 + 0.34 * WS))
  PT <- gamma / (delta + gamma * (1 + 0.34 * WS))
  
  # Wind effect adjustment (TT)
  TT <- (900 / (Tmean + 273)) * WS
  
  # Saturation vapor pressure calculations (kPa)
  eT <- 0.6108 * exp((17.27 * Tmean) / (Tmean + 237.3))
  eTmax <- 0.6108 * exp((17.27 * Tmax) / (Tmax + 237.3))
  eTmin <- 0.6108 * exp((17.27 * Tmin) / (Tmin + 237.3))
  eS <- (eTmax + eTmin) / 2
  
  # Actual vapor pressure (ea) using relative humidity if available, else use eTmin
  if (!all(is.na(Humid))) {
    ea <- (Humid / 100) * ((eTmax + eTmin) / 2)
  } else {
    ea <- eTmin
  }
  
  # Calculate extraterrestrial radiation (Ra) components
  dr <- 1 + 0.033 * cos(((2 * pi) / 365) * YearDay)
  theta <- 0.409 * sin(((2 * pi) / 365) * YearDay - 1.39)
  psi <- (pi / 180) * Latitude
  omega_s <- acos(-tan(psi) * tan(theta))
  Ra <- ((24 * 60) / pi) * 0.082 * dr * ((omega_s * sin(psi) * sin(theta)) +
                                           (cos(psi) * cos(theta) * sin(omega_s)))
  
  # Clear-sky solar radiation (Rso)
  Rso <- (0.75 + (2 * 10^-5) * Altitude) * Ra
  
  # Net shortwave radiation (Rns)
  Rns <- (1 - 0.23) * Rs
  
  # Net longwave radiation (Rnl)
  Rnl <- (4.903e-9) * (((Tmax + 273.16)^4 + (Tmin + 273.16)^4) / 2) *
    (0.34 - (0.14 * sqrt(ea))) * ((1.35 * (Rs / Rso)) - 0.35)
  
  # Net radiation (Rn)
  Rn <- Rns - Rnl
  
  # Convert net radiation to equivalent evaporation (Rng)
  Rng <- 0.408 * Rn
  
  # Calculate the radiation and wind components of ETo
  ETrad <- DT * Rng
  ETwind <- PT * TT * (eS - ea)
  
  # Compute reference evapotranspiration (ETo), rounded to 2 decimals
  ETo <- round(ETwind + ETrad, 2)
  
  # If precipitation is provided, calculate water balance as Rain - ETo.
  if (!all(is.na(Rain))) {
    WBalance <- round(Rain - ETo, 2)
    return(data.frame(ETo = ETo, WBalance = WBalance))
  } else {
    return(data.frame(ETo = ETo))
  }
}