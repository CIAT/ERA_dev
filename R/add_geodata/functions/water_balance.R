# ==========================================================
# watbal_all_in_one.R
# ==========================================================
# This script contains a set of functions for:
#  - AWC calculations (Hodnett & Tomasella, 2002)
#  - Soil capacity calculations up to a rooting depth
#  - Priestley-Taylor PET
#  - Daily water-balance iteration
#  - An all-in-one wrapper to perform the entire pipeline
#
# Author: Peter Steward, Fabio Castro, Julian Rameriz
# Date: 2025-03-15
# ==========================================================

#' @title AWCPTF
#' @description Pedo-transfer function for available water capacity (AWC),
#'   based on Hodnett and Tomasella (2002). Calculates volumetric water content
#'   at various suctions (e.g. field capacity, wilting point), and from these
#'   values, derives available water capacity.
#'   
#'   The code is taken from [Wösten, J.H.M., Verzandvoort, S.J.E., Leenaars, J.G.B., 
#'   Hoogland, T., & Wesseling, J.G. (2013). Soil hydraulic information for river basin 
#'   studies in semi-arid regions. *Geoderma, 195–196*, 79–86.](https://doi.org/10.1016/j.geoderma.2012.11.021)
#'
#' @param SNDPPT Numeric vector. Sand content (fraction or percent).
#' @param SLTPPT Numeric vector. Silt content (fraction or percent).
#' @param CLYPPT Numeric vector. Clay content (fraction or percent).
#' @param ORCDRC Numeric vector. Soil organic carbon (e.g., g/kg or similar).
#' @param BLD Numeric vector. Bulk density (kg/m^3). Defaults to 1400.
#' @param CEC Numeric vector. Cation exchange capacity (cmol/kg).
#' @param PHIHOX Numeric vector. Soil pH in H2O times 10 (commonly from SoilGrids).
#' @param h1,h2,h3,pwp Numeric. The suction heads (kPa) at which volumetric
#'   water content is derived. Defaults: -10 (near field capacity), -20, -31.6,
#'   and -1585 kPa (wilting point).
#' @param PTF.coef A data.frame of regression coefficients for the function.
#'   If missing, the Hodnett and Tomasella (2002) defaults are used.
#' @param fix.values Logical. If TRUE, attempts to ensure WWP <= volumetric content
#'   at any of h1, h2, h3 (so we don't get negative AWCs).
#' @param print.coef Logical. If TRUE, attach the coefficients as an attribute
#'   for reference.
#'
#' @return A data.frame with columns:
#' \describe{
#'   \item{AWCh1}{AWC at suction h1}
#'   \item{AWCh2}{AWC at suction h2}
#'   \item{AWCh3}{AWC at suction h3 (commonly used for field capacity)}
#'   \item{WWP}{Volumetric water content at wilting point (-1585 kPa)}
#'   \item{tetaS}{Saturated volumetric water content}
#' }
#'
#' @references
#' Hodnett, M. G., & Tomasella, J. (2002). Marked differences between
#'   van Genuchten soil water-retention parameters for temperate and tropical soils:
#'   A new water-retention pedo-transfer functions developed for tropical soils.
#'   \emph{Geoderma}, 108(3–4), 155–180.
#'
#' @examples
#' # Example usage:
#' # AWCPTF(SNDPPT=40, SLTPPT=30, CLYPPT=30, ORCDRC=10, BLD=1300, CEC=15, PHIHOX=50)
#'
#' @export
AWCPTF <- function(
    SNDPPT,
    SLTPPT,
    CLYPPT,
    ORCDRC,
    BLD = 1400,
    CEC,
    PHIHOX,
    h1 = -10,
    h2 = -20,
    h3 = -31.6,
    pwp = -1585,
    PTF.coef,
    fix.values = TRUE,
    print.coef = TRUE
){
  # If no coefficient table given, use defaults from Hodnett & Tomasella (2002).
  if (missing(PTF.coef)){
    PTF.coef <- data.frame(
      lnAlfa = c(-2.294,  0, -3.526,  0,    2.44,  0, -0.076, -11.331,  0.019,  0,     0,     0),
      lnN    = c(62.986,  0,  0,      -0.833,-0.529,0,  0,      0.593,   0,     0.007, -0.014, 0),
      tetaS  = c(81.799,  0,  0,       0.099, 0,    -31.42,0.018, 0.451,   0,     0,      0,     -5e-04),
      tetaR  = c(22.733, -0.164,0,     0,     0,     0,     0.235, -0.831,  0,     0.0018, 0,     0.0026)
    )
  }
  
  if (fix.values){
    sum.tex <- CLYPPT + SLTPPT + SNDPPT
    # Convert to percentages if needed
    CLYPPT <- CLYPPT / sum.tex * 100
    SLTPPT <- SLTPPT / sum.tex * 100
    SNDPPT <- SNDPPT / sum.tex * 100
    BLD[BLD < 100]  <- 100
    BLD[BLD > 2650] <- 2650
  }
  
  clm <- data.frame(
    SNDPPT,
    SLTPPT,
    CLYPPT,
    ORCDRC = ORCDRC / 10,
    BLD    = BLD * 0.001,
    CEC,
    PHIHOX,
    SLTPPT_sq = SLTPPT^2,
    CLYPPT_sq = CLYPPT^2,
    SNDxSLT   = SNDPPT * SLTPPT,
    SNDxCLY   = SNDPPT * CLYPPT
  )
  
  alfa <- apply(clm, 1, function(x) {
    exp((PTF.coef$lnAlfa[1] + sum(PTF.coef$lnAlfa[-1] * x)) / 100)
  })
  
  N <- apply(clm, 1, function(x) {
    exp((PTF.coef$lnN[1] + sum(PTF.coef$lnN[-1] * x)) / 100)
  })
  
  tetaS <- apply(clm, 1, function(x) {
    (PTF.coef$tetaS[1] + sum(PTF.coef$tetaS[-1] * x)) / 100
  })
  
  tetaR <- apply(clm, 1, function(x) {
    (PTF.coef$tetaR[1] + sum(PTF.coef$tetaR[-1] * x)) / 100
  })
  
  # Fix extremes
  tetaR[tetaR < 0]   <- 0
  tetaS[tetaS > 100] <- 100
  
  # Compute volumetric water content at the given suctions
  m       <- 1 - 1/N
  tetah1  <- tetaR + (tetaS - tetaR) / ((1 + (alfa * -1 * h1)^N))^m
  tetah2  <- tetaR + (tetaS - tetaR) / ((1 + (alfa * -1 * h2)^N))^m
  tetah3  <- tetaR + (tetaS - tetaR) / ((1 + (alfa * -1 * h3)^N))^m
  WWP     <- tetaR + (tetaS - tetaR) / ((1 + (alfa * -1 * pwp)^N))^m
  
  # If WWP is larger than one of the tetahs, clamp
  if (fix.values){
    sel <- which(WWP > tetah1 | WWP > tetah2 | WWP > tetah3)
    if (length(sel) > 0){
      WWP[sel] <- apply(
        data.frame(tetah1[sel], tetah2[sel], tetah3[sel]),
        1,
        min,
        na.rm=TRUE
      )
      warning(paste("Wilting point capacity is higher than h1/h2/h3 for",
                    length(sel), "points. Adjusted."))
    }
  }
  
  # Available Water Content for each suction
  AWCh1 <- tetah1 - WWP
  AWCh2 <- tetah2 - WWP
  AWCh3 <- tetah3 - WWP
  
  out <- data.frame(
    AWCh1 = signif(AWCh1, 3),
    AWCh2 = signif(AWCh2, 3),
    AWCh3 = signif(AWCh3, 3),
    WWP   = signif(WWP,   3),
    tetaS = signif(tetaS, 3)
  )
  
  if (print.coef){
    attr(out, "coef")      <- as.list(PTF.coef)
    attr(out, "PTF.names") <- list(
      variable = c("ai1","sand","silt","clay","oc","bd","cec","ph","silt^2",
                   "clay^2","sand*silt","sand*clay")
    )
  }
  
  return(out)
}


#' @title soilcap_calc
#' @description Computes total water-holding capacity (mm) up to a given
#'   rooting depth, based on horizon-wise AWC values and lower-bound depths.
#'
#' @param x A numeric vector of volumetric fractions (e.g., AWCh3) for each horizon.
#' @param y A numeric vector of horizon lower limits (in cm).
#' @param rdepth Numeric. Rooting depth (cm). Default = 60.
#' @param minval Numeric. Minimum plausible rooting depth (cm).
#' @param maxval Numeric. Maximum plausible rooting depth (cm).
#'
#' @details Interpolates a horizon boundary if the rooting depth is between
#'   horizon intervals, then calculates \code{(horizon thickness in cm) * (vol fraction)}.
#'   Finally multiplies by 10 to convert cm * fraction to mm.
#'
#' @return Numeric. Total capacity (mm) from surface to \code{rdepth}.
#'
#' @examples
#' soilcap_calc(x = c(0.15, 0.14, 0.12), y = c(20, 40, 60), rdepth=60)
#'
#' @export
soilcap_calc <- function(x, y, rdepth=60, minval=45, maxval=100){
  if (length(x) != length(y)){
    stop("Length of x and y must be the same.")
  }
  # Force rdepth within [minval, maxval]
  rdepth <- max(c(rdepth, minval))
  rdepth <- min(c(rdepth, maxval))
  
  wc_df <- data.frame(depth=y, wc=x)
  
  # Interpolate if needed
  if (!rdepth %in% wc_df$depth){
    wc_df1 <- wc_df[wc_df$depth < rdepth, ]
    wc_df2 <- wc_df[wc_df$depth > rdepth, ]
    if (nrow(wc_df1) > 0 && nrow(wc_df2) > 0){
      y1 <- wc_df1$wc[nrow(wc_df1)]
      y2 <- wc_df2$wc[1]
      x1 <- wc_df1$depth[nrow(wc_df1)]
      x2 <- wc_df2$depth[1]
      ya <- (rdepth - x1)/(x2 - x1)*(y2 - y1) + y1
      wc_df <- rbind(
        wc_df1,
        data.frame(depth=rdepth, wc=ya),
        wc_df2
      )
    }
    # If the entire soil is shallower or deeper than rdepth,
    # the standard logic proceeds anyway.
  }
  
  wc_df <- wc_df[wc_df$depth <= rdepth, ]
  wc_df$soilthick <- wc_df$depth - c(0, wc_df$depth[-nrow(wc_df)])
  wc_df$soilcap   <- wc_df$soilthick * wc_df$wc
  
  # Convert from cm * fraction to mm
  soilcp <- sum(wc_df$soilcap) * 10
  return(soilcp)
}


#' @title calc_pet
#' @description Calculates potential evapotranspiration (PET, mm/day)
#'   using a Priestley-Taylor approach.
#'
#' @param srad Numeric vector. Solar radiation (MJ/m2/day).
#' @param tmin Numeric vector. Minimum temperature (degrees C).
#' @param tmax Numeric vector. Maximum temperature (degrees C).
#' @param tmean Numeric vector or NULL. Mean temperature (degrees C).
#'   If NULL, \code{(tmin + tmax)/2} is used.
#' @param albedo Numeric. Albedo (fraction). Default=0.2.
#' @param vpd_cte Numeric. A scaling factor for VPD. Default=0.7.
#' @param psycho Numeric. Psychrometric constant (Pa/K). Default=62 (kPa-based).
#' @param rlat_ht Numeric. Latent heat of vaporization (J/kg). Default=2.26e6.
#' @param rho_w Numeric. Water density (kg/m3). Default=997.
#'
#' @return Numeric vector. PET (mm/day).
#'
#' @examples
#' calc_pet(srad=18, tmin=14, tmax=26)
#'
#' @export
calc_pet <- function(
    srad,
    tmin,
    tmax,
    tmean=NULL,
    albedo=0.2,
    vpd_cte=0.7,
    psycho=62,
    rlat_ht=2.26e6,
    rho_w=997
){
  if (is.null(tmean)){
    tmean <- (tmin + tmax)/2
  }
  # Net radiation
  rn <- (1 - albedo)*srad
  
  # Slope of saturation vapor pressure curve
  a_eslope <- 611.2
  b_eslope <- 17.67
  c_eslope <- 243.5
  eslope <- a_eslope*b_eslope*c_eslope/(tmean + c_eslope)^2 * exp(b_eslope*tmean/(tmean + c_eslope))
  
  # Estimate VPD
  esat_min <- 0.61120*exp((17.67*tmin)/(tmin+243.5))
  esat_max <- 0.61120*exp((17.67*tmax)/(tmax+243.5))
  vpd      <- vpd_cte * (esat_max - esat_min)  # kPa
  
  # Priestley-Taylor factor
  pt_const <- 1.26
  vpd_ref  <- 1
  pt_fact  <- 1
  pt_coef  <- pt_fact * pt_const
  pt_coef2 <- 1 + (pt_coef - 1) * (vpd / vpd_ref)
  
  # Convert flux to mm water
  et_max <- (pt_coef2*rn*eslope/(eslope+psycho)*1e6 / rlat_ht * 100/rho_w)*10
  return(et_max)
}


#' @title calc_daily_watbal
#' @description Computes the daily water-balance update using the ratio
#'   of actual ET (Ea) to potential ET (Ep), given a current water availability
#'   and total soil capacities.
#'
#' @param soilcp Numeric. Field-capacity water-holding capacity (mm).
#' @param soilsat Numeric. Additional capacity above field capacity
#'   (from field capacity to saturation, mm).
#' @param avail Numeric. Current water content in the soil (mm).
#' @param rain Numeric. Rainfall (mm/day).
#' @param pet Numeric. Potential ET (mm/day).
#' @param cropfc Numeric. Crop factor (default=1).
#'
#' @details The function:
#'   \enumerate{
#'     \item Clamps \code{avail} at \code{soilcp} if it exceeds capacity
#'     \item Calculates Ea/Ep via an empirical function based on \code{soilcp} and \code{avail}
#'     \item Deducts ET demand, adds rain
#'     \item Excess water above \code{soilcp} is logged as \code{LOGGING}, but limited to \code{soilsat}
#'     \item Surplus beyond saturation becomes \code{RUNOFF}
#'   }
#'   
#' @importFrom data.table setkey rbindlist
#' @importFrom pbapply pblapply
#' @importFrom future.apply future_lapply
#' @importFrom progressr with_progress progressor
#'
#' @return A data.frame with columns:
#'   \describe{
#'     \item{AVAIL}{New soil water availability (mm)}
#'     \item{DEMAND}{ET demand (mm)}
#'     \item{ERATIO}{Ratio of actual to potential ET (unitless)}
#'     \item{LOGGING}{Water above field capacity but below saturation (mm)}
#'     \item{RUNOFF}{Excess water beyond saturation capacity (mm)}
#'   }
#'
#' @examples
#' calc_daily_watbal(soilcp=100, soilsat=50, avail=50, rain=10, pet=6)
#'
#' @export
calc_daily_watbal <- function(
    soilcp,
    soilsat,
    avail,
    rain,
    pet,
    cropfc=1
){
  # e/a to e/p subfunction
  eabyep_subfun <- function(soilcp, avail){
    if (soilcp == 0) return(0)
    percwt <- avail/soilcp * 100
    percwt <- max(1, min(100, percwt))
    eratio <- min(percwt/(97 - 3.868*sqrt(soilcp)), 1)
    return(eratio)
  }
  
  # Clamp to field capacity if needed
  new_avail <- min(soilcp, avail)
  
  # Calculate fraction of PET that can be supplied
  eratio <- eabyep_subfun(soilcp, new_avail)
  demand <- eratio*cropfc*pet
  
  # After water usage + new rainfall
  result  <- new_avail + rain - demand
  
  # Water above field capacity
  logging <- result - soilcp
  logging <- ifelse(logging < 0, 0, logging)
  logging <- ifelse(logging > soilsat, soilsat, logging)
  
  # New available water
  new_avail <- min(soilcp, result)
  new_avail <- ifelse(new_avail < 0, 0, new_avail)
  
  # Runoff is beyond saturation
  runoff <- result - (soilcp + logging)
  runoff <- ifelse(runoff < 0, 0, runoff)
  
  data.frame(
    AVAIL   = new_avail,
    DEMAND  = demand,
    ERATIO  = eratio,
    LOGGING = logging,
    RUNOFF  = runoff
  )
}


#' @title calc_watbal_series
#' @description Runs a day-by-day water-balance across a single site's time series,
#'   updating water availability and storing outputs in columns.
#'
#' @param dt A data.table or data.frame with daily columns:
#'   \enumerate{
#'     \item RAIN (mm/day)
#'     \item SRAD (MJ/m2/day)
#'     \item TMIN,TMAX (degrees C)
#'     \item TMEAN (optional; if missing, is computed)
#'   }
#' @param soilcp Numeric. The soil capacity at field capacity (mm).
#' @param soilsat Numeric. Additional capacity from field capacity to saturation (mm).
#' @param init_avail Numeric. Initial soil water availability (mm). Default=0.
#'
#' @details For each row/day:
#'   \itemize{
#'     \item PET is calculated via \code{\link{calc_pet}}
#'     \item \code{\link{calc_daily_watbal}} is used to update water availability
#'   }
#'
#' @return The same \code{dt}, but with new columns:
#'   \enumerate{
#'     \item ETMAX (PET, mm)
#'     \item AVAIL
#'     \item DEMAND
#'     \item ERATIO
#'     \item LOGGING
#'     \item RUNOFF
#'   }
#'
#' @examples
#' library(data.table)
#' mydt <- data.table(
#'   DAY=1:5,
#'   RAIN=c(0, 10, 0, 5, 0),
#'   SRAD=c(15, 18, 20, 16, 19),
#'   TMIN=c(12,13,15,14,13),
#'   TMAX=c(25,26,28,25,24)
#' )
#' calc_watbal_series(mydt, soilcp=100, soilsat=40)
#'
#' @export
calc_watbal_series <- function(
    dt,
    soilcp,
    soilsat,
    init_avail=0
){
  # checks
  stopifnot("RAIN" %in% names(dt))
  stopifnot("SRAD" %in% names(dt))
  stopifnot("TMIN" %in% names(dt))
  stopifnot("TMAX" %in% names(dt))
  
  # If TMEAN missing, handle inside calc_pet
  # Add columns
  dt[["ETMAX"]]   <- calc_pet(
    srad  = dt[["SRAD"]],
    tmin  = dt[["TMIN"]],
    tmax  = dt[["TMAX"]],
    tmean = if ("TMEAN" %in% names(dt)) dt[["TMEAN"]] else NULL
  )
  dt[["AVAIL"]]   <- numeric(nrow(dt))
  dt[["DEMAND"]]  <- numeric(nrow(dt))
  dt[["ERATIO"]]  <- numeric(nrow(dt))
  dt[["LOGGING"]] <- numeric(nrow(dt))
  dt[["RUNOFF"]]  <- numeric(nrow(dt))
  
  # iterative update
  cur_avail <- init_avail
  for (i in seq_len(nrow(dt))){
    wb <- calc_daily_watbal(
      soilcp  = soilcp,
      soilsat = soilsat,
      avail   = cur_avail,
      rain    = dt[["RAIN"]][i],
      pet     = dt[["ETMAX"]][i]
    )
    dt[["AVAIL"]][i]   <- wb$AVAIL
    dt[["DEMAND"]][i]  <- wb$DEMAND
    dt[["ERATIO"]][i]  <- wb$ERATIO
    dt[["LOGGING"]][i] <- wb$LOGGING
    dt[["RUNOFF"]][i]  <- wb$RUNOFF
    cur_avail          <- wb$AVAIL
  }
  return(dt)
}


#' @title run_full_water_balance
#' @description
#' An all-in-one workflow to:
#' \enumerate{
#'   \item Compute AWC for each soil horizon, using Hodnett & Tomasella (2002)
#'         via \code{\link{AWCPTF}} (e.g., \code{AWCh3}).
#'   \item Compute total water-holding capacity up to a specified root depth
#'         (field capacity) and capacity to saturation.
#'   \item Merge daily weather by site.
#'   \item Run a day-by-day water balance for each site using
#'         \code{\link{calc_watbal_series}}.
#' }
#'
#' @param horizon_data A \code{data.table} or \code{data.frame} containing
#'   one row per horizon (depth interval) per site, with:
#'   \describe{
#'     \item{\code{Site.Key}}{(Character) An identifier for each site.}
#'     \item{\code{depth}}{(Numeric, cm) Lower boundary of the horizon in centimeters.}
#'     \item{\code{SNDPPT}}{(Numeric, g/kg) Sand content in grams per kilogram of soil.}
#'     \item{\code{SLTPPT}}{(Numeric, g/kg) Silt content in grams per kilogram of soil.}
#'     \item{\code{CLYPPT}}{(Numeric, g/kg) Clay content in grams per kilogram of soil.}
#'     \item{\code{ORCDRC}}{(Numeric, g/kg) Soil organic carbon content.}
#'     \item{\code{BLD}}{(Numeric, kg/m^3) Bulk density. Often ~1300--1600.}
#'     \item{\code{CEC}}{(Numeric, cmol(+)/kg) Cation exchange capacity.}
#'     \item{\code{PHIHOX}}{(Numeric, pH*10) Soil pH \emph{times} 10, e.g. 55 = pH 5.5.}
#'   }
#'
#' @param weather_data A \code{data.table} or \code{data.frame} containing
#'   one row per day per site, with:
#'   \describe{
#'     \item{\code{Site.Key}}{(Character) The same site ID as in \code{horizon_data}.}
#'     \item{\code{DATE}}{(Date) The calendar day.}
#'     \item{\code{TMIN}}{(Numeric, °C) Daily minimum temperature.}
#'     \item{\code{TMAX}}{(Numeric, °C) Daily maximum temperature.}
#'     \item{\code{TMEAN}}{(Optional, Numeric, °C) Daily mean temperature.
#'       If missing, \code{(TMIN + TMAX)/2} is used.}
#'     \item{\code{RAIN}}{(Numeric, mm) Daily total rainfall.}
#'     \item{\code{SRAD}}{(Numeric, MJ/m^2/day) Incident solar radiation.}
#'   }
#'
#' @param root_depth Numeric. Rooting depth in centimeters (default 60).
#' @param min_depth Numeric. Minimum bounding for root depth (cm). Default 45.
#' @param max_depth Numeric. Maximum bounding for root depth (cm). Default 100.
#' @param init_avail Numeric. Initial soil water availability (mm). Default 0.
#' @param worker_n Integer. Number of parallel workers to use. 
#'   \itemize{
#'     \item If \code{worker_n == 1}, the function uses \code{pblapply} (serial) with a progress bar.
#'     \item If \code{worker_n > 1}, the function uses \code{future_lapply} in parallel and 
#'           displays a progress bar with \code{progressr}.
#'   }
#'
#' @details
#' \enumerate{
#'   \item \strong{Soil Horizons}: Each horizon's \code{SNDPPT}, \code{SLTPPT}, \code{CLYPPT}
#'         should be in the same scale (commonly g/kg from SoilGrids). The function
#'         \code{\link{AWCPTF}} normalizes these internally and calculates volumetric
#'         water content for field capacity, wilting point, etc.
#'   \item \strong{Capacity Calculation}: \code{\link{soilcap_calc}} is used to sum water-holding
#'         capacity (in mm) from the surface down to \code{root_depth}, clamped to
#'         [\code{min_depth}, \code{max_depth}].
#'   \item \strong{Weather Merge}: The function merges \code{cap_table} (i.e., \code{scp}/\code{ssat}) onto the
#'         daily weather, by matching \code{Site.Key}.
#'   \item \strong{Water-Balance Loop}: For each site, \code{\link{calc_watbal_series}} is called
#'         in chronological order. This computes daily PET (Priestley-Taylor) and updates
#'         \code{AVAIL}, \code{DEMAND}, \code{ERATIO}, \code{LOGGING}, and \code{RUNOFF}.
#' }
#'
#' @return A \code{data.table} with the merged weather plus new columns:
#'   \itemize{
#'     \item \code{scp} and \code{ssat} for each site,
#'     \item \code{ETMAX} (PET),
#'     \item \code{AVAIL}, \code{DEMAND}, \code{ERATIO}, \code{LOGGING}, \code{RUNOFF}.
#'   }
#'
#' @examples
#' \dontrun{
#' library(pbapply)
#' library(data.table)
#' library(future.apply)
#' library(progressr)
#'
#' horizon_data <- data.table(
#'   Site.Key="SiteA",
#'   depth=c(5,15,30),
#'   SNDPPT=c(500, 450, 400),
#'   SLTPPT=c(300, 350, 400),
#'   CLYPPT=c(200, 200, 200),
#'   ORCDRC=c(10, 9, 8),
#'   BLD=c(1300, 1320, 1350),
#'   CEC=c(15, 16, 14),
#'   PHIHOX=c(55, 55, 55)
#' )
#'
#' weather_data <- data.table(
#'   Site.Key="SiteA",
#'   DATE=seq.Date(as.Date("2020-01-01"), by="day", length.out=5),
#'   TMIN=c(12,13,12,15,14),
#'   TMAX=c(24,25,24,27,25),
#'   RAIN=c(0,5,0,12,0),
#'   SRAD=c(16,17,19,15,18)
#' )
#'
#' # Run in serial mode (worker_n=1) with pblapply:
#' result_serial <- run_full_water_balance(
#'   horizon_data, weather_data,
#'   root_depth=60, min_depth=45, max_depth=100, init_avail=0,
#'   worker_n=1
#' )
#'
#' # Run in parallel mode (worker_n=2) with future_lapply + progressr:
#' result_parallel <- run_full_water_balance(
#'   horizon_data, weather_data,
#'   worker_n=2
#' )
#' }
#' 
#' @export
run_full_water_balance <- function(
    horizon_data,
    weather_data,
    id_field="Site.Key",
    root_depth=60,
    min_depth=45,
    max_depth=100,
    init_avail=0,
    worker_n=1
){
  
  setnames(horizon_data,id_field,"Site.Key")
  setnames(weather_data,id_field,"Site.Key")
  
  # 1) Compute AWC in each horizon (AWCh3, etc.), plus tetaFC, AWSat
  horizon_data[, c("AWCh1","AWCh2","AWCh3","WWP","tetaS") :=
                 AWCPTF(
                   SNDPPT, SLTPPT, CLYPPT, ORCDRC, BLD, CEC, PHIHOX,
                   h1=-10, h2=-20, h3=-33
                 ),
               by=.(Site.Key, depth)]
  
  horizon_data[, tetaFC := WWP + AWCh3]
  horizon_data[, AWSat  := tetaS - tetaFC]
  
  # Clamp negative values to 0 if they’re small
  horizon_data[, AWSat := ifelse(AWSat < 0 & AWSat > -0.01, 0, AWSat)]
  
  # 2) Summarize by site to get scp + ssat
  cap_table <- horizon_data[,
                            .(
                              scp  = soilcap_calc(x=AWCh3, y=depth, rdepth=root_depth,
                                                  minval=min_depth, maxval=max_depth),
                              ssat = soilcap_calc(x=AWSat, y=depth, rdepth=root_depth,
                                                  minval=min_depth, maxval=max_depth)
                            ),
                            by=.(Site.Key)
  ]
  
  # 3) Merge scp+ssat onto weather
  data.table::setkey(cap_table, "Site.Key")
  data.table::setkey(weather_data, "Site.Key")
  big_dt <- cap_table[weather_data, nomatch=0]
  
  # 4) Split by site, run the water-balance
  site_list <- split(big_dt, by="Site.Key")
  
  if (worker_n > 1) {
    # ---------------------------
    # PARALLEL: future_lapply + progressr
    # ---------------------------
    # Set up parallel plan:
    future::plan(future::multisession, workers = worker_n)
    
    # Use progressr:
    progressr::with_progress({
      p <- progressr::progressor(steps = length(site_list))
      result_list <- future.apply::future_lapply(
        1:length(site_list),
        FUN = function(i) {
          p()  # increment progress
          subdt<-site_list[[i]]
          sscp  <- unique(subdt$scp)
          sssat <- unique(subdt$ssat)
          if (length(sscp) != 1 || length(sssat) != 1) {
            stop("Multiple scp/ssat values found for the same Site.Key. Check data.")
          }
          subdt <- subdt[order(subdt$DATE),]
          out   <- calc_watbal_series(
            dt         = subdt,
            soilcp     = sscp,
            soilsat    = sssat,
            init_avail = init_avail
          )
          return(out)
        }
      )
    })
  } else {
    # ---------------------------
    # SERIAL
    # ---------------------------
    result_list <- lapply(
      1:length(site_list),
      FUN = function(i) {
        subdt<-site_list[[i]]
        sscp  <- unique(subdt$scp)
        sssat <- unique(subdt$ssat)
        cat("Processing site",unique(subdt$Site.Key),i,"/",length(site_list),"         \r")
        if (length(sscp) != 1 || length(sssat) != 1) {
          stop(paste0("Multiple scp/ssat values found for the same Site.Key. Check data. Site Key = ",unique(subdt$Site.Key)))
        }
        subdt <- subdt[order(DATE)]
        out   <- calc_watbal_series(
          dt         = subdt,
          soilcp     = sscp,
          soilsat    = sssat,
          init_avail = init_avail
        )
        return(out)
      }
    )
  }
  
  final_dt <- data.table::rbindlist(result_list)
  setnames(final_dt,"Site.Key",id_field)
  return(final_dt)
}

#' @title validate_watbal
#'
#' @description
#' Performs diagnostic checks and generates simple summaries/plots
#' to validate a daily water-balance output table (e.g., `watbal_result`).
#'
#' @param watbal A \code{data.table} or \code{data.frame} with at least the columns:
#'   \describe{
#'     \item{\code{Site.Key}}{(Character) Site identifier}
#'     \item{\code{scp}}{(Numeric) Soil water-holding capacity at field capacity (mm)}
#'     \item{\code{DATE}}{(Date or character) Daily date}
#'     \item{\code{TMIN}}{(Numeric, °C) Daily min temperature}
#'     \item{\code{TMAX}}{(Numeric, °C) Daily max temperature}
#'     \item{\code{RAIN}}{(Numeric, mm) Daily precipitation}
#'     \item{\code{SRAD}}{(Numeric, MJ/m^2/day) Daily solar radiation}
#'     \item{\code{ETMAX}}{(Numeric, mm) Calculated PET (potential ET)}
#'     \item{\code{AVAIL}}{(Numeric, mm) Soil water availability}
#'     \item{\code{DEMAND}}{(Numeric, mm) ET demand}
#'     \item{\code{ERATIO}}{(Numeric, dimensionless) Ea/Ep ratio}
#'     \item{\code{LOGGING}}{(Numeric, mm) Water above field capacity (if any)}
#'     \item{\code{RUNOFF}}{(Numeric, mm) Water beyond saturation or infiltration capacity}
#'   }
#' @param site (Optional) Character. If provided, validates only for the
#'   specified site (must match \code{Site.Key}). If NULL (default), validates all sites.
#' @param make_plots Logical. If TRUE (default), returns simple \code{ggplot2} plots.
#'
#' @return A named \code{list} with the following elements:
#'   \item{summary_stats}{A data.table of summary statistics (mean/min/max) for key variables.}
#'   \item{invalid_avail}{Rows where \code{AVAIL} is out of bounds (<0 or >scp). Might be empty.}
#'   \item{invalid_logging}{Rows where \code{LOGGING} < 0. Might be empty.}
#'   \item{plots}{A list of two ggplot objects (\code{avail_plot}, \code{rain_runoff_plot}) if \code{make_plots=TRUE}.}
#'
#' @examples
#' \dontrun{
#'   # Example usage:
#'   library(data.table)
#'   library(ggplot2)
#'
#'   # Suppose watbal_result is a data.table with the columns described above
#'   val_out <- validate_watbal(watbal_result, site="MySite", make_plots=TRUE)
#'
#'   # Show summary stats
#'   print(val_out$summary_stats)
#'
#'   # Inspect any out-of-bounds AVAIL or LOGGING rows:
#'   print(val_out$invalid_avail)
#'   print(val_out$invalid_logging)
#'
#'   # Display the plots:
#'   print(val_out$plots$avail_plot)
#'   print(val_out$plots$rain_runoff_plot)
#' }
#' @export
validate_watbal <- function(watbal, site = NULL, make_plots = TRUE) {
  # --- 1) Load required packages (or ensure they are imported in your NAMESPACE)
  stopifnot(requireNamespace("data.table", quietly=TRUE))
  stopifnot(requireNamespace("ggplot2", quietly=TRUE))
  
  # Convert to data.table if not already
  watbal <- data.table::as.data.table(watbal)
  
  # (a) Optionally filter for the chosen site
  if (!is.null(site)) {
    watbal <- watbal[Site.Key == site]
    if (nrow(watbal) == 0) {
      stop("No rows found for Site.Key = '", site, "'")
    }
  }
  
  # --- 2) Basic checks for required columns
  required_cols <- c("Site.Key","scp","DATE","TMIN","TMAX","RAIN","SRAD","ETMAX","AVAIL","DEMAND","ERATIO","LOGGING","RUNOFF")
  missing_cols  <- setdiff(required_cols, names(watbal))
  if (length(missing_cols) > 0) {
    stop("Missing required columns in 'watbal': ", paste(missing_cols, collapse=", "))
  }
  
  # --- 3) Identify out-of-bounds or weird values
  # AVAIL < 0 or AVAIL > scp
  invalid_avail <- watbal[ AVAIL < 0 | AVAIL > scp ]
  
  # LOGGING < 0
  invalid_logging <- watbal[ LOGGING < 0 ]
  
  # More checks: e.g. TMIN <= TMAX?
  # minTempError <- watbal[ TMIN > TMAX ]
  
  # --- 4) Summaries
  summary_stats <- watbal[
    ,
    .(
      minDate    = as.character(min(DATE, na.rm=TRUE)),
      maxDate    = as.character(max(DATE, na.rm=TRUE)),
      nDays      = .N,
      mean_TMIN  = mean(TMIN, na.rm=TRUE),
      min_TMIN   = min(TMIN, na.rm=TRUE),
      max_TMIN   = max(TMIN, na.rm=TRUE),
      mean_TMAX  = mean(TMAX, na.rm=TRUE),
      mean_RAIN  = mean(RAIN, na.rm=TRUE),
      max_RAIN   = max(RAIN, na.rm=TRUE),
      mean_ETMAX = mean(ETMAX, na.rm=TRUE),
      max_ETMAX  = max(ETMAX, na.rm=TRUE),
      mean_AVAIL = mean(AVAIL, na.rm=TRUE),
      max_AVAIL  = max(AVAIL, na.rm=TRUE)
    )
    ,
    by="Site.Key"
  ]
  
  # --- 5) Optional ggplot2 Plots
  plots_list <- list()
  if (make_plots) {
    # Choose the first site in the filtered data, or keep them all
    # If you prefer a single site, pick the first one or do a facet
    site_to_plot <- unique(watbal$Site.Key)[1]
    dt_plot      <- watbal[Site.Key == site_to_plot][order(DATE)]
    
    p_avail <- ggplot2::ggplot(dt_plot, ggplot2::aes(x = DATE)) +
      ggplot2::geom_line(ggplot2::aes(y = AVAIL), color="blue") +
      ggplot2::geom_hline(ggplot2::aes(yintercept = scp), 
                          color="darkblue", linetype="dashed") +
      ggplot2::labs(
        title     = paste("Soil Water Availability:", site_to_plot),
        subtitle  = "Dashed line = scp (field capacity)",
        x         = NULL,
        y         = "AVAIL (mm)"
      ) +
      ggplot2::theme_minimal()
    
    p_rain_runoff <- ggplot2::ggplot(dt_plot, ggplot2::aes(x = DATE)) +
      ggplot2::geom_line(ggplot2::aes(y = RAIN), color="darkgreen", size=1) +
      ggplot2::geom_line(ggplot2::aes(y = RUNOFF), color="red", linetype="dashed", size=1) +
      ggplot2::labs(
        title = paste("Rain vs. Runoff:", site_to_plot),
        x     = NULL,
        y     = "mm/day"
      ) +
      ggplot2::theme_minimal()
    
    plots_list$avail_plot       <- p_avail
    plots_list$rain_runoff_plot <- p_rain_runoff
  }
  
  # --- 6) Return all as a list
  return(list(
    summary_stats   = summary_stats,
    invalid_avail   = invalid_avail,
    invalid_logging = invalid_logging,
    plots           = plots_list
  ))
}