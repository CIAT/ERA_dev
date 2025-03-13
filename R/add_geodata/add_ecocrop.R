#' Add EcoCrop Data to Product List
#'
#' This function retrieves EcoCrop temperature and growth cycle parameters for a given list of products.
#'
#' @param crop_names Character vector. List of crop names.
#' @param ecocrop Data.table. EcoCrop dataset containing species temperature and cycle information.
#' @param EUCodes ERA product codes (available from vocab/era_master_sheet.xlsx)
#'
#' @import data.table
#' @import ERAg
#'
#' @return Data.table containing the EcoCrop temperature and cycle parameters for the specified products.
#' @export
add_ecocrop <- function(crop_names, ecocrop,EUCodes) {
  # Match product names to EcoCrop species names
  E.Names <- data.table(EUCodes)[match(crop_names, Product), ECOCROP.Name]
  
  # Extract EcoCrop data for the matched species
  ecocrop_dat <- ecocrop[match(E.Names, species), c("species", 
                                                    "temp_opt_min", 
                                                    "Temp_Opt_Max", 
                                                    "Temp_Abs_Min", 
                                                    "Temp_Abs_Max", 
                                                    "cycle_min", 
                                                    "cycle_max")]
  
  # Rename temperature columns for clarity
  colnames(ecocrop_dat)[2:5] <- c("Topt.low", "Topt.high", "Tlow", "Thigh")
  
  # Convert cycle and temperature values to numeric format
  ecocrop_dat[, `:=`(cycle_min, as.numeric(cycle_min))]
  ecocrop_dat[, `:=`(cycle_max, as.numeric(cycle_max))]
  ecocrop_dat[, `:=`(Topt.low, as.numeric(Topt.low))]
  ecocrop_dat[, `:=`(Topt.high, as.numeric(Topt.high))]
  
  # Convert 'Tlow' values to numeric, replacing NULLs with NA
  X <- ecocrop_dat[, Tlow]
  X <- unlist(lapply(X, FUN = function(X) {
    if (is.null(X)) {
      NA
    } else {
      X
    }
  }))
  ecocrop_dat[, `:=`(Tlow, as.numeric(X))]
  
  # Convert 'Thigh' values to numeric, replacing NULLs with NA
  X <- ecocrop_dat[, Thigh]
  X <- unlist(lapply(X, FUN = function(X) {
    if (is.null(X)) {
      NA
    } else {
      X
    }
  }))
  ecocrop_dat[, `:=`(Thigh, as.numeric(X))]
  
  return(ecocrop_dat)
}
