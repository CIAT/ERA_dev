#' Estimate missing season lengths from existing values in ERA
#'
#' This function estimates missing season lengths (i.e., when the harvest date is unknown) in an ERA dataset by using existing values from the same `Product` or `Product.Subtype`, 
#' within the same year and growing season, and from spatially nearby locations. 
#'
#' Season length is calculated as the mean difference between the midpoint of `Harvest.Start` and `Harvest.End` dates, and the midpoint of `Plant.Start` and `Plant.End` dates.
#'
#' The function iteratively searches for season length values at five levels of increasing spatial distance, using latitude and longitude rounded from 5 to 1 decimal places. 
#' If multiple values exist for a `Product` in a given season (`M.Year`), their median value is used. If no season lengths are found matching `Product`, the function attempts 
#' matching based on `Product.Subtype` (e.g., cereals or legumes).
#'
#' @param data A data.table object containing ERA dataset (e.g., `ERA.Compiled`).
#' @return The function appends the following fields to the input dataset:
#' - `Data.SLen`: Estimated season length in days.
#' - `Data.SLen.Acc`: Accuracy indicator, representing the decimal places used for latitude/longitude matching and whether matching was based on `Product` (`P.Sub`) or `Product.Subtype` (`P`).
#' @export
#' @import data.table
est_slen <- function(data) {
  
  # Remove existing 'Data.SLen' column if present
  if ("Data.SLen" %in% colnames(data)) {
    data <- data[, !"Data.SLen"]
  }
  
  # Convert date columns to Date format if they are not already
  data[, Harvest.Start := if (!inherits(Harvest.Start, "Date")) as.Date(Harvest.Start, "%d.%m.%Y") else Harvest.Start]
  data[, Harvest.End   := if (!inherits(Harvest.End, "Date")) as.Date(Harvest.End, "%d.%m.%Y") else Harvest.End]
  data[, Plant.Start   := if (!inherits(Plant.Start, "Date")) as.Date(Plant.Start, "%d.%m.%Y") else Plant.Start]
  data[, Plant.End     := if (!inherits(Plant.End, "Date")) as.Date(Plant.End, "%d.%m.%Y") else Plant.End]
  
  # Calculate season length (SLen) as the difference between the midpoint of harvest and planting dates
  data[, SLen := (as.numeric(Harvest.Start) + (as.numeric(Harvest.End) - as.numeric(Harvest.Start)) / 2) - 
         (as.numeric(Plant.Start) + (as.numeric(Plant.End) - as.numeric(Plant.Start)) / 2)]
  
  # Initialize new columns for estimated season length and accuracy
  data[, Data.SLen.Acc := as.character(NA)]
  data[, Data.SLen := as.numeric(NA)]
  
  # Extract unique existing season lengths with corresponding spatial and temporal information
  valid_records <- unique(data[!is.na(SLen), .(SLen, Latitude, Longitude, M.Year, Product)])
  
  # Aggregate season length statistics by spatial and temporal factors
  valid_records <- valid_records[, .(
    SL.Mean   = mean(SLen, na.rm = TRUE),
    SL.Median = median(SLen, na.rm = TRUE),
    SL.N      = sum(!is.na(SLen))
  ), by = .(Latitude, Longitude, M.Year, Product)]
  
  # Iteratively search for matching season lengths at decreasing spatial resolutions (from 3 to 2 decimal places)
  # Note 1 degree at the equator is 110km, 0.1 = 11km, 0.01 = 1.1km
  
  # Include spatial and temporal specificity
  for (i in 3:2) {
    
    # Create spatial matching codes based on rounded latitude and longitude
    valid_records[, Code := paste(round(Latitude, i), round(Longitude, i), M.Year, Product)]
    
    # Generate matching code for the dataset
    data[, MCode := paste(round(Latitude, i), round(Longitude, i), M.Year, Product)]
    
    # Apply estimated season length where missing
    data[is.na(SLen) & is.na(Data.SLen), Data.SLen.Acc := paste0(i, "-P")]
    data[is.na(SLen) & is.na(Data.SLen), Data.SLen := valid_records$SL.Median[match(MCode, valid_records$Code)]]
    
    # Reset accuracy column if no match is found
    data[is.na(Data.SLen), Data.SLen.Acc := NA]
  }
  
  # Include spatial specificity only
  valid_records<-valid_records[,.(SL.Median=round(mean(SL.Median,na.rm=T),0)),by=.(Latitude,Longitude,Product)]
  for (i in 3:2) {
    
    # Create spatial matching codes based on rounded latitude and longitude
    valid_records[, Code := paste(round(Latitude, i), round(Longitude, i), Product)]
    
    # Generate matching code for the dataset
    data[, MCode := paste(round(Latitude, i), round(Longitude, i), Product)]
    
    # Apply estimated season length where missing
    data[is.na(SLen) & is.na(Data.SLen), Data.SLen.Acc := paste0(i, "-P")]
    data[is.na(SLen) & is.na(Data.SLen), Data.SLen := valid_records$SL.Median[match(MCode, valid_records$Code)]]
    
    # Reset accuracy column if no match is found
    data[is.na(Data.SLen), Data.SLen.Acc := NA]
  }
  
  # Remove temporary matching code column
  data[, MCode := NULL]
  
  return(data)
}
