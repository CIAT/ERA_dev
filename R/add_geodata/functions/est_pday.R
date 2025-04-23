#' Estimate Missing Planting Dates Using Nearby Observations
#'
#' This function estimates missing planting dates in an ERA dataset by searching for spatially nearby records
#' with the same `Product` or `Product.Subtype`, year, and growing season.
#'
#' The function performs an **iterative search** within five levels of increasing distance, reducing latitude and
#' longitude precision from 5 to 1 decimal place.
#'
#' - If multiple planting dates exist for a `Product x M.Year.Start x M.Year.End x Season.Start x Season.End` combination,
#'   values are **averaged**.
#' - If no matches are found at the `Product` level, the function searches at the **Product.Subtype** level (e.g., cereals, legumes).
#' - If a location has both **defined seasons (1 or 2)** and **missing seasons (NA)**, the observation is flagged as a potential
#'   temporal inconsistency and excluded from estimation.
#' - Observations with **irrigation in both control and treatment** are excluded, as their planting dates are unlikely to reflect
#'   rainfed conditions.
#'
#' @param data A `data.table` ERA dataset (e.g., `ERA.Compiled`).
#'
#' @return A list with two elements:
#' 
#' 1. `result` – A `data.table` with the following appended fields:
#'    - `Data.PS.Date` – Estimated planting start date (Date).
#'    - `Data.PE.Date` – Estimated planting end date (Date).
#'    - `Data.Date.Acc` – String describing the spatial matching accuracy level (e.g., `3-P.Sub`).
#' 2. `issues` – A `data.table` identifying spatial locations with inconsistent seasonal records (Season 1/2 vs NA).
#'
#' @export
#' @import data.table
est_pday<-function(data){
  
  data<-data.table(data)
  
  # Add cols for estimated dates
  data[,Data.PS.Date:=as.Date(NA)][,Data.PE.Date:=as.Date(NA)]
  
  # Identify records with valid planting dates, excluding irrigated trials (allow exception for Rice)
  valid_records<-unique(data[!is.na(Plant.Start) & !(Irrigation.C & Irrigation.T & Product!="Rice"),list(Plant.Start,Plant.End,Latitude,Longitude,Product.Simple,M.Year.Start,M.Year.End,Season.Start,Season.End)])
  
  # Compute summary statistics for planting dates per location
  valid_records<-valid_records[,list(PS.Mean=mean(Plant.Start,na.rm=T),
                       PS.Median=as.Date(median(Plant.Start,na.rm=T)),
                       PE.Mean=mean(Plant.End,na.rm=T),
                       PE.Median=as.Date(median(Plant.End,na.rm=T)),
                       PS.N=length(!is.na(Plant.Start))),
                 by =list(Latitude,Longitude,Product.Simple,M.Year.Start,M.Year.End,Season.Start,Season.End)]
  
  # Iterative search reducing spatial resolution (5 to 1 decimal places)
  for(i in 5:1){
    valid_records<-valid_records[!is.na(PS.Median)
    ][,LatRound:=round(Latitude,i)
    ][,LonRound:=round(Longitude,i)
    ][,Code:=paste(LatRound,LonRound,M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Simple)]
    
    # Identify inconsistencies in season recording
    valid_records[,Season.Flag:=any(is.na(Season.Start)) & any(!is.na(Season.Start)),by=list(LatRound,LonRound)]
    
    if(i==5){
      Flagged.Product<-valid_records[Season.Flag==T][,Data.Date.Acc:=paste0(i,"-P")]
    }else{
      A<-valid_records[Season.Flag==T][,Data.Date.Acc:=paste0(i,"-P")]
      Flagged.Product<-rbind(Flagged.Product,A)
    }
    
    # Exclude flagged records
    valid_records<-valid_records[Season.Flag==F]
    
    suppressWarnings(
      data[,MCode:=paste(round(Latitude,i),round(Longitude,i),M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Simple)
      ][is.na(Plant.Start) & is.na(Data.PS.Date),Data.Date.Acc:=paste0(i,"-P")
      ][is.na(Plant.Start)  & is.na(Data.PS.Date),Data.PS.Date:=valid_records$PS.Median[match(MCode,valid_records$Code)]
      ][is.na(Plant.Start)  & is.na(Data.PE.Date),Data.PE.Date:=valid_records$PE.Median[match(MCode,valid_records$Code)]]
    )
  }
  
  valid_records<-unique(data[,list(Plant.Start,Plant.End,Latitude,Longitude,M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Subtype)])
  
  # Assign estimated planting dates
  valid_records<-data[,list(PS.Mean=mean(Plant.Start,na.rm=T),
                     PS.Median=as.Date(median(Plant.Start,na.rm=T)),
                     PE.Mean=mean(Plant.End,na.rm=T),
                     PE.Median=as.Date(median(Plant.End,na.rm=T)),
                     PS.N=length(!is.na(Plant.Start))),
               by =list(Latitude,Longitude,M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Subtype)]
  
  
  for(i in 5:1){
    valid_records<-valid_records[!is.na(PS.Median)
    ][,LatRound:=round(Latitude,i)
    ][,LonRound:=round(Longitude,i)
    ][,Code:=paste(LatRound,LonRound,M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Subtype)]
    
    # If there is a mixture of with and without seasons at a location flag data and exclude
    valid_records[,Season.Flag:=any(is.na(Season.Start)) & any(!is.na(Season.Start)),by=list(LatRound,LonRound)]
    
    if(i==5){
      Flagged.Product.Subtype<-valid_records[Season.Flag==T][,Data.Date.Acc:=paste0(i,"-P")]
    }else{
      A<-valid_records[Season.Flag==T][,Data.Date.Acc:=paste0(i,"-P")]
      Flagged.Product.Subtype<-rbind(Flagged.Product.Subtype,A)
    }
    
    valid_records<-valid_records[Season.Flag==F]
    
    data[,MCode:=paste(round(Latitude,i),round(Longitude,i),M.Year.Start,M.Year.End,Season.Start,Season.End,Product.Subtype)
    ][is.na(Plant.Start)  & is.na(Data.PS.Date),Data.Date.Acc:=paste0(i,"-P.Sub")
    ][is.na(Plant.Start)  & is.na(Data.PS.Date),Data.PS.Date:=valid_records$PS.Median[match(MCode,valid_records$Code)]
    ][is.na(Plant.Start)  & is.na(Data.PE.Date),Data.PE.Date:=valid_records$PE.Median[match(MCode,valid_records$Code)]]
    
  }
  
  data[,MCode:=NULL]
  
  # Compile issues into a single report
  Flagged.Product[,EULevel:="Product.Simple"]
  Flagged.Product.Subtype[,EULevel:="Product.Subtype"]
  
  setnames(Flagged.Product,"Product.Simple","Product.Name")
  setnames(Flagged.Product.Subtype,"Product.Subtype","Product.Name")
  
  issues<-rbind(Flagged.Product,Flagged.Product.Subtype,use.names = T)
  
  Y5<-unique(data[,LatLon:=paste(Latitude,Longitude)][,list(LatLon,Code)][,Code:=paste(unique(Code),collapse = "|"),by=LatLon])
  Y4<-unique(data[,LatLon:=paste(round(Latitude,4),round(Longitude,4))][,list(LatLon,Code)][,Code:=paste(unique(Code),collapse = "|"),by=LatLon])
  Y3<-unique(data[,LatLon:=paste(round(Latitude,3),round(Longitude,3))][,list(LatLon,Code)][,Code:=paste(unique(Code),collapse = "|"),by=LatLon])
  Y2<-unique(data[,LatLon:=paste(round(Latitude,2),round(Longitude,2))][,list(LatLon,Code)][,Code:=paste(unique(Code),collapse = "|"),by=LatLon])
  Y1<-unique(data[,LatLon:=paste(round(Latitude,1),round(Longitude,1))][,list(LatLon,Code)][,Code:=paste(unique(Code),collapse = "|"),by=LatLon])
  
  issues[,LatLon5:=paste(round(Latitude,5),round(Longitude,5))]
  issues[,Code5:=Y5[match(issues[,LatLon5],LatLon),Code]]
  
  issues[,LatLon4:=paste(round(Latitude,4),round(Longitude,4))]
  issues[,Code4:=Y4[match(issues[,LatLon4],LatLon),Code]]
  
  issues[,LatLon3:=paste(round(Latitude,3),round(Longitude,3))]
  issues[,Code3:=Y3[match(issues[,LatLon3],LatLon),Code]]
  
  issues[,LatLon2:=paste(round(Latitude,2),round(Longitude,2))]
  issues[,Code2:=Y2[match(issues[,LatLon2],LatLon),Code]]
  
  issues[,LatLon1:=paste(round(Latitude,1),round(Longitude,1))]
  issues[,Code1:=Y1[match(issues[,LatLon1],LatLon),Code]]
  
  # Return processed data and flagged seasonal inconsistencies
  return(list(result=data,issues=issues))
  
}


#' Estimate Planting Dates Using Historical DOY Midpoints at Same or Nearby Sites
#'
#' This function provides fallback estimates of missing planting dates in an ERA dataset by
#' using historical planting behavior. It computes the circular (angular) mean of the day-of-year
#' midpoints from available observations (i.e. the average of \code{yday(Plant.Start)} and \code{yday(Plant.End)}) 
#' and then applies an uncertainty window to define a planting period. This method entirely
#' removes the year from the averaging process, using only the Julian days (DOY), and then converts 
#' the resultant DOY back into an actual date using the observed \code{M.Year.Start} as the reference.
#' 
#' Note, for the function's application to be maximized season names will need to be harmonized.
#'
#' Matching is performed in two steps:
#' \enumerate{
#'   \item \strong{Site History:} The function first searches for historical planting dates at the same site 
#'         (\code{Site.Key}), optionally constraining matches to the same \code{Product} (when \code{by_product = TRUE})
#'         and ensuring that the seasonal marker (\code{Season.Start}) is the same (or both missing).
#'   \item \strong{Nearby History:} If no site-specific match is found, the function iteratively searches through 
#'         increasing distance thresholds (specified by \code{max_distance_km}) and uses the available observations
#'         that meet the criteria.
#' }
#'
#' For each matching set, the function calculates:
#' \itemize{
#'   \item The circular mean of the DOY midpoints (using the \code{circular} package) to obtain a representative 
#'         Julian day.
#'   \item Uncertainty bounds by subtracting and adding \code{uncertainty_days} (with proper wrapping around the 365-day year).
#'   \item Conversion of these Julian day values to actual dates using \code{as.Date} with an origin determined 
#'         by the observed \code{M.Year.Start}.
#' }
#'
#' The new estimated fields are stored in:
#' \describe{
#'   \item{\code{Data.PS.Date}}{Estimated planting start date.}
#'   \item{\code{Data.PE.Date}}{Estimated planting end date.}
#'   \item{\code{Plant.Source}}{A character string indicating the source of the estimate, including whether it was 
#'          derived from a SiteHistory or a NearbyHistory match, the distance threshold used (if applicable), 
#'          and whether matching was restricted by product.}
#' }
#'
#' @param data A \code{data.table} containing ERA observations.
#' @param uncertainty_days Integer. The number of days to subtract from and add to the circular mean DOY to create the 
#'        planting interval (default = 14).
#' @param max_distance_km Numeric vector of increasing distance thresholds (e.g., \code{c(1, 5, 10)}). When site-based 
#'        matching fails, the function searches for nearby matches within these distance limits.
#' @param by_product Logical. If \code{TRUE}, only observations with the same \code{Product} are used for matching;
#'        if \code{FALSE}, the search is conducted regardless of product. This choice is also reflected in the \code{Plant.Source} field.
#'
#' @return The input \code{data.table} with three new columns appended:
#' \describe{
#'   \item{\code{Data.PS.Date}}{Estimated planting start date (Date).}
#'   \item{\code{Data.PE.Date}}{Estimated planting end date (Date).}
#'   \item{\code{Plant.Source}}{A character string indicating the estimation method, any product constraint, 
#'          the distance threshold (if applicable), and the uncertainty window used.}
#' }
#'
#' @import data.table
#' @importFrom terra vect distance
#' @importFrom lubridate yday year
#' @import circular
#' @export
est_pday2 <- function(data, uncertainty_days = 14, max_distance_km = c(1,10,50), by_product = TRUE,verbose=F) {
  require(data.table)
  require(geosphere)
  
  data <- data.table::copy(data)

  # Dev Note: This section could be made far more efficient by running unique on missing for Site x Product x M.Year
  known <- data[!is.na(Plant.Start) & !is.na(Plant.End)]
  missing <- data[is.na(Plant.Start) & is.na(Data.PS.Date) & !is.na(M.Year.Start)]
  
  for (i in seq_len(nrow(missing))) {
    
    if(verbose){
    cat("Processing row",i,"/",nrow(missing),"\n")
    }
    
    row <- missing[i]
    row_coords <- c(row$Longitude, row$Latitude)
    
    # Match same product if by_product is TRUE
    # Make sure same season is enforced
    if (by_product) {
      known_subset <- known[Product == row$Product & (Season.Start==row$Season.Start|(is.na(Season.Start) & is.na(row$Season.Start)))]
    } else {
      known_subset <- known[Season.Start==row$Season.Start|(is.na(Season.Start) & is.na(row$Season.Start))]
    }
    
    if(nrow(known_subset)>0){
    # 1. Try matching by Site.Key
    site_hist <- known_subset[Site.Key == row$Site.Key ]
    if (nrow(site_hist) > 0) {

      # Calculate circular mean of day-of-year midpoints
      day_midpoints <- ((yday(site_hist$Plant.Start) + yday(site_hist$Plant.End)) / 2) %% 365
      rads <- circular(2 * base::pi * day_midpoints / 365, units = "radians", modulo = "2pi")
      mean_rad <- mean(rads, na.rm = TRUE)
      mean_doy <- (as.numeric(mean_rad) * 365) / (2 * base::pi) %% 365
      
      # Circular uncertainty bounds
      ps_doy <- (mean_doy - uncertainty_days) %% 365
      pe_doy <- (mean_doy + uncertainty_days) %% 365
      
      # Convert to dates
      pe_date<-as.Date(pe_doy, origin = paste0(row$M.Year.Start - 1, "-12-31"))
      if(ps_doy>=pe_doy){
        ps_date<-as.Date(ps_doy, origin = paste0(row$M.Year.Start - 2, "-12-31"))
      }else{
        ps_date<-as.Date(ps_doy, origin = paste0(row$M.Year.Start - 1, "-12-31"))
      }
      
      # Ensure there can be no confusion with dates that traverse year end boundaries
      if (!is.na(ps_date) & row$M.Year.Start==year(ps_date)) {
   
        data[Index == row$Index, `:=`(
          Data.PS.Date = ps_date,
          Data.PE.Date = pe_date,
          Plant.Source = paste0("SiteHistory", if (by_product) "_Product" else "", " ±", uncertainty_days, "d")
        )]
        next
      }
    }
    
    # 2. Search by increasing distance bands
    for (d in max_distance_km) {
      # Create a SpatVector for known_subset coordinates
      coords_known <- known_subset[, .(Longitude, Latitude)]
      known_vect <- terra::vect(coords_known, geom = c("Longitude", "Latitude"), crs = "EPSG:4326")
      
      # Create a SpatVector for the target point using row_coords.
      # (Assuming row_coords is in the order: c(Longitude, Latitude))
      target_vect <- terra::vect(data.frame(Longitude = row_coords[1], Latitude = row_coords[2]),
                                 geom = c("Longitude", "Latitude"), crs = "EPSG:4326")
      
      # Compute distances (in meters) from the target point to all known_subset points.
      dists <- terra::distance(target_vect, known_vect) / 1000  # Convert to kilometers
      
      # Add the distances to known_subset (as a numeric vector)
      known_subset[, dist := as.numeric(dists)]
      
      nearby <- known_subset[dist <= d]
      
      if (nrow(nearby) > 0) {
        
        # Calculate circular mean of day-of-year midpoints
        day_midpoints <- ((yday(nearby$Plant.Start) + yday(nearby$Plant.End)) / 2) %% 365
        rads <- circular(2 * base::pi * day_midpoints / 365, units = "radians", modulo = "2pi")
        mean_rad <- mean(rads, na.rm = TRUE)
        mean_doy <- (as.numeric(mean_rad) * 365) / (2 * base::pi) %% 365
        
        # Circular uncertainty bounds
        ps_doy <- (mean_doy - uncertainty_days) %% 365
        pe_doy <- (mean_doy + uncertainty_days) %% 365
        
        # Convert to dates
        pe_date<-as.Date(pe_doy, origin = paste0(row$M.Year.Start - 1, "-12-31"))
        if(ps_doy>=pe_doy){
          ps_date<-as.Date(ps_doy, origin = paste0(row$M.Year.Start - 2, "-12-31"))
        }else{
          ps_date<-as.Date(ps_doy, origin = paste0(row$M.Year.Start - 1, "-12-31"))
        }
        
        if (!is.na(ps_date) & row$M.Year.Start==year(ps_date)) {

          data[Index==row$Index, `:=`(
            Data.PS.Date = ps_date,
            Data.PE.Date = pe_date,
            Plant.Source = paste0("NearbyHistory_", d, "km", if (by_product) "_Product" else "", " ±", uncertainty_days, "d")
          )]
          break
      }
    }
   }
    }
  }
  
  return(data)
}


