#' Estimate uncertain planting dates from rainfall data
#'
#' The `est_pday_rain` function can be used to estimate planting datesfrom daily rainfall where there is uncertainty as calculated by substracting `Plant.Start` from `Plant.End`.
#'
#' A daily rainfall dataset extracted for the data supplied in the `data` argument needs to be supplied to this function. In the `rain_dataset_name` argument please supply the name of the rainfall dataset (e.g. `CHIRPS`).
#' Use the `rain_field` argument to specify the name of the column containing the rainfall amount. Matching between `data` and `rain_data` uses the
#' `Daycount` and a location identity field as named using the `id_field` argument, as such the datasets must share the same point of origin for calculation
#' of `Daycount` and the same name for the `id_field` field.
#'
#' Planting dates will estimated for rows in `data` where the difference between `Plant.Start` and `Plant.End` is greater than or equal to the
#' `uncertainty_min` argument and less than or equal to the `uncertainty_max` argument.
#'
#' `est_pday_rain` uses the  \link[zoo]{rollapply} function to sum rainfall within a scanning window, planting is assumed to occur the day **after**
#' summed rainfall surpasses a threshold amount.
#'
#' For each row of `Data` with appropriate planting uncertainty the function initially searches for rainfall events in `rain_data` in-between and including the corresponding `Plant.Start` and `Plant.End` dates.
#' This temporal search period can be modified using the `days_before` and `multiply_win` arguments.`days_before` extends the `Plant.Start` date backwards by a number of days.
#' `multiply_win` is a proportion that multiplies the difference between `Plant.Start` and `Plant.End` dates increasing the size of the period forwards in time.
#' If `Plant.Start = 2018-01-01` and `Plant.End = 2018-01-11` the difference between them is 10 days, if `MultiplyWin = 1.5` then `10*1.5=15` and the
#' end of the initial search window becomes `2018-01-01 + 15 = 2018-01-16`. The width (in days) of the `rollapply` scanning window is specified
#' by the `Width`argument and the amount of rainfall (mm) required to trigger planting within the scanning window is specified by the `rain_thresholds` argument.
#'
#' Up to two additional temporal search periods after the above can be specified in days using the `window` arguments, for each extra window added `width` and
#' `rain_thresholds` arguments require values to added in sequence. If no trigger events are found in the initial scanning window then subsequent windows
#' are searched accordingly.
#
#'
#' @param data A dataset containing planting date information (e.g. `ERA.Compiled`).
#' @param id_field Column name identifying unique locations in the dataset.
#' @param rain_data A data.table containing daily rainfall data, with `Date` as a `Date` field,
#'        a numeric field for rainfall named as per the `rain_field` argument, and a location ID field matching `data`.
#' @param rain_dataset_name A string representing the name of the rainfall dataset (e.g. "CHIRPS").
#' @param rain_field Column name of the rainfall field in `rain_data`.
#' @param days_before Number of days before `Plant.Start` to extend the search window.
#' @param multiply_win Multiplier to extend `Plant.End` forward in time.
#' @param window Vector specifying up to two additional temporal search periods in days.
#' @param widths Integer vector specifying the scanning window width for rainfall summation.
#' @param rain_thresholds Integer vector specifying the amount of rainfall required within each window.
#' @param uncertainty_min Minimum planting uncertainty (difference between `Plant.Start` and `Plant.End`).
#' @param uncertainty_max Maximum planting uncertainty.
#' @param add_values If `TRUE`, appends results to `data`; otherwise, returns a separate table.
#' @param use_data_dates If `TRUE`, replaces NA values in `Plant.Start` and `Plant.End` with pre-estimated dates.
#'
#' @return A dataset with estimated planting dates appended as `paste0("UnC.", rain_dataset_name, ".P.Date")`.
#' @export
#' @import data.table
#' @importFrom zoo rollapply as.zoo
est_pday_rain <- function(data,
                        id_field,
                        rain_data,
                        rain_dataset_name,
                        rain_field,
                        days_before = 0,
                        multiply_win = 1,
                        window = c(14,14),
                        widths = c(2,3,3),
                        rain_thresholds,
                        uncertainty_min = 7,
                        uncertainty_max = 90,
                        add_values = TRUE,
                        use_data_dates = FALSE) {
  
  # Convert inputs to data.table format
  data <- data.table(data)
  rain_data <- data.table(rain_data)
  
  # Split rainfall data by site for faster lookup
  rain_split <- split(rain_data, by = id_field)
  
  # Replace missing planting dates with estimated values if enabled
  if (use_data_dates) {
    data[is.na(Plant.Start), Plant.Start := as.Date(Data.PS.Date)]
    data[is.na(Plant.End), Plant.End := as.Date(Data.PE.Date)]
  }
  
  # Convert date columns to proper Date format and calculate planting period statistics
  data[, Plant.Start := as.Date(Plant.Start, "%d.%m.%Y")]
  data[, Plant.End := as.Date(Plant.End, "%d.%m.%Y")]
  data[, Harvest.Start := as.Date(Harvest.Start, "%d.%m.%Y")]
  data[, Harvest.End := as.Date(Harvest.End, "%d.%m.%Y")]
  
  data[, Plant.Diff := as.numeric(Plant.End - Plant.Start)]
  data[, Plant.Avg := Plant.Start + Plant.Diff / 2]
  data[, Harvest.Diff := as.numeric(Harvest.End - Harvest.Start)]
  data[, Harvest.Avg := Harvest.Start + Harvest.Diff / 2]
  
  # Prepare metadata for analysis
  meta_data <- list(window = window, 
                    uncertainty_min = uncertainty_min, 
                    uncertainty_max = uncertainty_max, 
                    widths = widths, 
                    rain_thresholds = rain_thresholds, 
                    days_before = days_before)
  
  # Filter dataset for locations with uncertain planting dates
  SS <- unique(data[Plant.Diff >= uncertainty_min & Plant.Diff <= uncertainty_max, 
                    c(..id_field, "Plant.Start", "Plant.End", "Plant.Diff", "Plant.Avg")])
  #setnames(SS, id_field, "id_field")
  
  # Identify sites needing estimation
  Sites <- unique(unlist(SS[,..id_field]))
  
  # Remove sites missing rainfall data
  Missing.Sites <- setdiff(Sites, names(rain_split))
  if (length(Missing.Sites) > 0) {
    cat("Warning: Some sites are missing rainfall data\n")
    SS <- SS[!SS[,..id_field] %in% Missing.Sites]
    Sites <- setdiff(Sites, Missing.Sites)
  }
  
  # Estimate planting dates for each site
  estimated_pdate <- rbindlist(pblapply(1:length(Sites), FUN = function(i) {
    Rain <- rain_split[[Sites[i]]]
    X <- SS[unlist(SS[,..id_field]) == Sites[i]]
    
    Z <- rbindlist(lapply(1:nrow(X), FUN = function(j) {
      Y <- X[j, Plant.Start]
      Diff <- X[j, Plant.Diff]
      
      # Search for rainfall trigger events in the primary planting window
      R <- which(zoo::rollapply(zoo::as.zoo(unlist(Rain[Date >= (Y - days_before) & Date <= Y + Diff * multiply_win, ..rain_field])), 
                                width = widths[1], sum) > rain_thresholds[1])
      
      # If no event is found, extend search to additional windows
      if (length(R) == 0 && !is.na(window[1])) {
        R <- which(zoo::rollapply(zoo::as.zoo(unlist(Rain[Date >= Y + Diff * multiply_win + 1 & Date <= Y + Diff * multiply_win + window[1], ..rain_field])), 
                                  width = widths[2], sum) > rain_thresholds[2])
      }
      if (length(R) == 0 && !is.na(window[2])) {
        R <- which(zoo::rollapply(zoo::as.zoo(unlist(Rain[Date >= Y + Diff * multiply_win + window[1] + 1 & Date <= Y + Diff * multiply_win + window[1] + window[2], ..rain_field])), 
                                  width = widths[3], sum) > rain_thresholds[3])
      }
      
      # Assign estimated planting date
      R <- ifelse(length(R) > 0, Y + R[1], as.Date(NA))
      
      data.table(Rain.Start.End = sum(Rain[Date >= Y & Date <= Y + Diff, ..rain_field]), Est.PDate = R)
    }))
    
    cbind(X, Z)
  }))
  
  setnames(estimated_pdate, "Est.PDate", paste0("UnC.", rain_dataset_name, ".P.Date"))
  
  if (add_values) {
    
    cat('\r                                                                                                                                          ')
    cat('\r',"Matching Planting Stats to Data")
    flush.console()
    
    N<-!colnames(data) %in% paste0("UnC.",rain_dataset_name,"P.Date")
    data<-data[,..N]
    
    data[,R:=1:nrow(data)]
    
    data[, MCode := paste(get(id_field), Plant.Start, Plant.End, sep = "-"), by = .(get(id_field), Plant.Start, Plant.End)]
    
    A<-estimated_pdate
    A[,R:=1:nrow(A)]
    A[,MCode:=paste(get(id_field),Plant.Start,Plant.End,sep="-"),by=R]
    

    N<-match(data[,MCode],A[,MCode])
    
    data<-cbind(data,A[N,!(c(id_field,"Plant.Start","Plant.End","Plant.Diff","Plant.Avg","R","MCode")),with=F])
    
    data<-data[,!c("R","MCode","Plant.Diff","Harvest.Diff","Plant.Avg","Harvest.Avg")]
    
    return(data)
  } else {
    return(estimated_pdate)
  }
}
