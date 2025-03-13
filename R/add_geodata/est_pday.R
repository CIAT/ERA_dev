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
  
  # Remove existing estimated planting date fields if present
  if(any(c("Data.PS.Date","Data.PE.Date") %in% colnames(data))){
    data<-data[,!c("Data.PS.Date","Data.PE.Date")]
  }
  
  # Convert planting dates to Date format and initialize new fields
  data[,Plant.Start:=as.Date(Plant.Start,"%d.%m.%Y")
  ][,Plant.End:=as.Date(Plant.End,"%d.%m.%Y")
  ][,Data.PS.Date:=as.Date(NA)][,Data.PE.Date:=as.Date(NA)]
  
  # Identify records with valid planting dates, excluding irrigated trials
  valid_records<-unique(data[!is.na(Plant.Start) & !(Irrigation.C & Irrigation.T),list(Plant.Start,Plant.End,Latitude,Longitude,Product.Simple,M.Year.Start,M.Year.End,Season.Start,Season.End)])
  
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
