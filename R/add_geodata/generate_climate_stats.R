# First run:
  # R/0_set_env.R
  # If you are a super-user and need to calculate from scratch or add new sites then you may also need to run:
    # R/add_geodata/chirps.R,
    # R/add_geodata/power.R
    # R/elevation.R

# 1). Set-up environment ####
  ## 1.1) Load Packages & source functions ####
  
  p_load(ggplot2,circular,data.table,zoo,pbapply,arrow,ERAg,dismo,hexbin)
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/add_ecocrop.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/est_pday.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/est_slen.R")


  ## 1.2) Read in ERA compiled dataset ####
  
  options(scipen=999)
  
  era_file<-list.files(era_dirs$era_masterdata_dir,"era_compiled",full.names = T)
  era_file<-tail(grep("parquet",era_file,value=T),1)
  
  era_data<-data.table(arrow::read_parquet(era_file))
  
   ## 1.2.1) Create season code field & turn M.Year blanks to NAs ####
  
  era_data[Season.Start==1 & Season.End==1,M.Year.Code:="1"
  ][Season.Start==2 & Season.End==2,M.Year.Code:="2"
  ][Season.Start==1 & Season.End==2,M.Year.Code:="1&2"
  ][M.Year=="",M.Year:=NA]
  
  ## 1.3) Load and merge geodata ####
    ### 1.3.1) Climate (POWER, CHIRPS) ####
    files<-list.files(era_dirs$era_geodata_dir,"power.*parquet",full.names = T,ignore.case = T)
    (files<-files[!grepl("annual|ltavg",files)])
    (files<-files[!grepl("annual|ltavg",files)])
    
    power<-arrow::read_parquet(files)
    
    files<-list.files(era_dirs$era_geodata_dir,"chirps.*parquet",full.names = T,ignore.case = T)
    (files<-files[!grepl("annual|ltavg",files)])
    (files<-files[!grepl("annual|ltavg",files)])
    
    chirps<-arrow::read_parquet(files) 
    
    # Replace rain in power with rain from chirps
    setnames(chirps,c("Rain","day_count"),c("Rain_chirps","DayCount"),skip_absent = T)
    
    power_chirps<-merge(power,chirps[,.(Site.Key,DayCount,Rain_chirps)],by=c("Site.Key","DayCount"),all.x=T,sort=F)  
    
    
    # correlation between power and chirps daily
    correlation <- cor(power_chirps$Rain, power_chirps$Rain_chirps, use = "complete.obs", method = "pearson")
    print(correlation)
    
    # Determine axis limits
    max_val <- max(c(power_chirps$Rain, power_chirps$Rain_chirps), na.rm = TRUE)  # Max of both variables
    
    ggplot(power_chirps, aes(x = Rain, y = Rain_chirps)) +
      geom_hex(bins = 30) +  # Adjust `bins` for resolution
      geom_abline(slope = 1, intercept = 0, color = "red", linetype = "dashed", size = 1) +  # 1:1 reference line
      coord_fixed(xlim = c(0, max_val), ylim = c(0, max_val)) +  # Fix axes to max value
      scale_fill_viridis_c(trans = "log", name = "Count") +  # Log scale for density
      labs(title = paste("Hexbin Plot: Rain POWER vs CHIRPS (r =", round(correlation, 2), ")"),
           x = "Rain (POWER)",
           y = "Rain (CHIRPS)") +
      theme_minimal()
    
    # Daily correlation is not great, monthly or dekadal correlations might be better.
    # CHIRPS should be superior data.
    
    # Replace POWER rain with CHIRPS rain
    power_chirps[,Rain:=Rain_chirps][,Rain_chirps:=NULL]
    power<-NULL
    chirps<-NULL
    
    ### 1.3.2) Load elevation & merge with era_data ####
    elevation<-fread(file.path(era_dirs$era_geodata_dir,"elevation.csv"))
    elevation<-elevation[variable=="elevation" & stat=="mean",.(Site.Key,value)][,value:=as.integer(value)]
    setnames(elevation,"value","Altitude.DEM")
    
    era_data<-merge(era_data,elevation,by=id_field,all.x=T,sort=F)
    
    # Replace blanks
    era_data[,Altitude:=Elevation
    ][is.na(Altitude)|Altitude=="",Altitude:=Altitude.DEM]
    
  ## 1.4) Set analysis and plotting parameters #####
  # Set cores for parallel processing
  worker_n<-max(1, parallel::detectCores() - 1)
  
  id_field<-"Site.Key"
  
  # Set plotting parameters
  DPI<-600
  Width<-210-40 #mm 210 × 297 = A4
  Type<-"png"
  Units<-"mm"
  
  
  
  
  ## 1.5) Load master codes ####
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)
  
  PracticeCodes<-master_codes$prac
  OutcomeCodes<-master_codes$out
  EUCodes<-master_codes$prod
  
  ## 1.6) Load eco crop ####
  ecocrop<-fread(file.path(era_dirs$ecocrop_dir,"ecocrop.csv"))
  
# 2) Subset ERA data #####

# Subset data to crop yields of annual products with no temporal aggregation and no combined products
perennial_subtypes<-c("Fibre & Wood","Tree","Fodders","Nuts") 
perennial_crops<-c("Sugar Cane (Cane)","Sugar Cane (Sugar)","Coffee","Gum arabic","Cassava or Yuca","Pepper","Vanilla","Tea",
                   "Plantains and Cooking Bananas","Jatropha (oil)","Palm Oil","Olives (fruits)" , "Olives (oil)",
                   "Taro or Cocoyam or Arrowroot","Palm Fruits (Other)","Turmeric","Ginger", "Cardamom",
                   "Apples","Oranges","Grapes (Wine)","Peaches and Nectarines","Jujube","Jatropha (seed)","Vegetables (Other)",
                   "Herbs","Pulses (Other)","Beans (Other)","Leafy Greens (Other)","Bananas (Ripe Sweet)","Jatropha") 
  
era_data_yields<-era_data[Outcode == 101 &  # Subset to crop yield outcomes
                                !nchar(M.Year)>6 &  # Remove observations aggregated over time
                                !Product.Subtype %in% perennial_subtypes & # Remove observations where yields are from perennial crops
                                !Product %in% perennial_crops # Remove observations where yields are from perennial crops
                              ][!grepl(" x |-",Product),] # Remove observations where yields are from multiple products are combined

# Remove sites spatially aggregated over a large area
era_data_yields<-era_data_yields[!Buffer>50000]

# 3) Add Ecocrop ####
# When harvest dates are not reported in the studies that comprise ERA we can substitute season length values from the EcoCrop dataset. 

# First we need to merge EcoCrop parameters for the crops in ERA
ecocrop_data<-add_ecocrop(crop_names = era_data_yields[,Product],ecocrop = ecocrop, EUCodes = EUCodes)
ERA<-cbind(era_data_yields,ecocrop_data)

# 4) Refine cycle lengths ####

# EcoCrop data for crop cycle length estimates (how long a crop takes to grow) can have a large range, especially for crops that can be 
# grown in temperate and tropical regions. Maize, for example, as a season length of 65-365 days with midpoint 215 days, 
# this is a rather unrealistically long season for most of sub-Saharan Africa. We can mine ERA data where we have crop planting 
# and harvest dates of reasonably certainty to esimate season length and replace EcoCrop estimates.

ERA.Yields<-ERA[Out.SubInd=="Crop Yield"]

# This script estimates crop season length based on planting and harvest dates, allowing for a 7-day uncertainty in planting and harvest periods.

season_data <- ERA.Yields

# Filter out records with missing planting or harvest dates
season_data <- unique(season_data[!(is.na(Plant.Start) | is.na(Plant.End) | is.na(Harvest.Start) | is.na(Harvest.End))
][, Planting_Avg := Plant.Start + (Plant.End - Plant.Start) / 2
][, Harvest_Avg := Harvest.Start + (Harvest.End - Harvest.Start) / 2
  # Exclude records where planting or harvest uncertainty exceeds 7 days
][!(((Plant.End - Plant.Start) > 7) | ((Harvest.End - Harvest.Start) > 7))
  
  # Calculate season length and filter out unrealistic values
][, Season_Length := round(Harvest_Avg - Planting_Avg, 0)
][!Season_Length < 30, c("Product", ..id_field, "Code", "Variety", "M.Year", "Planting_Avg", "Season_Length", "Country")])

# Aggregate season length statistics by product
season_summary <- season_data[, .(Season_Length_Mean = mean(Season_Length),
                                  Season_Length_Median = median(Season_Length),
                                  Observations = .N), by = "Product"
][, Season_Length_Mean := round(as.numeric(Season_Length_Mean), 0)
][, Season_Length_Median := round(as.numeric(Season_Length_Median), 0)]

# Add EcoCrop-based estimated season length
season_summary[, Season_Length_EcoCrop := round(apply(ERA.Yields[match(season_summary$Product, ERA.Yields$Product), c("cycle_min", "cycle_max")], 1, mean), 0)]

# Retain records where there are at least 5 observations
season_summary <- season_summary[Observations >= 5]

# Save estimated crop season lengths
fwrite(season_summary, file.path(era_dirs$era_geodata_dir, "Crop_Season_Lengths_Estimated_From_Data.csv"))

# Update ERA.Yields dataset where season length values are missing
product_match <- match(ERA.Yields$Product, season_summary$Product)
valid_indices <- which(!is.na(product_match))
ERA.Yields$cycle_min[valid_indices] <- season_summary$Season_Length_Median[product_match[valid_indices]]
ERA.Yields$cycle_max[valid_indices] <- season_summary$Season_Length_Median[product_match[valid_indices]]

# Remove empty product entries
ERA.Yields <- ERA.Yields[Product != ""]

# 5) Estimate unreported plantings dates using nearby data ####
# This script substitutes missing planting date (`Plant.Start`) values in the `ERA.Yields` dataset using estimates from the `EstPDayData` function.
 
# The `EstPDayData` function searches for planting dates based on spatial proximity, matching on `Product` or `Product.Subtype` (e.g., cereals, legumes), year, and growing season.
# The function runs an iterative search over five levels of increasing distance by reducing the precision of recorded latitude and longitude (from 5 to 1 decimal places).
# If multiple planting date values exist for the same `Product x M.Year.Start x M.Year.End x Season.Start x Season.End` combination, they are averaged.
 
# If no exact `Product` matches are found, the function attempts substitution using `Product.Subtype`.

# Handling Spatial-Temporal Inconsistencies:
# If a location (`Latitude + Longitude`) has a mixture of `Season = 1` or `Season = 2` alongside missing (`NA`) values, this suggests inconsistencies in temporal recording at that location. Such observations are excluded from estimation and flagged in the `[[Season.Issues]]` list output.

# Apply EstPDayData to Estimate Missing Planting Dates
ERA.Yields <- est_pday(data = ERA.Yields)$result

# Estimate the percentage of missing planting dates that were substituted
missing_planting_substituted <- round(sum(!is.na(ERA.Yields$Data.PS.Date)) / sum(is.na(ERA.Yields$Plant.Start)) * 100, 2)
cat("Missing data subsituted using nearby data = ",missing_planting_substituted,"%")

# Identify ERA observations with high planting date uncertainty
ERA.Yields[, Planting_Uncertainty := as.integer(Plant.End - Plant.Start), by = list(Plant.Start, Plant.End)]

# Define the maximum uncertainty threshold for substitution
Uncertainty_Max <- 90  # Days

# Extract spatial accuracy in kilometers
N <- as.numeric(unlist(ERA.Yields[, lapply(strsplit(Data.Date.Acc, "-"), "[[", 1)]))

# Label sources of planting date information
ERA.Yields[!is.na(Plant.Start), Plant.Source := "Published"]

# Substituting planting dates using spatially nearby data:
# Nearby 1km: Used when at least one other source exists (`N > 1`) and the planting uncertainty exceeds the threshold.
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N > 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.Source := "Nearby 1km"]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N > 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.End := Data.PE.Date]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N > 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.Start := Data.PS.Date]

# Nearby 10km Used when only one other source exists (`N == 1`) and the planting uncertainty exceeds the threshold.
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N == 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.Source := "Nearby 10km"]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N == 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.End := Data.PE.Date]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N == 1 & (Planting_Uncertainty > Uncertainty_Max | is.na(Planting_Uncertainty)), Plant.Start := Data.PS.Date]


# 7) Estimate season lengths where harvest dates are missing using nearby data ####
# Using similar methods to the `EstPDayData` function, the `EstSLenData` estimates season length values from similar crops and locations nearby to missing values.
ERA.Yields<-est_slen(ERA.Yields)

# Use nearby data where other sources of data are not available
N<-as.numeric(unlist(ERA.Yields[,lapply(strsplit(Data.SLen.Acc,"-"),"[[",1)]))

ERA.Yields[!is.na(SLen),SLen.Source:=paste0(Plant.Source," + Pub")]

ERA.Yields[is.na(SLen) & !is.na(Data.SLen.Acc) & N>1,SLen.Source:=paste0(Plant.Source," + Nearby 1km")]
ERA.Yields[is.na(SLen) & !is.na(Data.SLen.Acc) & N>1,SLen:=Data.SLen]

ERA.Yields[is.na(SLen) & !is.na(Data.SLen.Acc) & N==1,SLen.Source:=paste0(Plant.Source," + Nearby 10km")]
ERA.Yields[is.na(SLen) & !is.na(Data.SLen.Acc) & N==1,SLen:=Data.SLen]


# 8) Where we have no information about planting dates at all substitute SOS rain onset data ####
# based on logic provided by Chris Funk, UC Davis

SOS<-miceadds::load.Rdata2(file="Large Files/ERA_SOS.RData")

# Add Dekad to daily weather data
POWER.CHIRPS[,Dekad:=ERAg::SOS_Dekad(Date,type="year"),by=Date]
# Convert Year to Numeric
POWER.CHIRPS[,Year:=as.numeric(as.character(Year))]

# Find start dates of dekads
Dekad.Dates<-POWER.CHIRPS[,list(Dekad.Start=Date[1]),by=list(Dekad,Year)]
setnames(Dekad.Dates,c("Dekad","Year"),c("SOS","Start.Year"))

# Access Seasonal Rain onset data
SeasonalSOS<-SOS$Seasonal_SOS2
SeasonalSOS[,Season:=Season.Ordered][,EstSeasons:=Seasons]

# Merge start dates with Seasonal SOS data
SeasonalSOS<-merge(SeasonalSOS,Dekad.Dates,by=c("Start.Year","SOS"),all.x=T)

# We are matching on planting date rather than measurement season (M.Year + Season) as the planting year could be the same, before or after the planting year.
# As such we've revisited ERA records to ensure some information about planting date is present even if this is an entire year.
# There will always be some risk of error though.

# We use a maximum of 90 days uncertainty in the next section, which scans rainfall for an event that is likely to stimulate planting. Planting date will begin 
# at SOS and end at +90 days. The rules for SOS onset are not necessarily those that a farmer would use to determine planting and the next section allows you to customize the planting rules.
# So, here, where uncertainty is >90 days we will substitute onset data if a single onset date exists within the planting window provided. If >1 onset date is present NA is returned. 
# Start of Season (rainfall onset) data should  occur before Plant.Start so we will need to substract some days from Plant Start.

MatchSOS<-function(SeasonalSOS,id_field,Plant.Start,Plant.End){
  X<-SeasonalSOS[Site.Key == id_field & Dekad.Start>=Plant.Start & Dekad.Start<=Plant.End]
  if(nrow(X)>1 | length(X[,Dekad.Start])==0){
    return(as.Date(NA))
  }else{
    return(X[,Dekad.Start])
  }
}


# Recalculate Plant.Diff.Raw
ERA.Yields[,Plant.Diff.Raw:=as.integer(Plant.End-Plant.Start),by=list(Plant.Start,Plant.End)]

# Look for onset of rains date where planting uncertainty is high
# Rain onset may occur before the planting window indicated as there is usually a lag between onset of rains and planting of crops
# In that case you may want to consider subtracting days from Plant.Start and amending the logic where SOS>=Plant.Start in the subsequent section
ERA.Yields[Plant.Diff.Raw>Uncertainty.Max,SOS:=MatchSOS(SeasonalSOS = data.table::copy(SeasonalSOS),
                                                        id_field=Site.Key[1],
                                                        Plant.Start=Plant.Start[1],
                                                        Plant.End=Plant.End[1]),
           by=list(Site.Key,Plant.Start,Plant.End)]


# Over-write Plant.Start and Plant.End values where Plant.Diff>N
# Note we subtract 5 days from Plant.Start in case this is an event enough to trigger the planting threshold too
ERA.Yields[Plant.Diff.Raw>Uncertainty.Max & !is.na(SOS) & SOS>=Plant.Start & SOS<=Plant.End,Plant.Source:=paste0("SOS + ",Plant.Source)
][Plant.Diff.Raw>Uncertainty.Max & !is.na(SOS) & SOS>=Plant.Start & SOS<=Plant.End,Plant.Start:=SOS-5
][Plant.Diff.Raw>Uncertainty.Max  & !is.na(SOS) & (SOS+Uncertainty.Max)<Plant.End,Plant.End:=SOS+Uncertainty.Max]


# For sites with no planting information if they are unimodal with a seasonality that does not overlap the end of year boundary then we can use the M.Year value in the ERA data to match to the estimated onset data from CHIRPS

SOS.LT<-SOS$LTAvg_SOS3

# Subset to locations where there is 1 season, start of season is before end of season (i.e. EOS is not on the other side of the year boundary) and where EOS is not in December (where there is a risk the measurement date (harvest) could fall in the subsequent year)
SOS.XB<-SeasonalSOS[Seasons==1]
SOS.XB<-SOS.XB[,XBoundary:=any(SOS>EOS,na.rm = T),by=Site.Key][XBoundary==F,unique(Site.Key)]

Date.Lookup<-data.table(Date=seq(as.Date("1984-01-01"),as.Date("2019-12-31"),1))
Date.Lookup[,Dekad:=SOS_Dekad(Date,type="year")
][,Year:=format(Date,"%Y")]

SOSLTmatch<-function(id_field,SOS.XB,SeasonalSOS,M.Year){
  M.Year<-as.numeric(M.Year)
  if(id_field %in% SOS.XB & !is.na(M.Year)){
    DekadX<-SeasonalSOS[Site.Key==id_field & Start.Year==M.Year,SOS]
    if(length(DekadX)==0){
      X<-as.Date(NA)  
    }else{  
      X<-Date.Lookup[Year==M.Year & Dekad == DekadX,min(Date)]
    }
  }else{
    X<-as.Date(NA)
  }
  return(X)
}

ERA.Yields[is.na(Plant.Start) & !grepl("[.]",M.Year),
           SOS2:=SOSLTmatch(id_field=Site.Key[1],SOS.XB=SOS.XB,SeasonalSOS=SeasonalSOS,M.Year=M.Year[1]),
           by=list(Site.Key,M.Year)]

# Note we subtract 5 days from Plant.Start in case this is an event enough to trigger the planting threshold too
ERA.Yields[is.na(Plant.Start) & !is.na(SOS2),Plant.Source:="SOS only"
][is.na(Plant.Start) & !is.na(SOS2),Plant.End:=SOS2+Uncertainty.Max
][is.na(Plant.Start) & !is.na(SOS2),Plant.Start:=SOS2-5]

# Update SLen

# Use nearby data where other sources of data are not available
N<-as.numeric(unlist(ERA.Yields[,lapply(strsplit(Data.SLen.Acc,"-"),"[[",1)]))

ERA.Yields[grepl("SOS",Plant.Source) & !is.na(Harvest.Start),SLen.Source:=paste0(Plant.Source," + Pub")
][grepl("SOS",Plant.Source) & !is.na(Harvest.Start),SLen:=Harvest.End-(Plant.Start+(Plant.End-Plant.Start)/2)]

ERA.Yields[grepl("SOS",Plant.Source) & is.na(Harvest.Start) & N>1,SLen.Source:=paste0(Plant.Source," + Nearby 1km")]
ERA.Yields[grepl("SOS",Plant.Source) & is.na(Harvest.Start) & N==1,SLen.Source:=paste0(Plant.Source," + Nearby 10km")]

# 9) Refine uncertain planting dates using rainfall data ####
# It is not uncommon for studies in ERA to report vague dates such as "May-June" creating uncertainty around the start of the growing season.
# Where there is a large difference between `Plant.Start` and `Plant.End` we can use the `EstPDayRain` function to estimate more precise planting dates using rainfall data.  
# Planting dates are estimated for rows in `ERA.Yield` where the difference between `Plant.Start` and `Plant.End` is greater than or equal to the `Uncertainty.Min` argument and less than or equal to the `Uncertainty.Max` argument.  
# `EstPDayRain` uses the rollapply function to sum rainfall within a scanning window, planting is assumed to occur the day after summed rainfall surpasses a threshold amount.  
# For each row of `ERA.Yield` with appropriate planting uncertainty the function initially searches for rainfall events in `CHIRPS` in-between and including the corresponding `Plant.Start` and `Plant.End` dates. This temporal search period can be modified using the `DaysBefore` and `MultiplyWin` arguments. 
# `DaysBefore` extends the `Plant.Start` date backwards by a number of days. `MultiplyWin` is a proportion that multiplies the difference between `Plant.Start` and `Plant.End` dates increasing the size of the period forwards in time. 
# If `Plant.Start = 2018-01-01` and `Plant.End = 2018-01-11` the difference between them is 10 days, if `MultiplyWin = 1.5` then `10*1.5=15` and the end of the initial search window becomes `2018-01-01 + 15 = 2018-01-16`. 
# The width (in days) of the rollapply scanning window is specified by the `Width` argument and the amount of rainfall (mm) required to trigger planting within the scanning window is specified by the `Rain.Thresholds` argument.  
# Up to two additional temporal search periods after the above can be specified in days using the `Window` arguments, for each extra window added the `Width` and `Rain.Thresholds` arguments require corresponding values to added in sequence.
# If no trigger events are found in the initial scanning window then subsequent windows are searched in order.
# By setting the `Use.Data.Dates` argument to `TRUE` we are substituting `NA` planting dates in ERA with values calculated by `EstPDayData` and refining these with the `EstPDayRain` function.

ERA.Yields<-ERAg::EstPDayRain(Data=ERA.Yields, 
                              id_field=id_field, 
                              Rain.Data = POWER.CHIRPS, 
                              Rain.Data.Name = "CHIRPS", 
                              Rain.Field ="Rain", 
                              # DaysBefore: When searching for rainfall events in-between the uncertain planting start/end dates
                              # supplied in ERA.Yields extend the planting start date backwards by 2 days
                              DaysBefore = 5,
                              #  MultiplyWin: a proportion that changes the size of the difference between plant start and plant 
                              # end, 1 = no change
                              MultiplyWin = 1, 
                              # Window: add two addition temporal periods beyond the initial temporal window (as specified by plantings dates) of 14 days
                              Window = c(14,14), 
                              # Widths: We need to set a threshold for the rainfall amount in mm that triggers planting within each 
                              # window in this case there are 3 windows 1 + the 2 extra windows specified in the `Window` argument
                              Widths = c(3,3,3), 
                              # Rain.Thresholds: We need to set a threshold for the rainfall amount in mm that triggers planting in each 
                              # window if `Widths[1]=2` and `Rain.Thresholds[1]=30` then 30mm needs to fall over 2 days within the initial
                              # window of plant start to plant end dates (as modified by DaysBefore and  MultiplyWin arguments) for 
                              # planting to occur. If the threshold is not met then function iteratively goes to `Window[1]`, `Widths[2]` and 
                              # `Rain.Thresholds[2]`.
                              Rain.Thresholds = c(30,20,15),
                              # Uncertainty.Min/Uncertainty.Max: refine planting dates with uncertainty of between 7-90 days
                              Uncertainty.Min = 7, 
                              Uncertainty.Max = Uncertainty.Max, 
                              Add.Values = T, 
                              Use.Data.Dates = T)

# 10) Consolidate Dates ####
SLen<-function(RName,DATA,ErrorDir,EC.Diff,Exclude.EC.Diff,id_field,Uncertainty.Max){
  RName1<-RName
  RName<-paste0("UnC.",RName,".P.Date")
  DATA$Focus<-DATA[,..RName]
  
  # Calc planting difference
  DATA[,PDiff:=Plant.End-Plant.Start]
  
  # In case of any situations where a planting date has been estimated outside of the specified range
  DATA[Focus<Plant.Start|Focus>Plant.End,Focus:=NA]
  
  # Where difference is > max acceptable uncertainty use planting date estimated from rainfall data
  DATA[!is.na(Plant.Start),P.Date.Merge.Source:=Plant.Source]
  DATA[PDiff>=Uncertainty.Max & !is.na(Focus),P.Date.Merge.Source:=paste(Plant.Source,RName1)
  ][PDiff>=Uncertainty.Max & !is.na(Focus),P.Date.Merge:=Focus]
  
  # Calculate max season length using rainfall based estimate from EstPDayRain function
  DATA[PDiff>=Uncertainty.Max & !is.na(Focus) & !is.na(Harvest.End), SLen.Source:=paste(RName1,SLen.Source)
  ][PDiff>=Uncertainty.Max & !is.na(Focus) & !is.na(Harvest.End), SLen:=Harvest.End-Focus
  ][is.na(SLen),SLen.Source:=NA]
  
  DATA[,Focus:=NULL]
  
  # Logic for merging data sources for planting date and season length
  # 1) Use mid-point of planting date range
  # 2) Where planting dates are NA, substitute data from nearby sites - TO DO: RESTRICT TO MAX DISTANCE
  # 3) Where Slen is unknown substitute from data
  # 4) Set ecocrop seasonlength to the mean of the cycle length
  DATA[PDiff<Uncertainty.Max ,P.Date.Merge:=Plant.Start+(Plant.End-Plant.Start)/2
  ][is.na(P.Date.Merge) & !is.na(Data.PS.Date) & PDiff<Uncertainty.Max,P.Date.Merge.Source:=paste(Plant.Source,"Nearby")
  ][is.na(P.Date.Merge) & !is.na(Data.PS.Date) & PDiff<Uncertainty.Max,P.Date.Merge:=Data.PS.Date+(Data.PE.Date-Data.PS.Date)/2
  ][,SLen.EcoCrop:=(cycle_min+cycle_max)/2]
  
  DATA[,P.Date.Merge.Source:=gsub("NA ","",P.Date.Merge.Source),by=P.Date.Merge.Source]
  DATA[,SLen.Source:=gsub("NA ","",SLen.Source),by=SLen.Source]
  
  
  # Create unique dataset of sites x seasons, removing data where is there is no data on planting date or season length
  SS<-unique(DATA[,c(..id_field,"Code","Latitude","Longitude","EU","Product","M.Year","M.Year.Code","P.Date.Merge","SLen.EcoCrop",
                     "Plant.Start","Plant.End","Plant.Diff.Raw","Harvest.Start","Harvest.End","SLen","Data.SLen","Data.PS.Date","Data.PE.Date","SOS","SOS2","Topt.low", "Topt.high","Tlow", "Thigh","P.Date.Merge.Source","SLen.Source")])
  
  # Create Error Save Directory if  Required
  if(!is.na(ErrorDir)){
    if(!dir.exists(ErrorDir)){
      dir.create(ErrorDir,recursive = T)
    }
    
    Errors<-unique(SS[(abs(SLen-SLen.EcoCrop)/SLen.EcoCrop)>0.6 & !is.na(Harvest.Start),])
    
    if(nrow(Errors)>0){
      fwrite(Errors,paste0(ErrorDir,"Suspected Date Error - Season Length Over 60 Perc Diff to EcoCrop Mean - ",RName,".csv"))
    }
  }
  
  
  # If the difference between mean Ecocrop cycle length and season length derived from the data is greater that Exclude.EC.Diff proportion of the data
  # derived season length then exclude these rows.
  if(EC.Diff){
    SS<-SS[!(
      (abs(SLen-SLen.EcoCrop)/SLen.EcoCrop)>EC.Diff &
        !((is.na( SLen) & !is.na(SLen.EcoCrop))) |
        (!is.na( SLen) & is.na(SLen.EcoCrop))
    ),][!(is.na(SLen) & is.na(SLen.EcoCrop)),]
  }
  
  return(list(SS=SS,DATA=DATA))
}

# Remove any rows with blank products
ERA.Yields<-ERA.Yields[Product!=""]

ERA.Yields[Season.Start==1 & Season.End==1,M.Year.Code:="1"
][Season.Start==2 & Season.End==2,M.Year.Code:="2"
][Season.Start==1 & Season.End==2,M.Year.Code:="1&2"
][M.Year=="",M.Year:=NA]

ERA.Yields[,Plant.Start:=if(class(Plant.Start)=="Date"){Plant.Start}else{as.Date(Plant.Start,"%d.%m.%Y")}
][,Plant.End:=if(class(Plant.End)=="Date"){Plant.End}else{as.Date(Plant.End,"%d.%m.%Y")}
][,Harvest.Start:=if(class(Harvest.Start)=="Date"){Harvest.Start}else{as.Date(Harvest.Start,"%d.%m.%Y")}
][,Harvest.End:=if(class(Harvest.End)=="Date"){Harvest.End}else{as.Date(Harvest.Start,"%d.%m.%Y")}]


# Create unique Site x EU x Date combinations
SS<-SLen(RName="CHIRPS",
         DATA=data.table::copy(ERA.Yields),
         ErrorDir=NA,
         EC.Diff=F,
         Exclude.EC.Diff=0.6,
         id_field="Site.Key",
         Uncertainty.Max=20)

ERA.Yields<-SS$DATA

# Check that SLen is not very low
N<-50
SLenError<-ERA.Yields[SLen<=N]

if(nrow(SLenError)>0){
  print(paste0("ERROR: ",ERA.Yields[SLen<=N,paste(unique(Code),collapse = ", ")]," contains season lengths <",N," days"))
}

ERA.Yields<-ERA.Yields[(!SLen<=N)|is.na(SLen)]
SS<-SS$SS
SS<-SS[(!SLen<=N)|is.na(SLen)]

SS<-SS[!(is.na(SLen) & is.na(SLen.EcoCrop)) & !is.na(P.Date.Merge)]

SS[,PlantingDate:=P.Date.Merge
][,SeasonLength.Data:=SLen
][,SeasonLength.EcoCrop:=SLen.EcoCrop
][,M.Season:=M.Year.Code]

# 11) Generate Climate Variables  ####
Climate<-ERAg::CalcClimate2(Data=SS,
                      id_field=id_field,
                      CLIMATE=POWER.CHIRPS,
                      Rain.Data.Name="CHIRPS", 
                      Temp.Data.Name="POWER",
                      Rain.Windows = c(6 * 7, 4 * 7, 2 * 7, 2 * 7),
                      Rain.Window.Widths = c(3, 3, 2, 2),
                      Rain.Window.Threshold = c(30, 30, 20, 15),
                      Temp.Threshold = data.table(Threshold = c(20, 30, 35), Direction = c("lower", "higher","higher")),
                      TSeqLen = c(5, 10, 15),
                      Rain.Threshold = data.table(Threshold = c(0.1, 1, 5), Direction = c("lower", "lower","lower")),
                      RSeqLen = c(5, 10, 15),
                      ER.Threshold = c(0.5, 0.25, 0.1),
                      ERSeqLen = c(5, 10, 15),
                      LSeqLen = c(5, 10, 15),
                      PrePlantWindow = 10,
                      Win.Start = 1,
                      Do.LT.Avg = T,
                      Max.LT.Avg = 2010,
                      Do.BioClim = T,
                      Windows = data.table(Name = "Plant.1-30", Start = 1, End = 30),
                      SaveDir = "Climate Stats/",
                      ErrorDir = "Climate Stats/Errors/",
                      EC.Diff = 0.6,
                      ROUND = 5)



Climate<-CalcClimate2(DATA=era_data_yields,
                id_field=id_field,
                CLIMATE=POWER.CHIRPS,
                Rain.Data.Name="CHIRPS", 
                Temp.Data.Name="POWER",
                Rain.Windows = c(6*7,4*7,2*7,2*7),  # Before,After,After,After planting date
                Widths = c(3,3,2,2),  
                Rain.Threshold = c(30,30,20,15), 
                Win.Start=-3,
                Max.LT.Avg=2010, 
                Do.LT.Avg=T, 
                Do.BioClim=T, 
                Windows=data.table(Name="Plant.0-30",Start=1,End=30), 
                SaveDir=ClimatePast,
                ErrorDir=NA,
                Exclude.EC.Diff=F,
                EC.Diff=0.6,
                ROUND=3)    


