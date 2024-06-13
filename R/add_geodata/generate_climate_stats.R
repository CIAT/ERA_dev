# 1.1) Load Packages ####

if(!require("pacman", character.only = TRUE)){
  install.packages("pacman",dependencies = T)
}

required.packages <- c("ggplot2","circular","data.table","doSNOW","zoo","pbapply","ERAg","dismo","miceadds")
p_load(char=required.packages,install = T,character.only = T)

# 1.2) Set directories and read in ERA dataset ####

options(scipen=999)

# Set the data directory (should be /Data folder in the Git repository, otherwise please specify for your system)
setwd(paste0(getwd(),"/Data"))

# Read in compendium - it should be stored in the \CSA-Compendium\Data\Compendium Master Database folder of the Git
list.dirs(recursive = F)[grep("Comb", list.dirs(recursive = F))]

FilenameSimple<-list.dirs(recursive = F,full.names = T)[grep("Comb", list.dirs(recursive = F))][1]
grep("Wide.R",list.files(FilenameSimple),value=T)
Filename<-paste0(FilenameSimple,"/",grep("Wide.R",list.files(FilenameSimple),value=T))

# Read in Meta.Table ####
Meta.Table<-miceadds::load.Rdata2(filename=Filename)

# Set paths for save directories ####

# Create a Save Directory for Historical Climate Data:
ClimatePast<-paste0(getwd(),"/Climate/Climate Past/")
if(!dir.exists(ClimatePast)){
  dir.create(ClimatePast,recursive = T)
}

# Create a Save Directory for strange or incorrect compendium values to investigate:
ErrorDir<-paste0(FilenameSimple,"/Errors/")
if(!dir.exists(ErrorDir)){
  dir.create(ErrorDir,recursive = T)
}

# Set paths for data directories ####

# Set Crop Data Directory:
CROP_dir<-paste0(getwd(),"/Crops/")

#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# 1.3) Set analysis and plotting parameters #####

# Set date origin for meta-analysis
M.ORIGIN<-"1900-01-01"

# Set cores for parallel processing
cores<-max(1, parallel::detectCores() - 1)

ID<-"Site.Key"

# Set plotting parameters
DPI<-600
Width<-210-40 #mm 210 × 297 = A4
Type<-"png"
Units<-"mm"

# 1.4) Create season code field & turn M.Year blanks to NAs ####

Meta.Table[Season.Start==1 & Season.End==1,M.Year.Code:="1"
           ][Season.Start==2 & Season.End==2,M.Year.Code:="2"
             ][Season.Start==1 & Season.End==2,M.Year.Code:="1&2"
               ][M.Year=="",M.Year:=NA]

# 1.5) Add altitude ####

# Update missing altitudes with those estimated from DEM
Physical<-data.table(ERA_Physical)
suppressWarnings(Meta.Table[!is.na(Latitude),Altitude.DEM:=Physical[match(unlist(Meta.Table[!is.na(Latitude),..ID]),unlist(Physical[,..ID])),Altitude.mean]])

# Replace blanks
Meta.Table[,Altitude:=Elevation
           ][is.na(Altitude)|Altitude=="",Altitude:=Altitude.DEM]

# 2) Read in and merge climate datasets ####
MergeClimate=F

if(MergeClimate){
  
  # CHIRPS 
  CHIRPS<-miceadds::load.Rdata2("CHIRPS.RData",path="Large Files/")
  colnames(CHIRPS)[which(colnames(CHIRPS)=="RAIN")]<-"Rain"
  CHIRPS<-as.data.frame(CHIRPS)
  
  #POWER
  POWER<-data.table(miceadds::load.Rdata2("POWER.RData",path="Large Files/"))
  colnames(POWER)[which(colnames(POWER)=="SRad")]<-"Solar.Rad"
  
  # ADD PET TO AgMERRA 
  POWER$Altitude<-Physical[match(as.factor(unlist(POWER[,..ID])),as.factor(unlist(Physical[,..ID]))),Altitude.mean]
  POWER<-data.frame(POWER[!is.na(Altitude)][,!"Rain"])
  
  POWER.CHIRPS<-dplyr::left_join(POWER,CHIRPS[,c("DayCount",ID,"Rain")],by = c(ID,"DayCount"))
  
  POWER.CHIRPS$ETo<-PETcalc(Tmin=POWER.CHIRPS$Temp.Min,
                            Tmax=POWER.CHIRPS$Temp.Max,
                            SRad=POWER.CHIRPS$Solar.Rad ,
                            Wind=POWER.CHIRPS$WindSpeed,
                            Rain=POWER.CHIRPS$Rain,
                            Pressure=POWER.CHIRPS$Pressure,
                            Humid=POWER.CHIRPS$Humid,
                            YearDay=POWER.CHIRPS$Day,
                            Latitude=POWER.CHIRPS$Latitude,
                            Altitude=POWER.CHIRPS$Altitude)[,1]
  
  str(POWER.CHIRPS)
  POWER.CHIRPS<-data.table(POWER.CHIRPS)[,Site.Key :=as.factor(Site.Key )
  ][,Year:=as.factor(POWER.CHIRPS$Year)
  ][,Day:=as.factor(POWER.CHIRPS$Day)
  ][,Altitude:=as.integer(Altitude)]
  
  save(POWER.CHIRPS,file=paste0(ClimatePast,"POWER.CHIRPS.RData"))
  
  rm(POWER,Physical,CHIRPS)
  
}else{
  POWER.CHIRPS<-miceadds::load.Rdata2("POWER.CHIRPS.RData","Large Files/")
  CHIRPS<-miceadds::load.Rdata2("CHIRPS.RData","Large Files/")
  colnames(CHIRPS)[which(colnames(CHIRPS)=="RAIN")]<-"Rain"
}


# 2) Subset ERA data #####

# Ensure code tables are data.table format
PracticeCodes<-data.table(PracticeCodes)
OutcomeCodes<-data.table(OutcomeCodes)
EUCodes<-data.table(EUCodes)

# Subset data to crop yields of annual products with no temporal aggregation and no combined products

Meta.Table.Yields<-Meta.Table[Outcode == 101 &  # Subset to crop yield outcomes
                                !nchar(M.Year)>6 &  # Remove observations aggregated over time
                                !Product.Subtype %in% c("Fibre & Wood","Tree","Fodders","Nuts") & # Remove observations where yields are from perennial crops
                                !Product %in% c("Sugar Cane (Cane)","Sugar Cane (Sugar)","Coffee","Gum arabic","Cassava or Yuca","Pepper","Vanilla","Tea",
                                                "Plantains and Cooking Bananas","Jatropha (oil)","Palm Oil","Olives (fruits)" , "Olives (oil)",
                                                "Taro or Cocoyam or Arrowroot","Palm Fruits (Other)","Turmeric","Ginger", "Cardamom",
                                                "Apples","Oranges","Grapes (Wine)","Peaches and Nectarines","Jujube","Jatropha (seed)","Vegetables (Other)",
                                                "Herbs","Pulses (Other)","Beans (Other)","Leafy Greens (Other)","Bananas (Ripe Sweet)","Jatropha")  # Remove observations where yields are from perennial crops
                              ][!grepl(" x |-",Product),] # Remove observations where yields are from multiple products are combined

# Remove sites spatially aggregated over a large area
Meta.Table.Yields<-Meta.Table.Yields[!Buffer>50000]

# 4) Add Ecocrop ####

# When harvest dates are not reported in the studies that comprise ERA we can substitute season length values from the EcoCrop dataset. 
# First we need to merge EcoCrop data with `ERAg::AddEcoCrop` function.

ERA<-cbind(Meta.Table.Yields,ERAgON::AddEcoCrop(Products = Meta.Table.Yields[,Product]))

# 5) Refine Ecocrop cycle lengths ####

# EcoCrop data for crop cycle length estimates (how long a crop takes to grow) can have a large range, especially for crops that can be 
# grown in temperate and tropical regions. Maize, for example, as a season length of 65-365 days with midpoint 215 days, 
# this is a rather unrealistically long season for most of sub-Saharan Africa. We can mine ERA data where we have crop planting 
# and harvest dates of reasonably certainty to esimate season length and replace EcoCrop estimates.

ERA.Yields<-ERA[Out.SubInd=="Crop Yield"]

# Estimate season length from date allowing 7 days uncertainty in planting and harvest dates
A<-ERA.Yields
A<-unique(A[!(is.na(Plant.Start)|is.na(Plant.End)|is.na(Harvest.End)|is.na(Harvest.End))
][,PLANT.AVG:=Plant.Start+(Plant.End-Plant.Start)/2
][,HARVEST.AVG:=Harvest.Start+(Harvest.End-Harvest.Start)/2
][!(((Plant.End-Plant.Start)>7)|((Harvest.End-Harvest.Start)>7))
][,SLEN:=round(HARVEST.AVG-PLANT.AVG,0)
][!SLEN<30,c("Product",..ID,"Code","Variety","M.Year","PLANT.AVG","SLEN","Country")])

B<-A[,list(SLEN.mean=mean(SLEN),SLEN.med=median(SLEN),N=.N),by=c("Product")
][,SLEN.mean:=round(as.numeric(SLEN.mean),0)
][,SLEN.med:=round(as.numeric(SLEN.med),0)]

B[,SLEN.EcoC:=round(apply(ERA.Yields[match(B$Product,ERA.Yields$Product),c("cycle_min","cycle_max")],1,mean),0)]

# Replace EcoCrop values where we have >=5 observation from our data
B<-B[N>=5]

# Save estimate crop length datasets
#fwrite(B,paste0(CROP_dir,"Crop Season Lengths Estimated From Data.csv"))

# Add estimate season lengths to ERA where season length values are missing
N<-match(ERA.Yields$Product,B$Product)
N1<-which(!is.na(N))
ERA.Yields$cycle_min[N1]<-B$SLEN.med[N[N1]]
ERA.Yields$cycle_max[N1]<-B$SLEN.med[N[N1]]

ERA.Yields<-ERA.Yields[Product!=""]

# 6) Estimate unreported plantings dates using nearby data ####

# We may wish to substitute NA values of planting date in ERA with estimates derived from the `EstPDayData` function which searches for 
# planting dates for the same `Product/Product.Subtype`, year and growing season which are spatially nearby.  
# The function uses an iterative search over five levels of increasing distance corresponding to latitude and longitude recorded from
# 5 to 1 decimal places. If there is more than one planting date value for a `Product x M.Year.Start x M.Year.End x Season.Start x Season.End` 
# combination then values are averaged. If no corresponding planting dates are found matching on product then matching is attempted on 
# `Product.Subtype` (e.g. cereals or legumes).  
# If Latitude + Longitude combinations have a mixture of season = 1 or 2 and season = NA values then this suggests a potential issue with 
# the consistency of temporal recording at the spatial coordinates. Observations with such issues are excluded from the `EstPDayData` 
# analysis and flagged in an output `[[Season.Issues]]` list object.

ERA.Yields<-ERAg::EstPDayData(DATA=ERA.Yields)$DATA

# Estimate % of missing data that could be substituted using EstPDayData estimates
round(sum(!is.na(ERA.Yields$Data.PS.Date))/sum(is.na(ERA.Yields$Plant.Start))*100,2)

# Which ERA observations have high planting uncertainty?
ERA.Yields[,Plant.Diff.Raw:=as.integer(Plant.End-Plant.Start),by=list(Plant.Start,Plant.End)]

# Uncertainty needed to substitute planting dates
Uncertainty.Max<-90

# Use nearby data where other sources of data are not available
N<-as.numeric(unlist(ERA.Yields[,lapply(strsplit(Data.Date.Acc,"-"),"[[",1)]))

ERA.Yields[!is.na(Plant.Start),Plant.Source:="Pub"]

ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N>1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.Source:="Nearby 1km"]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N>1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.End:=Data.PE.Date]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N>1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.Start:=Data.PS.Date]

ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N==1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.Source:="Nearby 10km"]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N==1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.End:=Data.PE.Date]
ERA.Yields[is.na(Plant.Start) & !is.na(Data.PS.Date) & N==1 & (Plant.Diff.Raw>Uncertainty.Max|is.na(Plant.Diff.Raw)),Plant.Start:=Data.PS.Date]

# 7) Estimate season lengths where harvest dates are missing using nearby data ####
# Using similar methods to the `EstPDayData` function, the `EstSLenData` estimates season length values from similar crops and locations nearby to missing values.
ERA.Yields<-ERAg::EstSLenData(DATA=ERA.Yields)

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

MatchSOS<-function(SeasonalSOS,ID,Plant.Start,Plant.End){
  X<-SeasonalSOS[Site.Key == ID & Dekad.Start>=Plant.Start & Dekad.Start<=Plant.End]
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
                                                        ID=Site.Key[1],
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

SOSLTmatch<-function(ID,SOS.XB,SeasonalSOS,M.Year){
  M.Year<-as.numeric(M.Year)
  if(ID %in% SOS.XB & !is.na(M.Year)){
    DekadX<-SeasonalSOS[Site.Key==ID & Start.Year==M.Year,SOS]
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
           SOS2:=SOSLTmatch(ID=Site.Key[1],SOS.XB=SOS.XB,SeasonalSOS=SeasonalSOS,M.Year=M.Year[1]),
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
                              ID=ID, 
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
SLen<-function(RName,DATA,ErrorDir,EC.Diff,Exclude.EC.Diff,ID,Uncertainty.Max){
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
  SS<-unique(DATA[,c(..ID,"Code","Latitude","Longitude","EU","Product","M.Year","M.Year.Code","P.Date.Merge","SLen.EcoCrop",
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
         ID="Site.Key",
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
                      ID=ID,
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



Climate<-CalcClimate2(DATA=Meta.Table.Yields,
                ID=ID,
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


