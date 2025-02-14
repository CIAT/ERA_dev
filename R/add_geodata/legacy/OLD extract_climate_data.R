##########################################################
# ERA Climate Data Download
# 
# Input: Compiled era dataset
# Output: Climate information compiled for each era site
#
# Author: p.steward@cgiar.org
# 07 Aug 2018
# Last edited: 12 June 2024
##########################################################


##############################################################
# 0) Load packages and create functions #####


  required.packages <- c("data.table","doSNOW","ERAg","ERAgON","miceadds","ncdf4","raster","RCurl","rgeos","sp","terra")
  p_load(char=required.packages,install = T,character.only = T)

  curlSetOpt(timeout = 190)
  options(scipen=999)
  
  # Misc functions ####
      is.leapyear=function(year){
        return(((year %% 4 == 0) & (year %% 100 != 0)) | (year %% 400 == 0))
      }
    
    # Round numeric columns in data.frame
      round_df <- function(x, digits) {
        # round all numeric variables
        # x: data frame 
        # digits: number of digits to round
        numeric_columns <- sapply(x, mode) == 'numeric'
        x[numeric_columns] <-  round(x[numeric_columns], digits)
        x
      }

# 1) Set directories####

  # Create a Temporary Directory for Data Processing:
     TempDir<-"temp"
     if(!dir.exists(TempDir)){
       dir.create(TempDir,recursive = T)
     }

    ErrorDir<-era_dirs$era_error_dir

    # Set paths for data directories ####
   
  # Location of climate datasets:
     DataRoot<- "C:/Datasets/"
 
  # Set Climate Data Directories:
    AgMERRA_dir<-paste0(DataRoot,"AgMERRA/AgMERRA/") #1981 - 2010 (agroclim)
    CHIRPS_dir<-paste0(DataRoot,"CHIRPS/") # 1988 to present (precip)

  # Set Crop Data Directory:
    CROP_dir<-paste0(getwd(),"/Crops/")

# 2) Set analysis parameters #####
    # Maximum allowable distance of weather station data from compendium observation for station data to be included
      MDist<-20  # All station variables other than pressure (units = km)
      MDistP<-50 # Pressure only (units = km)
      
    # Maximum altitudinal difference between weather station and compendium observation for station data to be included
      MAltD<-250 #(units = m)
      
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
    
  #<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
# 3) Load and prepare era data ####
    # Read in ERA
    Meta.Table<-data.table(arrow::read_parquet(file.path(era_dirs$era_masterdata_dir,"era.compiled.parquet")))
    
  # 3.1) Clean M.Year field #####
      Meta.Table[,M.Year:=gsub(":",".",M.Year)]
      X<-strsplit(Meta.Table$M.Year,"[.]")
      # Add in seasons
      Season<-rep(NA,length(X))
      Season[unlist(lapply(X,FUN=function(x)(!is.na(match("1",x)))))]<-"1"
      Season[unlist(lapply(X,FUN=function(x)(!is.na(match("2",x)))))]<-"2"
      Season[unlist(lapply(X,FUN=function(x)(!is.na(match("1",x)) & !is.na(match("2",x)))))]<-"1&2"
      
      MinYear<-unlist(lapply(X,FUN=function(x){min(as.numeric(x[nchar(x)==4]))}))
      MinYear[is.infinite(MinYear)]<-NA
      MaxYear<-unlist(lapply(X,FUN=function(x){min(as.numeric(x[nchar(x)==4]))}))
      MaxYear[is.infinite(MaxYear)]<-NA
      
      Meta.Table$M.Year.Start<-MinYear
      Meta.Table$M.Year.End<-MaxYear
      Meta.Table$M.Year.Code<-Season
      
      Meta.Table[M.Year=="",M.Year:=NA]
    
  # 3.2) Add altitude #####
  Physical<-data.table(ERAgON::ERA_Physical)
  # Read in Altitudes previously estimated from DEMS (see CSA-Soils.R) - NA values at this stage probably come from sites excluded due to location issues
  setnames(Physical,"Altitude.mean","Altitude.DEM")
  Meta.Table<-merge(Meta.Table,Physical[,list(Site.Key,Altitude.DEM)],by="Site.Key",all.x=T)
      
  # Average across different reported altitudes
  Meta.Table[,Elevation:=as.numeric(unlist(lapply(strsplit(Elevation,"[.][.]"),FUN=function(X){mean(as.numeric(X),na.rm=T)}))),by=Elevation]
  
  # Set overall altitude
  Meta.Table[,Altitude:=Altitude.DEM]
  Meta.Table[is.na(Altitude),Altitude:=Elevation]
  Meta.Table[,Altitude:=mean(Altitude),by=Site.Key]
  
  #<><><><><><><><><><><><><><><><><><<><><><><><><><><><><><><><><><><><><>
# 2) CLIMATE EXTRACTION FUNCTIONS ####
  # 2.1) TERRACLIMATE (depreciated) #### 
  
  # https://www.nature.com/articles/sdata2017191#data-records
  # http://www.climatologylab.org/uploads/2/2/1/3/22133936/terraclimate_downloadv2.r
  
  
  # variable names: http://thredds.northwestknowledge.net:8080/thredds/terraclimate_aggregated.html
    # aet (Actual Evapotranspiration) 
    # def (Climate Water Deficit) 
    # pet (Potential evapotranspiration) 
    # ppt (Precipitation) 
    # q (Runoff) 
    # soil (Soil Moisture) 
    # srad (Downward surface shortwave radiation) 
    # swe (Snow water equivalent) 
    # tmax (Max Temperature) 
    # tmin (Min Temperature) 
    # vap (Vapor pressure) 
    # ws (Wind speed) 
    # vpd (Vapor Pressure Deficit) 
    # PDSI (Palmer Drought Severity Index) 
  
  Variables<-c("aet","def","pet","PDSI","soil")
  
  # Create data.frame of unique locations
  SS<-unique(Meta.Table[!is.na(Latitude),c("Latitude","Longitude","Buffer",..ID)])
  
  # Filter out observations with any NA values
  SS<-SS[!(is.na(Latitude)|is.na(Longitude)|is.na(Buffer)|is.na(ID))]
  
  pbuf<-Pbuffer(SS,ID,Projected=F)

  TERRA<-rbindlist(lapply(1:length(Variables),FUN=function(j){

    var<-Variables[j]
    
    baseurlagg <- paste0(paste0("http://thredds.northwestknowledge.net:8080/thredds/dodsC/agg_terraclimate_",var),"_1958_CurrentYear_GLOBE.nc")
    nc <- open.nc(baseurlagg)
    lon <- var.get.nc(nc, "lon")
    lat <- var.get.nc(nc, "lat")

    
    
    TerraDir<-paste0(ClimatePast,"TERRA/",var)
    if(!dir.exists(TerraDir)){
      dir.create(TerraDir,recursive = T)
    }
    
    X<-rbindlist(lapply(1:nrow(SS),FUN=function(i){
      
      Filename<-paste0(TerraDir,"/",SS[i,..ID],".RData")
      
      if(!file.exists(Filename)){
      
        cat('\r                                                                                                                                          ')
        cat('\r',paste0("Downloading & Saving Variable: ",var," ",j,"/",length(Variables)," | Site: ",i,"/",nrow(SS)))
        flush.console()
      
        BB<-bbox(pbuf[i])
        #Enter lat and lon ranges
        lat.range<-sort(c(BB[2,1],BB[2,2]))  
        lon.range<-sort(c(BB[1,1],BB[1,2]))
        
        p.xmin <- which(abs(lon - BB[1,1]) == min(abs(lon - BB[1,1])))
        p.xmax <- which(abs(lon - BB[1,2]) == min(abs(lon - BB[1,2])))
        p.ymin <- which(abs(lat - BB[2,1]) == min(abs(lat - BB[2,1])))
        p.ymax <- which(abs(lat - BB[2,2]) == min(abs(lat - BB[2,2])))
   
        lat.index <-sort(p.ymin:p.ymax) 
        lon.index <- sort(p.xmin:p.xmax)  
        lat.n <- length(lat.index)                               
        lon.n <- length(lon.index)
        start <- c(lon.index[1], lat.index[1], 1)
        count <- c(lon.n, lat.n, NA)                            #! parameter change: 'NA' instead of '-1' to signify entire dimension
        
        # read in the full period of record using aggregated files
        
        data <-var.get.nc(nc, variable = var,start = start, count,unpack=TRUE)   
        
        # process data
        if(!(lat.n+lon.n)==2){
          
          data<-data.table(
          Mean=round(apply(data,length(dim(data)),mean,na.rm=T),2),
          Median=apply(data,length(dim(data)),median,na.rm=T),
          SD=round(apply(data,length(dim(data)),sd,na.rm=T),2),
          N=apply(data,length(dim(data)),FUN=function(i){sum(!is.na(i))})
          )

        }else{
          
          data<-data.table(Mean=data)
          data[,Median:=NA]
          data[,SD:=NA]
          data[,N:=1]
          
        }
        
          Site<-SS[i,..ID]
          data[,ID:=Site]
          data[,Variable:=var]
          data[,Year:=rep(1958:(1958+nrow(data)/12-1),each=12)]
          data[,Month:=rep(1:12,nrow(data)/12)]  
          save(data,file=Filename)
        
      }else{
        cat('\r                                                                                                                                          ')
        cat('\r',paste0("Loading Variable:",var," ",j,"/",length(Variables)," | Site: ",i,"/",nrow(SS)))
        flush.console()
        
        data<-load(paste0(TerraDir,"/",SS[i,..ID],".RData")) 
      }
      
      data
    }))
    X
  
  }))

  TERRA[,LT.Mean:=round(mean(Mean),3),by=list(ID,Variable,Month)]
  TERRA[,LT.sd:=round(sd(Mean,na.rm=T),3),by=list(ID,Variable,Month)]
  TERRA[,Deviance:=Mean-LT.Mean]
  
  save(TERRA,file=paste0(ClimatePast,"/TERRA/TERRA.RData"))
  
  # 2.2) AgMERRA (depreciated?) ####
     if(F){
    
     Meta.Table[,Site.Key:=Site.ID]
     
     system.time(AgMERRA<-ERAgON::ExtractAgMERRA(DATA=Meta.Table[Buffer<50000],
                                         ID="Site.Key",
                                         AgMERRA_dir=paste0(DataRoot,"AgMERRA/AgMERRA/"),
                                         TempDir=TempDir,
                                         Save_Dir= "Large Files/",
                                         cores=4,
                                         Years=c(1980,2010),
                                         M.ORIGIN=M.ORIGIN))
     }
    
  # 2.3) POWER ####

    # 2.3.1) Set Parameters ####
    # Need to incorporate correction for PS to PSC - so altitude data from physical layer needs to be read in here
    Parameters<-paste0(
      "ALLSKY_SFC_SW_DWN,", # Insolation Incident on a Horizontal Surface - MJ/m^2/day
      "PRECTOT,", # Precipitation - mm day-1
      #"PSC,", # Corrected Atmospheric Pressure (Adjusted For Site Elevation) *(see Site Elevation section below) - kPa - Throws an error!
      "PS,", # Surface Pressure - kPa
      "QV2M,", # Specific Humidity at 2 Meters - kg kg-1
      "RH2M,", # Relative Humidity at 2 Meters - %
      "T2M,", # Temperature at 2 Meters - C
      "T2M_MAX,", # Maximum Temperature at 2 Meters - C
      "T2M_MIN,", # Minimum Temperature at 2 Meters - C
      "WS2M" # Wind Speed at 2 Meters - m/s
    )
    
    # 2.3.2) Run function ####
    curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
    
  system.time(
    POWER<-ERAgON::ExtractPOWER(Data=data.table::copy(Meta.Table[Buffer<100000]),
                                  ID="Site.Key",
                                  Parameters=c("ALLSKY_SFC_SW_DWN", "PRECTOT", "PS","QV2M","RH2M","T2M","T2M_MAX","T2M_MIN","WS2M"),
                                  Rename= c("SRad","Rain","Pressure","Specific.Humid","Humid","Temp.Mean","Temp.Min","Temp.Max","WindSpeed"),
                                  StartDate="1984-01-01",
                                  EndDate="2019-12-31",
                                  Save_Dir="Climate/Climate Past/POWER",
                                  PowerSave = "Climate/Climate Past/POWER/Downloads",
                                  Delete=F,
                                  MaxBuffer=240000,
                                  AddDate=T,
                                  AddDayCount=T,
                                  Origin = M.ORIGIN,
                                  Quiet=T)
    )
    
    
    
    # 2.3.5) Substitute missing Srad values ####
    

      # If missing Srad values these can be substituted from AgMERRA data (1980-2010):
    if(POWER[,sum(is.na(SRad))]>0){
      
      # 2.3.5.1) Use AgMERRA values ####
      if(!exists("AgMERRA")){
        AgMERRA<-data.table(load("Data/Large Files/AgMERRA.RData"))
      }else{
        AgMERRA<-data.table(AgMERRA)
      }
  
      N<-POWER[,which(is.na(SRad))]
      M<-match(paste(unlist(POWER[N,..ID]),POWER[N,DayCount]),paste(unlist(AgMERRA[,..ID]),AgMERRA$DayCount))
      NN<-which(!is.na(M))
      POWER[N[NN],SRad:=AgMERRA[M[NN],Solar.Rad]]
      POWER[N[NN],SRad.Sub:="AgMERRA"]
      rm(N,NN)
      
      # 2.3.5.2) Use nearby values with similar rainfall amounts ####
      # Substitute missing values with nearby values with similar rainfall
      R<-ERAgON::ReplaceSRAD(SRad = POWER[,SRad],Rain = POWER[,Rain],SeqLen=3)
      
      POWER[is.na(SRad) & !is.na(R),SRad.Sub:="Nearby"]
      POWER[,SRad:=R]
      
      rm(R)
      
      # Check that there are no values<0
      hist(POWER$SRad)
      
      }
    
    # 2.3.6) Calculate ETo ####
      # SI unit details for PET calculations:
        # ALLSKY_SFC_SW_DWN = Insolation Incident on a Horizontal Surface - MJ/m^2/day
        # PRECTOT = Precipitation - mm day-1
        # PS = Surface Pressure - kPa
        # QV2M = Specific Humidity at 2 Meters - kg kg-1
        # RH2M = Relative Humidity at 2 Meters - %
        # T2M = Temperature at 2 Meters - C
        # T2M_MAX = Maximum Temperature at 2 Meters - C
        # T2M_MIN = Minimum Temperature at 2 Meters - C
        # WS2M = Wind Speed at 2 Meters - m/s
      
    POWER[,ETo:=ERAg::PETcalc(Tmin=Temp.Min,
                      Tmax=Temp.Max,
                      SRad=SRad,
                      Wind=WindSpeed,
                      Rain=Rain,
                      Pressure=Pressure,
                      Humid=Humid,
                      YearDay=Day,
                      Latitude=Latitude,
                      Altitude=Altitude)[,1]]
  
    # 2.3.7) Save updated POWER dataset ####

    save(POWER,file="Large Files/POWER.RData")
  
    # 2.3.8) Calculate annual averages ####
    
    # Load POWER
        if(!exist(POWER)){
          POWER<-data.table(load(paste0(ClimatePast,"POWER","/POWER.RData")))
        }
    
      # 2.3.8.1) Remove incomplete years ####
      POWER<-POWER[!Year==POWER[Site.Key==POWER$Site.Key[1],list(N=.N),by=Year][N<365,Year]]
      
      # 2.3.8.2) Calculate annual data ####
      POWER.Annual<-POWER[,list(Total.Rain=sum(Rain),
                                Total.ETo=sum(ETo),
                                S.Humid.Mean=mean(Specific.Humid),
                                Humid.Mean=mean(Humid),
                                Temp.Mean.Mean=mean(Temp.Mean),
                                Mean.N.30.Days=sum(Temp.Mean>30),
                                Temp.Max.Mean=mean(Temp.Max),
                                Temp.Max=max(Temp.Max),
                                Max.N.40.Days=sum(Temp.Max>40),
                                Temp.Min.Mean=mean(Temp.Min),
                                Temp.Min=min(Temp.Min)),by=c("Site.Key","Year")]
      
      # 2.3.8.3) Calculate LT Data ####
      POWER.LT<-POWER.Annual[,list(Total.Rain.Mean=mean(Total.Rain),
                                   Total.ETo.Mean=mean(Total.ETo),
                                   S.Humid.Mean=mean(S.Humid.Mean),
                                   Humid.Mean=mean(Humid.Mean),
                                   Temp.Mean.Mean=mean(Temp.Mean.Mean),
                                   Mean.N.30.Days=mean(Mean.N.30.Days),
                                   Temp.Max.Mean=mean(Temp.Max.Mean),
                                   Temp.Max=mean(Temp.Max),
                                   Max.N.40.Days=mean(Max.N.40.Days),
                                   Temp.Min.Mean=mean(Temp.Min.Mean),
                                   Temp.Min.Mean=mean(Temp.Min),
                                   Total.Rain.sd=sd(Total.Rain),
                                   Total.ETo.sd=mean(Total.ETo),
                                   S.Humid.Mean.sd=sd(S.Humid.Mean),
                                   Humid.Mean.sd=sd(Humid.Mean),
                                   Temp.Mean.Mean.sd=sd(Temp.Mean.Mean),
                                   Mean.N.30.Days.sd=sd(Mean.N.30.Days),
                                   Temp.Max.Mean.sd=sd(Temp.Max.Mean),
                                   Temp.Max.sd=sd(Temp.Max),
                                   Max.N.40.Days.sd=sd(Max.N.40.Days),
                                   Temp.Min.Mean.sd=sd(Temp.Min.Mean),
                                   Temp.Min.sd=sd(Temp.Min)
      ),by=c("Site.Key")]
          
      # Round results
      POWER.Annual[,3:ncol(POWER.Annual)]<-round(POWER.Annual[,-c(1:2)],3)
      POWER.LT[,2:ncol(POWER.LT)]<-round(POWER.LT[,-c(1)],3)
      
      # 2.3.8.4) Save Annual & LT datasets ####
      save(POWER.Annual,file=paste0(ClimatePast,"POWER/POWER Annual.RData"))
      save(POWER.LT,file=paste0(ClimatePast,"POWER/POWER.LT.RData"))
      
  # 2.4) CHIRPS (Precip) & CHIRTS (Tmax & TMin) 01/01/1981 - Present (Precip) 0.05 degrees ####
    
      if(F){ # Not yet implemented
        # Download CHIRTS ####
       # https://data.chc.ucsb.edu/products/CHIRTSdaily/
       # https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/africa_netcdf_p05/
       
       CHIRTS_Dir<-"C:/Datasets/CHIRTS/"
       
       DownloadCHIRTs<-function(StartYear,EndYear,SaveDir,TMax,TMin,HeatIndex,AFNET,GlobalDaily){
         
         numberOfDays <- function(date) {
           m <- format(date, format="%m")
           
           while (format(date, format="%m") == m) {
             date <- date + 1
           }
           
           return(as.integer(format(date - 1, format="%d")))
         }
         
         
         if(StartYear<1983){StartYear<-1983}
         if(EndYear>2016){EndYear<-2016}
         
         for(i in StartYear:EndYear){ # MinYear:MaxYear (Min = 1983 Max = Present)
           if(!dir.exists(paste0(SaveDir,"i"))){
             dir.create(paste0(SaveDir,i),recursive=T)
           }
           
           if(AFNET & TMax){
             URL<-paste0("https://data.chc.ucsb.edu/products/CHIRTSdaily/africa_netcdf_p25/Tmax.",i,".nc")
             destfile<-paste0(SaveDir,"Tmax.",i,".nc")
             if(!file.exists(destfile)){
               download.file(URL, destfile)
             }
           }
           
           if(AFNET & TMin){
             URL<-paste0("https://data.chc.ucsb.edu/products/CHIRTSdaily/africa_netcdf_p25/Tmin.",i,".nc")
             destfile<-paste0(SaveDir,"Tmin.",i,".nc")
             if(!file.exists(destfile)){
               download.file(URL, destfile)
             }
           }
           
           
           for(j in 1:12){
             MDays<-numberOfDays(as.Date(paste0(i,"-",j,"-01")))
             for(k in 1:MDays){
               kk<-if(k<10){paste0(0,k)}else{k}
               jj<-if(j<10){paste0(0,j)}else{j}
               
               if(GlobalDaily & TMin){
                 URL<-paste0("https://data.chc.ucsb.edu/products/CHIRTSdaily/global_tifs_p05/Tmin-Tmax/",i,"/Tmin.",i,".",jj,".",kk,".tif")
                 destfile<-paste0(SaveDir,i,"/Tmin.",i,".",jj,".",kk,".tif")
                 if(!file.exists(destfile)){
                   download.file(URL, destfile)
                 }}
               
               if(GlobalDaily & TMax){
                 URL<-paste0("https://data.chc.ucsb.edu/products/CHIRTSdaily/global_tifs_p05/Tmin-Tmax/",i,"/Tmax.",i,".",jj,".",kk,".tif")
                 destfile<-paste0(SaveDir,i,"/Tmax.",i,".",jj,".",kk,".tif")
                 if(!file.exists(destfile)){
                   download.file(URL, destfile)
                 }}
               
               if(GlobalDaily & HeatIndex){
                 URL<-paste0("https://data.chc.ucsb.edu/products/CHIRTSdaily/global_tifs_p05/HeatIndex/",i,"/HeatIndex.",i,".",jj,".",kk,".tif")
                 destfile<-paste0(SaveDir,i,"/HeatIndex.",i,".",jj,".",kk,".tif")
                 if(!file.exists(destfile)){
                   download.file(URL, destfile)
                 }}
               
               
             }}}
       }
       
       DownloadCHIRTs(StartYear=1983,EndYear=2019,SaveDir=CHIRTS_Dir,TMax=T,TMin=F,HeatIndex = F,AFNET=T,GlobalDaily = F)
       
       # Africa netcdf files
       FILES<-data.table(File=list.files(path = CHIRTS_Dir,".nc",full.names = T))
       FILES[,Year:=as.numeric(as.character(substr(FILES[,File],25,28)))]
       
       fname<-FILES[1,File]
       
       find_open<-nc_open(fname)
       find_lat <- ncvar_get(find_open, "latitude", start = c(min_lat), count = c(min_lat_c)) 
       find_lon <- ncvar_get(find_open, "longitude", start = c(min_lon), count = c(min_lon_c))
       nc_close(find_open)
      }
  
   # Download CHIRPS ####
    
      DownloadCHIRPS<-function(StartYear=1980,EndYear=2021,EndDay=136,SaveDir,quiet){
        
        URLmaster<-"https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05/"
        
        
        if(!dir.exists(SaveDir)){
          dir.create(SaveDir,recursive=T)
        }
        
        
        for(YEAR in StartYear:EndYear){ # MinYear:MaxYear (Min = 1983 Max = Present)
          
          if(YEAR==EndYear){
            ENDDAY<-EndDay
          }else{
            ENDDAY<-as.numeric(format(as.Date(paste0(YEAR,"-12-31")),"%j"))
          }
          
          for(DAY in 1:ENDDAY){
            
            
            if(quiet){
              # Display progress
              cat('\r                                                                                                                                          ')
              cat('\r',paste0("Downloading file: ",DAY,"/",YEAR))
              flush.console()
            }
            
            
            DATE<-as.Date(paste0(YEAR,"-",DAY),format="%Y-%j")
            
            DAY<-format(DATE,"%d")
            MONTH<-format(DATE,"%m")
            
            FILE<-paste0("chirps-v2.0.",YEAR,".",MONTH,".",DAY,".tif.gz")
            
            URL<-paste0(URLmaster,YEAR,"/",FILE)
            destfile<-paste0(SaveDir,"/",FILE)
            if(!file.exists(destfile)){
              download.file(URL, destfile,quiet=quiet)
            }
          }
        }
      }
      
      DownloadCHIRPS(StartYear=2019,EndYear = 2021,EndDay=365,SaveDir = "D:/GIS Resources/Climate/CHIRPS",quiet=T )

      
  # Restructure CHIRPS ####
      # slow the first time, but speeds things up in the long run.
  
  if(F){
    ERAgON::ReformatCHIRPS(CHIRPS_dir="D:/GIS Resources/Climate/CHIRPS",
                           Save_dir="C:/Datasets/CHIRPS/")
    }
    
 
  # Extract CHIRPS ####
  if(F){
  # Old faster method, has potential bug so do not use
  system.time(CHIRPS<-ERAgON::ExtractCHIRPS(Data=Meta.Table[Buffer<50000],
                                    ID="Site.Key",
                                    CHIRPS_dir="C:/Datasets/CHIRPS/",
                                    Save_Dir="Large Files/",
                                    YStart=1981,
                                    YEnd=2019,
                                    Round=2,
                                    Origin=M.ORIGIN)) 
  }

    # Slower more reliable method using terra::extract
    # If parallel can be added then speeds can be increased
    # see https://github.com/rspatial/terra/issues/36 using pack and unpack functions will make serializable SpatRaster and SpatVector objects
      
    FileDirectory<-"D:/datasets/chirps/raw"
      
    SS<-unique(Meta.Table[!(is.na(Latitude)|is.na(Longitude)|is.na(Buffer)|Buffer>50000),list(Latitude,Longitude,Buffer,Site.Key)])
    ExtractBy<-vect(Pbuffer(Data=SS,ID="Site.Key",Projected = T))
    
    # Generate names for .tif files in CHIRPS directory, in this case the files represent dates
    Dates<-list.files(FileDirectory)
    Dates<-as.Date(gsub("[.]tif","",gsub("chirps-v2[.]0[.]","",Dates)),format="%Y.%m.%d")
      
    CHIRPS<-ERAgON::ExtractFolder(FileDirectory=FileDirectory,
                          MaxChunkSize=100,
                          ExtractBy=ExtractBy,
                          ID.Target=Dates,
                          ID.Target.Name="Date",
                          ID.ExtractBy=SS[,Site.Key],
                          ID.ExtractBy.Name="Site.Key",
                          Function = mean,
                          value.name="Rain",
                          NAValue=-9999,
                          Round=4)
    
    CHIRPS[,DayCount:=as.integer(floor(unclass(Date-as.Date(M.ORIGIN)))),by=Date]
    
    
    save(CHIRPS,file="Large Files/CHIRPS.RData")
    
  
  # Calculate Annual & LT Data ####  
  # Load CHIRPS
  CHIRPS<-miceadds::load.Rdata2("CHIRPS.RData",path="Large Files")

  CHIRPS[,Year:=format(Date,"%Y"),by=Date]
  
  # Remove incomplete years
  if(length(CHIRPS[Site.Key==CHIRPS$Site.Key[1],list(N=.N),by=Year][N<365,Year])>0){
    CHIRPS<-CHIRPS[!Year==CHIRPS[Site.Key==CHIRPS$Site.Key[1],list(N=.N),by=Year][N<365,Year]]
  }
  
  # Calculate annual data 
  CHIRPS.Annual<-CHIRPS[,list(Total.Rain=sum(Rain)),by=c("Site.Key","Year")]
  
  # Calculate LT Data
  CHIRPS.LT<-CHIRPS.Annual[,list(Total.Rain=mean(Total.Rain),Total.Rain.sd=sd(Total.Rain)),by=c("Site.Key")]
  
  # Round results
  CHIRPS.Annual[,3:ncol(CHIRPS.Annual)]<-round(CHIRPS.Annual[,-c(1:2)],3)
  CHIRPS.LT[,2:ncol(CHIRPS.LT)]<-round(CHIRPS.LT[,-c(1)],3)
  
  # Save results
  save(CHIRPS.Annual,file=paste0(ClimatePast,"CHIRPS/CHIRPS Annual.RData"))
  save(CHIRPS.LT,file=paste0(ClimatePast,"CHIRPS/CHIRPS.LT.RData"))

  
  # 2.5) Combine POWER and CHIRPS ####

  POWER<-load("Large Files/POWER.RData","")
  CHIRPS<-load("Large Files/CHIRPS/CHIRPS.RData")

  POWER[,Rain:=NULL]
  POWER.CHIRPS<-merge(POWER,CHIRPS[,list(Site.Key,DayCount,Rain)],by=c("Site.Key","DayCount"))

  save(POWER.CHIRPS,file="Large Files/POWER.CHIRPS.RData")
  
  
  # 2.6) Downloads Hobbins RefET ####
  
  SaveDir<-"C:/Datasets/HobbinsRefET"
  
  DownloadRefET<-function(StartYear=1980,EndYear=2021,EndDay=136,SaveDir,quiet=T){
    
    URLmaster<-"https://data.chc.ucsb.edu/products/Hobbins_RefET/ETos_p05_dekad_global/tifs/"
    
    
    if(!dir.exists(SaveDir)){
      dir.create(SaveDir,recursive=T)
    }
    
    
    for(YEAR in StartYear:EndYear){ # MinYear:MaxYear (Min = 1983 Max = Present)
      
      if(YEAR==EndYear){
        ENDDAY<-EndDay
      }else{
        ENDDAY<-as.numeric(format(as.Date(paste0(YEAR,"-12-31")),"%j"))
      }
      
      for(DAY in 1:ENDDAY){
        
        if(quiet){
          # Display progress
          cat('\r                                                                                                                                          ')
          cat('\r',paste0("Downloading file: ",DAY,"/",YEAR))
          flush.console()
        }
        
        
        
        if(DAY<10){
          DAY<-paste0("0",as.character(DAY))
        }
        
        FILE<-paste0("ETos_p05.",YEAR,DAY,".tif")
        
        URL<-paste0(URLmaster,FILE)
        destfile<-paste0(SaveDir,FILE)
        if(!file.exists(destfile)){
          download.file(URL, destfile,quiet=quiet)
        }
      }
    }
  }
  
# 2.5) WorldClim 2 - derived from 1970-2000 (Bioclimatic) 30 sec ####
  WorldClim<-"D:/Datasets/WorldClim2.1/Historic/30s/"
  
    # Download files from here https://www.worldclim.org/data/worldclim21.html (currently using bio 30s)

    # Find bioclim zip saved in WorldClim directory 
    FILES<-list.files(WorldClim,"tif",full.names = T)

    BioClim<-ERAgON::ExtractRasters(Data=Meta.Table[Buffer<50000],
                                       ID=ID,
                                       Files=FILES,
                                       Save_Dir=paste0(ClimatePast,"BioClim_2.1_30s/"),
                                       Save_Name="BioClim")
    
    
    # Tidy up
    unlink(unzip(FILES,list=T)$Name)
