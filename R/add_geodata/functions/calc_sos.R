# First run: 
  # R/0_set_env.R
  # R/add_geodata/chirps.R
  # R/add_geodata/power.R

# 0) Prepare environment ####
  ## 0.1) Load packages and source functions ####
  p_load(lubridate,data.table,ggplot2,miceadds,sp)
  options(scipen=999)
  
  ## 0.2) Load Climate (POWER, CHIRPS) ####
  
  # Climate dataset requires fields: 
    # 1) Site.Key (Unique identity for location); 
    # 2) Date;
    # 3) Rain (daily rainfall in mm);
    # 4) ETo (potential evapotranspiration in mm).
  
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
  
  # Replace POWER rain with CHIRPS rain
  power_chirps[,Rain:=Rain_chirps][,Rain_chirps:=NULL]
  power<-NULL
  chirps<-NULL
  
# 1) Estimate SOS####
## 1.1) Add aridity index, dekad, year and month
climate_data<-power_chirps[,list(Site.Key,Date,Rain,ETo)
][,AI:=round(Rain/ETo,3) # Calculate aridity index (AI)
][,Dekad:=SOS_Dekad(Date,type="year"),by=Date # Add dekad
][!is.na(AI) # Remove rows with missing data
][,Year:=as.numeric(format(Date,"%Y")),by=Date # Add year
][,Month:=as.numeric(format(Date,"%m")),by=Date] # Add month

  ## 1.2) Find wet months ####
  # Set Minimum rainfall required for second season
  Min.Rain<-200
  # Set separation distance in months of first and second season
  SeasonSeparation<-2
  
  climate_data_monthly<-climate_data[,list(Rain.M.Sum=sum(Rain)),by=list(Site.Key,Year,Month) # Sum rainfall by month within year and site
  ][,Rain.M.Sum3:=c(zoo::rollsum(Rain.M.Sum,3),NA,NA),by=list(Site.Key) # Take 3 month rolling average of monthly rainfall
  ][,list(Rain.M.Sum3.Mean=mean(Rain.M.Sum3,na.rm=T)),by=list(Site.Key,Month) # Average monthly rainfall by month and site across all years
  ][,Rain.Season:=SOS_MaxMonth(Rain=Rain.M.Sum3.Mean,Month=Month,Pad=2),by=list(Site.Key) # Code months for 2 x 3 month seasons with highest rainfall
  ][Rain.Season==2,Rain.Filter:=max(Rain.M.Sum3.Mean)>=Min.Rain,by=list(Site.Key,Rain.Season) # Is season 2  rainfall higher than threshold specified?
  ][Rain.Filter==F & Rain.Season==2,Rain.Season:=NA] # Set season to NA if rainfall threshold is not met
  
  # Add season code to daily climate information
  climate_data<-merge(climate_data,climate_data_monthly[,list(Site.Key,Month,Rain.Season)],by=c("Site.Key","Month"))
  # Ensure dataset is correctly ordered
  climate_data<-climate_data[order(Site.Key,Year,Dekad)]
  
  ## 1.3) Classify rainy seasons ####
  
  # For a 150 day season planting starts 7.5 dekads before mid-point of three month wet period (e.g. dekad 4.5 - 7.5 = -3)
  # We therefore pad the rainy month periods by 3 dekads in each direction
  PadBack<-3
  PadForward<-3
  
  # Much of Africa is unimodal so we exclude second seasons with low rainfall.
  MinLength<-4 # Minimum length of second growing season in dekads
  
  climate_data_dekad<-climate_data[,list(Rain.Dekad=sum(Rain),AI=mean(AI)),by=list(Site.Key,Year,Dekad,Rain.Season) # Sum rainfall and take mean aridity Site.Key by dekad  (within year, site and rain season)
  ][,Dekad.Season:=SOS_SeasonPad(Data=Rain.Season,PadBack=PadBack,PadForward=PadForward),by=Site.Key # Pad rainy seasons (for growing season > 150 days)
  ][,Dekad.Seq:=SOS_UniqueSeq(Dekad.Season),by=Site.Key # Sequences within sites need a unique ID
  ][,Complete:=length(Dekad)==36,by=list(Site.Key,Year) # Calculate dekads within a year
  ][Complete==T | (Dekad %in% 34:36 & Year==min(Year)) # Remove incomplete years but keep last three dekads (when wet period start is Jan we need to look 3 dekads before this) 
  ][,Complete:=NULL # Tidy up
  ][,Rain.sum2:=round(c(zoo::rollsum(Rain.Dekad,3),NA,NA)-Rain.Dekad,3) # Rainfall for next two dekads
  ][,SOSmet:=Rain.sum2>=20 & Rain.Dekad>=25 # Is rainfall of current dekad >=25 and sum of next 2 dekads >=20?
  ][,AI.mean:=round(mean(AI,na.rm=T),2),by=list(Site.Key,Dekad) # Calculate mean aridity Site.Key per dekad across timeseries
  ][,AI.0.5:=AI.mean>=0.5,by=AI.mean # Is aridity >=0.5?
  ][!(is.na(Dekad.Season)),AI.Seq1:=SOS_RSeason(RAIN=SOSmet,AI=AI.0.5),by=list(Site.Key,Dekad.Seq) # Look for sequences of AI>=0.5 starting when rainfall criteria met
  ][!(is.na(Dekad.Season)),AI.Seq:=SOS_SeqMerge(Seq=AI.Seq1,AI=AI.0.5,MaxGap=1,MinStartLen=2,MaxStartSep=1,ClipAI=F),by=list(Site.Key,Dekad.Seq)
  ][!is.na(AI.Seq),SOS:=Dekad[1],by=list(Site.Key,AI.Seq,Dekad.Seq) # Start of season (SOS) is first dekad of each sequence
  ][!is.na(AI.Seq),EOS:=Dekad[length(Dekad)],by=list(Site.Key,AI.Seq,Dekad.Seq) # End of season (EOS) is last dekad of each sequence
  ][SOS<EOS,LGP:=EOS-SOS # Length of growing period (LGP) is SOS less EOS
  ][SOS>EOS,LGP:=36-SOS+EOS # Deal with scenario where SOS is in different year to EOS
  ][SOS==EOS,c("AI.Seq","SOS","EOS"):=NA # Remove observations where SOS == EOS (sequence is length 1)
  ][Year==max(Year) & EOS==36,c("LGP","EOS"):=NA # remove EOS and LGP where EOS is the last dekad of the available data
  ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Start.Year:=Year[1],by=list(Site.Key,Dekad.Seq) # Add starting year for seasons
  ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Tot.Rain:=sum(Rain.Dekad),by=list(Site.Key,Dekad.Seq,AI.Seq)] # Add total rainfall for season
  
  ## 1.4) Calculate Seasonal Values ####
  Len<-climate_data_dekad[,length(unique(Year))]
  Seasonal<-unique(climate_data_dekad[!(is.na(Dekad.Season)|is.na(Start.Year)),list(Site.Key,Start.Year,SOS,EOS,LGP,Dekad.Season,Tot.Rain)])
  # Remove second seasons that are too short
  #Seasonal<-Seasonal[!(Dekad.Season==2 & LGP<MinLength)]
  
  Seasonal[!is.na(Dekad.Season),Seasons.Count:=.N,by=list(Site.Key,Dekad.Season)
  ][,Season2Prop:=Seasons.Count/Len,by=Site.Key]
  
  if(F){
    # Remove second seasons that are present for less than 1/4 the time of first seasons
    Seasonal<-Seasonal[Season2Prop>0.25]
  }
  
  Seasonal[,Seasons:=length(unique(Dekad.Season)),by=Site.Key]
  
  ## 1.5) Roll back SOS where SOS is fixed ####
  # Add similarity field (proportion of SOS dekads which are the same as the most frequent SOS dekads)
  SameSOS<-function(SOS){
    N<-length(SOS)
    SOS<-SOS[!is.na(SOS)]
    if(length(SOS)>0){
      X<-table(SOS)
      return(round(max(X)/N,2))
    }else{
      return(NA)
    }
  }
  
  Seasonal[,SOSsimilarity:=SameSOS(SOS),by=list(Site.Key,Dekad.Season)
  ][,SOSNA:=sum(is.na(SOS))/.N,by=list(Site.Key,Dekad.Season)]
  
  # Subset to very similar planting dates and sites where lots of NAs are not present
  X<-unique(Seasonal[SOSsimilarity>0.95 & SOSNA<0.2,list(Site.Key,Dekad.Season,Seasons)])
  
    ### 1.5.1) Scenario 1: SOS fixed and one season present ####
    # This is a simple case of rolling back the one season
    
    Sites<-X[Seasons==1,Site.Key]
    
    if(length(Sites)>0){
      # Double padding rainy of season start date
      climate_data.Dekad2<-climate_data[Site.Key %in% Sites,list(Rain.Dekad=sum(Rain),AI=mean(AI)),by=list(Site.Key,Year,Dekad,Rain.Season) # Sum rainfall and take mean aridity Site.Key by dekad  (within year, site and rain season)
      ][,Dekad.Season:=SOS_SeasonPad(Data=Rain.Season,PadBack=PadBack*2,PadForward=PadForward*0),by=Site.Key # Pad rainy seasons (for growing season > 150 days)
      ][,Dekad.Seq:=SOS_UniqueSeq(Dekad.Season),by=Site.Key # Sequences within sites need a unique ID
      ][,Complete:=length(Dekad)==36,by=list(Site.Key,Year) # Calculate dekads within a year
      ][Complete==T | (Dekad %in% 34:36 & Year==min(Year)) # Remove incomplete years but keep last three dekads (when wet period start is Jan we need to look 3 dekads before this) 
      ][,Complete:=NULL # Tidy up
      ][,Rain.sum2:=round(c(zoo::rollsum(Rain.Dekad,3),NA,NA)-Rain.Dekad,3) # Rainfall for next two dekads
      ][,SOSmet:=Rain.sum2>=20 & Rain.Dekad>=25 # Is rainfall of current dekad >=25 and sum of next 2 dekads >=20?
      ][,AI.mean:=round(mean(AI,na.rm=T),2),by=list(Site.Key,Dekad) # Calculate mean aridity Site.Key per dekad across timeseries
      ][,AI.0.5:=AI.mean>=0.5,by=AI.mean # Is aridity >=0.5?
      ][!(is.na(Dekad.Season)),AI.Seq1:=SOS_RSeason(RAIN=SOSmet,AI=AI.0.5),by=list(Site.Key,Dekad.Seq) # Look for sequences of AI>=0.5 starting when rainfall criteria met
      ][!(is.na(Dekad.Season)),AI.Seq:=SOS_SeqMerge(Seq=AI.Seq1,AI=AI.0.5,MaxGap=1,MinStartLen=2,MaxStartSep=1,ClipAI=F),by=list(Site.Key,Dekad.Seq)
      ][!is.na(AI.Seq),SOS:=Dekad[1],by=list(Site.Key,AI.Seq,Dekad.Seq) # Start of season (SOS) is first dekad of each sequence
      ][!is.na(AI.Seq),EOS:=Dekad[length(Dekad)],by=list(Site.Key,AI.Seq,Dekad.Seq) # End of season (EOS) is last dekad of each sequence
      ][SOS<EOS,LGP:=EOS-SOS # Length of growing period (LGP) is SOS less EOS
      ][SOS>EOS,LGP:=36-SOS+EOS # Deal with scenario where SOS is in different year to EOS
      ][SOS==EOS,c("AI.Seq","SOS","EOS"):=NA # Remove observations where SOS == EOS (sequence is length 1)
      ][Year==max(Year) & EOS==36,c("LGP","EOS"):=NA # remove EOS and LGP where EOS is the last dekad of the available data
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Start.Year:=Year[1],by=list(Site.Key,Dekad.Seq) # Add starting year for seasons
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Tot.Rain:=sum(Rain.Dekad),by=list(Site.Key,Dekad.Seq,AI.Seq)] # Add total rainfall for season
      
      climate_data_dekad<-rbind(climate_data_dekad[!Site.Key %in% Sites],climate_data.Dekad2)
    }
    
    ### 1.5.2) Scenario 2: SOS fixed and two seasons present ####
    
    # Subset data
    Data<-climate_data_dekad[Site.Key %in% X[Seasons==2,Site.Key]]
    
    # Calculate season separation
    SeasonSpacing<-function(SOS,EOS,Dekad.Season){
      if(length(unique(Dekad.Season))>=2){
        Data<-unique(data.table(SOS=SOS,EOS=EOS,Dekad.Season=Dekad.Season))
        
        SOSEOS<-Data[!is.na(Dekad.Season),list(SOS=median(SOS,na.rm = T),EOS=median(EOS,na.rm = T)),by=list(Dekad.Season)]
        
        # Difference between start season two and end season one 
        SOS<-SOSEOS[Dekad.Season==2,SOS]
        EOS<-SOSEOS[Dekad.Season==1,EOS]
        if(SOS<EOS){
          SOS<-36-EOS+1
          EOS<-1
        }
        Diff.1vs2<-SOS-EOS
        
        # Difference between start season one and end season two
        SOS<-SOSEOS[Dekad.Season==1,SOS]
        EOS<-SOSEOS[Dekad.Season==2,EOS]
        if(SOS<EOS){
          SOS<-36-EOS+1
          EOS<-1
        }
        
        Diff.2vs1<-SOS-EOS
        
        Diffs<-c(Diff.1vs2,Diff.2vs1)
        
        Diff<-data.table(sepmin=min(Diffs)[1],sepmax=max(Diffs)[1],order=which(Diffs==min(Diffs))[1])
        
        
      }else{
        Diff<-data.table(sepmin=NA,sepmax=NA,order=NA)
      }
      
      return(Diff)
    }
    
    # If order ==1 then adjacent seasons are ordered 1 then 2, if 2 vice versa
    Data[,Season.Sep.Min:=SeasonSpacing(SOS,EOS,Dekad.Season)$sepmin,by=Site.Key
    ][,Season.Sep.Max:=SeasonSpacing(SOS,EOS,Dekad.Season)$sepmax,by=Site.Key
    ][,Season.Order:=SeasonSpacing(SOS,EOS,Dekad.Season)$order,by=Site.Key]
    
    # Calculate number of fixed season and flag which seasons are fixed
    X.Seasons<-X[Seasons==2,list(FixedSeasons.N=length(unique(Dekad.Season)),FixedSeasons=paste(unique(Dekad.Season),collapse = "-")),by=Site.Key]
    Data<-merge(Data,X.Seasons,by="Site.Key")
    
    ### 1.5.3) Seasons are Adjacent ######
    DataAdjacent<-Data[Season.Sep.Min<2]
    
    # If leading season!=fixed season then there is nothing to change (fixed season immediately adjacent to leading season)
    DataAdjacentFixed1<-DataAdjacent[(FixedSeasons.N==1 & Season.Order==FixedSeasons)|FixedSeasons.N==2]
    
    # If we have adjacent seasons and the first season is fixed (i.e. date need adjusting back) then we adjust the start of the season
    # window for both season 1 and season 2. This should help balance the lengths of the two seasons where the rainy season is long enough
    # to accommodate two growing seasons.
    
    Sites<-DataAdjacentFixed1[,unique(Site.Key)]
    
    if(length(Sites)>0){
      # Double padding of rainy season start date remove padding of end date
      # Note for non-adjacent seasons a different method is used that can accommodate flexible padding length by Site
      climate_data.Dekad1<-climate_data[Site.Key %in% Sites,list(Rain.Dekad=sum(Rain),AI=mean(AI)),by=list(Site.Key,Year,Dekad,Rain.Season)
      ][,Dekad.Season:=SOS_SeasonPad(Data=Rain.Season,PadBack=PadBack*2,PadForward=PadForward*0),by=Site.Key
      ][,Dekad.Seq:=SOS_UniqueSeq(Dekad.Season),by=Site.Key # Sequences within sites need a unique ID
      ][,Complete:=length(Dekad)==36,by=list(Site.Key,Year) # Calculate dekads within a year
      ][Complete==T | (Dekad %in% 34:36 & Year==min(Year)) # Remove incomplete years but keep last three dekads (when wet period start is Jan we need to look 3 dekads before this) 
      ][,Complete:=NULL # Tidy up
      ][,Rain.sum2:=round(c(zoo::rollsum(Rain.Dekad,3),NA,NA)-Rain.Dekad,3) # Rainfall for next two dekads
      ][,SOSmet:=Rain.sum2>=20 & Rain.Dekad>=25 # Is rainfall of current dekad >=25 and sum of next 2 dekads >=20?
      ][,AI.mean:=round(mean(AI,na.rm=T),2),by=list(Site.Key,Dekad) # Calculate mean aridity Site.Key per dekad across timeseries
      ][,AI.0.5:=AI.mean>=0.5,by=AI.mean # Is aridity Site.Key >=0.5?
      ][!(is.na(Dekad.Season)),AI.Seq1:=SOS_RSeason(RAIN=SOSmet,AI=AI.0.5),by=list(Site.Key,Dekad.Seq) # Look for sequences of AI>=0.5 starting when rainfall criteria met
      ][!(is.na(Dekad.Season)),AI.Seq:=SOS_SeqMerge(Seq=AI.Seq1,AI=AI.0.5,MaxGap=1,MinStartLen=2,MaxStartSep=1,ClipAI=F),by=list(Site.Key,Dekad.Seq)
      ][!is.na(AI.Seq),SOS:=Dekad[1],by=list(Site.Key,AI.Seq,Dekad.Seq) # Start of season (SOS) is first dekad of each sequence
      ][!is.na(AI.Seq),EOS:=Dekad[length(Dekad)],by=list(Site.Key,AI.Seq,Dekad.Seq) # End of season (EOS) is last dekad of each sequence
      ][SOS<EOS,LGP:=EOS-SOS # Length of growing period (LGP) is SOS less EOS
      ][SOS>EOS,LGP:=36-SOS+EOS # Deal with scenario where SOS is in different year to EOS
      ][SOS==EOS,c("AI.Seq","SOS","EOS"):=NA # Remove observations where SOS == EOS (sequence is length 1)
      ][Year==max(Year) & EOS==36,c("LGP","EOS"):=NA # remove EOS and LGP where EOS is the last dekad of the available data
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Start.Year:=Year[1],by=list(Site.Key,Dekad.Seq) # Add starting year for seasons
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Tot.Rain:=sum(Rain.Dekad),by=list(Site.Key,Dekad.Seq,AI.Seq)] # Add total rainfall for season
      
      climate_data_dekad<-rbind(climate_data_dekad[!Site.Key %in% c(Sites)],climate_data.Dekad1)
      Clim.Dekad1<-NULL
    }
    
    
    ### 1.5.4) Seasons are not adjacent ######
    # Need to count back for fixed season but not beyond EOS of other season
    
    # Subset to seasons with a separation of at least 1 dekad 
    DataNonAdjacent<-Data[Season.Sep.Min>=2] # >= 2 is correct
    
    DataNonAdjacent[Season.Order==2 & Rain.Season==2 & grepl(2,FixedSeasons),Season.Sep:=Season.Sep.Max]
    DataNonAdjacent[Season.Order==2 & Rain.Season==1 & grepl(1,FixedSeasons),Season.Sep:=Season.Sep.Min]
    DataNonAdjacent[Season.Order==1 & Rain.Season==1 & grepl(1,FixedSeasons),Season.Sep:=Season.Sep.Min]
    DataNonAdjacent[Season.Order==1 & Rain.Season==2 & grepl(2,FixedSeasons),Season.Sep:=Season.Sep.Max]
    DataNonAdjacent[!is.na(Rain.Season) & is.na(Season.Sep),Season.Sep:=0]
    
    # Set a limit on maximum number of dekads to roll back           
    DataNonAdjacent[Season.Sep>PadBack,Season.Sep:=PadBack]
    
    Sites<-DataNonAdjacent[,unique(Site.Key)]
    
    if(length(Sites)>0){
      climate_data.Dekad1<-climate_data[Site.Key %in% Sites,list(Rain.Dekad=sum(Rain),AI=mean(AI)),by=list(Site.Key,Year,Dekad,Rain.Season)]
      
      # Merge season separation with climate data
      climate_data.Dekad1<-merge(climate_data.Dekad1,
                         unique(DataNonAdjacent[!is.na(Rain.Season),list(Site.Key,Rain.Season,Season.Sep)]),
                         by=c("Site.Key","Rain.Season"),all.x=T)
      
      # Merge fixed season identity with climate data
      climate_data.Dekad1<-merge(climate_data.Dekad1,
                         unique(DataNonAdjacent[!is.na(Rain.Season),list(Site.Key,FixedSeasons)]),
                         by=c("Site.Key"),all.x=T)
      
      # Revert to original order
      climate_data.Dekad1<-climate_data.Dekad1[order(Site.Key,Year,Dekad)]
      
      # Increase padding rainy of season start date and reduce padding of end date
      climate_data.Dekad1[Rain.Season==1,Season1:=Rain.Season
      ][Rain.Season==2,Season2:=Rain.Season
      ][,Dekad.Season1:=SOS_SeasonPad(Data=Season1,
                                      PadBack=PadBack+Season.Sep[Rain.Season==1 & !is.na(Rain.Season)][1],
                                      PadForward=PadForward-Season.Sep[Rain.Season==1 & !is.na(Rain.Season)][1]),by=Site.Key 
      ][,Dekad.Season2:=SOS_SeasonPad(Data=Season2,
                                      PadBack=PadBack+Season.Sep[Rain.Season==2 & !is.na(Rain.Season)][1],
                                      PadForward=PadForward-Season.Sep[Rain.Season==2 & !is.na(Rain.Season)][1]),by=Site.Key 
      ]
      
      # Recombine dekad season numbering
      climate_data.Dekad1[FixedSeasons==1,Dekad.Season:=Dekad.Season1
      ][is.na(Dekad.Season)  &  FixedSeasons==1,Dekad.Season:=Dekad.Season2
      ][FixedSeasons==2,Dekad.Season:=Dekad.Season2
      ][is.na(Dekad.Season)  &  FixedSeasons==2,Dekad.Season:=Dekad.Season1
      ][!FixedSeasons %in% c(1,2),Dekad.Season:=Dekad.Season1
      ][is.na(Dekad.Season)  & !FixedSeasons %in% c(1,2),Dekad.Season:=Dekad.Season2
      ][,Dekad.Season1:=NULL
      ][,Dekad.Season2:=NULL
      ][,Season1:=NULL
      ][,Season2:=NULL
      ][,FixedSeasons:=NULL
      ][,Season.Sep:=NULL]
      
      climate_data.Dekad1<-climate_data.Dekad1[,Dekad.Seq:=SOS_UniqueSeq(Dekad.Season),by=Site.Key # Sequences within sites need a unique ID
      ][,Complete:=length(Dekad)==36,by=list(Site.Key,Year) # Calculate dekads within a year
      ][Complete==T | (Dekad %in% 34:36 & Year==min(Year)) # Remove incomplete years but keep last three dekads (when wet period start is Jan we need to look 3 dekads before this) 
      ][,Complete:=NULL # Tidy up
      ][,Rain.sum2:=round(c(zoo::rollsum(Rain.Dekad,3),NA,NA)-Rain.Dekad,3) # Rainfall for next two dekads
      ][,SOSmet:=Rain.sum2>=20 & Rain.Dekad>=25 # Is rainfall of current dekad >=25 and sum of next 2 dekads >=20?
      ][,AI.mean:=round(mean(AI,na.rm=T),2),by=list(Site.Key,Dekad) # Calculate mean aridity Site.Key per dekad across timeseries
      ][,AI.0.5:=AI.mean>=0.5,by=AI.mean # Is aridity Site.Key >=0.5?
      ][!(is.na(Dekad.Season)),AI.Seq1:=SOS_RSeason(RAIN=SOSmet,AI=AI.0.5),by=list(Site.Key,Dekad.Seq) # Look for sequences of AI>=0.5 starting when rainfall criteria met
      ][!(is.na(Dekad.Season)),AI.Seq:=SOS_SeqMerge(Seq=AI.Seq1,AI=AI.0.5,MaxGap=1,MinStartLen=2,MaxStartSep=1,ClipAI=F),by=list(Site.Key,Dekad.Seq)
      ][!is.na(AI.Seq),SOS:=Dekad[1],by=list(Site.Key,AI.Seq,Dekad.Seq) # Start of season (SOS) is first dekad of each sequence
      ][!is.na(AI.Seq),EOS:=Dekad[length(Dekad)],by=list(Site.Key,AI.Seq,Dekad.Seq) # End of season (EOS) is last dekad of each sequence
      ][SOS<EOS,LGP:=EOS-SOS # Length of growing period (LGP) is SOS less EOS
      ][SOS>EOS,LGP:=36-SOS+EOS # Deal with scenario where SOS is in different year to EOS
      ][SOS==EOS,c("AI.Seq","SOS","EOS"):=NA # Remove observations where SOS == EOS (sequence is length 1)
      ][Year==max(Year) & EOS==36,c("LGP","EOS"):=NA # remove EOS and LGP where EOS is the last dekad of the available data
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Start.Year:=Year[1],by=list(Site.Key,Dekad.Seq) # Add starting year for seasons
      ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Tot.Rain:=sum(Rain.Dekad),by=list(Site.Key,Dekad.Seq,AI.Seq)] # Add total rainfall for season
      
      climate_data_dekad<-rbind(climate_data_dekad[!Site.Key %in% Sites],climate_data.Dekad1)
      Clim.Dekad1<-NULL
    }
    
    ### 1.5.5) Recalculate seasonal values #####
    
    Seasonal2<-unique(climate_data_dekad[!(is.na(Dekad.Season)|is.na(Start.Year)),list(Site.Key,Start.Year,SOS,EOS,LGP,Dekad.Season,Tot.Rain)])
    # Remove second seasons that are too short
    Seasonal2<-Seasonal2[!(Dekad.Season==2 & LGP<MinLength)]
    Seasonal2<-Seasonal2[!is.na(Dekad.Season),Seasons.Count:=.N,by=list(Site.Key,Dekad.Season)][,Season2Prop:=Seasons.Count/Len]
    
    # Remove second seasons that are present for less than 1/3 the time of first seasons
    Seasonal2<-Seasonal2[Season2Prop>0.33]
    
    # How many seasons present at a site?
    Seasonal2[,Seasons:=length(unique(Dekad.Season)),by=Site.Key]
    
    # What is the similarity of SOS within the site?
    Seasonal2[,SOSsimilarity:=SameSOS(SOS),by=list(Site.Key,Dekad.Season)]
    
    X1<-unique(Seasonal2[SOSsimilarity>0.95,list(Site.Key,Dekad.Season,Seasons)])
    
    X[,length(unique(Site.Key))]
    X1[,length(unique(Site.Key))]
    
  ## 1.6) Add separation #####
  climate_data_dekad[!(is.na(EOS)|is.na(SOS)|is.na(Dekad.Season)),Season.Sep.Min:=SeasonSpacing(SOS,EOS,Dekad.Season)$sepmin,by=Site.Key
  ][!(is.na(EOS)|is.na(SOS)|is.na(Dekad.Season)),Season.Sep.Max:=SeasonSpacing(SOS,EOS,Dekad.Season)$sepmax,by=Site.Key
  ][!(is.na(EOS)|is.na(SOS)|is.na(Dekad.Season)),Season.Order:=SeasonSpacing(SOS,EOS,Dekad.Season)$order,by=Site.Key]
  
  
  ## 1.7) Is planting possible in the off season - is this a humid region? ####
  
  # Consider using AIseq here rather than Dekad.Season?
  Sites<-Seasonal2[Seasons==2,unique(Site.Key)]
  
  climate_data.Dekad1<-data.table::copy(climate_data_dekad)[Site.Key %in% Sites
  ][is.na(Dekad.Season),Dekad.Season1:=3
  ][!is.na(Dekad.Season),Dekad.Season:=NA
  ][,Dekad.Season:=Dekad.Season1
  ][,Dekad.Season1:=NULL
  ][,Dekad.Seq:=SOS_UniqueSeq(Dekad.Season),by=Site.Key
  ]
  
  # Function to shrink third season by one dekad at each end
  ShrinkX<-function(X){
    X<-unlist(X)
    X[1]<-NA
    X[length(X)]<-NA
    return(X)
  }
  
  if(F){
    climate_data.Dekad1<-climate_data.Dekad1[,Dekad.Seq2:=Dekad.Seq
    ][,Dekad.Seq2:=ShrinkX(Dekad.Seq2),by=list(Site.Key,Dekad.Seq)
    ][,Dekad.Seq:=Dekad.Seq2
    ][,Dekad.Seq2:=NULL
    ]
  }
  
  climate_data.Dekad1<-climate_data.Dekad1[Dekad.Season==3
  ][,Rain.sum2:=round(c(zoo::rollsum(Rain.Dekad,3),NA,NA)-Rain.Dekad,3) # Rainfall for next two dekads
  ][,SOSmet:=Rain.sum2>=20 & Rain.Dekad>=25 # Is rainfall of current dekad >=25 and sum of next 2 dekads >=20?
  ][,AI.mean:=round(mean(AI,na.rm=T),2),by=list(Site.Key,Dekad) # Calculate mean aridity Site.Key per dekad across timeseries
  ][,AI.0.5:=AI.mean>=0.5,by=AI.mean # Is aridity Site.Key >=0.5?
  ][!(is.na(Dekad.Season)),AI.Seq1:=SOS_RSeason(RAIN=SOSmet,AI=AI.0.5),by=list(Site.Key,Dekad.Seq) # Look for sequences of AI>=0.5 starting when rainfall criteria met
  ][!(is.na(Dekad.Season)),AI.Seq:=SOS_SeqMerge(Seq=AI.Seq1,AI=AI.0.5,MaxGap=1,MinStartLen=2,MaxStartSep=1,ClipAI=F),by=list(Site.Key,Dekad.Seq)
  ][!is.na(AI.Seq),SOS:=Dekad[1],by=list(Site.Key,AI.Seq,Dekad.Seq) # Start of season (SOS) is first dekad of each sequence
  ][!is.na(AI.Seq),EOS:=Dekad[length(Dekad)],by=list(Site.Key,AI.Seq,Dekad.Seq) # End of season (EOS) is last dekad of each sequence
  ][SOS<EOS,LGP:=EOS-SOS # Length of growing period (LGP) is SOS less EOS
  ][SOS>EOS,LGP:=36-SOS+EOS # Deal with scenario where SOS is in different year to EOS
  ][SOS==EOS,c("AI.Seq","SOS","EOS"):=NA # Remove observations where SOS == EOS (sequence is length 1)
  ][Year==max(Year) & EOS==36,c("LGP","EOS"):=NA # remove EOS and LGP where EOS is the last dekad of the available data
  ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Start.Year:=Year[1],by=list(Site.Key,Dekad.Seq) # Add starting year for seasons
  ][!(is.na(AI.Seq)|is.na(Dekad.Seq)),Tot.Rain:=sum(Rain.Dekad),by=list(Site.Key,Dekad.Seq,AI.Seq)] # Add total rainfall for season
  
  Seasonal3<-unique(climate_data.Dekad1[!(is.na(Dekad.Season)|is.na(Start.Year)),list(Site.Key,Start.Year,SOS,EOS,LGP,Dekad.Season,Tot.Rain)])
  # Remove second seasons that are too short
  Seasonal3<-Seasonal3[!(Dekad.Season==3 & LGP<MinLength)]
  Seasonal3[Dekad.Season==3,Seasons.Count:=sum(Dekad.Season==3),by=Site.Key
  ][,Season3Prop:=Seasons.Count/Len,by=Site.Key]
  
  # Remove second seasons that are present for less than 1/3 of the time 
  Seasonal3<-Seasonal3[Season3Prop>0.33]
  
  if(nrow(Seasonal3)>0){
    Sites<-Seasonal3[,unique(Site.Key)]
    
    # Replace NA seasons with 
    climate_data_dekad.3<-rbind(climate_data.Dekad1[Site.Key %in% Sites],climate_data_dekad[!(Site.Key %in% Sites & is.na(Dekad.Season))])
    
    # Update Seasonal3 Stats
    Seasonal3<-unique(climate_data_dekad.3[!(is.na(Dekad.Season)|is.na(Start.Year)),list(Site.Key,Start.Year,Dekad.Season,SOS,EOS,LGP,Tot.Rain)])
    Seasonal3<-Seasonal3[base::order(Site.Key,Start.Year,Dekad.Season,decreasing=c(FALSE,FALSE,FALSE),method="radix")]
    # Remove second seasons that are too short
    Seasonal3<-Seasonal3[!(Dekad.Season %in% c(2,3) & LGP<MinLength)]
    Seasonal3[,Seasons.Count:=.N,by=list(Site.Key,Dekad.Season)
    ][,SeasonProp:=Seasons.Count/Len,by=Site.Key]
    
    # Remove second seasons that are present for less than 1/2 the time of first seasons
    Seasonal3<-Seasonal3[SeasonProp>=0.5]
    
    Seasonal3[,Seasons:=length(unique(Dekad.Season)),by=Site.Key]
    
    Seasonal3[,SOSsimilarity:=SameSOS(SOS),by=list(Site.Key,Dekad.Season)]
  }
  
  ## 1.8) Site.Details ####
  # Proportion of dekads, entire time series, where SOS or AI rule is true
  Site.Details<-climate_data_dekad[,list(AI.0.5.Prop=sum(AI.0.5)/.N,
                                 AI.1.0.Prop=sum(AI.mean>=1)/.N,
                                 SOSmet.Prop=sum(SOSmet,na.rm = T)/.N,
                                 MAP=sum(Rain.Dekad)/length(unique(Year))),by=Site.Key]
  
  
  ## 1.9) Long term average SOS, EOS, LGP and Total Rainfall ####
  
  # The below also calculates the proportion of records where EOS<SOS or (EOS+15)>36 to show where a season typically traverses the year boundary
  # It also shows the range of SOS to indicate sites where this crosses the year boundary. 
  
  
  LTAvg_SOS2<-Seasonal2[!is.na(Dekad.Season),list(SOS.mean=round(mean(SOS,na.rm=T),1),
                                                  SOS.median=median(SOS,na.rm=T),
                                                  SOS.min=suppressWarnings(min(SOS,na.rm=T)),
                                                  SOS.max=suppressWarnings(max(SOS,na.rm=T)),
                                                  Total.Seasons=.N,
                                                  EOS=round(mean(EOS,na.rm=T),1),
                                                  LGP=round(mean(LGP,na.rm=T),1),
                                                  Tot.Rain=round(mean(Tot.Rain,na.rm=T),1),
                                                  SOS.EOS.XYearEnd=round(sum(EOS[!is.na(EOS)]<SOS[!is.na(EOS)])/length(EOS[!is.na(EOS)]),2),
                                                  SOS.add15.XYearEnd=round(sum((SOS[!is.na(SOS)]+15)>36,na.rm=T)/length(SOS[!is.na(SOS)]),2)),
                        by=list(Site.Key,Dekad.Season)]
  
  LTAvg_SOS2[!is.na(Dekad.Season),Seasons:=length(unique(Dekad.Season)),by=Site.Key]
  
  # Order seasons by SOS
  LTAvg_SOS2[!(is.na(SOS.mean)|is.na(Seasons)|is.na(Dekad.Season)),Season.Ordered:=(1:length(Dekad.Season))[order(SOS.mean)],by=Site.Key
  ][Seasons==1,Season.Ordered:=NA]
  
  # Merge LT season order and year end data with Seasonal
  Seasonal2<-merge(Seasonal2,LTAvg_SOS2[,list(Site.Key,Dekad.Season,Season.Ordered,SOS.EOS.XYearEnd,SOS.add15.XYearEnd,SOS.min,SOS.max,Total.Seasons)],by=c("Site.Key","Dekad.Season"),all.x=T)
  
  
  LTAvg_SOS3<-Seasonal3[!is.na(Dekad.Season),list(SOS.mean=round(mean(SOS,na.rm=T),1),
                                                  SOS.median=median(SOS,na.rm=T),
                                                  SOS.min=suppressWarnings(min(SOS,na.rm=T)),
                                                  SOS.max=suppressWarnings(max(SOS,na.rm=T)),
                                                  Total.Seasons=.N,
                                                  EOS=round(mean(EOS,na.rm=T),1),
                                                  LGP=round(mean(LGP,na.rm=T),1),
                                                  Tot.Rain=round(mean(Tot.Rain,na.rm=T),1),
                                                  SOS.EOS.XYearEnd=round(sum(EOS[!is.na(EOS)]<SOS[!is.na(EOS)])/length(EOS[!is.na(EOS)]),2),
                                                  SOS.add15.XYearEnd=round(sum((SOS[!is.na(SOS)]+15)>36,na.rm=T)/length(SOS[!is.na(SOS)]),2)),
                        by=list(Site.Key,Dekad.Season)]
  
  LTAvg_SOS3[!is.na(Dekad.Season),Seasons:=length(unique(Dekad.Season)),by=Site.Key]
  
  # Order seasons by SOS
  LTAvg_SOS3[!(is.na(SOS.mean)|is.na(Seasons)|is.na(Dekad.Season)),Season.Ordered:=(1:length(Dekad.Season))[order(SOS.mean)],by=Site.Key
  ][Seasons==1,Season.Ordered:=NA]
  
  # Merge LT season order and year end data with Seasonal
  Seasonal3<-merge(Seasonal3,LTAvg_SOS3[,list(Site.Key,Dekad.Season,Season.Ordered,SOS.EOS.XYearEnd,SOS.add15.XYearEnd,SOS.min,SOS.max,Total.Seasons)],by=c("Site.Key","Dekad.Season"),all.x=T)
  
  ## 1.10) Save data ####
  ERA_SOS<-list(Dekadal_SOS=climate_data_dekad,Seasonal_SOS2=Seasonal2,LTAvg_SOS2=LTAvg_SOS2,Seasonal_SOS3=Seasonal3,LTAvg_SOS3=LTAvg_SOS3)
  save(ERA_SOS,file=file.path(era_dirs$era_geodata_dir,paste0("sos_",Sys.Date(),".RData")))
  
# 2) Explore sites to validate ####
SITE<-Seasonal[,sample(unique(Site.Key),1)]
Caption<-paste(t(ERAg::ERA.Compiled[Site.Key==SITE,list(Latitude,Longitude,Country)][1]),collapse = " ")
Z<-Seasonal[Site.Key==SITE]

Z<-melt(Z,id.vars=c("Site.Key","Dekad.Season"),measure.vars = c("SOS","EOS","LGP","Tot.Rain"))
ggplot(Z,aes(x=value,fill=as.factor(Dekad.Season)))+
  geom_histogram(alpha=0.6,col="black", bins=10)+
  facet_grid(Dekad.Season~variable,scales="free_x")+labs(caption=Caption)

LTAvg_SOS[Site.Key==SITE]
Seasonal[Site.Key==SITE]
View(climate_data_dekad[Site.Key==SITE])

 ## 2.1) Plot seasons to validate ####
# No Seasons
Sites<-climate_data[!Site.Key %in% Seasonal[,unique(Site.Key)],unique(Site.Key)]
Sites<-climate_data[!Site.Key %in% Seasonal3[,unique(Site.Key)],unique(Site.Key)]

ZeroSeasons<-unique(ERAg::ERA.Compiled[Site.Key %in% Sites,list(Latitude,Longitude,Country)])

write.table(ZeroSeasons,"clipboard",row.names = F,sep="\t")


AfricaMap<-rworldmap::getMap(resolution = "high")
AfricaMap<-AfricaMap[AfricaMap$REGION=="Africa"&!is.na(AfricaMap$REGION),]
AfricaMap<-spTransform(AfricaMap,"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 

coordinates(ZeroSeasons) <- ~ Longitude + Latitude
proj4string(ZeroSeasons) <- "+init=epsg:4326"
ZeroSeasons<-spTransform(ZeroSeasons,"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 

plot(AfricaMap)
plot(ZeroSeasons,add=T,col="Red")

# One Season
OneSeason<-unique(ERAg::ERA.Compiled[Site.Key %in% LTAvg_SOS[Seasons==1,Site.Key],list(Latitude,Longitude,Country)])
write.table(OneSeason,"clipboard",row.names = F,sep="\t")

TwoSeason<-unique(ERAg::ERA.Compiled[Site.Key %in% LTAvg_SOS[Seasons==2,Site.Key],list(Latitude,Longitude,Country)])
write.table(TwoSeason,"clipboard",row.names = F,sep="\t")

ThreeSeason<-unique(ERAg::ERA.Compiled[Site.Key %in% LTAvg_SOS[Seasons==3,Site.Key],list(Latitude,Longitude,Country)])
write.table(ThreeSeason,"clipboard",row.names = F,sep="\t")


coordinates(OneSeason) <- ~ Longitude + Latitude
proj4string(OneSeason) <- "+init=epsg:4326"
OneSeason<-spTransform(OneSeason,"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 

coordinates(TwoSeason) <- ~ Longitude + Latitude
proj4string(TwoSeason) <- "+init=epsg:4326"
TwoSeason<-spTransform(TwoSeason,"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 

coordinates(ThreeSeason) <- ~ Longitude + Latitude
proj4string(ThreeSeason) <- "+init=epsg:4326"
ThreeSeason<-spTransform(ThreeSeason,"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs") 

plot(AfricaMap)
plot(TwoSeason,add=T,col="Blue")
plot(OneSeason,add=T,col="Red")
plot(ZeroSeasons,add=T,col="Black")
plot(ThreeSeason,add=T,col="Green")

