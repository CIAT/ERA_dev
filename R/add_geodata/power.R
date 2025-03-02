# This script downloads NASA POWER data for unique locations in ERA. 
# Run ERA_dev/R/0_set_env.R
# This script is also dependent on altitude data having been calculated for ERA locations:
# Run ERA_dev/R/add_geodata/download_dem.R before executing this script
# Functions used are here https://raw.githubusercontent.com/CIAT/ERA_dev/main/R/0_functions.R

# 0) Set-up workspace ####
  ## 0.1) Load packages & functions #####
  p_load(terra,sf,data.table,RCurl,future,future.apply,progressr,pbapply)
  
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/PETcalc.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/download_power.R")
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/process_power.R")

  ## 0.2) Subset era sites according to buffer distance #####
  # Set max buffer allowed
  SS<-era_locations[Buffer<50000]
  pbuf_g<-era_locations_vect_g[era_locations_vect_g$Buffer<50000,]
  
  ## 0.3) Merge altitude data ####
  era_elevation<-fread(file.path(era_dirs$era_geodata_dir,"elevation.csv"))
  altitude_data<-era_elevation[variable=="elevation" & stat=="mean",.(Site.Key,value)]
  setnames(altitude_data,"value","Altitude")
  
  # Merge altitude with site buffer vector
  pbuf_g<-merge(pbuf_g,altitude_data,by="Site.Key")
  
  ## 0.3) Create POWER parameter object #####
  # Need to incorporate correction for PS to PSC - so altitude data from physical layer needs to be read in here
  parameters<-c(
    SRad="ALLSKY_SFC_SW_DWN", # Insolation Incident on a Horizontal Surface - MJ/m^2/day
    Rain="PRECTOT", # Precipitation - mm day-1
    Pressure.Corrected="PSC", # Corrected Atmospheric Pressure (Adjusted For Site Elevation) *(see Site Elevation section below) - kPa - Throws an error!
    Pressure="PS", # Surface Pressure - kPa
    Specific.Humid="QV2M", # Specific Humidity at 2 Meters - kg kg-1
    Humid="RH2M", # Relative Humidity at 2 Meters - %
    Temp.Mean="T2M", # Temperature at 2 Meters - C
    Temp.Max="T2M_MAX", # Maximum Temperature at 2 Meters - C
    Temp.Min="T2M_MIN", # Minimum Temperature at 2 Meters - C
    WindSpeed="WS2M" # Wind Speed at 2 Meters - m/s
  )
  
  ## 0.5) Set power data start and end dates #####
  date_start<-"1984-01-01"
  date_end<-"2024-12-31"
  
  ## 0.6) Set workers for parallel downloads ####
  worker_n<-10
  
# 1) Download POWER ####
  ## 1.1) Run download function ####
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  
  # Need to create loop that incorporates updated dates
  # Names for min and max were the wrong way around in the previous version of this function
  # Split download and compilation functions
  overwrite<-F
    
  dl_errors<-download_power(site_vect=pbuf_g,
                 date_start=date_start,
                 date_end=date_end,
                 altitude_field="Altitude",
                 save_dir=era_dirs$power_dir,
                 parameters=parameters,
                 user_name="CIAT", 
                 id="Site.Key",
                 verbose=F,
                 attempts=3,
                 worker_n=worker_n,
                 overwrite=overwrite)
  
  # Check for incomplete downloads (files should be >900kb)
  files <- list.files(era_dirs$power_dir,".csv",full.names = T)
  file_sizes <- file.info(files)$size/10^3
  hist(file_sizes)
  
  # 2.3) Recompile Data
  files<-list.files(era_dirs$power_dir,".csv$",full.names = T)
  
  power_data<-process_power(files=files,
                                parameters = parameters,
                                rename = names(parameters),
                                id_field= "Site.Key",
                                altitude_field="Altitude",
                                add_date=T,
                                add_daycount = T,
                                time_origin = time_origin,
                                add_pet=T,
                                altitude_data=altitude_data,
                                worker_n=worker_n)
  
  save_file<-file.path(era_dirs$era_geodata_dir,paste0("POWER_",Sys.Date(),".parquet"))
  arrow::write_parquet(power_data,save_file)

# 2) Calculate annual averages ####
  ## 2.1.1) Remove incomplete years ####
  power_data<-power_data[,N:=.N,by=list(Site.Key,Year)][!N<365][,N:=NULL]
  
  ## 2.1.2) Calculate annual data ####
  power_data_annual<-power_data[,list(
                            Total.Rain=as.integer(sum(Rain)),
                            Total.ETo=as.integer(sum(ETo)),
                            S.Humid.Mean=round(mean(Specific.Humid),2),
                            Humid.Mean=round(mean(Humid),2),
                            Temp.Mean.Mean=round(mean(Temp.Mean),2),
                            Mean.N.30.Days=as.integer(sum(Temp.Mean>30)),
                            Mean.N.35.Days=as.integer(sum(Temp.Mean>35)),
                            Temp.Max.Mean=round(mean(Temp.Max),2),
                            Temp.Max=max(Temp.Max),
                            Max.N.40.Days=as.integer(sum(Temp.Max>40)),
                            Temp.Min.Mean=round(mean(Temp.Min),2),
                            Temp.Min=min(Temp.Min)),by=.(Site.Key,Year)]
  
  ## 2.1.3) Calculate LT Data ####
  power_data_lt<-power_data_annual[,list(Total.Rain.Mean=mean(Total.Rain),
                               Total.ETo.Mean=mean(Total.ETo),
                               S.Humid.Mean=mean(S.Humid.Mean),
                               Humid.Mean=mean(Humid.Mean),
                               Temp.Mean.Mean=mean(Temp.Mean.Mean),
                               Mean.N.30.Days=mean(Mean.N.30.Days),
                               Mean.N.35.Days=mean(Mean.N.35.Days),
                               Temp.Max.Mean=mean(Temp.Max.Mean),
                               Temp.Max=mean(Temp.Max),
                               Max.N.40.Days=mean(Max.N.40.Days),
                               Temp.Min.Mean=mean(Temp.Min.Mean),
                               Temp.Min=mean(Temp.Min),
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
                               ),by=Site.Key ]


  ## 2.1.4) Round numeric columns ####
  n_dp<-2
  
  numeric_cols <- names(power_data_lt)[sapply(power_data_lt, is.numeric)]
  power_data_lt[, (numeric_cols) := lapply(.SD, round, n_dp), .SDcols = numeric_cols]
  
  numeric_cols <- names(power_data_annual)[sapply(power_data_annual, is.numeric)]
  power_data_annual[, (numeric_cols) := lapply(.SD, round, n_dp), .SDcols = numeric_cols]
    
  ## 2.1.5) Save annual & LT datasets ####
  arrow::write_parquet(power_data_annual,gsub(".parquet$","_annual.parquet",save_file))
  arrow::write_parquet(power_data_lt,gsub(".parquet$","_ltavg.parquet",save_file))
  
    