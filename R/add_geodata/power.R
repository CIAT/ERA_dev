# This script downloads NASA POWER data for unique locations in ERA. 
# Run ERA_dev/R/0_set_env before executing this script
# This script is also dependent on altitude data having been calculated for ERA locations:
# Run ERA_dev/R/add_geodata/elevation.R before executing this script

# 0) Set-up workspace ####
  # 0.1) Load packages and create functions #####
packages<-c("terra","data.table","arrow","pbapply","ERAgON")
p_load(char=packages)

process_power<-function(files,
                        parameters,
                        rename,
                        id_field,
                        add_date=T,
                        add_daycount=T,
                        time_origin,
                        add_pet=T,
                        altitude_data){
  cat("Loading power csv files","\n")   
  data<-rbindlist(pbapply::pblapply(1:length(files),FUN=function(i){
    file<-files[i]
    id<-basename(file)
    data<-data.table(id=id,data.table::fread(file))
  }),use.names = T,fill = T)
  
  data[,id:=gsub(" -"," m",basename(id[1])),by=id]
  data[,id:=gsub("m","-",unlist(data.table::tstrsplit(gsub("POWER ","",id[1]),"-",keep=1))),by=id]
  
  param_cols<-colnames(data)[-(1:3)]
  data[,N:=.N,by=list(id,YEAR,DOY)]
  
  # Calculate means and round them for each group
  cat("Averaging data for sites with multiple cells, this may take some time.","\n")  
  
  means <- data[N>1, lapply(.SD, function(x) mean(x, na.rm = TRUE)), 
                .SDcols = param_cols, by = .(id, YEAR, DOY)]
  
  means[, `:=`((param_cols), round(.SD, 2)), .SDcols = param_cols]
  
  data<-rbind(data[N==1][,N:=NULL],means)
  
  data[,Latitude:=as.numeric(unlist(tstrsplit(id[1]," ",keep=1))),by=id
       ][,Longitude:=as.numeric(unlist(tstrsplit(id[1]," ",keep=1))),by=id]
  
  setnames(data,
           c("id", "DOY", "YEAR","PRECTOTCORR"),
           c(id_field, "Day", "Year","PRECTOT"),
           skip_absent = T)
  
  data<-merge(data,altitude_data[,Altitude])
  
  if(add_pet){
    data[,ETo:=ERAgON::PETcalc(Tmin=T2M_MIN,
                                Tmax=T2M_MAX,
                                SRad=ALLSKY_SFC_SW_DWN,
                                Wind=WS2M,
                                Rain=PRECTOT,
                                Pressure=PSC,
                                Humid=RH2M,
                                YearDay=Day,
                                Latitude=Latitude,
                                Altitude=Altitude)[,1]]
    }
  
  setnames(data,parameters,rename,skip_absent = T)
  
  if(add_date) {
    data[, Date:=as.Date(paste0(Year[1],"-",Day[1]),format="%Y-%j"), by = list(Day, Year)]
  }
  
  if (add_daycount) {
    data[, DayCount:=as.integer(floor(unclass(Date[1] -time_origin))), by = Date]
  }
  
    return(data)
}

download_power<-function(vect_data,date_start,date_end,altitude,save_dir,parameters,id,verbose,overwrite){
  
  vect_ext<-round(terra::ext(vect_data),5)
  base_url <- "https://power.larc.nasa.gov/api/temporal/daily/"
  
  date_start <- as.Date(date_start)
  date_end <- as.Date(date_end)
  options(warn = -1)
  
  lonmin <- vect_ext[1]
  lonmax <- vect_ext[2]
  latmin <- vect_ext[3]
  latmax <- vect_ext[4]
  
  lat_vals <- seq(-89.75, 89.75, 0.5)
  lon_vals <- seq(-179.75, 179.75, 5/8)
  
  lonmax <- lon_vals[which.min(abs(lon_vals - lonmax))]
  lonmin <- lon_vals[which.min(abs(lon_vals - lonmin))]
  latmax <- lat_vals[which.min(abs(lat_vals - latmax))]
  latmin <- lat_vals[which.min(abs(lat_vals - latmin))]
  n_cells <- expand.grid(lat = unique(c(latmin, latmax)), 
                         lon = unique(c(lonmin, lonmax)))
  
  for (j in 1:nrow(n_cells)) {
    file_name <- file.path(save_dir, paste0("POWER ", id, "-",j, ".csv"))
    if (!file.exists(file_name)|overwrite) {
      URL <- paste0(base_url, 
                    "point?start=", format(date_start,"%Y%m%d"), 
                    "&end=", format(date_end, "%Y%m%d"), 
                    "&latitude=", n_cells[j, "lat"], 
                    "&longitude=",n_cells[j, "lon"], 
                    "&community=ag", 
                    "&parameters=",paste(parameters, collapse = "%2C"),
                    "&format=csv", 
                    "&user=ICRAF2", 
                    "&header=true&time-standard=lst", 
                    "&site-elevation=", altitude)
      
      if(verbose){
        cat("\r                                                                                                                                          ")
        cat("\r", "POWER: Downloading site", id,"| Cell", j,"/",nrow(n_cells))
        flush.console()
      }
      
      tryCatch(download.file(URL, file_name, method = "libcurl", 
                             quiet = !verbose, mode = "w", cacheOK = TRUE), 
               error = function(e) print(paste(file_name, "did not work out")))
    }
  }
  options(warn = 0)
}

  # 0.2) Subset era sites according to buffer distance #####
  # Set max buffer allowed
  SS<-era_locations[Buffer<50000]
  pbuf_g<-era_locations_vect_g[era_locations[,Buffer<50000]]

  # 0.3) Create POWER parameter object #####
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
  # 0.4) Set power data start and end dates #####
  date_start<-"1984-01-01"
  date_end<-"2023-12-31"
  
  # 0.5) Update sites whose max(date)<end_date? #####
  update_missing_times<-F
  
# 1) Download existing Data from S3 ####
  # 1.1) Altitude #####
  # Load altitude data
  era_elevation<-data.table(arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"era_site_topography.parquet")))
  # Merge altitude with site buffer vector
  pbuf_g<-merge(pbuf_g,era_elevation[,list(Site.Key,Altitude.mean)])
  
  altitude_data<-era_elevation[,list(Site.Key,Altitude.mean)]
  setnames(altitude_data,"Altitude.mean","Altitude")
  
  # 1.2) POWER ##### 
    # 1.2.1) Download POWER files ######
  s3_file<-gsub(era_s3,era_s3_http,era_dirs$power_S3_file)
  zip_file<-gsub(era_dirs$power_S3,era_dirs$power_dir,era_dirs$power_S3_file)
  
  list.files(era_dirs$power_dir)
  
  update<-F
  rm_zip<-T
  if(update){
    download.file(url=s3_file,destfile=zip_file)
    unzip(zipfile=zip_file,overwrite = T,exdir=era_dirs$power_dir,junkpaths = T)
    if(rm_zip){
      unlink(x=zip_file,recursive = T)
    }
  }
  
    # 1.2.2) Process POWER files ######
  update<-F
  power_file<-file.path(era_dirs$era_geodata_dir,"POWER.parquet")
  
  if(update){
    files<-list.files(era_dirs$power_dir,".csv$",full.names = T)
    
    power_data<-process_power(files=files,
                              parameters = parameters,
                              rename = names(parameters),
                              id_field= "Site.Key",
                              add_date=T,
                              add_daycount = T,
                              time_origin = time_origin,
                              add_pet=T,
                              altitude_data=altitude_data)
    
    arrow::write_parquet(power_data,power_file)
  }else{
   power_data<-arrow::read_parquet(power_file)
  }
              
# 1.3) Subset sites #####
  if(update_missing_times){
    exclude_sites<-power_data[,list(exclude_site=max(Date)>=date_end),list(Site.Key)]
    exclude_sites<-exclude_sites[exclude_site==T,unique(Site.Key)]
  }else{
    exclude_sites<-power_data[,unique(Site.Key)]
  }
  
  pbuf_g<-pbuf_g[!pbuf_g$Site.Key %in% exclude_sites,]

# 2) Download POWER ####
  # 2.2) Run download function ####
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  
  # Need to create loop that incorporates updated dates
  # Names for min and max were the wrong way around in the previous version of this function
  # Split download and compilation functions
  
  for(i in 1:length(pbuf_g)){

  cat("\r", "POWER: Downloading site", i,"/",length(pbuf_g),"\n")
    
  save_dir<-era_dirs$power_dir
  
  download_power(vect_data=pbuf_g[i],
                 date_start = date_start,
                 date_end=date_end,
                 altitude=pbuf_g[i]$Altitude.mean,
                 save_dir=era_dirs$power_dir,
                 parameters = parameters,
                 id=pbuf_g[i]$Site.Key,
                 verbose=F,
                 overwrite=update_missing_times)
  }
  

 # 2.3) Recompile Data
  files<-list.files(era_dirs$power_dir,".csv$",full.names = T)
  files<-grep(paste0(pbuf_g$Site.Key,collapse="|"),files,value=T)
  power_data_new<-process_power(files=files,
                            parameters = parameters,
                            rename = names(parameters),
                            id_field= "Site.Key",
                            add_date=T,
                            add_daycount = T,
                            time_origin = time_origin,
                            add_pet=T,
                            altitude_data=altitude_data)
  
  power_data_new[!grep("B",Site.Key),unique(Site.Key)]
  
  power_data<-unique(rbind(power_data,power_data_new))
  
  arrow::write_parquet(power_data,power_file)
  
  # Problem rows
  power_data[,is.na(Year)]
  

  # 2.4) Calculate annual averages ####
  power_data<-arrow::read_parquet(power_data,power_file)
  
    # 2.4.1) Remove incomplete years ####
    power_data<-power_data[,N:=.N,by=list(Site.Key,Year)][!N<365][,N:=NULL]
    
    # 2.4.2) Calculate annual data ####
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
                              Temp.Min=min(Temp.Min)),by=c("Site.Key","Year")]
    
    # 2.4.3) Calculate LT Data ####
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
    ),by=c("Site.Key")]
  
  # Identify numeric columns
  numeric_cols <- names(power_data_lt)[sapply(power_data_lt, is.numeric)]
  
  # Round numeric columns to 2 decimal places
  power_data_lt[, (numeric_cols) := lapply(.SD, round, 1), .SDcols = numeric_cols]
    
    # 2.4.4) Save Annual & LT datasets ####
    arrow::write_parquet(power_data_annual,file.path(era_dirs$era_geodata_dir,"POWER_annual.parquet"))
    arrow::write_parquet(power_data_lt,file.path(era_dirs$era_geodata_dir,"POWER_ltavg.parquet"))
    