# First run:
  # R/0_set_env.R
  # R/add_geodata/chirps.R,
  # R/add_geodata/power.R
  # R/add_geodata/soilgrids.R

# 0) Set-up workspace ####
  ## 0.1) Load packages & source functions ####
  p_load(data.table,future,future.apply,progressr,pbapply,parallel,arrow)
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/water_balance.R")
  
  ## 0.2) Set workers for parallel processing ####
  worker_n<-detectCores()-1
  
# 1) Read in horizon data ####
horizon_data <- arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"era_site_soil_af_isda.parquet"))
(horizon_metadata<-unique(fread(file.path(era_dirs$era_geodata_dir,"soil_af_isda_metadata.csv"))))
vars<-data.table(from=c("depth","sand.tot.psa","silt.tot.psa","clay.tot.psa","oc","db.od","ecec.f","ph.h2o"),
                 to=c("depth","SNDPPT", "SLTPPT", "CLYPPT", "ORCDRC", "BLD", "CEC", "PHIHOX"),
                 conversion=c(NA,10,10,10,1,1,1,10))

horizon_data<-horizon_data[variable %in% vars[,from] & stat=="mean"]
horizon_data<-dcast(horizon_data,Site.Key+depth~variable,value.var = "value")
horizon_data[,depth:=gsub("0-20cm",10,depth)][,depth:=gsub("20-50cm",35,depth)][,depth:=as.numeric(depth)]

for (i in seq_len(nrow(vars))) {
  old_col <- vars$from[i]
  new_col <- vars$to[i]
  conv    <- vars$conversion[i]
  
  # Rename the column
  data.table::setnames(horizon_data, old = old_col, new = new_col)
  
  # Multiply by the conversion factor if not NA
  if (!is.na(conv)) {
    horizon_data[[new_col]] <- horizon_data[[new_col]] * conv
  }
}

setDT(horizon_data)

# 2) Read in daily weather data ####
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

weather_data<-merge(power,chirps[,.(Site.Key,DayCount,Rain_chirps)],by=c("Site.Key","DayCount"),all.x=T,sort=F)  

# Replace POWER rain with CHIRPS rain
weather_data[,Rain:=Rain_chirps][,Rain_chirps:=NULL]
power<-NULL
chirps<-NULL

# Rename fields
from<-c("Site.Key","Date","Temp.Min","Temp.Max","Temp.Mean","Rain","SRad")
to<-c("Site.Key", "DATE", "TMIN", "TMAX", "TMEAN", "RAIN", "SRAD")

setnames(weather_data,from,to)

weather_data<-weather_data[,to,with=F]

# Remove rows with any NA values
weather_data<-weather_data[!is.na(TMIN) & !is.na(RAIN)]

# 3) Run the water balance pipeline ####
site<-horizon_data[,unique(Site.Key)[1]]

options(future.globals.maxSize = 11 * 1024^3)  # 11 GB limit

watbal_result <- run_full_water_balance(
  horizon_data  = horizon_data,
  weather_data  = weather_data,
  worker_n=worker_n,
  root_depth    = 60,
  min_depth     = 45,
  max_depth     = 100,
  id_field="Site.Key"
)

  ## 3.1) Save results ####
  arrow::write_parquet(watbal_result,file.path(era_dirs$era_geodata_dir,paste0("water_balance_",Sys.Date(),".parquet")))

# 4) Validate Results ####
val_out <- validate_watbal(watbal_result, site=site, make_plots=TRUE)

 # Show summary stats
   print(val_out$summary_stats)

 # Inspect any out-of-bounds AVAIL or LOGGING rows:
   print(val_out$invalid_avail)
   print(val_out$invalid_logging)
   
   watbal_result[,unique(LOGGING)]

 # Display the plots:
   print(val_out$plots$avail_plot)
   print(val_out$plots$rain_runoff_plot)

