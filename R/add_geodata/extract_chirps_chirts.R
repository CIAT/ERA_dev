# 0) Set-up workspace ####
  # 0.1) Load packages and create functions #####
  packages<-c("terra","data.table","arrow","pbapply","future","future.apply","chirps")
  p_load(char=packages)
  
  # 0.2) Subset era sites according to buffer distance #####
  # Set max buffer allowed
  SS<-era_locations[Buffer<50000]
  pbuf_g<-era_locations_vect_g[era_locations[,Buffer<50000]]
  
  # 0.4) Set chirps data start and end dates #####
  date_start<-"1981-01-01"
  date_end<-"2023-12-31"
  
  # 0.5) Update sites whose max(date)<end_date? #####
  update_missing_times<-F

# 1) Download existing Data from S3 ####
  # 1.1) Download files ######
  s3_file<-gsub(era_s3,era_s3_http,era_dirs$chirps_S3_file)
  zip_file<-gsub(era_dirs$chirps_S3,era_dirs$chirps_dir,era_dirs$chirps_S3_file)
  
  list.files(era_dirs$chirps_dir)
  
  update<-F
  rm_zip<-T
  
  if(update){
    download.file(url=s3_file,destfile=zip_file)
    unzip(zipfile=zip_file,overwrite = T,exdir=era_dirs$chirps_dir,junkpaths = T)
    if(rm_zip){
      unlink(x=zip_file,recursive = T)
    }
  }
  # 1.2) Process files ######
  update<-F
  chirps_file<-file.path(era_dirs$era_geodata_dir,"chirps.parquet")
  
  if(update){
    files<-list.files(era_dirs$chirps_dir,".csv$",full.names = T)
    
    chirps_data<-process_chirps(files=files,
                              parameters = parameters,
                              rename = names(parameters),
                              id_field= "Site.Key",
                              add_date=T,
                              add_daycount = T,
                              time_origin = time_origin,
                              add_pet=T,
                              altitude_data=altitude_data)
    
    arrow::write_parquet(chirps_data,chirps_file)
  }else{
    chirps_data<-arrow::read_parquet(chirps_file)
  }
  
  # 1.3) Subset sites #####
  if(update_missing_times){
    exclude_sites<-chirps_data[,list(exclude_site=max(Date)>=date_end),list(Site.Key)]
    exclude_sites<-exclude_sites[exclude_site==T,unique(Site.Key)]
  }else{
    exclude_sites<-chirps_data[,unique(Site.Key)]
  }
  
  pbuf_g<-pbuf_g[!pbuf_g$Site.Key %in% exclude_sites,]
  
# 2) Download CHIRPS ####
  # 2.2) Run download function ####
curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection

# Need to create loop that incorporates updated dates
# Names for min and max were the wrong way around in the previous version of this function
# Split download and compilation functions

  date_start<-"1981-01-02"
  date_end<-"2020-12-31"
  
  remove_trailing_slash <- function(path) {
    if (stringr::str_ends(path, "/")) {
      path <- stringr::str_sub(path, 1, -2)
    }
    return(path)
  }
  
  create_chirps_paths<-function(base_url,format,startyear,endyear,variable_dir,variable,dataset){
    base_url<-remove_trailing_slash(base_url)
    if(format=="tif"){
      file_paths <- data.table(date=seq.Date(from = as.Date(paste0(startyear, "-01-01")),
                                            to = as.Date(paste0(endyear, "-12-31")),
                                            by = "day")
                              )[,year:=format(date,"%Y")]
      
      if(!is.na(variable_dir)){
        file_paths<-file_paths[,file_path:=file.path(base_url,variable_dir,year,paste0(variable,".",gsub("-",".",date),".",format))
                                  ][,file_path]
      }else{
        file_paths<-file_paths[,file_path:=file.path(base_url,year,paste0(variable,".",gsub("-",".",date),".",format))
                   ][,file_path]
      }
      
    }
    
    if(grepl(".nc",format)){
      if(dataset=="chirts"){
        if(is.na(variable_dir)){
          file_paths<-file.path(base_url,paste0(variable,".",year,".",format))
        }else{
          file_paths<-file.path(base_url,variable_dir,paste0(variable,".",year,".",format))
        }
      }
    }
    
    if(exists("file_paths")){
      return(file_paths)
    }else{
      return(NA)
    }
  }

  chirps_index<-rbind(
  data.table(dataset="chirts",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/global_tifs_p05/",
             variable=c("HeatIndex","RH","Tmax","Tmin","vpd"),
             variable_dir=c("HeatIndex","RHum","Tmax","Tmin","vpd"),
             startyear=1983,
             endyear=2016,
             format="tif",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirts",
              region="africa",
              base_url="https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/africa_netcdf_p05/",
              variable=c("Tmax","Tmin"),
              variable_dir=NA,
              startyear=1983,
              endyear=2016,
              format="nc",
             timestep="daily",
              resolution=5),
  data.table(dataset="chirts",
              region="africa",
              base_url="https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/africa_netcdf_p25/",
              variable=c("Tmax","Tmin"),
              variable_dir=NA,
              startyear=1983,
              endyear=2016,
              format="nc",
             timestep="daily",
              resolution=25),
  data.table(dataset="chirts",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRTSdaily/v1.0/global_netcdf_p05/",
             variable=c("Tmax","Tmin"),
             variable_dir=c("Tmax","Tmin"),
             startyear=1983,
             endyear=2016,
             format="nc",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirps",
             region="africa",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirps",
             region="africa",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/africa_daily/tifs/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=5),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/tifs/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="tif.gz",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p25",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="days_p25.nc",
             timestep="daily",
             resolution=25),
  data.table(dataset="chirps",
             region="global",
             base_url="https://data.chc.ucsb.edu/products/CHIRPS-2.0/global_daily/netcdf/p05",
             variable=c("chirps-v2.0"),
             variable_dir=NA,
             startyear=1981,
             endyear=2023,
             format="days_p05.nc",
             timestep="daily",
             resolution=5)
  )
  
  chirps_index<-lapply(1:nrow(chirps_index),FUN=function(i){
    x<-data.frame(chirps_index[i])
    file_paths<-create_chirps_paths(base_url=x$base_url,
                                    format=x$format,
                                    startyear=x$startyear,
                                    endyear=x$endyear,
                                    variable_dir=x$variable_dir,
                                    variable=x$variable,
                                    dataset=x$dataset)
    
    x$file_paths<-list(file_paths)
    return(x)
  })

  chirps_index<-data.table(do.call("rbind",chirps_index))
  
  chirps_index[grepl("chirps",variable),variable:="prec"
               ][,variable:=tolower(variable)
                 ][,variable_dir:=NULL
                   ][grepl("tif",format),format:="tif"
                     ][grepl("nc",format),format:="nc"
                       ][,base_url:=NULL]
  
  chirps_index[,!"file_paths"]
  
  
  download_chirps<-function(chirps_index,var,reg,res,type,startyear,endyear,save_dir){
    
  files<-unlist(chirps_index[variable==var & region==reg & resolution==res & format==type][,file_paths])
  
  files<-data.frame(url=files[grepl(paste(startyear:endyear,collapse="|"),files)],
                    local_path=file.path(save_dir,basename(url)))
  
      
  }
  
  
