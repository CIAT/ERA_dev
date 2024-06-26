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
  
for(i in 1:length(pbuf_g)){
  
  cat("Downloading site", i,"/",length(pbuf_g),"buffer size =",pbuf_g$Buffer[i],"\n")
  
  save_dir<-era_dirs$chirps_dir
  
  file<-file.path(era_dirs$chirps_dir,pbuf_g$Site.Key[i])
  
  data<-chirps::get_chirps(object=pbuf_g,
                           dates=as.character(c(date_start,date_end)),
                           server = "CHC",
                           as.matrix=T)
  
  
}