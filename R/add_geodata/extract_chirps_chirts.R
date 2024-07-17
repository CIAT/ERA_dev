# First run R/0_set_env.R and R/add_geodata/download_chirps_chirts.R
# Updated script was lost by SCiO in CGlabs, but needs updating to an exactextractR based approach
# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####
packages<-c("terra","data.table","arrow","pbapply","future","future.apply","sf","exactextractr")
pacman::p_load(char=packages)


# 1) Prepare ERA data ####
SS<-era_locations[Buffer<50000]
extract_by<-era_locations_vect_g[era_locations[,Buffer<50000]]
extract_by<-sf::st_as_sf(extract_by)

# 2) Index climate files ####
# 2.1) Chirps ####
file_index_chirps<-data.table(file_path=list.files(chirps_dir,".tif$",full.names = T,recursive = T))
file_index_chirps[,file_name:=basename(file_path)
][,date:=gsub(".tif","",gsub("chirps-v2.0.","",file_name))
][,date:=as.Date(date,format="%Y.%m.%d")
][,file_size:=(file.info(file_path)$size/10^6)
][,year:=format(date,"%Y")
][,var:="prec"
][,dataset:="chirps_v2.0"]

# File size
(file_size<-file_index[,sum(file_size[1])])
# Total dataset size Gb
if(T){
  file_index[,sum(file_size/1000)]
  file_index[year==2000,sum(file_size)/1000]
}

# 2.2) Chirts ####
if(F){
  file_index_chirts<-data.table(file_path=list.files(chirts_dir,".tif$",full.names = T,recursive = T))
}

# 3) Extract era buffers
# 3.1) Chirps ####
# Chunk by years
file_index<-file_index_chirps

worker_n<-30
files_per_worker<-10
worker_n*file_size*files_per_worker/1000

file_index[, index := ceiling(.I / 10)] # I means the row number in data.table

future::plan("multisession",workers=worker_n)

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

p<-progressr::with_progress({
  progress <- progressr::progressor(along =1:max(file_index$index))
  
  data_ex<-future.apply::future_lapply(1:max(file_index$index),FUN=function(i){
    progress(sprintf("Block %d/%d", i, max(file_index$index)))
    files<-unlist(file_index[file_index$index==i,"file_path"])
    data<-terra::rast(files)
    data_ex<-data.table::rbindlist(exact_extract(data,extract_by,fun=NULL,include_cols="Site.Key"))
    return(data_ex)
  })
  
})

future::plan(sequential)

# Melt and average extracted data
data_ex_melt<-rbindlist(pbapply::pblapply(1:length(data_ex),FUN=function(i){
  dat<-data_ex[[i]]
  dat<-melt(dat,id.vars = c("Site.Key","coverage_fraction"))
  dat<-dat[,list(mean=weighted.mean(value,coverage_fraction,na.rm=T),
                 max=max(value,na.rm = T),
                 min=min(value,na.rm = T)),by=.(Site.Key,variable)]
  return(dat)
}))


data_ex_melt[,mean:=round(mean,1)
             ][,max:=round(max,1)
               ][,min:=round(min,1)
                 ][,date:=as.Date(gsub("chirps-v2.0.","",variable),format="%Y.%m.%d")
                   ][,variable:=NULL]

# 3.1.1) Save chirps ######
arrow::write_parquet(data_ex_melt,file.path(era_dirs$era_geodata_dir,paste0("CHIRPS_",Sys.Date(),".parquet")))
