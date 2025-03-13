# This script downloads elevation data for unique locations in ERA. 
# Run ERA_dev/R/0_set_env before executing this script
# 0) Set-up workspace ####
# 0.1) Load packages and create functions #####
  packages<-c("elevatr","terra","sf","data.table")
  pacman(char=packages)


  # Function for making a circle path for geom_poly() in R
  circleFun <- function(center = c(0,0),diameter = 1, npoints = 100){
    r = diameter / 2
    tt <- seq(0,2*pi,length.out = npoints)
    xx <- center[1] + r * cos(tt)
    yy <- center[2] + r * sin(tt)
    return(data.frame(x = xx, y = yy))
  }

  # Set cores for parallel processing
  cores<-max(1, detectCores() - 1)
  
  options(scipen=999)

# 0.2) SET DATA DIRECTORIES ####
  # Set locations
  DataRoot<- "D:/Datasets/"

  
  # Specify DEM data directory:
  DEMdir<-paste0(DataRoot,"JF DEMS/")
  
  TempDir<-paste0(DataRoot,"era_temp_files/")
  if(!dir.exists(TempDir)){
    dir.create(TempDir,recursive = T)
  }
  
  SoilDir<-paste0(DataRoot,"SoilGrids19/")

# 3) SET ANALYSIS & PLOTTING PARAMETERS - INPUT REQUIRED ####

  # Name of Site.Key Column
  ID<-"Site.Key"
  
  # Note any SoilGrids files where values refer to categories and we should record the modal cell values not the mean:
  modalFiles<-c("TAXOUSDA_250m.tif","TAXNWRB_250m.tif")

# 4) Prepare ERA data ####
  SS<-era_locations[Buffer<50000]
  pbuf<-era_locations_vect_p[era_locations[,Buffer<50000]]
  pbuf_g<-era_locations_vect_g[era_locations[,Buffer<50000]]

# 5) Download DEM data ####
  update<-F
  
  era_topo_file<-file.path(era_dirs$era_geodata_dir,"era_site_topography.parquet")
  
  # Note this section should be update to use the terra terrain function to estimte slope and aspect
  # This will require padding the site buffer by a couple of cells so the DEM downloaded extends beyond
  # the edge of the buffer allowing slope and aspect to be calculated for all cells within in the buffer.
  
  # Get high resolution DEM of Africa
  # for info on z values https://github.com/tilezen/joerd/blob/master/docs/data-sources.md#what-is-the-ground-resolution
  # z =12 is about 75m resoltion
  
   era_elevation<-rbindlist(pblapply(1:length(pbuf_g),FUN=function(i){
      site_vect<-pbuf_g[i]
      dem_file<-file.path(era_dirs$dem_dir,paste0(site_vect$Site.Key,".tif"))
      
      if(!file.exists(dem_file)|update==T){
  
        # Create empty rast with extent of africa
        site_rast <- terra::rast()
        ext(site_rast) <- terra::ext(site_vect)
        site_dem <- terra::rast(elevatr::get_elev_raster(site_rast, z = 11, clip = "bbox",src="aws",neg_to_na=T,verbose=F))
        site_dem<-terra::mask(site_dem,site_vect)
        writeRaster(site_dem,dem_file, overwrite = TRUE)
        
       
      }else{
        site_dem<-terra::rast(dem_file)
      }
      
      site_dem<-values(site_dem)
      site_dem<-site_dem[!is.na(site_dem)]
      
      PData<-data.table(
        DEMcells=length(site_dem),
        Altitude.med=median(site_dem,na.rm=T),
        Altitude.mean=round(mean(site_dem,na.rm=T),0),
        Altitude.sd=round(sd(site_dem,na.rm=T),0),
        Altitude.max=max(site_dem,na.rm=T),
        Altitude.min=min(site_dem,na.rm=T)
      )
      
      PData
      
    }))
   era_elevation<-cbind(Site.Key=pbuf_g$Site.Key,era_elevation)
 # Save resulting dataset
   
   arrow::write_parquet(era_elevation,era_topo_file)
  
# 8) EXTRACT & SAVE SOILGRIDS DATA ####
  # File name codes: http://www.isric.org/explore/soilgrids/faq-soilgrids
  # Variable descriptions - http://www.isric.org/sites/default/files/isric_report_2015_02.pdf & https://github.com/ISRICWorldSoil/SoilGrids250m/blob/master/grids/models/META_GEOTIFF_1B.csv
  # Data source ftp://ftp.soilgrids.org/data/recent/


  # Can could existing rasters into a list for each variable to make retrival faster, but I suspect it is the extraction that is slow:
    # get files names using spilt on "_" and then apply(X, `[[`, 1)
    # get folder names load and add velox rasters that match the name to a list and save
  # This is a very memory instensive function so parallelization seems unwise
  # "OCDENS_M_sl2_250m.tif" causing an issue with South Africa - Failure during raster IO, redownloaded raster, did not fix issue. OCDENS excluded within function at present.
  # Does not allow masking according to slope, will need to create a function to mask the buffers instead
  # VeloxComplete saves reading in the SoilGrids rasters if all the velox raster have already been generated, logical (T/F)
  # Function modified to look for existing dataset with extracted and update as needed for missing sites
  
  
  ExtractFolder<-function(Data,ID,DataDir,ROUND,Verbose,Wide,cores){
    
    # Subset data
    SS<-unique(Data[!(is.na(Data$Latitude)|is.na(Data$Longitude)),c("Latitude","Longitude","Buffer",..ID)])
    

        # Make buffer function
    Pbuffer<-function(DATA,ID,Projected=F,VERBOSE){
      DATA<-data.frame(DATA) 
      SS<-unique(DATA[,c("Latitude","Longitude",ID,"Buffer")])
      # CRS of Site co-ordinates (lat/long)
      CRS.old<- "+init=epsg:4326"
      # CRS when creating buffers in meters (WGS 1984 World Mercator)
      CRS.new<-"+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"
      
      # Buffer points
      points <- SpatialPoints(cbind(SS$Longitude, SS$Latitude),proj4string=CRS(CRS.old))
      points <- spTransform(points, CRS.new)
      
      pbuf1<-lapply(1:nrow(SS),FUN=function(i){
        if(VERBOSE){
          print(i)
        }
        pbuf<- gBuffer(points[i], widt=SS$Buffer[i])
        pbuf<- spChFIDs(pbuf, paste(i, row.names(pbuf), sep="."))
      })
      
      pbuf1<-SpatialPolygons(lapply(pbuf1, function(x){x@polygons[[1]]}),proj4string=CRS(CRS.new))
      
      if(Projected==T){
        return(pbuf1)
      }else{
        return(spTransform(pbuf1,CRS(CRS.old)))
      }
    }
    
     # Progress update
    if(Verbose){
    cat('\r                                                                                                              ')
    cat('\r',"Step 1: Buffering points" )
    flush.console()
    }
    
    # List Soilgrids Files
    Files<-list.files(DataDir,".tif")

    # Buffer points - projected - geographic
    pbuf_g<-Pbuffer(DATA = SS,ID = ID ,Projected=F,VERBOSE=F)
    pbuf_g<-vect(pbuf_g)
    
    RASTER<-rast(paste0(DataDir,Files[1]))
    pbuf_g<-project(pbuf_g,RASTER)

    getmode <- function(v) {
      v<-v[!is.na(v)]
      uniqv <- unique(v)
      uniqv[which.max(tabulate(match(v, uniqv)))]
    }
    

    # Loop through each soil grids file
    if(cores>1){
      
      if(Verbose){
        cat('\r                                                                                                              ')
        cat('\r',"Step 2: Extracting Data")
        flush.console()
      }
      
      
     cl<-makeCluster(cores)
     clusterEvalQ(cl, list(library(data.table),library(terra)))
     clusterExport(cl,list("Files","DataDir","pbuf_g","Wide","ID","getmode"),envir=environment())
     registerDoSNOW(cl)
     
     Cvals<-parLapply(cl,1:length(Files),fun=function(i){
       
       Layer<-gsub(".tif","",Files[i])
       RASTER<-rast(paste0(DataDir,Files[i]))
       
       X<-data.table(terra::extract(RASTER,pbuf_g))
       
       colnames(X)[2]<-"Variable"
       
       Cvals<-X[,list(Mean=mean(Variable,na.rm=T),
                      Median=median(Variable,na.rm=T),
                      Mode=getmode(Variable),
                      SD=sd(Variable,na.rm=T),
                      Quantiles=paste(quantile(Variable,na.rm=T),collapse="|")),
                by=ID]
       Cvals
       
      })
    
     stopCluster(cl)
     
    }else{
      
      Cvals<-pblapply(1:length(Files),FUN=function(i){
        
        if(Verbose){
          cat('\r                                                                                                              ')
          cat('\r',paste0("Step 2: Extracting Data: ",i,"/",length(Files)," - ",Files[i]))
          flush.console()
        }
        
        Layer<-gsub(".tif","",Files[i])
        RASTER<-rast(paste0(DataDir,Files[i]))
        
        X<-data.table(terra::extract(RASTER,pbuf_g))
        
        colnames(X)[2]<-"Variable"
        
           Cvals<-X[,list(Mean=mean(Variable,na.rm=T),
                         Median=median(Variable,na.rm=T),
                         Mode=getmode(Variable),
                         SD=sd(Variable,na.rm=T),
                         Quantiles=paste(quantile(Variable,na.rm=T),collapse="|")),
                   by=ID]
          
        if(Wide){
          colnames(Cvals)[2:4]<-paste0(Layer,".",colnames(Cvals)[2:4])
        }else{
          Cvals[,Variable:=Layer]
        }
      
        
        Cvals
      })
    }
    
    if(Wide==T){
      Cvals<-do.call("cbind",Cvals)
      SS<-cbind(SS,Cvals[,-1])
    }else{
      Cvals<-rbindlist(Cvals)
      SS<-cbind(SS[Cvals[,ID]],Cvals[,-1])
    }
      
  
    return(SS)
  }
  
  system.time(SOILS_DATA2<-ExtractFolder(Data = Meta.Table[Buffer<50000],
                                         ID = ID,
                                         DataDir = SoilDir,
                                         ROUND=2,
                                         Verbose=T,
                                         Wide=F,
                                         cores=1))
  
  

  if(!is.na(paste0(Folder,"/Soils"))){
    if(!dir.exists(paste0(Folder,"/Soils"))){
      dir.create(paste0(Folder,"/Soils"),recursive = T)
    }
    fwrite(SOILS_DATA2,paste0(Folder,"/Soils/ERA_SoilGrids19.csv"),row.names = F)
  }
  


# Download soilgrids data ####
  # https://cran.r-project.org/web/packages/geodata/geodata.pdf
  # https://files.isric.org/soilgrids/latest/data/ - most recent data
  
  # https://files.isric.org/public/afsis250m/ - uploaded 2019, superceded by better 1km product but with fewer variables? (see link above)
  
  vars<-c("clay","sand","silt","coarse","SOC","BLKD","poros","AWpF2.0","AWpF2.3","AWpF2.5","AWpF4.2","BDR","pH","ECN","acid-exch","bases-exch","CEC",
          "Al-extr","Al-exch","Ca-exch","K-exch","Mg-exch","Na-exch","Ntot")
  
  depths<-c(5,15,30,60,100,200)
  
  path<-"C:/Datasets/SoilGrids"
  
  path<-paste0(path,"/Downloads ",Sys.Date())
  
  if(!dir.exists(path)){
    dir.create(path,recursive = T)
  }
  
  require(geodata)
  
  vars.subset<-c("clay","sand","silt","SOC","BLKD","CEC","pH")
  depths.subset<-c(60,100,200)
  
  DLs<-expand.grid(vars.subset,depths.subset)
  
  lapply(1:nrow(DLs),FUN=function(i){
    print(paste("Downloading: ",DLs[i,1],DLs[i,2]))
    soil_af(var=DLs[i,1], depth=DLs[i,2], path=tempdir())
  })
  
# Extract soilgrids data ####


  
  