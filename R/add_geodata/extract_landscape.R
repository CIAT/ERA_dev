############################################################################
# CSA Compendium Landscape/Landuse Data Extraction

# AUTHOR: Peter R Steward 
# EMAIL: p.r.steward@leeds.ac.uk or peetmate@gmail.com
# Version: 2.0
# Date Created: 16 Aug 2018
# Last edited: 26 May 2021

############################################################################

# Note a more efficient version of this function was developed for the BirdLasser project, integrate with this script as required.

# 1) CREATE FUNCTIONS & LOAD LIBRARIES ####
require(rgdal)
require(raster)
require(rgeos)
require(terra)
require(data.table)

# 2) SET DATA DIRECTORIES  & READ IN META-DATASET - INPUT REQUIRED ####
    options(scipen=999)

    # Set the data directory (should be /Data folder in the Git repository, otherwise please specify for your system)
    setwd(paste0(getwd(),"/Data"))

    # Read in compendium - it should be stored in the \CSA-Compendium\Data\Compendium Master Database folder of the Git
    list.dirs(recursive = F)[grep("Comb", list.dirs(recursive = F))]
    
    FilenameSimple<-list.dirs(recursive = F,full.names = T)[grep("Comb", list.dirs(recursive = F))][1]
    grep("Wide",list.files(FilenameSimple),value=T)
    Filename<-paste0(FilenameSimple,"/",grep("Wide.RData",list.files(FilenameSimple),value=T))
    
    Meta.Table<-miceadds::load.Rdata2(filename=Filename)

  # Choose location of Landcover datasets:
    DataRoot<- "C:/Datasets/"
    LCDir<-paste0(DataRoot,"ESACCI-LC/")
    
  # Set temporary save directory when processing landcover data:
    TempDir<-paste0("C:/CSA Compendium/Datasets/Temp/LC/")
    if(!dir.exists(TempDir)){
      dir.create(TempDir,recursive = T)
    }
    
  # Create a Save Directory for Landscape Data:
    SaveDir<-paste0(getwd(),"/Landscape/")

 # Choose a save name for output file
    SaveName<-"ESA-CCI-LC-2015"
    
# 3) SET ANALYSIS PARAMETERS - INPUT REQUIRED ####
  # Define unique identifier column in dataset
  ID<-"Site.Key"
  # Set Buffer Distance
  Buffer.Dist<-2000
  # Use Fixed Buffer (= Buffer.Dist) or only use buffer when site buffer is <Buffer.Dist (otherwise using buffer specified in dataset)
  FixedBuffer<-F
# 4) READ IN LANDCOVER DATA & CROP ####
  File<-"ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif"
  
  if(!file.exists(paste0(LCDir,"Africa ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif"))){
  CRS.LC<-"+proj=longlat +datum=WGS84 +no_defs +ellps=WGS84 +towgs84=0,0,0"

  CCI.LC<-rast(paste0(LCDir,File))
  
  # Save unique values
  Vals<-unique(CCI.LC)
  fwrite(data.table(Vals),paste0(LCDir,"Vals.csv"))
  
  # Create shape file of Africa
  AfricaMap<-rworldmap::getMap(resolution = "high")
  AfricaMap<-AfricaMap[AfricaMap$REGION=="Africa"&!is.na(AfricaMap$REGION),]
  AfricaMap<-spTransform(AfricaMap,CRS.LC)
  
  # Crop Landcover Map to Africa - VERY SLOW, RUN ONCE AND SAVE!
  CCI.LC<-crop(CCI.LC,AfricaMap)
  
  # reclassify 0 values as NAs
  CCI.LC<-reclassify(CCI.LC,matrix(c(0,0,NA),ncol=3))
  
   }else{
     CCI.LC<-rast(paste0(LCDir,"Africa ESACCI-LC-L4-LCCS-Map-300m-P1Y-2015-v2.0.7.tif"))
   }
  
  # Read in Landcover Legend
  LC.Legend<-fread(paste0(LCDir,"ESACCI-LC-Legend.csv"))

# 5) EXTRACT LANDSCAPE DATA ####
  
    readLandCover<-function(DATA,Landcover,Buffer.Dist,FixedBuffer,ID,SaveDir,SaveName){  
      
    # Subset data for unique spatial locations and IDs
    DATA<-data.table(DATA)
    SS<-unique(DATA[!(is.na(Latitude)|is.na(Longitude)|is.na(Buffer)),c("Latitude","Longitude","Buffer",..ID)])
    
    # Set Site Buffers
    if(FixedBuffer){
      SS[,Buffer:=Buffer.Dist]
    }else{
      SS[Buffer<Buffer.Dist,Buffer:=Buffer.Dist]
    }
    
  # Buffer points
    pbuf<-terra::vect(ERAg::Pbuffer(Data = SS,ID = ID ,Projected=F))
    pbuf<-terra::project(pbuf,Landcover)
    
 # Extract data
    X<-data.table(terra::extract(Landcover,pbuf,exact=T))
    colnames(X)[2]<-"Value"
    LandCover<-dcast(X,ID~Value,fun.aggregate = length)
    
    LandCover[,N.Cells:=X[,list(Sum=.N),by=ID][,Sum]]
    
    LandCover$ID<-SS[,..ID]
    
    setnames(LandCover,"ID","Site.Key")

    if(!is.na(SaveDir)){
      if(!dir.exists(SaveDir)){
        dir.create(SaveDir,recursive = T)
      }
      
      fwrite(LandCover,paste0(SaveDir,SaveName,".csv"))
    }
    
    return(LandCover)
    
    }
    
    LC<-readLandCover(DATA=Meta.Table[Buffer<50000],  # Compendium file with latitude, longtiude, ID and buffer distance (m) fields
                  Landcover=CCI.LC,  # Vrt file of landuse
                  Buffer.Dist=Buffer.Dist,   # Fixed buffer radius
                  FixedBuffer=F, # Use fixed buffer radius or buffer supplied in DATA? if F buffers<Buffer.Dist are set to Buffer.Dist
                  ID = ID, # Name of unique site ID field in DAT 
                  SaveDir = SaveDir, # Directory to save output analysis
                  SaveName = SaveName) # Name of output file
