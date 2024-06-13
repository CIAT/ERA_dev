##########################################################
# ERA Other Data Extraction
# 
# Input: Compiled compendium dataset + Folder of rasters
# Output: Raster summary table for each ERA site + buffer
#
# Author: p.steward@cgiar.org
# 04 Feb 2022
# Last edited: 04 Feb 2022
##########################################################

# 1) Load functions and set wd

if(!require("pacman", character.only = TRUE)){
  install.packages("pacman",dependencies = T)
}

required.packages <- c("data.table","ERAg","miceadds","sp","terra")
p_load(char=required.packages,install = T,character.only = T)

options(scipen=999)

# Set the data directory (should be /Data folder in the Git repository, otherwise please specify for your system)
setwd(paste0(getwd(),"/Data"))

# Read in Meta.Table ####
ERA<-miceadds::load.Rdata2(path="ERA Current Version",file="ERA Wide.RData")


# 2) Extract data
Other_dir<-"C:/Datasets/Other ERA Datasets/"

FILES<-list.files(Other_dir,"tif",recursive = T,full.names = T)
FILES<-FILES[!grepl(".aux",FILES)]
FILES<-gsub("//","/",FILES)

system.time(
  Other_Linked_Datasets<-ExtractRasters(Data=ERA[,Buffer:=as.numeric(Buffer)][Buffer<100000],
                                        ID="Site.Key",
                                        Files=FILES,
                                        Save_Dir="Other Linked Datasets/",
                                        Save_Name=paste0("Other Linked Datasets ",Sys.Date()))
)


