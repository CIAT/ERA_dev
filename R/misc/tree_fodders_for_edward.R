# Exploring ERA livestock data - tree fodders
# Author - pete Steward

# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(data.table,treemap)

if(!require(ERAg)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAg",build_vignettes = T)
  library(ERAg)
}

# See these vignettes to understand more about the ERAg package 
browseVignettes("ERAg")

# This is the processed ERA dataset: 
head(ERA.Compiled)

# The descriptions of each field in the dataset can be found here"
ERAg::ERACompiledFields

# Note this is a greatly simplified version of the livestock we have, take a look at the full livestock data 
# extraction template to see the more detailed data we collate.
# https://github.com/CIAT/ERA_dev/raw/main/data_entry/skinny_cow_2022/excel_data_extraction_template/V1.06.3%20-%20Skinny%20Cow.xlsm


# Here you can find descriptions of major ERA concepts (practices, outcomes, products)
head(ERAg::PracticeCodes)
head(ERAg::OutcomeCodes)
head(ERAg::EUCodes) # Products (e.g. livestock species)

# These are the papers included in ERA
head(ERAg::ERA_Bibliography)

# This is linked to the ERA.Compiled table by the publication code
ERA_Bibliography[ERACODE==ERA.Compiled$Code[7231]]

# 1) Subset data to experiments using tree fodders
PracticeCodes[,unique(Theme)]
PracticeCodes[Theme=="Animals",unique(Practice)]
PracticeCodes[Practice %in% c("Feed Addition","Feed Substitution"),Subpractice]
(focal_pracs<-PracticeCodes[Practice %in% c("Feed Addition","Feed Substitution") 
                            & grepl("Agrofor",Subpractice),Code])

data<-ERA.Compiled[grepl(paste(focal_pracs,collapse = "|"),plist)]

# 2) Subset to cattle and small ruminants
EUCodes[,unique(Product.Type)]
EUCodes[Product.Type=="Animal",unique(Product.Simple)]

focal_prods<-c("Cattle","Goat","Sheep")

data<-data[Product.Simple %in% focal_prods]

# 3) Subset to outcomes of interest
OutcomeCodes[Pillar=="Productivity",Subindicator]
focal_out<-c("Meat Yield","Weight Gain","Milk Yield")

data<-data[Out.SubInd %in% focal_out]

# 3) Explore results

# Subpractice Level
data[,.(no_studies=length(unique(Code)),
        no_observations=.N,
        no_countries=length(unique(Country))),by=.(SubPrName,Product.Simple,Out.SubInd)
     ][order(no_studies,decreasing=T)]

# Practice Level
data[,.(no_studies=length(unique(Code)),
        no_observations=.N,
        no_countries=length(unique(Country))),by=.(PrName,Product.Simple,Out.SubInd)
     ][order(no_studies,decreasing=T)]

# Remove mechanical processing (this is just going to be chopping tree forages, probably not of interest)
data[,SubPrName:=gsub("-Feed Mech Process|-Grazing Cut & Carry","",SubPrName)]

data[,.(no_studies=length(unique(Code)),
        no_observations=.N,
        no_countries=length(unique(Country))),by=.(SubPrName,Product.Simple,Out.SubInd)
][order(no_studies,decreasing=T)]

# Looking at any practices than involve tree fodder addition or substitution
data<-data[,tree_fodder_add:=F][grepl("Feed AgFor (Add)",SubPrName,fixed=T),tree_fodder_add:=T]
data<-data[,tree_fodder_sub:=F][grepl("Feed AgFor (Sub)",SubPrName,fixed=T),tree_fodder_sub:=T]

# Addition
data[tree_fodder_add==T,.(no_studies=length(unique(Code)),
        no_observations=.N,
        no_countries=length(unique(Country))),by=.(Product.Simple,Out.SubInd)
][order(no_studies,decreasing=T)]

# Substitution
data[tree_fodder_sub==T,.(no_studies=length(unique(Code)),
                          no_observations=.N,
                          no_countries=length(unique(Country))),by=.(Product.Simple,Out.SubInd)
][order(no_studies,decreasing=T)]

# Unfortunately there is very little error data in this dataset
# It will be available in an improved and updated dataset that I need to run through some comparison logic to recreate the ERA.Compiled dataset

# Example of how to access values
data_subset<-data[Product.Simple=="Goat" & Out.SubInd=="Weight Gain" & tree_fodder_sub==T,.(Code,Country,Site.ID,MeanC,MeanC.Error,MeanT,MeanT.Error,Rep,Units,Duration,Tree,Diversity,Variety)]

# Harmonize units
data_subset[Units=="kg" & !is.na(Duration),c("MeanT","MeanC","Units"):=.(round(1000*MeanT/(365*Duration),2),round(1000*MeanC/(365*Duration),2),"*g/d")
            ][Units=="kg/d",c("MeanT","MeanC","Units"):=.(round(1000*MeanT,2),round(1000*MeanC,2),"g/d")]
