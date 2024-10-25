# Exploring ERA livestock data - tree fodders
# Author - Pete Steward

# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(data.table,treemap,s3fs,arrow,devtools)

if(!require(ERAgON)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAgON",build_vignettes = T)
  library(ERAgON)
}

if(!require(ERAg)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAg",build_vignettes = T)
  library(ERAg)
}

# options(arrow.unsafe_metadata=TRUE)

# 1) Explore ERA ####
# See these vignettes to understand more about the ERAg package 
browseVignettes("ERAg")

# This is the processed ERA dataset: 
head(ERA.Compiled)
dim(ERA.Compiled)
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

# 1.1) List the names of feed practices that are involved in agroforestry 
PracticeCodes[,unique(Theme)]
PracticeCodes[Theme=="Animals",unique(Practice)]
PracticeCodes[Practice %in% c("Feed Addition","Feed Substitution"),Subpractice]
(focal_pracs<-PracticeCodes[Practice %in% c("Feed Addition","Feed Substitution") 
                            & grepl("Agrofor",Subpractice),Code])

# This is the normal source of information for ERA (stored in the ERAg package)
data<-ERA.Compiled[grepl(paste(focal_pracs,collapse = "|"),plist)]

  # 1.2) Subset to cattle and small ruminants #####
  EUCodes[,unique(Product.Type)]
  EUCodes[Product.Type=="Animal",unique(Product.Simple)]
  
  focal_prods<-c("Cattle","Goat","Sheep")
  
  data<-data[Product.Simple %in% focal_prods]
  
  # 1.3) Subset to outcomes of interest  #####
  OutcomeCodes[Pillar=="Productivity",Subindicator]
  focal_out<-c("Meat Yield","Weight Gain","Milk Yield")
  
  data<-data[Out.SubInd %in% focal_out]
  
# 2) Download most recent Alpha version of ERA livestock data ####
# This is data we have recently compiled from a more comprehensive extraction of livestock data,it is stored in our S3 bucket
s3<-s3fs::S3FileSystem$new(anonymous = T)
era_s3<-"s3://digital-atlas/era"

# List the files in the s3 bucket
files<-s3$dir_ls(file.path(era_s3,"data"))
# This should be the most recent version of the data era_compiled-v1.0_2018-v1.1_2020-skinny_cow_2022_YYYY_MM_DD.parquet
files<-tail(grep("parquet",grep("era_compiled",files,value=T),value=T),1)

# Set a save location for the dataset (amend to something more suitable for your needs)
save_path<-file.path(getwd(),basename(files))

if(!file.exists(save_path)){
  s3$file_download(files,save_path,overwrite = T)
}

ERA.Compiled_new<-arrow::read_parquet(save_path)
data_new<-ERA.Compiled_new[Version=="skinny_cow_2022" & grepl(paste(focal_pracs,collapse = "|"),plist)]
dim(data_new)
data_new[,length(unique(Code))]

# Remove confounding subpractices
data_new<-data_new[!grepl("Concentrates|Feed Crop|Breed|Feed NonCrop",SubPrName)]
dim(data_new)
data_new[,length(unique(Code))]

# Check results from different data sources
# Fewer comparisons in new data, but more studies overall
dim(data)
dim(data_new)
data[,length(unique(Code))]
data_new[,length(unique(Code))]

# Encoding of practices is quite different though
data[,.(N.Studies=length(unique(Code))),by=PrName][order(N.Studies,decreasing = T)]
data_new[,.(N.Studies=length(unique(Code))),by=PrName][order(N.Studies,decreasing = T)]

# The newer dataset on the s3 should be superior to old dataset so let's use it
data<-data_new

# 3) Summarize data availability, subset to analytical focus  #####

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
  
  # 3.1) Example of how to access values #####
  data_subset<-data[Product.Simple=="Goat" & Out.SubInd=="Weight Gain" & tree_fodder_sub==T,
                    .(PrName,Code,Country,Site.ID,TID,T.Descrip,CID,C.Descrip,MeanC,MeanC.Error,MeanT,MeanT.Error,Mean.Error.Type,Rep,Rep.Animals,Units,Duration,Tree.Feed,Diversity,Variety)]
  
  # 3.2) Harmonize units #####
  data_subset[Units=="kg" & !is.na(Duration),c("MeanT","MeanC","Units"):=.(round(1000*MeanT/(365*Duration),2),round(1000*MeanC/(365*Duration),2),"g/individual/day")
              ][Units=="kg/individual/day",c("MeanT","MeanC","Units"):=.(round(1000*MeanT,2),round(1000*MeanC,2),"g/individual/day")]
  
  # 3.3) Explore tree forages #####
  data_subset[,.(no_studies=length(unique(Code))),by=Tree.Feed][order(no_studies,decreasing = T)]
  data[,.(no_studies=length(unique(Code))),by=Tree.Feed][order(no_studies,decreasing = T)]
  
  # Which trees are NA (For Pete debugging tree data)
  unique(data[is.na(Tree.Feed),.(Code,T.Descrip)])
  tree_missing<-data[is.na(Tree.Feed),unique(Code)]
  
  # 3.4) Explore errors #####
  data_subset[Mean.Error.Type=="",Mean.Error.Type:=NA]
  tail(data_subset[!is.na(MeanC.Error),.(PrName,C.Descrip,MeanC,MeanC.Error,T.Descrip,MeanT,MeanT.Error,Mean.Error.Type)])
  # What % of different error types
  data_subset[,round(100*table(Mean.Error.Type,useNA = "ifany")/.N,1)]
  
  # 3.5) Explore reps #####
  data_subset[,Rep.Animals:=as.numeric(Rep.Animals)]
  data_subset[,.(Code,T.Descrip,Rep,Rep.Animals)]
  # Substiute average from other studies where n animals is missing
  avg_animals<-data_subset[!is.na(Rep.Animals),.(value=round(mean(unique(Rep.Animals)),0)),by=Code][,round(mean(value),0)]
  data_subset[is.na(Rep.Animals),Rep.Animals:=avg_animals]
# 4) Where to find more information about the diet fed to the animals ####
  # 4.1) Download & import data #####
  # First we need to get the detailed dataset containing the management information that relates to the experiment, this is in the Atlas S3 Bucket
  s3<-s3fs::S3FileSystem$new(anonymous = T)
  era_s3<-"s3://digital-atlas/era"
  
  # List the files in the s3 bucket
  files<-s3$dir_ls(file.path(era_s3,"data"))
  # This is the most recent version of the datas3://digital-atlas/era/data/skinny_cow_2022-YYYY-MM-DD.RData (substitute most recent date into filepath)
  files<-tail(grep(".RData",grep("skinny_cow_2022",files,value=T),value=T),1)
  
  # Set a save location for the dataset (amend to something more suitable for your needs)
  save_path<-file.path(getwd(),basename(files))
  
  if(!file.exists(save_path)){
    s3$file_download(files,save_path,overwrite = T)
  }
  
  livestock_metadata<- miceadds::load.Rdata2(file=basename(save_path),path=dirname(save_path))
  names(livestock_metadata)
  
  # 4.2) Explore  metadata #####
  # This table is a high level description of the diet
  diet_summary<-livestock_metadata$Animals.Out
  head(diet_summary)
  
  # We are interested in the tables Animals.Diet (what is in the diet? how much is fed?), Animals.Diet.Comp (nutritional information about diet and ingredients), and Animals.Diet.Digestibility (digestibility information about diet and ingredients) 
  diet_ingredients<-livestock_metadata$Animals.Diet
  head(diet_ingredients)
  
  diet_nutrition<-livestock_metadata$Animals.Diet.Comp
  head(diet_nutrition)
  
  diet_digestibility<-livestock_metadata$Animals.Diet.Digest
  head(diet_digestibility)
  
  # 4.3) How to understand what the fields are #####
  # Some of the columns should be described in the ERA metadata, which can be found in our master vocabulary (we will be updating the metadata soon)
    # 4.3.1) Download the era master vocab ######
  era_vocab_url<-"https://github.com/peetmate/era_codes/raw/main/era_master_sheet.xlsx"
  era_vocab_local<-file.path(getwd(),basename(era_vocab_url))
  download.file(era_vocab_url, era_vocab_local, mode = "wb")  # Download and write in binary mode
  
  # Import the vocab
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)
  
  era_fields<-master_codes$era_fields
  era_fields[Table=="Animals.Diet",.(Field,Field_Description)]
  
  # 4.4) How to link an observation to it's meta-data #####
  # Take an example observation
  i<-20
  data_subset[i]
  # We will used the keyfields T.Descrip and Code to link the metadata table that describes what practices are being applied
  data_subset[i,.(Code,T.Descrip)]
  
  # This metadata table describes the treatments
  treatments<-livestock_metadata$MT.Out
  head(treatments)
  
  # It links to the observation on the Code and T.Descrip fields like this:
  treatments[B.Code==data_subset$Code[i] & T.Name==data_subset$T.Descrip[i]]
  
  # These are the key fields that link the treatment to descriptions of practices
  grep("[.]Level[.]",colnames(treatments),value=T)
  
  # We are interested in the A.Level.Name field, this links us to the animal diet data
  (keyfields<-treatments[B.Code==data_subset$Code[i] & T.Name==data_subset$T.Descrip[i],.(B.Code,T.Name,A.Level.Name)])
  
    # 4.4.1) Explore the overall description of the animal practice ######
    diet_summary[B.Code==keyfields$B.Code & A.Level.Name==keyfields$A.Level.Name,.(B.Code,A.Level.Name,A.Notes,A.Diet.Trees,A.Diet.Other)]
    
    # Look at all the diets in the experiment
    diet_summary[B.Code==keyfields$B.Code,.(B.Code,A.Level.Name,A.Notes,A.Diet.Trees,A.Diet.Other)]
    
    # 4.4.2) Diet ingredients ######
    diet_ingredients[B.Code==keyfields$B.Code & A.Level.Name==keyfields$A.Level.Name]
    # We can see 5 items are in group "Supplement 2"
    # We can also see a sixth row that has "Supplement 2" in the D.Type column and D.Is.Group is TRUE, this means that 5 items create a compound element called "Supplement 2" and this element is fed at 2% Body Mass per day. 
    diet_ingredients[B.Code==keyfields$B.Code & A.Level.Name==keyfields$A.Level.Name,.(D.Type,D.Item_raw,D.Item.Group,D.Is.Group,D.Amount,D.Unit.Amount,D.Unit.Time,D.Unit.Animals)]
    
    # 4.4.3) Diet nutritional composition ######
    # Is there any nutritional data for this diet? (the lapply code removes any columns with no data from the table)
    nutrition_dat<-diet_nutrition[B.Code==keyfields$B.Code, lapply(.SD, function(col) if (!all(is.na(col))) col), .SDcols = names(diet_nutrition)] 
    nutrition_dat
    
    # There can be three types of information here: 
    # 1) rows where is_entire_diet==T this is the nutritional composition for the entire diet being fed (the value in the D.Item column is the A.Level.Name keyfield associated with an experimental diet)
    nutrition_dat[is_entire_diet==T,1:11]
    # 2) rows where is_entire_diet==F & is_group==F refer to individual diet ingredients in the diet_ingredients table
    nutrition_dat[is_entire_diet==F & is_group==F,1:11]
    # 3) rows where is_entire_diet==F & is_group==T refer to groupings of diet ingredients in the diet_ingredients table (values in diet_ingredients$D.Item.Group table)
    
    # 4.4.4) Diet digestibility ######
    # Is there any digestibility data for this diet? (the lapply code removes any columns with no data from the table)
    digest_dat<-diet_digestibility[B.Code==keyfields$B.Code, lapply(.SD, function(col) if (!all(is.na(col))) col), .SDcols = names(diet_digestibility)] 
    digest_dat
    
    # 4.5) Feed intake #####
    feed_intake<-livestock_metadata$Data.Out[Out.Subind=="Feed Intake",.(B.Code,T.Name,A.Level.Name,Out.Subind,ED.Intake.Item,ED.Intake.Item.Raw,is_group,is_entire_diet,Out.Unit)]  
    # These have some residual issues with the raw data we are resolving
    feed_intake[is.na(ED.Intake.Item)]
    feed_intake<-feed_intake[!is.na(ED.Intake.Item)]
    
    # Subset to entire diet
    feed_intake<-feed_intake[is_entire_diet==T]
    feed_intake[,length(unique(B.Code))]
    
    # If ED.Intake.Item is not NA and is_group==F and is_entire_diet==F
    (feed_intake_subset<-feed_intake[is_group==F & is_entire_diet==F,.(B.Code,A.Level.Name,ED.Intake.Item)][20])
    diet_ingredients[B.Code==feed_intake_subset$B.Code & 
                       A.Level.Name==feed_intake_subset$A.Level.Name ]    
    
    
    