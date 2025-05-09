# ERA Project Environment Setup – 0_set_env.R ####
#
# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)
#
# This script sets up the working environment for ERA-related scripts and data pipelines.
# It ensures consistent directory structures, project paths, shared resources, and credentials
# across different import, processing, and packaging scripts.
#
# ## Key functions:
# 1. **Loads required packages** – with fallback installation via `pacman::p_load`.
# 2. **Sets up project directories** – including `era_dirs`, `era_projects`, `era_vocab`, etc.
# 3. **Configures cloud and local paths** – e.g., S3 buckets, local mounts, GDrive aliases.
# 4. **Loads shared helper functions and vocabularies** – such as practice and outcome codebooks.
# 5. **Initializes global options and external tool integrations** – e.g., `s3fs`, `arrow`, parallel settings.
#
# ## Assumptions:
# - Must be sourced at the beginning of all ERA project scripts.
# - Relies on consistent naming and directory structure for reproducibility.
# - Optional AWS or cloud-related credentials may be handled via `.Renviron` or credential profiles.
#
# ## Dependencies:
# - External configuration files (e.g., credentials, vocabularies)
# - R libraries: data.table, jsonlite, arrow, s3fs, miceadds, etc.
#
# 0) Load libraries and functions ####
  ## 0.1) Record R-project location #####
# Function to add or update an environment variable in the .Renviron file
set_env_variable <- function(var_name, var_value, renviron_file = "~/.Renviron") {
  # Read the .Renviron file if it exists
  if (file.exists(renviron_file)) {
    env_vars <- readLines(renviron_file)
  } else {
    env_vars <- character(0)
  }
  
  # Check if the variable already exists
  var_exists <- grepl(paste0("^", var_name, "="), env_vars)
  
  if (any(var_exists)) {
    # Update the existing variable
    env_vars[var_exists] <- paste0(var_name, "=", var_value)
  } else {
    # Add the new variable
    env_vars <- c(env_vars, paste0(var_name, "=", var_value))
  }
  
  # Write the updated .Renviron file
  writeLines(env_vars, renviron_file)
}

# Check if project_dir is already set in the environment
if (!nzchar(Sys.getenv("era_project_dir"))) {
  project_dir <- getwd()
  Sys.setenv(project_dir = project_dir)
  
  # Add or update the project_dir variable in the .Renviron file
  set_env_variable("era_project_dir", project_dir)
  
  # Reload .Renviron so that the change takes effect in the current session
  readRenviron("~/.Renviron")
}

# Confirm project_dir was set
(project_dir <- Sys.getenv("era_project_dir"))


  ## 0.2) Install/load packages & source functions ####
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(s3fs,paws.storage,rnaturalearth,terra,remotes,data.table,treemap,spatstat.geom,install=TRUE)

if(!require(ERAgON)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAgON",build_vignettes = T)
  library(ERAgON)
}

if(!require(ERAg)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAg",build_vignettes = T)
  library(ERAg)
}

if(!require(openalexR)){
  devtools::install_github("https://github.com/ropensci/openalexR")
  library(openalexR)
}

# Use the isciences version and not the CRAN version of exactextractr
if(!require("exactextractr")){
  remotes::install_github("isciences/exactextractr")
}


# Includes functions to upload data to S3 bucket 
source("https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/refs/heads/main/R/haz_functions.R")

# Source ERA functions script
source(file.path(project_dir,"R/functions.R"))

# 1) Set directories & parameters ####
  ## 1.1) Set era s3 dir #####
  era_s3<-"s3://digital-atlas/era"
  era_s3_http<-"https://digital-atlas.s3.amazonaws.com/era"
  
  # If the project directory does not exist you will need to provide credentials to create it
  if(F){
    if(length(s3fs::s3_dir_ls(era_s3))==0){
      s3fs::s3_dir_create(era_s3)
    }
  }
  
  s3<-s3fs::S3FileSystem$new(anonymous = T)
  
  ## 1.2) Set local working directory #####
  # CGlabs server
  CGlabs<-F
  if(project_dir=="/home/jovyan/rstudio/ERA_dev"){
    era_dir<-"/home/jovyan/common_data/era"
    CGlabs<-T
  }
  
  # Working locally
  if(project_dir =="C:/rprojects/ERA_dev"){
    era_dir<-"C:/rprojects/common_data/era"
  }
  
  if(project_dir =="/Users/pstewarda/Documents/rprojects/ERA_dev"){
    era_dir<-"/Users/pstewarda/Documents/rprojects/common_data/era"
  }
  
  if(project_dir == "C:/rprojects/common_data/era"){
    project_dir<-"C:/rprojects/ERA_dev"   
    era_dir<-"C:/rprojects/common_data/era"  
    }
  
  Aflabs<-F
  if(project_dir=="/home/psteward/rprojects/ERA_dev"){
    Aflabs<-T
    era_dir<-"/cluster01/workspace/era"
  }
  
  
  if(!dir.exists(era_dir)){
    dir.create(era_dir,recursive=T)
  }
  setwd(era_dir)
  
  ## 1.3) ERA projects #####
  era_projects<-list(v1.0_2018="v1.0_2018",
                     majestic_hippo_2020="majestic_hippo_2020",
                     skinny_cow_2022="skinny_cow_2022",
                     industrious_elephant_2023="industrious_elephant_2023",
                     courageous_camel_2024="courageous_camel_2024")
  
  ## 1.4) Create ERA paths #####
  era_dirs<-list()
  
  # era master datasets
  era_dirs$era_masterdata_dir<-file.path(era_dir,"data")
  era_dirs$era_masterdata_s3<-file.path(era_s3,"data")
  
  # errordir
  era_dirs$era_error_dir<-file.path(era_dirs$era_masterdata_dir,"errors")
  
  # packaged dir
  era_dirs$era_packaged_dir<-file.path(era_dirs$era_masterdata_dir,"packaged")
  era_dirs$era_packaged_s3<-file.path(era_dirs$era_masterdata_s3,"packaged")
  
  # search history
  era_dirs$era_search_prj<-file.path(project_dir,"data/search_history")
  era_dirs$era_search_dir<-file.path(era_dir,"search_history")
  era_dirs$era_search_s3<-file.path(era_s3,"search_history")
  
  # data entry folders
  era_dirs$era_dataentry_prj<-file.path(project_dir,"data_entry")
  era_dirs$era_dataentry_dir<-file.path(era_dir,"data_entry")
  era_dirs$era_dataentry_s3<-file.path(era_s3,"data_entry")

  # extracted geodata folders
  era_dirs$era_geodata_dir<-file.path(era_dir,"geodata")
  era_dirs$era_geodata_s3<-file.path(era_s3,"geodata")
  
  # ancillary data folders
  era_dirs$ancillary_dir<-file.path(era_dir,"ancillary_datasets")
  era_dirs$ancillary_s3<-file.path(era_s3,"ancillary_datasets")
  
  era_dirs$dem_dir<-file.path(era_dirs$ancillary_dir,"dem_download")
  era_dirs$dem_s3<-file.path(era_dirs$ancillary_s3,"dem_download")
  
  era_dirs$soilgrid_dir<-file.path(era_dirs$ancillary_dir,"soilgrids_download")
  era_dirs$soilgrid_s3<-file.path(era_dirs$ancillary_s3,"soilgrids_download")
  
  era_dirs$power_dir<-file.path(era_dirs$ancillary_dir,"power_download")
  era_dirs$power_S3<-file.path(era_dirs$ancillary_s3,"power_download")
  era_dirs$power_S3_file<-file.path(era_dirs$power_S3,"power_download.zip")
  
  era_dirs$chirps_dir<-file.path(era_dirs$ancillary_dir,"chirps_download")
  era_dirs$chirps_S3<-file.path(era_dirs$ancillary_s3,"chirps_download")
  era_dirs$chirps_S3_file<-file.path(era_dirs$chirps_S3,"chirps_download.zip")
  
  era_dirs$aez_dir<-file.path(era_dirs$ancillary_dir,"aez")

  era_dirs$ecocrop_dir<-file.path(era_dirs$ancillary_dir,"ecocrop")
  
  # vocabulary
  era_dirs$vocab_dir<-file.path(era_dir,"vocab")
  era_dirs$vocab_url<-"https://github.com/peetmate/era_codes/raw/main/era_master_sheet.xlsx"
  era_dirs$vocab_file<-file.path(era_dir,"vocab/era_master_sheet.xlsx")
  
  # create folders if they do not exist
  for(i in grep("_dir",names(era_dirs))){
    dir_focus<-era_dirs[[i]]
    if(!dir.exists(dir_focus)){
      dir.create(dir_focus,recursive = T)
    }
  }

  ## 1.5) Set urls #####
  # 1.4) Set directories of external datasets (e.g. chirps)
  if(CGlabs){
    chirps_dir<-"/home/jovyan/common_data/chirps_wrld"
    chirts_dir<-"/home/jovyan/common_data/chirts"
  }
  
  if(Aflabs){
    chirps_dir<-"/cluster01/workspace/common/climate/chirps/global_daily/tifs/p05"
    chirts_dir<-NA
  }
  ## 1.6) Set parameter time_origin ####
  time_origin<-as.Date("1900-01-01")
  # 1.7) Set parameter field_absent
  field_absent<-"Not in template"
  field_absent_num<-"-9999"
# 2) Download core datasets ####
  ## 2.1) ERA master datasets #####
  # Dev note: Needs to be updated to download most recent versions of data only with option to delete old files ####
  update<-F
    # List files in the specified S3 bucket and prefix
    files_s3<-suppressWarnings(s3$dir_ls(era_dirs$era_masterdata_s3))
    files_local<-gsub(era_dirs$era_masterdata_s3,era_dirs$era_masterdata_dir,files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }
    
  ## 2.2) ERA geodata #####
    update<-F
    # List files in the specified S3 bucket and prefix
    files_s3<-suppressWarnings(s3$dir_ls(era_dirs$era_geodata_s3))
    files_s3<-files_s3[!grepl(".csv|ESA-CCI|/archive",files_s3)]
    files_local<-gsub(era_dirs$era_geodata_s3,era_dirs$era_geodata_dir,files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }
  ## 2.3) Vocab - era_master_sheet.xlsx #####
    era_vocab_local<-era_dirs$vocab_file
    update<-T
    
    if(update){
      download.file(era_dirs$vocab_url, era_vocab_local, mode = "wb")  # Download and write in binary mode
    
    ### 2.3.1) Load master codes ####
    sheet_names <- readxl::excel_sheets(era_vocab_local)
    sheet_names <- sheet_names[!grepl("sheet|Sheet", sheet_names)]
    
    # Read each sheet into a list named master_codes
    master_codes <- sapply(
      sheet_names,
      FUN = function(x) {
        data.table::data.table(readxl::read_excel(era_vocab_local, sheet = x))
      },
      USE.NAMES = TRUE
    )
    
    ### 2.3.2 Subset & Modify tables ####
    
    tables<-c("era_fields_v1","era_fields_v2","lookup_levels","prac","prod","prod_comp",
                       "out","out_econ","fert","chem","countries","journals","trees","site_list","residues",
                       "vars","vars_animals","var_traits","AOM","ani_diet")
    
    master_codes<-master_codes[tables]
    
    # Remove uncessary fields, rename as needed
    data<-master_codes$chem
    chem<-data[,.(AOM,C.Type...3,C.Type.AI,C.Name,C.Is.Name.Commercial,C.AI)]
    setnames(chem,"C.Type...3","C.Type",skip_absent = T)
    chem<-chem[!is.na(C.Type)]
    
    chem.comm<-data[,.(C.Name.Commercial,C.Name.AI...10,C.Animal,C.Crop,C.Is.Chem,C.Type...14)]
    setnames(chem.comm,c("C.Name.AI...10","C.Type...14"),c("C.Name.AI","C.Type"),skip_absent = T)
    chem.comm<-chem.comm[!is.na(C.Name.Commercial)]
    
    chem.ai<-data[,.(C.Name.AI...16,C.Name.AI.CHEBI,C.Tname.AI.Type_gpt)]
    setnames(chem.ai,c("C.Name.AI...16"),c("C.Name.AI"))
    chem.ai<-chem.ai[!is.na(C.Name.AI)]
    
    master_codes$chem<-chem
    master_codes$chem.ai<-chem.ai
    master_codes$chem.comm<-chem.comm
    
    data<-master_codes$vars
    setnames(data,c("V.Product"),c("Product.Simple"),skip_absent = T)
    data[,V.Var:=V.Var1][,V.Species:=V.Species1][,c("V.Var1","V.Species1","V.Subspecies1","V.Animal.Practice"):=NULL]
    master_codes$vars<-unique(data)
    
    data<-master_codes$vars_animals
    setnames(data,c("V.Product"),c("Product.Simple"),skip_absent = T)
    data[,V.Var:=V.Var1][,V.Species:=V.Species1][,c("Round","V.Var1","V.Species1","V.Subspecies1","...16","...17","V.Maturity"):=NULL]
    master_codes$vars_animals<-unique(data)
    
    master_codes$era_fields_v2<-master_codes$era_fields_v2[industrious_elephant_2023==T,.(Table,Table_Description,Field,Calculated,Display_Name,Data_Type,Field_Description,Notes,Values,Validation)]
    master_codes$lookup_levels<-master_codes$lookup_levels[,.(Table,Field,Values_Old,Values_New,Description)]
    
    master_codes$prac<-master_codes$prac[Depreciated!=T,.(Code,Theme,Practice,Subpractice,Definition,Notes,Linked.Tab,Linked.Col)]
    
    master_codes$out<-master_codes$out[Depreciated!=T,!c("Depreciated","Previous.Names","Original.Outcome","Pillar.Code","Subpillar.Code","Indicator.Code","Subindicator.Short","Subindicator.Code")]
    
    master_codes$trees<-master_codes$trees[,!c("GBIF","Tree.Subspecies","Tree.Variety")]
    
    master_codes$site_list<-unique(master_codes$site_list[Include==T,!c("Include","Harmonization")])
    
    data<-master_codes$var_traits
    data<-data[V.Trait==V.Trait1,V.Trait:=NA][,.(V.Trait.Old=paste(unique(na.omit(V.Trait)),collapse=";")),by=V.Trait1]
    setnames(data,"V.Trait1","V.Trait")
    
    file<-file.path(era_dirs$era_masterdata_dir,paste0("master_codes-",Sys.Date(),".json"))
    
    ### 2.3.3) Save result ####
    jsonlite::write_json(master_codes, file, pretty = TRUE, auto_unbox = TRUE)
    }
    
  ## 2.4) Worldbank/FAO economic data #####
    currency_dir<-file.path(era_dirs$ancillary_dir,"currency_conversions")
    if(!dir.exists(currency_dir)){
      dir.create(currency_dir)
    }
    
    update<-F
    
    file_path<-file.path(currency_dir,"PA.NUS.PPP.csv")
    if(!file.exists(file_path)|update){
      ppp_data <- data.table(wbstats::wb_data("PA.NUS.PPP", country="countries_only"))
      fwrite(ppp_data,file=file_path)
      rm(ppp_data)
    }
    era_dirs$era_currency_files$ppp<-file_path
    
    file_path<-file.path(currency_dir,"PA.NUS.FCRF.csv")
    if(!file.exists(file_path)|update){
      exchange_rates <- data.table(wbstats::wb_data("PA.NUS.FCRF",country="countries_only"))
      fwrite(exchange_rates,file=file_path)
      rm(exchange_rates)
    }
    era_dirs$era_currency_files$exchange_rates<-file_path
    
    
    file_path<-file.path(currency_dir,"FP.CPI.TOTL.csv")
    if(!file.exists(file_path)|update){
      cpi_data <- data.table(wbstats::wb_data("FP.CPI.TOTL", country="countries_only"))
      fwrite(cpi_data,file=file_path)
      rm(cpi_data)
    }
    era_dirs$era_currency_files$cpi_data<-file_path
    
    file_path<-file.path(currency_dir,"Deflators_E_All_Data_(Normalized).csv")
    if (!file.exists(file_path)|update) {
      # Define the URL and set the save path
      url <- "https://fenixservices.fao.org/faostat/static/bulkdownloads/Deflators_E_All_Data_(Normalized).zip"
      zip_file_path <- gsub("[.]csv",".zip",file_path)
      
      # Download the file
      download.file(url, zip_file_path, mode = "wb")
      
      # Unzip the file
      unzip(zip_file_path, exdir = dirname(zip_file_path))
      
      # Delete the ZIP file
      unlink(zip_file_path)
    }
    
    era_dirs$era_currency_files$deflators<-file_path
    
  ## 2.5) Majestic hippo legacy harmonization sheets #####
    update<-F
    
    s3_bucket<-file.path(era_dirs$era_dataentry_s3,era_projects$majestic_hippo_2020,"legacy_harmonization_files")
    local_folder<-file.path("data_entry",era_projects$majestic_hippo_2020,"legacy_files")
    
    # List files in the specified S3 bucket and prefix
    files_s3<-s3$dir_ls(s3_bucket)
    
    files_local<-gsub(s3_bucket,local_folder,files_s3)
    
    for(i in 1:length(files_local)){
      file<-files_local[i]
      if(!file.exists(file)|update==T){
        s3$file_download(files_s3[i],file)
      }
    }
  ## 2.6) AEZ data ####
    update<-F
    # If aez data does not exist locally download from havard dataverse
  
    if(update==T|!any(grepl("AEZ16_CLAS--SSA.tif",list.files(era_dirs$aez_dir)))){
      aez_file <- file.path(era_dirs$aez_dir,"AEZ5 AEZ8 AEZ16 r2.0 - TIF.zip")

      if(!file.exists(aez_file)){
      api_url <- "https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/M7XIUB/PW7APO"
      
      # Perform the API request and save the file
      response <- httr::GET(url = api_url, httr::write_disk(aez_file, overwrite = TRUE))
      
      # Check if the download was successful
      if (httr::status_code(response) == 200) {
        print(paste0("File  downloaded successfully."))
        
        unzip(aez_file,exdir=era_dirs$aez_dir,junkpaths = T)
        files<-list.files(era_dirs$aez_dir,"zip",full.names = T)
        files<-files[files!=aez_file]
        for(i in 1:length(files)){
          unzip(files[i],exdir=era_dirs$aez_dir,junkpaths = T)
        }
        
        unlink(list.files(era_dirs$aez_dir,"zip",full.names = T))
        
      } else {
        print(paste("Failed to download file Status code:", httr::status_code(response)))
      }
      }
    }
    
    # Note while a version of the file below exists in zipped format on harvard dataverse, do not use it, it is corrupted!
    if(update==T|!any(grepl("004_afr-aez_09.tif",list.files(era_dirs$aez_dir)))){
      aez_file <- file.path(era_dirs$aez_dir,"004_afr-aez_09.tif")

      if(!file.exists(aez_file)){
    
         url<-"https://files.africasis.isric.org/aez/004_afr-aez_09.tif"

        # Perform the API request and save the file
        cat("downloading file (~171mb) - the isric server is slow so this may take some time")
        response <- httr::GET(url = url, httr::write_disk(aez_file, overwrite = TRUE))

        # Check if the download was successful
        if (httr::status_code(response) == 200) {
          print(paste0("File  downloaded successfully."))
         
         # Download value mappings 
         api_url2<-"https://dataverse.harvard.edu/api/access/datafile/:persistentId?persistentId=doi:10.7910/DVN/HJYYTI/YAICMA" 
         response<-httr::GET(url = api_url2, httr::write_disk(aez_file2, overwrite = TRUE))
          
          unzip(aez_file,exdir=era_dirs$aez_dir,junkpaths = T)
          unlink(list.files(era_dirs$aez_dir,"zip",full.names = T))
          
          # Convert to tif and add mappings
          file_path<-file.path(era_dirs$aez_dir,"004_afr-aez_09.tif")
          dat_rast<-terra::rast(file_path)+0 # +0 just forces it into memory
          mappings<-fread(aez_file2)
          colnames(mappings)<-c("value","category")
          levels<-data.frame(value=as.numeric(unique(values(dat_rast))))
          levels<-merge(levels,mappings)
          levels(dat_rast)<-list(levels)
          
          terra::writeRaster(dat_rast,file_path,overwrite=T)
          
        } else {
          print(paste("Failed to download file Status code:", httr::status_code(response)))
        }
      }
    }
    

  ## 2.6) Ancillary datasets ####
    # 2.6.1) EcoCrop
    file<-file.path(era_dirs$ecocrop_dir,"ecocrop.csv")
    
    if(!file.exists(file)){
    ecocrop_url<-"https://raw.githubusercontent.com/AdaptationAtlas/hazards_prototype/main/metadata/ecocrop.csv"
    ecocrop<-fread(ecocrop_url, showProgress = FALSE)
    fwrite(ecocrop,file)
    }
    
# 3) Create table of unique locations (for use with geodata functions) ####
    options(arrow.unsafe_metadata = TRUE)
    
  ## 3.1) Compiled dataset ####
  (file<-tail(list.files(era_dirs$era_masterdata_dir,"era_compiled-.*parquet",full.names = T),1))
  data<-arrow::read_parquet(file)
  data<-unique(data[!(is.na(Latitude)|is.na(Longitude)|Buffer==0),list(Site.Key,Latitude,Longitude,Buffer,Country)])
  data[,project:=era_projects$v1.0_2018]
  
  era_locations<-list()
  era_locations<-c(era_locations,list(data))
  
  ## 3.2) Skinny Cow ####
  (file<-tail(list.files(era_dirs$era_masterdata_dir,"skinny_cow.*json",full.names = T),1))
  data<-jsonlite::read_json(file,simplifyVector = T)
  data<-data.table(data$Site.Out)
  
  data<-data[!grepl("[.][.]",Site.ID),list(Site.Key,Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Buffer.Manual,Country)]
  setnames(data,c("Site.LatD","Site.LonD","Buffer.Manual"),c("Latitude","Longitude","Buffer"))
  data<-data[!(is.na(Latitude)|is.na(Longitude))
             ][,Latitude:=as.numeric(Latitude)
               ][,Longitude:=as.numeric(Longitude)
                 ][,Buffer:=as.numeric(Buffer)
                   ][,Site.Lat.Unc:=as.numeric(Site.Lat.Unc)
                     ][,Site.Lon.Unc:=as.integer(Site.Lon.Unc)]
  data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
  # Assign 5km buffer to missing buffer values
  data<-data[!(is.na(Buffer)|Buffer==0)]
  # Remove unecessary cols
  data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
  data[,project:=era_projects$skinny_cow_2022]
  
  era_locations<-c(era_locations,list(data))
  
  ## 3.3) Industrious Elephant ####
  
  (file<-tail(list.files(era_dirs$era_masterdata_dir,"industrious_elephant.*json",full.names = T),1))
  data<-jsonlite::read_json(file,simplifyVector = T)
  data<-data.table(data$Site.Out)
  
  data<-data[,list(Site.Key,Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Buffer.Manual,Country)]
  setnames(data,c("Site.LatD","Site.LonD","Buffer.Manual"),c("Latitude","Longitude","Buffer"))
  data<-data[!grepl("[.][.]",Site.ID)
             ][,Latitude:=as.numeric(Latitude)
               ][,Longitude:=as.numeric(Longitude)
                 ][,Buffer:=as.numeric(Buffer)
                 ][,Site.Lat.Unc:=as.numeric(Site.Lat.Unc)
                   ][,Site.Lon.Unc:=as.numeric(Site.Lon.Unc)
                     ][!(is.na(Latitude)|is.na(Longitude))]
  data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
  # Assign 5km buffer to missing buffer values
  data<-data[!(is.na(Buffer)|Buffer==0)]
  # Remove unecessary cols
  data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
  data[,project:=era_projects$industrious_elephant_2023]
  era_locations<-c(era_locations,list(data))
  
  ## 3.4) Majestic Hippo ####
  (file<-tail(list.files(era_dirs$era_masterdata_dir,"majestic_hippo.*json",full.names = T),1))
  data<-jsonlite::read_json(file,simplifyVector = T)
  data<-data.table(data$Site.Out)
  
  data<-data[,list(Site.Key,Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Site.Buffer.Manual,Country)]
  setnames(data,c("Site.LatD","Site.LonD","Site.Buffer.Manual"),c("Latitude","Longitude","Buffer"))
  data<-data[!grepl("[.][.]",Site.ID)
  ][,Latitude:=as.numeric(Latitude)
  ][,Longitude:=as.numeric(Longitude)
  ][,Buffer:=as.numeric(Buffer)
  ][,Site.Lat.Unc:=as.numeric(Site.Lat.Unc)
  ][,Site.Lon.Unc:=as.numeric(Site.Lon.Unc)
  ][!(is.na(Latitude)|is.na(Longitude))]
  data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
  # Assign 5km buffer to missing buffer values
  data<-data[!(is.na(Buffer)|Buffer==0)]
  # Remove unecessary cols
  data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
  data[,project:=era_projects$majestic_hippo_2020]
  
  era_locations<-c(era_locations,list(data))
  
  ## 3.5) Courageous Camel ####
  (file<-tail(list.files(era_dirs$era_masterdata_dir,"courageous_camel.*json",full.names = T),1))
  data<-jsonlite::read_json(file,simplifyVector = T)
  data<-data.table(data$Site.Out)
  
  data<-data[,list(Site.Key,Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Buffer.Manual,Country)]
  setnames(data,c("Site.LatD","Site.LonD","Buffer.Manual"),c("Latitude","Longitude","Buffer"))
  data<-data[!grepl("[.][.]",Site.ID)
  ][,Latitude:=as.numeric(Latitude)
  ][,Longitude:=as.numeric(Longitude)
  ][,Buffer:=as.numeric(Buffer)
  ][,Site.Lat.Unc:=as.numeric(Site.Lat.Unc)
  ][,Site.Lon.Unc:=as.numeric(Site.Lon.Unc)
  ][!(is.na(Latitude)|is.na(Longitude))]
  data[is.na(Buffer),Buffer:=(Site.Lat.Unc+Site.Lon.Unc)/4]
  # Assign 5km buffer to missing buffer values
  data<-data[!(is.na(Buffer)|Buffer==0)]
  # Remove unecessary cols
  data<-data[,list(Site.Key,Latitude,Longitude,Buffer,Country)]
  data[,project:=era_projects$courageous_camel_2024]
  
  era_locations<-c(era_locations,list(data))
  
  ## 3.6) Merge locations ####
  era_locations_tab<-rbindlist(era_locations,use.names = T)
  era_locations<-unique(copy(era_locations_tab)[,project:=NULL][,Latitude:=round(Latitude,4)][,Longitude:=round(Longitude,4)])

  # Check for any duplicates
  era_locations[,N:=.N,by=Site.Key]

  # Deal with rounding issues
  era_locations[N>1,lat:=unlist(tstrsplit(Site.Key," ",keep=1))
                ][N>1,lon:=unlist(tstrsplit(Site.Key," ",keep=2))
                  ][,lat_lon:=T
                    ][N>1,lat_lon:=Latitude==as.numeric(lat) & Longitude==as.numeric(lon)]
      
  era_locations[N>1][order(Site.Key)]
  
  era_locations<-era_locations[lat_lon==T][,N:=.N,by=Site.Key]
  
  # Check for remaining duplicates
  era_locations[N>1][order(Site.Key)]
  dup_sites<-unique(era_locations[N>1,Site.Key])
  era_locations_tab[Site.Key %in% dup_sites[1]]
  
  
  # Remove incorrect associations of ethiopian sites with wrong country
  era_locations<-era_locations[!(N==2 & Country!="Ethiopia")]
  
  # Tidy up
  era_locations[,c("lat","lon","lat_lon","N"):=NULL]
  
  ## 3.7) Create spatvect of site buffers #####

  # Buffer points - projected - geographic
  era_locations_vect_g<-ERAg::Pbuffer(Data = era_locations,ID = "Site.Key" ,Projected=F)
  era_locations_vect_g$Country<-era_locations$Country

  ## 3.8) Get a vector of Africa #####

  # Get the vector data for Africa - note this seems to cut off the islands
  africa_vector <- rnaturalearth::ne_countries(continent = "Africa", returnclass = "sf")
  
  # Convert to terra SpatVector
  africa_vect <- terra::vect(africa_vector)
  
  # Create empty rast with extent of africa
  africa_rast <- terra::rast()
  ext(africa_rast) <- terra::ext(africa_rast)
  
  ## 3.9) Create continental bounding boxes ####
    # Define bounding boxes as named vectors
    bbox_africa <- c(lon_min = -25, lon_max = 63, lat_min = -36, lat_max = 38)
    bbox_sam    <- c(lon_min = -120, lon_max = -30, lat_min = -36, lat_max = 38)
    bbox_asia   <- c(lon_min = 80, lon_max = 155, lat_min = -36, lat_max = 38)
    
    # Combine into a list if needed
    bounding_boxes <- list(
      Africa = bbox_africa,
      SouthCentralAmerica = bbox_sam,
      SouthSoutheastAsia = bbox_asia
    )
    
    african_countries <- c(
      "Mali", "Guinea", "Morocco", "Niger", "Togo", "Tunisia", "Nigeria", "Egypt",
      "Mozambique", "Madagascar", "Mauritius", "Sierra Leone", "South Africa",
      "Ethiopia", "Eritrea", "Malawi", "Kenya", "Uganda", "Tanzania", "Zimbabwe",
      "Ghana", "Senegal", "Burkina Faso", "Cameroon", "DRC", "Rwanda",
      "Ivory Coast", "Benin", "Sudan", "Zambia", "Gambia",
      "Libya", "Somalia", "Botswana", "Burundi", "Mauritania", "Cabo Verde",
      "Swaziland", "Algeria", "Congo (Democratic Republic of the)", "Chad",
      "South Sudan", "Namibia"
    )
    
# 4) Set time origin ####
    time_origin<-as.Date("1900-01-01")
    
    