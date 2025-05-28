# ERA Import Script – Courageous Camel 2024 ####
#
# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)
#
# This script is part of the ERA (Evidence for Resilient Agriculture) data ingestion workflow.
# It imports, processes, and standardizes the 2024 dataset codenamed *Courageous Camel*.
#
# ## Key functions:
# 1. **Reads source files** – typically Excel, CSV, or other tabular formats from raw input folders.
# 2. **Validates and harmonizes data** – including checks on practice codes, outcome values, and location metadata.
# 3. **Maps fields** into ERA’s internal data model for integration with existing compiled datasets.
# 4. **Saves cleaned outputs** as `.json`, `.parquet`, or `.RData` for downstream analysis.
#'
# ## Assumptions:
# - ERA vocabulary and codebook files must be available to validate field mappings.
# - The script uses `data.table`, `jsonlite`, `arrow`, and other standard packages for fast processing.
#
# ## Dependencies:
# R/0_set_env.R
# R/import/import_helpers.R
#
# 0.0) Install and load packages, load functions ####
if (!require(pacman)) install.packages("pacman")  # Install pacman if not already installed
pacman::p_load(data.table, 
               readxl,
               openxlsx,
               future, 
               future.apply,
               parallel,
               miceadds,
               pbapply,
               soiltexture,
               httr,
               stringr,
               stringi,
               rnaturalearth,
               rnaturalearthhires,
               sf,
               dplyr,
               tidyr,
               jsonlite,
               progressr)

source(file.path(project_dir,"R/import/import_helpers.R"))

## 0.1) Define the valid range for date checking #####
valid_start <- as.Date("1950-01-01")
valid_end <- as.Date("2024-12-01")

## 0.2) Set project directories and parallel cores ####

# Set cores for parallel processing
worker_n<-parallel::detectCores()-2

# Set the project name, this should usually refer to the ERA extraction template used
project<-era_projects$courageous_camel_2024

# Working folder where extraction excel files are stored during in active data entry
#excel_dir<-"G:/My Drive/Data Entry 2024"
#excel_dir<-"/Users/pstewarda/Library/CloudStorage/GoogleDrive-peetmate@gmail.com/My Drive/Data Entry 2024"

# Are we in the live data extraction phase?
ext_live<-F

# Where extraction excel files are stored longer term
if(!ext_live){
  excel_dir<-file.path(era_dirs$era_dataentry_dir,project,"excel_files")
  if(!dir.exists(excel_dir)){
    dir.create(excel_dir,recursive=T)
  }
}else{
  # You will need to set the directory path to where the excel files are stored locally (usually a google drive folder)
  excel_dir<-"/Users/pstewarda/Library/CloudStorage/GoogleDrive-peetmate@gmail.com/.shortcut-targets-by-id/1onn-IqY6kuHSboqNSZgEzggmKIv576BB/Data Entry 2024/"
}

# Where processed extraction files are stored
extracted_dir<-file.path(era_dirs$era_dataentry_dir,project,"extracted")
if(!dir.exists(extracted_dir)){
  dir.create(extracted_dir,recursive=T)
}

# Set directory for error and harmonization tasks
error_dir<-file.path(era_dirs$era_dataentry_prj,project,"data_issues")
if(!dir.exists(error_dir)){
  dir.create(error_dir,recursive=T)
}

harmonization_dir<-file.path(error_dir,"harmonization")
if(!dir.exists(harmonization_dir)){
  dir.create(harmonization_dir)
}

# Where compiled data is to be stored
data_dir<-era_dirs$era_masterdata_dir

# 1) Download  or update excel data ####
download<-F
update<-F

if(!ext_live){
  s3_file<-paste0("https://digital-atlas.s3.amazonaws.com/era/data_entry/",project,"/",project,".zip")
  
  # Check if the file exists
  if (grepl("success",httr::http_status(httr::HEAD(s3_file))$category,ignore.case = T)) {
    print("The file exists.")
    file_status<-T
  } else {
    print("The file does not exist.")
    file_status<-F
  }
  
  local_file<-file.path(excel_dir,basename(s3_file))
  
  if(file_status){
    if(length(list.files(excel_dir))<1|update==T){
      rm_files<-list.files(excel_dir,"xlsm$",full.names = T)
      unlink(rm_files)
      unlink(extracted_dir,recursive = T)
      options(timeout = 60*60*2) # 2hr timehour 
      if(download){
        download.file(s3_file, destfile = local_file)
      }
      unzip(local_file, exdir = excel_dir,overwrite=T,junkpaths=T)
      unlink(local_file)
    }
  }
}

if(!dir.exists(extracted_dir)){
  dir.create(extracted_dir)
}

# 2) Load data ####
  ## 2.1) Load era vocab #####
  
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)
  
  aom<-copy(master_codes$AOM)[,Path:=NULL][,index:=1:.N]
  # Collapse heirarchy
  aom[,Path:=paste(na.omit(c(L1,L2,L3,L4,L5,L6,L7,L8,L9,L10)),collapse="/"),by=index]
  
  ## 2.2) Load excel data entry template #####
  Master<-list.files(paste0(project_dir,"/data_entry/",project,"/excel_data_extraction_template"),"xlsm$",full.names = T)
  
  # List sheet names that we need to extract
  SheetNames<-excel_sheets(Master)
  SheetNames<-grep(".Out",SheetNames,fixed = T,value=T)
  
  # List column names for the sheets to be extracted
  master_template_cols<-sapply(SheetNames,FUN=function(SName){
    cat('\r                                                                                                                                          ')
    cat('\r',paste0("Importing Sheet = ",SName))
    flush.console()
    colnames(data.table(suppressWarnings(suppressMessages(readxl::read_excel(Master,sheet = SName)))))
  },USE.NAMES = T)
  
  master_version<-c("V1.2.7|V1.2.6")
  
  ## 2.3) List extraction excel files #####
  if(ext_live){
    # Should we limit to the most recent template only (this is primarily for development puposes and should usually be set to F)
    rm_old_version<-T
    
    ext_dirs<-grep("/Extracted$|Quality Controlled$",list.dirs(excel_dir),value=T)
    Files<-list.files(ext_dirs,".xlsm$",full.names=T,recursive = F)
    
  if(rm_old_version){
      Files<-grep(master_version,Files,value=T)
    }
    
  }else{
    Files<-list.files(excel_dir,".xlsm$",full.names=T)
  }
  
  ## 2.4) Check for duplicate files #####
  FNames<-unlist(tail(tstrsplit(Files,"Extracted/|Quality Controlled/"),1))
  FNames<-gsub(" ","",FNames)
  FNames<-gsub("- ","-",FNames,fixed=T)
  FNames<-unlist(tstrsplit(FNames,"-",keep=2))
  FNames<-gsub("[(]1[])]|[(]2[])]","",FNames)
  FNames<-gsub("_1|_2|_3|_4",".1|.2|.3|.4",FNames,fixed=T)
  FNames<-gsub("..",".",FNames,fixed=T)
  
  excel_files<-data.table(filename=Files,era_code=FNames)
  #excel_files[,status:="qced"][grepl("/Extracted/",filename),status:="not_qced"]
  
  # Flag any naming issues
  excel_files[grepl("_",era_code)]
  excel_files<-excel_files[!grepl("_",era_code)]
  
  # Look for duplicate files
  excel_files[,N:=.N,by=era_code]
  
  excel_files[N>1][order(era_code)]
  
  # Remove not qced duplicates
  #excel_files<-excel_files[!(N==2 & status=="not_qced")][,N:=.N,by=era_code]
  excel_files[N>1][order(era_code)]
  
  excel_files<-excel_files[!N>1][,N:=NULL]
  excel_files[, era_code2:=gsub(".xlsm", "", era_code)]
  
# 3) Process imported data ####
  # Update saved data and errorchecking (T) or skip if file has already been processed (F)
  overwrite<-T
  if(overwrite){
    ext_files<-list.files(extracted_dir,full.names = T)
    unlink(ext_files)
  }

  # Set up parallel back-end
  set_parallel_plan(n_cores=worker_n,use_multisession=F)
  
  # Enable progressr and set up handlers
  progressr::handlers("progress")
  progressr::handlers(global = TRUE)
  
  # Wrap the parallel processing in a with_progress call
 with_progress({
    # Define the progressor based on the number of files
    p <- progressr::progressor(along = 1:nrow(excel_files))
    
  results<-future.apply::future_lapply(1:nrow(excel_files), FUN = function(ii) {
    # Update the progress bar
    p()
  
 # results<-lapply(1:nrow(excel_files),FUN=function(ii){
  
    File <- excel_files$filename[ii]
  #cat("File",ii,basename(File),"\n")

    era_code <- excel_files$era_code2[ii]
    
  # For later development to save processed tables
  # save_name <- file.path(extracted_dir, paste0(era_code, ".RData"))
  
  # Create name for error csv file
  file_code<-gsub(".xlsx|.xlsm","",basename(File))
  filename_new<-paste0(file_code,"_errors")
  filepath_new<-file.path(dirname(File),paste0(filename_new,".csv"))
  
  save_file<-file.path(extracted_dir,paste0(file_code,".RData"))
  
  if(!file.exists(save_file)|overwrite==T){
  ## 3.0) Load excel data #####
  
  excel_dat <- tryCatch({
    lapply(SheetNames, FUN=function(SName){
      cat('\r', "Importing File ", ii, "/", nrow(excel_files), " - ", era_code, " | Sheet = ", SName,"               ")
      flush.console()
      data.table(suppressMessages(suppressWarnings(readxl::read_excel(File, sheet = SName, trim_ws = FALSE))))
    })
  }, error=function(e){
    cat("Error reading file: ", File, "\nError Message: ", e$message, "\n")
    return(NULL)  # Return NULL if there was an error
  })
  
  if (!is.null(excel_dat)) {
    names(excel_dat) <- SheetNames
    excel_dat$file.info<-file.info(File)
  }else{
    sin_bin<-file.path(dirname(File),"loading_issue")
    if(!dir.exists(sin_bin)){
      dir.create(sin_bin)
    }
    file.rename(File,file.path(sin_bin,basename(File)))
    warning(paste("Error file",File,"failed to load, moved to loading_issue folder."))
  }

    ### 3.0.1) Initiate error & harmonization lists ######
    errors<-list()
    h_tasks<-list()
    
  ## 3.1) Publication (Pub.Out) #####
table_name<-"Pub.Out"
template_cols<-c(master_template_cols[[table_name]][-7],"filename")
Pub.Out<-excel_dat[[table_name]][,-7]
Pub.Out$filename<-basename(File)

allowed_values<-data.table(allowed_values=list(master_codes$journals$B.Journal),
                           parent_tab_name=c("master_codes$journals"),
                           field=c("B.Journal"))

# Replace zeros with NAs
results<-validator(data=Pub.Out,
                   character_cols = template_cols,
                   compulsory_cols = c(filename="B.Code",filename="B.Author.Last",filename="B.Date"),
                   zero_cols=c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4"),
                   allowed_values = allowed_values,
                   trim_ws = T,
                   template_cols = template_cols,
                   tabname=table_name)

Pub.Out<-results$data
errors<-c(errors,list(results$errors))

# Pub.Out: Validation: Duplicate or mismatched B.Codes
Pub.Out<-merge(Pub.Out,excel_files[,list(filename,era_code2)],all.x=T)
Pub.Out[,code_issue:=F][,code_issue:=B.Code!=era_code2][,B.Code:=trimws(B.Code)][,era_code2:=trimws(era_code2)]

# Save any errors
error_dat<-Pub.Out[code_issue==T,list(B.Code,era_code2,filename,code_issue)
                ][,value:=paste0(table_name," = ",B.Code," Filename = ",era_code2)
                  ][,list(B.Code,value)
                    ][,table:=table_name
                      ][,field:="B.Code"
                        ][,issue:="Potential issues with study code, it does not match the filename."]
errors<-c(errors,list(error_dat))

# Set B.Code to match filename
Pub.Out[!is.na(era_code2),B.Code:=era_code2]
# Tidy up
Pub.Out[,c("era_code2","filename","code_issue"):=NULL]

  ## 3.2) Site.Out #####
  table_name<-"Site.Out"
  Site.Out<-excel_dat[[table_name]]
  template_cols<-c(master_template_cols[[table_name]],"B.Code")
  Site.Out$B.Code<-Pub.Out$B.Code
  Site.Out<-Site.Out[!is.na(Site.ID)]
  
  if(nrow(Site.Out)==0){
    error_dat<-data.table(B.Code=Site.Out$B.Code,value=NA,table=table_name,field="Site.ID",issue="No Site.ID exists")
    errors<-c(errors,list(error_dat))
    }
  
  # Fix LatD/LonD 0's that should be NAs
  Site.Out[Site.LatD==0 & Site.LonD==0,c("Site.LatD","Site.LonD"):=NA]
  
  # Read in data excluding files with non-match structure
  numeric_cols<-c("Site.LonD","Site.LatD","Site.Lat.Unc","Site.Lon.Unc","Buffer.Manual","Site.Rain.Seasons","Site.MAP","Site.MAT","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree","Site.MSP.S1","Site.MSP.S2")
  
  results<-validator(data=Site.Out,
                     character_cols = "B.Code",
                     zero_cols=colnames(Site.Out)[!colnames(Site.Out) %in% c("Site.LonD","Site.LatD","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree")],
                     numeric_cols=numeric_cols,
                     compulsory_cols = c(Site.ID="Site.Type",Site.ID="Country",Site.ID="Site.LatD",Site.ID="Site.LonD"),
                     extreme_cols=list(Site.MAT=c(10,35),
                                       Site.MAP=c(40,4000),
                                       Site.MSP.S1=c(20,3000),
                                       Site.MSP.S2=c(20,3000),
                                       Site.Elevation=c(0,4000),
                                       Site.Slope.Perc=c(0,50),
                                       Site.Slope.Degree=c(0,45)),
                     template_cols = template_cols,
                     unique_cols = "Site.ID",
                     date_cols=NULL,
                     trim_ws=T,
                     tabname=table_name)
  
  Site.Out<-results$data
  
  # Enforce class character for columns
  other_cols<-colnames(Site.Out)[!colnames(Site.Out) %in% numeric_cols]
  Site.Out[, (other_cols) := lapply(.SD, as.character),.SDcols = other_cols]
  
  error_dat<-results$errors
  # remove aggregated sites from error list
  error_dat<-error_dat[!grepl("[.][.]",value)]
  errors<-c(errors,list(error_dat))
  
  error_dat<-Site.Out[(is.na(Site.Lat.Unc)|is.na(Site.Lon.Unc)) & is.na(Buffer.Manual) & !grepl("[.][.]",Site.ID)
  ][,list(value=paste0(unique(Site.ID),collapse = "/")),by=list(B.Code)
  ][,table:=table_name
  ][,field:="Site.ID"
  ][,issue:="Missing value in compulsory field location uncertainty."]
  
  errors<-c(errors,list(error_dat))
  
  # Add ISO.3166.1.alpha.3
  c_dat<-master_codes$countries[,list(Country,`ISO.3166-1.alpha-3`)]
  colnames(c_dat)[2]<-"ISO.3166.1.alpha.3"
  Site.Out<-merge(Site.Out,c_dat,all.x=T)
  
  dat<-Site.Out[!(is.na(Site.LatD)|is.na(Site.LonD)|is.na(ISO.3166.1.alpha.3))]
  error_dat<-check_coordinates(data=dat[,list(Site.LatD,Site.LonD,ISO.3166.1.alpha.3)])
  error_dat<-dat[!error_dat][,list(value=paste0(paste0(Country,"-",Site.ID,": lat ",Site.LatD," lon ",Site.LonD),collapse="/")),by=B.Code
  ][,table:=table_name
  ][,field:="Site.LonD/Site.LatD"
  ][,issue:="Co-ordinates may not be in the country specified."]
  errors<-c(errors,list(error_dat))
  
    ### 3.2.1) Harmonization ######
    h_params<-data.table(h_table=table_name,
                         h_field=c("Site.Admin","Site.Start.S1","Site.End.S1","Site.Start.S2","Site.End.S2","Site.Type","Site.Soil.Texture"),
                         h_table_alt=c(NA,"Site.Out","Site.Out","Site.Out","Site.Out",NA,"Site.Out"),
                         h_field_alt=c(NA,"Site.Seasons","Site.Seasons","Site.Seasons","Site.Seasons",NA,"Soil.Texture"))
    
    results<-harmonizer_wrap(data=Site.Out,
                             h_params=h_params,
                             master_codes = master_codes)
    
    Site.Out<-results$data
    h_task<-results$h_tasks
    h_tasks<-c(h_tasks,list(h_task))
    
    h_task<-val_checker(data=Site.Out[grepl("Station",Site.Type) & !grepl("[.][.]",Site.ID)],
                          tabname=table_name,
                          master_codes=master_codes,
                          master_tab="site_list",
                          h_field="Site.ID",
                          h_field_alt=NA)
    
    h_tasks<-c(h_tasks,list(h_task))
    
    h_task<-val_checker(data=Site.Out,
                          tabname=table_name,
                          master_codes=master_codes,
                          master_tab="countries",
                          h_field="Country",
                          h_field_alt=NA)
    
    h_tasks<-c(h_tasks,list(h_task))
    
    ### 3.2.2) Harmonize Site.ID field #######
    master_sites<-master_codes$site_list[!(is.na(Synonyms) & is.na(Harmonization)),.(Site.ID,Country,Synonyms,Harmonization)
    ][,old_name:=Synonyms
    ][is.na(old_name),old_name:=Harmonization
    ][!is.na(Synonyms) & !is.na(Harmonization),old_name:=paste(c(Synonyms[1],Harmonization[1]),collapse=";"),by=.(Site.ID,Country,Synonyms,Harmonization)
    ][,c("Synonyms","Harmonization"):=NULL]
    
    master_sites<-unique(rbindlist(lapply(1:nrow(master_sites),FUN=function(i){
      old_names<-master_sites[i,unlist(strsplit(old_name,";"))]
      data.table(new_names=master_sites$Site.ID[i],Country=master_sites$Country[i],Site.ID=old_names)
    })))[,site_code_new:=paste0(Country,"|||",new_names)][,site_code:=paste0(Country,"|||",Site.ID)]
    
    Site.Out$Site.ID_raw<-Site.Out$Site.ID
    
    if(nrow(Site.Out)>0){
    for(i in 1:nrow(Site.Out)){
      country<-Site.Out$Country[i]
      site_id<-Site.Out$Site.ID_raw[i]
      site_id<-trimws(unlist(strsplit(site_id,"[.][.]")))
      site_codes<-paste0(country,"|||",site_id)
      
      site_codes_new<-master_sites[match(site_codes,site_code),site_code_new]
      site_codes[!is.na(site_codes_new)]<-site_codes_new[!is.na(site_codes_new)]
      
      site_ids_new<-unlist(tstrsplit(site_codes,"[|][|][|]",keep=2))
      site_ids_new<-paste0(site_ids_new,collapse="..")
      
      Site.Out$Site.ID[i]<-site_ids_new
    }
    }
    
    # Check for missing facilities in era_master_sheet
    master_site_list<-c(master_codes$site_list[,paste0(Country,"|||",Site.ID)],
                        master_codes$site_list[!is.na(Synonyms),paste0(Country,"|||",unlist(strsplit(Synonyms,";")))],
                        master_codes$site_list[!is.na(Harmonization),paste0(Country,"|||",unlist(strsplit(Harmonization,";")))])
    
    error_dat<-Site.Out[,.(B.Code,Country,Site.ID_raw,Site.Type)][,Code:=paste0(Country,"|||",Site.ID_raw)]
    error_dat<-error_dat[!Code %in% master_site_list & 
                        Site.Type == "Researcher Managed & Research Facility" &
                        !grepl("[.][.]",Site.ID_raw)
                     ][,.(value=paste(Site.ID_raw,collapse = "/")),by=B.Code
                       ][,table:=table_name
                         ][,field:="Site.ID"
                           ][,issue:="No match for facility in era_master_sheet site_list tab (inc. synonyms or harmonization fields)."]
  
    errors<-c(errors,list(error_dat))
    
    ### 3.2.3) Create Aggregated Site Rows #######
    mergedat<-Site.Out[grep("[.][.]",Site.ID)]
    
    if(nrow(mergedat)>0){
    result<-pblapply(1:nrow(mergedat),FUN=function(i){
      site_id<-mergedat$Site.ID[i]
      site_id<-trimws(unlist(strsplit(site_id,"[.][.]")))
      b_code<-mergedat[i,B.Code]
      agg_dat<-Site.Out[B.Code==b_code]
      N<-agg_dat[,match(site_id,Site.ID)]
      
      if(any(is.na(N))){
        cat("\n Non-match in aggregated site name",i,b_code,":",site_id[is.na(N)],"\n")
        list(error=data.table(B.Code=b_code,
                              value=paste0(mergedat$Site.ID[i],"||",paste(site_id[is.na(N)],collapse="/")),
                              table=table_name,
                              field="Site.ID",
                              issue="Value in used in aggregated site name not matching."))
      }else{
        agg_dat<-agg_dat[N]
        
        result <- agg_dat[, lapply(.SD, function(x) {
          paste(unique(x), collapse = "..")
        }), .SDcols = colnames(agg_dat)]
        
        list(data=result)
      }
    })
    
    mergedat<-rbindlist(lapply(result,"[[","data"))
    error_dat<-rbindlist(lapply(result,"[[","error"))
    errors<-c(errors,list(error_dat))
    
    # Replace aggregated site rows
    Site.Out<-rbind(Site.Out[!grepl("[.][.]",Site.ID)],mergedat)[order(B.Code,Site.ID)]
    }
    
    ### 3.2.4) Add Site Key ####
    Site.Out[!grepl("[.][.]",Site.ID),Site.Key:=create_site_key(lat=as.numeric(Site.LatD[1]),
                                                                lon=as.numeric(Site.LonD[1]),
                                                                buffer=Buffer.Manual[1],decimals=4),
             by=.(Site.ID,Country,Site.LatD,Site.LonD,Buffer.Manual)]
    
   # 3.3) Soil (Soil.Out) #####
  table_name<-"Soils.Out"
  Soil.Out<-excel_dat[[table_name]]
  Soil.Out$B.Code<-Pub.Out$B.Code
  Soil.Out<-Soil.Out[!is.na(Site.ID)]
  
  col_names<-colnames(Soil.Out)
  col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
  unit_cols<-grep("Unit",col_names,value=T)
  num_cols<-gsub("[.]Unit","",unit_cols)
  
  Soil.Out<-Soil.Out[,..col_names]
  
  Soil.Out[is.na(Soil.Lower),Soil.Upper:=NA]
  
  # Copy down units
  copy_down_cols<-c(unit_cols)
  Soil.Out <- Soil.Out[, (copy_down_cols) := lapply(.SD,function(x){x[1]}), .SDcols = copy_down_cols]
  
  # Update Site ID
  Soil.Out[,Site.ID_new:=Site.Out$Site.ID[match(Soil.Out$Site.ID,Site.Out$Site.ID_raw)]
  ][is.na(Site.ID_new),Site.ID_new:=Site.ID
  ][,Site.ID:=Site.ID_new
  ][,Site.ID_new:=NULL]
  
  unit_pairs<-data.table(unit=unit_cols,
                         var=num_cols,
                         name_field=num_cols)
  
  results<-validator(data=Soil.Out,
                     hilo_pairs = data.table(low_col="Soil.Upper",high_col="Soil.Lower",name_field="Site.ID"),
                     unit_pairs = unit_pairs,
                     numeric_cols= num_cols,
                     tabname=table_name,
                     trim_ws = T,
                     site_data=Site.Out)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Soil.Out<-results$data
  
    # 3.3.1) Soil.Out: Calculate USDA Soil Texture from Sand, Silt & Clay ####
    if(!"SND" %in% colnames(Soil.Out)){
      Soil.Out[,SND:=NA]
    }  
  
    if(!"SLT" %in% colnames(Soil.Out)){
      Soil.Out[,SLT:=NA]
    }  
    
    if(!"CLY" %in% colnames(Soil.Out)){
      Soil.Out[,CLY:=NA]
    }
  
    # Keep rows with 2 or more observations
    Soil.Out.Texture<-copy(Soil.Out)[,N:=is.na(CLY)+is.na(SLT)+is.na(SND)][N<2]
    
    # Add missing values where 2/3 are present
    Soil.Out.Texture[,x:=1:.N][,val:=sum(c(CLY,SLT,SND),na.rm=T),by=x][,x:=NULL]
    
    Soil.Out.Texture[is.na(SLT),SLT:=100-val
    ][is.na(CLY),CLY:=100-val
    ][is.na(SND),SND:=100-val]
    
    # Any values not 100
    if(nrow(Soil.Out.Texture)>0){
    error_dat<-unique(Soil.Out.Texture[N!=1 & (val>102|val<98),.(value=paste0(unique(Site.ID),collapse="/")),by=.(B.Code)])
    error_dat[,table:=table_name][,field:="Site.ID"][,issue:="Sand, silt, clay sum to greater than a 2% difference from 100%"]
    errors<-c(errors,list(error_dat))
    }
    
    # Subset to sensible soil textures
    Soil.Out.Texture<-Soil.Out.Texture[!(N!=1 & (val>102|val<98))]
    
    X<-Soil.Out.Texture[,list(CLY,SND,SLT)]
    setnames(X,c("CLY","SND","SLT"),c("CLAY","SAND","SILT"))
    Texture<-soiltexture::TT.points.in.classes(tri.data =X,class.sys = "USDA.TT",text.tol = 1) 
    Texture.FullName<-c("Clay","Silty Clay","Sandy Clay","Clay Loam","Silty Clay Loam","Sandy Clay Loam","Loam","Silty Loam",
                        "Sandy Loam","Silt","Loamy Sand","Sand")
    Texture<-unlist(apply(Texture,1,FUN=function(X){paste(Texture.FullName[which(X>=1)],collapse = "/")}))
    
    Soil.Out.Texture[,Site.Soil.Texture:=Texture][,c("N","val"):=NULL]
    
    # Merge texture results back to main dataset
    Soil.Out.Texture<-unique(Soil.Out.Texture[,list(SND,SLT,CLY,Site.Soil.Texture)])
    Soil.Out<-merge(Soil.Out,Soil.Out.Texture,all.x=T,by=c("SND","SLT","CLY"))
  
    # ***!!!TO DO!!!***  harmonize methods, units and variables ####
  # 3.4) Experimental Design (ExpD.Out) ####
    table_name<-"ExpD.Out"
    ExpD.Out<-excel_dat[[table_name]]
    template_cols<-c(master_template_cols[[table_name]],"B.Code")
    ExpD.Out$B.Code<-Pub.Out$B.Code
  
    results<-validator(data=ExpD.Out,
                       numeric_cols=c("Ex.Acclim"),
                       zero_cols = c("EX.Design","EX.Notes"),
                       template_cols=template_cols,
                       trim_ws = T,
                       tabname=table_name)
    
    ExpD.Out<-unique(results$data)
    
    errors<-c(errors,list(results$errors))
    
  # 3.5) Farming system (System.Out) #####
    table_name<-"System.Out"
    System.Out<-excel_dat[[table_name]]
    template_cols<-c(master_template_cols[[table_name]],"B.Code")
    System.Out$B.Code<-Pub.Out$B.Code
    System.Out<-System.Out[!is.na(LS.Level.Name)]
    
    # !!!TO DO!!! Allowed values ######
    # Need to make a lookup table to supply from master_codes$AOM
    
    if(nrow(System.Out)==0){
      error_dat<-data.table(B.Code=B.Code,value=NA,table=table_name,field="LS.Level.Name",issue="No livesock system name exists.")
      errors<-c(errors,list(error_dat))
    }
    
    results<-validator(data=System.Out,
                       zero_cols = template_cols,
                       template_cols=template_cols,
                       trim_ws = T,
                       tabname=table_name)
    
    System.Out<-unique(results$data)
    
    errors<-c(errors,list(results$errors))
    
  # 3.6) Time periods #####
    # 3.6.1) Times.Out ######
    table_name<-"Times.Out"
    Times.Out<-excel_dat[[table_name]][,1:9]
    template_cols<-c(master_template_cols[[table_name]][1:9],"B.Code")
    Times.Out$B.Code<-Pub.Out$B.Code
    Times.Out<-Times.Out[!is.na(M.Year)]
    
    results<-validator(data=Times.Out,
                       unique_cols = "M.Year",
                       numeric_cols=c("Time.Start.Day","Time.Start.Month","Time.Start.Year","Time.End.Day","Time.End.Month","Time.End.Year"),
                       compulsory_cols = c(B.Code="M.Year"),
                       trim_ws = T,
                       do_time=F,
                       do_site = F,
                       template_cols = template_cols,
                       tabname=table_name)
    
     error_dat<-results$errors
     errors<-c(errors,list(error_dat))
     
     Times.Out<-results$data
     
     setnames(Times.Out,"M.Year","Time")
      
    # 3.6.2) Time.Clim ######
     table_name<-"Times.Out"
     Time.Clim<-excel_dat[[table_name]][,-(1:10)]
     template_cols<-c(master_template_cols[[table_name]][-(1:10)],"B.Code")
     Time.Clim$B.Code<-Pub.Out$B.Code
     Time.Clim<-Time.Clim[!is.na(Time)]
     table_name<-"Time.Clim"
     
     # Update Site ID
     Time.Clim[,Site.ID_new:=Time.Clim$Site.ID[match(Soil.Out$Site.ID,Time.Clim$Site.ID_raw)]
     ][is.na(Site.ID_new),Site.ID_new:=Site.ID
     ][,Site.ID:=Site.ID_new
     ][,Site.ID_new:=NULL]
     
     results<-validator(data=Time.Clim,
                        numeric_cols=c("Time.Clim.SP","Time.Clim.TAP","Time.Clim.Temp.Mean","Time.Clim.Temp.Max","Time.Clim.Temp.Min"),
                        compulsory_cols = c(B.Code="Site.ID",B.Code="Time"),
                        trim_ws = T,
                        site_data = Site.Out,
                        time_data = Times.Out,
                        duplicate_field="Time",
                        duplicate_ignore_fields=colnames(Time.Clim)[!colnames(Time.Clim) %in% c("Time","Site.ID","B.Code")],
                        rm_duplicates=T,
                        do_time=F,
                        tabname=table_name,
                        extreme_cols=list(Time.Clim.Temp.Mean=c(10,35),
                                          Time.Clim.TAP=c(40,4000),
                                          Time.Clim.SP=c(20,3000),
                                          Time.Clim.Temp.Max=c(10,45),
                                          Time.Clim.Temp.Min=c(5,30)))
     
     error_dat<-results$errors
     errors<-c(errors,list(error_dat))
     
     Time.Clim<-results$data
     
    
  
  # 3.7) Herd (Herd.Out) ####
     table_name<-"Herd.Out"
     Herd.Out<-excel_dat[[table_name]][,1:20]
     template_cols<-c(master_template_cols[[table_name]][1:20],"B.Code")
     Herd.Out$B.Code<-Pub.Out$B.Code
     Herd.Out<-Herd.Out[!is.na(Herd.Level.Name)]
     
     setnames(Herd.Out,"Herd.Time","Time",skip_absent=T)
     template_cols[template_cols=="Herd.Time"]<-"Time"
     
     colnames(Herd.Out)<-unlist(tstrsplit(colnames(Herd.Out),"[.][.][.]",keep=1))
     template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
     # Enforce that level columns are character
     focal_cols<-grep("Level",colnames(Herd.Out),ignore.case = T)
     Herd.Out <- Herd.Out[, (focal_cols) := lapply(.SD,function(x){as.character(x)}), .SDcols = focal_cols]
     
     
     if(nrow(Herd.Out)==0){
       error_dat<-data.table(B.Code=Pub.Out$B.Code,value=NA,table=table_name,field="Herd.Level.Name",issue="No Herd.Level.Name exists, in older versions of the template this could be due to a missing row ID.")
       errors<-c(errors,list(error_dat))
            }
     
     # Create allowed value table
     
     # Product
     a_product<-as.vector(na.omit(aom[L1=="Species" & L2=="Animal",Edge_Value]))
     # Scientific Name
     a_sci_names<-as.vector(na.omit(aom[L1=="Species" & L2=="Animal",`Scientific Name`]))
     # Variety
     # Practice
     a_pracs<-master_codes$prac[Practice=="Genetic Improvement",Subpractice]
     # Sex
     a_sex<-master_codes$lookup_levels[Field=="Herd.Sex",Values_New]
     # Stage
     a_stage<-as.vector(na.omit(aom[L3=="Rearing Stage",unique(Edge_Value)]))
     # !! Improvement: a_stage should be species dependent ####
     # Parity
     a_parity<-master_codes$lookup_levels[Field=="Herd.Parity",Values_New]
     # Age.Unit
     a_age_unit<-master_codes$lookup_levels[Field=="Herd.Start.Age.Unit",Values_New]
     # Weight.Unit
     a_weight_unit<-master_codes$lookup_levels[Field=="Herd.Start.Weight.Unit",Values_New]
     # Herd.Unit
     a_n_unit<-master_codes$lookup_levels[Field=="Herd.N.Unit",Values_New]
     
     allowed_values<-data.table(allowed_values=list(a_product,
                                                    a_sci_names,
                                                    a_pracs,
                                                    a_sex,
                                                    a_stage,
                                                    a_parity,
                                                    a_age_unit,
                                                    a_weight_unit,
                                                    a_n_unit,
                                                    c(Times.Out$Time,"All Times")),
                                parent_tab_name=c("master_codes$AOM",
                                                  "master_codes$AOM",
                                                  "master_code$prac",
                                                  "mastercode$lookup_levels",
                                                  "mastercode$lookup_levels",
                                                  "mastercode$lookup_levels",
                                                  "mastercode$lookup_levels",
                                                  "mastercode$lookup_levels",
                                                  "mastercode$lookup_levels",
                                                  "Times.Out"),
                                field=c("V.Product",
                                        "V.Product.Sci.Name",
                                        "V.Animal.Practice",
                                        "Herd.Sex",
                                        "Herd.Stage",
                                        "Herd.Parity",
                                        "Herd.Start.Age.Unit",
                                        "Herd.Start.Weight.Unit",
                                        "Herd.N.Unit",
                                        "Time"))
     # Create table of unit pairs
     unit_pairs<-data.table(unit=c("Herd.Start.Age.Unit","Herd.Start.Weight.Unit","Herd.N.Unit"),
                            var=c("Herd.Start.Age","Herd.Start.Weight","Herd.N"),
                            name_field="Herd.Level.Name")
     
     # Add merge level and sublevel to make sure combinations are unique in validator
     Herd.Out[,Herd.LevelxSublevel:=paste0(Herd.Level.Name,"-",Herd.Sublevel.Name)]
     template_cols<-c(template_cols,"Herd.LevelxSublevel")
     
     results<-validator(data=Herd.Out,
                        tabname=table_name,
                        do_time =  F,
                        allowed_values = allowed_values,
                        compulsory_cols = c(Herd.Level.Name="V.Product.Sci.Name",
                                            Herd.Level.Name="Herd.Rep",
                                            Herd.Level.Name="V.Animal.Practice"),
                        hilo_pairs = data.table(low_col="Herd.Start.Age",high_col="Herd.End.Age",name_field="Herd.Row.ID"),
                        unique_cols = c("Herd.LevelxSublevel"),
                        numeric_cols = c("Herd.Start.Age","Herd.End.Age","Herd.Start.Weight","Herd.N","Herd.Rep"),
                        unit_pairs = unit_pairs,
                        template_cols = template_cols,
                        trim_ws = T)
     
     error_dat<-results$errors
     errors<-c(errors,list(error_dat))
     
     Herd.Out<-results$data
     Herd.Out[,Herd.LevelxSublevel:=NULL]
  
     # 3.7.1) Variety Harmonization ######
     # !!! TO DO !!! Consider names used in hybrids
     mergedat<-master_codes$vars_animals[,.(V.Product,V.Var,V.Code,V.Animal.Practice,V.Var1)]
     setnames(mergedat,c("V.Var1","V.Animal.Practice"),c("V.Var_new","V.Animal.Practice_new"))
     
     Herd.Out<-merge(Herd.Out,mergedat,by=c("V.Product","V.Var"),all.x=T,sort=F)
     
     # No match error
      error_dat<-Herd.Out[is.na(V.Var_new),
                         ][!grepl("[*][*][*]",V.Var),list(value=paste(V.Var,collapse="/")),by=B.Code
                           ][,table:=table_name
                             ][,field:="V.Var"
                               ][,issue:="No match for variety in the master_codes$vars_animals$V.Var1 field."]
     errors<-c(errors,list(error_dat))
     
     # Adjust names
     Herd.Out[!is.na(V.Animal.Practice_new),V.Animal.Practice:=V.Animal.Practice_new][,V.Animal.Practice_new:=NULL]
     Herd.Out[!is.na(V.Var_new),V.Var_Raw:=V.Var][!is.na(V.Var_new),V.Var:=V.Var_new][,V.Var_new:=NULL]
     
  # 3.8) Animals.Out ######
     table_name<-"Ingredients.Out"
     Animals.Out<-excel_dat[[table_name]][,1:21]
     template_cols<-c(master_template_cols[[table_name]][1:21],"B.Code")
     Animals.Out$B.Code<-Pub.Out$B.Code
     table_name<-"Ingredients.Out_table1"
     setnames(Animals.Out,c("M.Year...3","A.Level.Name...1"),c("Time","A.Level.Name"),skip_absent=T)
     template_cols <- dplyr::recode(
       template_cols,
       "M.Year...3"       = "Time",
       "A.Level.Name...1" = "A.Level.Name",
       .default = template_cols  # Keeps original names if no match
     )
     
     Animals.Out <- Animals.Out[, !grepl("[.][.][.]", colnames(Animals.Out)), with = FALSE]
     template_cols<-template_cols[!grepl("[.][.][.]",template_cols)]
     
     # Remove any rows without a keyfield entry
     Animals.Out<-Animals.Out[!is.na( A.Level.Name)]
     
     if(nrow(Animals.Out)==0){
       error_dat<-data.table(B.Code=Pub.Out$B.Code,value=NA,table=table_name,field="A.Level.Name",issue="No A.Level.Name exists")
       errors<-c(errors,list(error_dat))
     }
     unit_pairs<-data.table(unit=c("D.Amount.Unit"),
                            var=c("D.Amount"),
                            name_field="A.Level.Name")
     
     allowed_values<-data.table(allowed_values=list(aom[grepl("Mechanical Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Cheimcal Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Biological Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Thermal Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Dehydration Process",Path),unique(Edge_Value)],
                                                 c("Yes","No"),
                                                 "Entire Diet",
                                                 c("Yes","No"),
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Amount",Values_New],
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Time",Values_New],
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Animals",Values_New],
                                                 c("Yes","No","Unspecified"),
                                                 c("Yes","No","Unspecified"),
                                                 c(Times.Out$Time,"All Times")),
                                parent_tab_name=c("master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM",NA,NA,NA,"master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",NA,NA,"Times.Out"),
                                field=c("D.Process.Mech","D.Process.Chem","D.Process.Bio","D.Process.Therm","D.Process.Dehy","A.Grazing","D.Type","A.Hay","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","DC.Is.Dry","D.Ad.lib","Time"))
  
      results<-validator(data=Animals.Out,
                         numeric_cols = c("D.Amount","D.Day.Start","D.Day.End"),
                         unique_cols = "A.Level.Name",
                         allowed_values=allowed_values,      
                         hilo_pairs = data.table(low_col="D.Day.Start",high_col="D.Day.End",name_field="A.Level.Name"),
                         do_time = F,
                         trim_ws = T,
                         tabname=table_name)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    Animals.Out<-results$data
  
    # All NA rows 
    N<-apply(Animals.Out[,!c("A.Level.Name","B.Code")],1,FUN = function(x){all(is.na(x))})
    error_dat<-Animals.Out[N,.(value=paste(A.Level.Name,collapse = "/")),by=B.Code
    ][,table:=table_name
    ][,field:="A.Level.Name"
    ][,issue:="Possible error, row is entirely NA."
    ][value!="Base"]
    
    errors<-c(errors,list(error_dat))
    
    # 3.8.1) TO DO!!! ADD CODES ####
    Animals.Out[,A.Codes:=NA]
  
  # 3.9) Animal.Diet (Ingredients) ####
    table_name<-"Ingredients.Out"
    Animal.Diet<-excel_dat[[table_name]][,-(1:22)]
    template_cols<-c(master_template_cols[[table_name]][-(1:22)],"B.Code")
    Animal.Diet$B.Code<-Pub.Out$B.Code[1]
    table_name<-"Ingredients.Out_table2"
    
    setnames(Animal.Diet,c("M.Year...24","A.Level.Name...23"),c("Time","A.Level.Name"),skip_absent=T)
    template_cols <- dplyr::recode(
      template_cols,
      "M.Year...24"       = "Time",
      "A.Level.Name...23" = "A.Level.Name",
      .default = template_cols  # Keeps original names if no match
    )
    colnames(Animal.Diet)<-gsub("[.]It[.]",".",colnames(Animal.Diet))
    template_cols<-gsub("[.]It[.]",".",template_cols)
    
    Animal.Diet<-Animal.Diet[!is.na(A.Level.Name)]
    
    # Where Diet.Item is NA and Diet.Group  is not, move Diet.Group to Diet.Item
    focal_rows<-Animal.Diet[,is.na(D.Item) & !is.na(D.Item.Group)]
    Animal.Diet[focal_rows,D.Item:=D.Item.Group][focal_rows,D.Item.Group:=NA]
    
    # Make unit pair table
    unit_pairs<-data.table(unit=c("D.Amount.Unit","D.Unit.Time","D.Unit.Animals","DC.is.Dry"),
                           var=c("D.Amount","D.Amount","D.Amount","D.Amount"),
                           name_field="A.Level.Name")
    
    # List types of ingredient allowed
    ingredient_types<-master_codes$AOM[grepl("Feed Ingredient",Path),unique(unlist(tstrsplit(unlist(tstrsplit(Path,"Feed Ingredient/",keep=2)),"/",keep=1)))]
    ingredient_types<-ingredient_types[!grepl("Ingredient ",ingredient_types)]
    
    allowed_values<-data.table(allowed_values=list(master_codes$AOM[grepl("Mechanical Process",Path),unique(Edge_Value)],
                                                   master_codes$AOM[grepl("Chemical Process",Path),unique(Edge_Value)],
                                                   master_codes$AOM[grepl("Biological Process",Path),unique(Edge_Value)],
                                                   master_codes$AOM[grepl("Thermal Process",Path),unique(Edge_Value)],
                                                   master_codes$AOM[grepl("Dehydration Process",Path),unique(Edge_Value)],
                                                   master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Amount",Values_New],
                                                   master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Time",Values_New],
                                                   master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Animals",Values_New],
                                                   c("Yes","No","Unspecified"),
                                                   c("Yes","No","Unspecified"),
                                                   ingredient_types,
                                                   c(Times.Out$Time,"All Times")),
    parent_tab_name=c("master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",NA,NA,"master_codes$AOM","Times.Out"),
    field=c("D.Process.Mech","D.Process.Chem","D.Process.Bio","D.Process.Therm","D.Process.Dehy","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","DC.Is.Dry","D.Ad.lib","D.Type","Time"))
    
    results<-validator(data=Animal.Diet,
                       numeric_cols=c("D.Amount"),
                       numeric_ignore_vals="Unspecified",
                       unit_pairs = data.table(unit="D.Unit.Amount",var="D.Amount",name_field="A.Level.Name"),
                       hilo_pairs = data.table(low_col="D.Day.Start",high_col="D.Day.End",name_field="A.Level.Name"),
                       compulsory_cols = c(A.Level.Name="A.Level.Name"),
                       duplicate_field="D.Item",
                       check_keyfields=data.table(parent_tab=list(Animals.Out),
                                                  parent_tab_name="Animals.Out",
                                                  keyfield="A.Level.Name"),
                       do_time = F,
                       trim_ws = T,
                       tabname=table_name)
    
    error_dat<-results$errors[!(value=="Base" & issue=="Mismatch in field value between parent and child tables.")]
    errors<-c(errors,list(error_dat))
    
    Animal.Diet<-results$data
    
    # Add merged item plus process names
    Animal.Diet[,row_index:=1:.N][,D.ItemxProcess:=paste0(c(D.Item,na.omit(c(D.Process.Mech,D.Process.Chem,D.Process.Bio,D.Process.Therm,D.Process.Dehy,D.Process.Other))),collapse="||"),by=row_index]
    
    # Update delimiter used for processes
    Animal.Diet[,D.ItemxProcess:=gsub("; ","--",D.ItemxProcess)][,D.ItemxProcess:=gsub(";","--",D.ItemxProcess)]
    
    # Save mappings for later use
    mappings<-unique(Animal.Diet[,.(D.Item=paste0(c(D.Item,na.omit(c(D.Process.Mech,D.Process.Chem,D.Process.Bio,D.Process.Therm,D.Process.Dehy,D.Process.Other))),collapse="; "),
                             D.ItemxProcess=D.ItemxProcess),by=.(B.Code,row_index)][,row_index:=NULL])
    
    # Update process column delimters
    focal_cols<-grep("Process",colnames(Animal.Diet),value=T)
    Animal.Diet <- Animal.Diet[, (focal_cols) := lapply(.SD,function(x){gsub("; |;","--",x)}), .SDcols = focal_cols]
    
    # Add logic to indicate if a compound diet item
    Animal.Diet[,D.Is.Group:=D.Type %in% na.omit(D.Item.Group),by=B.Code]
    
    # Error check % 0-100, g/kg or mg/g 0-1000, g/g or kg/kg 0-1
    # Sum per unit and diet
    diet_sum<-Animal.Diet[D.Is.Group==F,.(value=sum(D.Amount,na.rm = T)),by=.(B.Code,A.Level.Name,D.Unit.Amount,D.Unit.Time,D.Unit.Animals)]
    diet_sum_base<-diet_sum[A.Level.Name=="Base",!"A.Level.Name"]
    diet_sum<-diet_sum[A.Level.Name!="Base"]
    setnames(diet_sum_base,"value","value_base")
    diet_sum<-merge(diet_sum,diet_sum_base,all.x=T)[!is.na(value_base),value:=value+value_base]
    
    error_dat<-Animal.Diet[grepl("g/kg|mg/g",D.Unit.Amount) & D.Amount>1000
    ][,list(value=paste0(unique(row_index),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="row_index"
    ][,issue:="Unit is g/kg or mg/g and value is > 1000."]
    errors<-c(errors,list(error_dat))
    
    error_dat<-diet_sum[grepl("g/kg|mg/g",D.Unit.Amount) & value>1000
    ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="A.Level.Name"
    ][,issue:="Unit is g/kg or mg/g and total diet sums to > 1000."]
    errors<-c(errors,list(error_dat))
    
    error_dat<-Animal.Diet[grepl("%",D.Unit.Amount) & D.Amount>100
    ][,list(value=paste0(unique(row_index),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="row_index"
    ][,issue:="Unit is % and value is > 100."]
    errors<-c(errors,list(error_dat))
    
    error_dat<-diet_sum[grepl("%",D.Unit.Amount) & value>100
    ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="A.Level.Name"
    ][,issue:="Unit is % and total diet sums to > 100."]
    errors<-c(errors,list(error_dat))
    
    error_dat<-Animal.Diet[grepl("g/g|mg/mg|kg/kg",D.Unit.Amount) & D.Amount>1
    ][,list(value=paste0(unique(row_index),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="row_index"
    ][,issue:="Unit is mg/mg, g/g, or kg/kg and value is > 1."]
    errors<-c(errors,list(error_dat))
    
    error_dat<-diet_sum[grepl("g/g|mg/mg|kg/kg",D.Unit.Amount) & value>1
    ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="A.Level.Name"
    ][,issue:="Unit is g/g, mg/mg, or kg/kg and total diet sums to > 1."]
    errors<-c(errors,list(error_dat))
    
    # Error where the entire diet is not being described and is.na(Diet.Item)
    error_dat<-Animal.Diet[(is.na(D.Type)|D.Type!="Entire Diet") & is.na(D.Item) & !D.Is.Group,
                          ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
                               ][,table:=table_name
                                 ][,field:="A.Level.Name"
                                   ][,issue:="Rows have no diet item selected and diet type is not Entire Diet or a diet group value."]
    
    errors<-c(errors,list(error_dat))
    
    # Error where a Diet.Item is listed but no amount or ad libitum is give
    error_dat<-Animal.Diet[is.na(D.Amount) & is.na(D.Ad.lib) & !grepl("unspecified",D.Notes,ignore.case = T),
    ][,list(value=paste0(unique(row_index),collapse="/")),by=B.Code
    ][,table:=table_name
    ][,field:="row_index"
    ][,issue:="No information on amount given in diet ingredients, check this is not an error. If no information is given please make sure the notes state unspecified somewhere."]
    
    errors<-c(errors,list(error_dat))
    
   
  # 3.10) Animal.Diet.Comp (Nutrition) ######
    table_name<-"Nutrition.Out"
    Animal.Diet.Comp<-excel_dat[[table_name]]
    
    if(!"D.Item" %in% colnames(Animal.Diet.Comp)){
      error_dat<-data.table(B.Code=Pub.Out$B.Code,
                            value=NA,
                            table=table_name,
                            field=NA,
                            issue="Critical column names missing, probably spill issue in excel Nurition.Out tab. Error checking of this table cannot proceed further.")
      errors<-c(errors,list(error_dat))
      Animal.Diet.Comp<-NULL
    }else{
      
      col_check<-unlist(tstrsplit(colnames(Animal.Diet.Comp),"...",keep=1,fixed=T))
      col_check<-table(col_check[!col_check %in% c("","0")])
      col_check<-col_check[col_check>1]
      if(length(col_check)>0){
        error_dat<-data.table(B.Code=Pub.Out$B.Code,
                              value=paste(names(col_check)[!grepl("Method|Unit|Notes",names(col_check))],collapse = "/"),
                              table=table_name,
                              field=NA,
                              issue="Duplicate variables are not allowed. If measured and estimated, keep measured only.")
        errors<-c(errors,list(error_dat))
        Animal.Diet.Comp<-NULL
        
      }else{
      
      
    # Remove rows where D.Item is NA
    Animal.Diet.Comp<-Animal.Diet.Comp[!is.na(D.Item)]
    
    # Add study code
    Animal.Diet.Comp$B.Code<-Pub.Out$B.Code[1]
    
    if(nrow(Animal.Diet.Comp)>0){
    # Split colnames into different types of field
    col_names<-colnames(Animal.Diet.Comp)
    col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
    unit_cols<-grep("Unit",col_names,value=T)
    num_cols<-gsub("[.]Unit","",unit_cols)
    method_cols<-grep("Method",col_names,value=T)
    notes_cols<-grep("Notes",col_names,value=T)
    
    Animal.Diet.Comp<-Animal.Diet.Comp[,..col_names]
    
    # Copy down units and methods
    copy_down_cols<-c("DN.is.DM",unit_cols,method_cols,notes_cols)
    Animal.Diet.Comp <- Animal.Diet.Comp[, (copy_down_cols) := lapply(.SD,function(x){x[1]}), .SDcols = copy_down_cols]
    
    # Update delimiters used in diet.item names with processes
    Animal.Diet.Comp<-merge(Animal.Diet.Comp,mappings,by=c("B.Code","D.Item"),all.x=T)
    Animal.Diet.Comp[!is.na(D.ItemxProcess),D.Item:=D.ItemxProcess][,D.ItemxProcess:=NULL]
    
    
    # Define compulsory unit pairings
    unit_pairs<-data.table(unit=c(unit_cols,method_cols),
                           var=rep(num_cols,2),
                           name_field=num_cols)
    
    item_options<-as.vector(na.omit(unique(c(Animals.Out$A.Level.Name,
                                             Animal.Diet$D.Item,
                                             Animal.Diet$D.Item.Group,
                                             Animal.Diet$D.ItemxProcess))))
    
    # Allowed values
    # NOTE if diet.item names are updated in the previous section this needs to be considered here.
    allowed_values<-data.table(allowed_values=c(list(item_options),
                                                   replicate(length(unit_cols),master_codes$lookup_levels[Field=="DC.Unit",Values_New],simplify=F),
                                                   replicate(length(method_cols),c("Measured","Estimated","Unspecified"),simplify=F)),
                               parent_tab_name=c("Ingredients.Out",
                                                 rep("master_code$lookup_levels$DC.Unit",length(unit_cols)),
                                                 rep(NA,length(method_cols))),
                               field=c("D.Item",unit_cols,method_cols))
    # Compulsory columns
    comp_cols<-c(unit_cols,method_cols)
    comp_cols<-c("DN.is.DM",comp_cols)
    names(comp_cols)<-rep("B.Code",length(comp_cols))
    
  results<-validator(data=Animal.Diet.Comp,
                     numeric_cols=num_cols,
                     zero_cols = c("DN.is.DM",unit_cols,method_cols,notes_cols),
                     unit_pairs = unit_pairs,
                     compulsory_cols = comp_cols,
                     allowed_values = allowed_values,
                     duplicate_field="D.Item",
                     trim_ws = T,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Animal.Diet.Comp<-results$data
  
  # Set non-numerics cols to character
  non_numeric_cols<-colnames(Animal.Diet.Comp)[!colnames(Animal.Diet.Comp) %in% num_cols]
  non_numeric_cols<-non_numeric_cols[!non_numeric_cols %in% c("is_group","is_entire_diet")] # Not needed?
  Animal.Diet.Comp <- Animal.Diet.Comp[, (non_numeric_cols) := lapply(.SD, as.character), .SDcols = non_numeric_cols]
  
  # Add columns to indicate if a "compound" diet item described by A.Level.Name or Diet.Group 
  diet_groups<-unique(Animal.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
  setnames(diet_groups,"D.Item.Group","D.Item")
  
  diet_entire<-unique(Animals.Out[,.(B.Code,A.Level.Name)][,is_entire_diet:=T])
  setnames(diet_entire,"A.Level.Name","D.Item")
  
  if(nrow(diet_groups)>0){
    Animal.Diet.Comp<-merge(Animal.Diet.Comp,diet_groups,by=c("B.Code","D.Item"),all.x=T)[is.na(is_group),is_group:=F]
  }else{
    Animal.Diet.Comp[,is_group:=F]
  }
  Animal.Diet.Comp<-merge(Animal.Diet.Comp,diet_entire,by=c("B.Code","D.Item"),all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
    
  # !!!TO DO!!! Add validation for unit amounts vs unit type (e.g., if % should not be >100) #####
    }}
    }
    # 3.10.1) Wrangle dataset into long form ######
    if(!is.null(Animal.Diet.Comp)){
      if(nrow(Animal.Diet.Comp)>1){
      col_names<-colnames(Animal.Diet.Comp)
      other_cols<-col_names[!col_names %in% c(num_cols,method_cols,unit_cols,notes_cols)]
      cols<-data.table(num_cols,unit_cols,method_cols,notes_cols)
      colnames(cols)<-c("DN.Value","DN.Unit","DN.Method","DN.Notes")
      
      Animal.Diet.Comp<-rbindlist(lapply(1:nrow(cols),FUN=function(i){
        target_cols<-unlist(cols[i])
        cols_selected<-c(other_cols,target_cols)
        variable<-gsub("DN.","",target_cols[1])
        dat<-Animal.Diet.Comp[,cols_selected,with=F][,DN.Variable:=variable]
        setnames(dat,target_cols,names(target_cols))
        return(dat)
      }))
      }else{
        Animal.Diet.Comp<-NULL
      }
    }
    
  # 3.11) Animal.Diet.Digest ######
  table_name<-"Digest.Out"
  Animal.Diet.Digest<-excel_dat[[table_name]]
  
  # Remove rows where D.Item is NA
  if(!"D.Item" %in% colnames(Animal.Diet.Digest)){
    error_dat<-data.table(B.Code=Pub.Out$B.Code,
                          value=NA,
                          table=table_name,
                          field="D.Item",
                          issue=paste0("Structure of sheet is corrupted, D.Item is missing."))
    errors<-c(errors,list(error_dat))
    Animal.Diet.Digest<-NULL
  }else{
      
  Animal.Diet.Digest<-Animal.Diet.Digest[!is.na(D.Item)]
  
  # Check for missing column headings
  col_check<-colnames(Animal.Diet.Digest)[5:17][which(!is.na(as.numeric(t(Animal.Diet.Digest[1,5:17]))))]
  
  if(length(col_check)>0){
    col_check<-col_check[unlist(tstrsplit(col_check,"[.][.][.]",keep=1))=="0"]
    if(length(col_check)>0){
      col_check<-unlist(tstrsplit(col_check,"[.][.][.]",keep=2))
    }
  }
  
  if(length(col_check)>0){
    error_dat<-data.table(B.Code=Pub.Out$B.Code,
                          value=paste(col_check,collapse="/"),
                          table=table_name,
                          field="column number",
                          issue=paste0("Values are present in a column, but the name of the column is blank. Error checking of this table cannot proceed further."))
    errors<-c(errors,list(error_dat))
    Animal.Diet.Digest<-NULL
  }else{
  
  if(nrow(Animal.Diet.Digest)>0){
    # Add study code
    Animal.Diet.Digest$B.Code<-Pub.Out$B.Code[1]
    
    # Update delimiters used in diet.item names with processes
    Animal.Diet.Digest<-merge(Animal.Diet.Digest,mappings,by=c("B.Code","D.Item"),all.x=T)
    Animal.Diet.Digest[!is.na(D.ItemxProcess),D.Item:=D.ItemxProcess][,D.ItemxProcess:=NULL]
    
    # Rename columns where we have the same variable more than once (this can be due to different measurement methods between diets or diet item)
    col_names<-colnames(Animal.Diet.Digest)
    
    col_check<-unlist(tstrsplit(col_names,"[.][.][.]",keep=1))
    col_check<-col_check[!col_check %in% c("","0")]
    col_check<-table(col_check)
    col_check<-col_check[col_check>1 & !grepl("Unit|Method|Note|Diet",names(col_check))]
    
    if(length(col_check)>0){
      for(k in 1:length(col_check)){
        cols<-grep(names(col_check[k]),col_names,value=T)
        cols2<-grep(paste0(names(col_check[k]),"[.][.][.]"),col_names,value=T)
        # Check the value cols are not all NA
        all_na <- Animal.Diet.Digest[, sapply(.SD, function(col) !all(is.na(col))), .SDcols = cols2]
        # Remove any all NA value cols
        cols<-cols[rep(all_na,length(cols)/length(cols2))]
        # Split name removing [.][.][.]
        cols_new<-unlist(tstrsplit(cols,"[.][.][.]",keep=1))
        # Add numeric suffix if more than one column for same variable exists
        if(sum(all_na)!=1){
          cols_new<-paste0(cols_new,".",rep(1:sum(all_na),length(cols_new)/sum(all_na)))
        }
        setnames(Animal.Diet.Digest,cols,cols_new)
      }
    }
    
    # Remove empty columns
    col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
    
    # Split colnames into different types of field
    unit_cols<-grep("Unit",col_names,value=T)
    num_cols<-gsub("[.]Unit","",unit_cols)
    method_cols<-grep("Method",col_names,value=T)
    focus_cols<-grep("Nut.or.Diet",col_names,value=T)
    notes_cols<-grep("Notes",col_names,value=T)
    
    Animal.Diet.Digest<-Animal.Diet.Digest[,..col_names]

    # Copy down units and methods
    copy_down_cols<-c(unit_cols,method_cols,notes_cols,focus_cols,"DD.is.DM")
    Animal.Diet.Digest <- Animal.Diet.Digest[, (copy_down_cols) := lapply(.SD,function(x){     
      if(all(is.na(x))){x[1]}else{x[!is.na(x)]}}), .SDcols = copy_down_cols]
    
    # Enforce non-numeric cols are character
    Animal.Diet.Digest[, (copy_down_cols) := lapply(.SD, as.character), .SDcols = copy_down_cols]
    
    # Add row_index
    Animal.Diet.Digest[,row_index:=1:.N]
    
    # Create unit pairings
    unit_pairs<-data.table(unit=c(unit_cols,method_cols),
                           var=rep(num_cols,2),
                           name_field=num_cols)
    
    # Allowed values
    item_options<-as.vector(na.omit(unique(c(Animals.Out$A.Level.Name,Animal.Diet$D.Item,Animal.Diet$D.ItemxProcess,Animal.Diet$D.Item.Group))))
    # NOTE if diet.item names are updated in the previous sections this needs to be considered here.
    
    allowed_values<-data.table(allowed_values=c(list(item_options),
                                                replicate(length(unit_cols),master_codes$lookup_levels[Field=="DC.Unit",Values_New],simplify = F),
                                                replicate(length(focus_cols),c("Nutrient","Diet","Unspecified"),simplify = F),
                                                replicate(length(method_cols),aom[grep("Digestibility Measurement Method",Path),Edge_Value],simplify = F)),
                               parent_tab_name=c("Ingredients.Out",
                                                 rep("master_code$lookup_levels$DC.Unit",length(unit_cols)),
                                                 rep(NA,length(focus_cols)),
                                                 rep("aom",length(method_cols))),
                               field=c("D.Item",unit_cols,focus_cols,method_cols))
    
    # Compulsory columns
    comp_cols<-c(unit_cols,method_cols,focus_cols)
    comp_cols<-c("DD.is.DM",comp_cols)
    names(comp_cols)<-rep("row_index",length(comp_cols))
    
    results<-validator(data=Animal.Diet.Digest,
                       numeric_cols=num_cols,
                       zero_cols = c("DD.is.DM",unit_cols,method_cols,notes_cols),
                       unit_pairs = unit_pairs,
                       compulsory_cols =comp_cols,
                       allowed_values = allowed_values,
                       duplicate_field="D.Item",
                       trim_ws = T,
                       tabname=table_name)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Animal.Diet.Digest<-results$data
    
    if(exists("diet_groups")){
      Animal.Diet.Digest<-merge(Animal.Diet.Digest,diet_entire,all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
    if(nrow(diet_groups)>0){
      Animal.Diet.Digest<-merge(Animal.Diet.Digest,diet_groups,all.x=T)[is.na(is_group),is_group:=F]
    }else{
      Animal.Diet.Digest[,is_group:=F]
    }
    }else{
      Animal.Diet.Digest[,is_group:=F][,is_entire_diet:=F]
    }
    
  }}
  }
  
  # Update D.Item names from Animal.Diet.Comp Table
    # 3.11.1) Wrangle dataset into long form #####
    if(!is.null(Animal.Diet.Digest)){
      if(nrow(Animal.Diet.Digest)>0){
        col_names<-colnames(Animal.Diet.Digest)
        
        other_cols<-col_names[!col_names %in% c(num_cols,method_cols,focus_cols,unit_cols,notes_cols)]
        cols<-data.table(num_cols,unit_cols,focus_cols,method_cols,notes_cols)
        colnames(cols)<-c("DD.Value","DD.Unit","DD.Nut.or.Diet","DD.Method","DD.Notes")
        
        Animal.Diet.Digest<-rbindlist(lapply(1:nrow(cols),FUN=function(i){
          target_cols<-unlist(cols[i])
          cols_selected<-c(other_cols,target_cols)
          variable<-gsub("DD.","",target_cols[1])
          dat<-Animal.Diet.Digest[,cols_selected,with=F][,DD.Variable:=variable]
          setnames(dat,target_cols,names(target_cols))
          return(dat)
        }))
        }else{
          Animal.Diet.Digest<-NULL
        }
    }
    
    # 3.11.2) If focus is diet then values cannot add up to more 100% #####
      # Note that DM should not be included
    if(!is.null(Animal.Diet.Digest)){
      Animal.Diet.Digest[,DD.Value_sum:=sum(DD.Value[!DD.Variable %in% c("DM","OM")],na.rm=T),by=.(D.Item,D.Time,DD.Unit,DD.Nut.or.Diet)]
      
      error_dat<-unique(Animal.Diet.Digest[DD.Nut.or.Diet=="Diet" & ((DD.Value_sum>100 & DD.Unit=="%")|(DD.Value_sum>1 & DD.Unit=="g/kg|mg/g")),
                                           .(B.Code,D.Item,D.Time,DD.Unit)])
      
      error_dat<-error_dat[,.(value=paste(unique(D.Item),collapse = "/")),by=B.Code
                            ][,table:=table_name
                              ][,field:="D.Item"
                                ][,issue:="Focus of digestibility is diet, but values across the diet item or diet sum to more than 100 for % units or more than 1 for g/kg units"]
                  
      errors<-c(errors,list(error_dat))
      
      Animal.Diet.Digest[,DD.Value_sum:=NULL]
    }
    
    # 3.11.3) !!!TO DO!!! If focus is diet then value should not be more than the equivalent amount in the nutrient composition table (units need to match of course) #####
    # This requires some more efforts to harmonize units, in particular around % DM and converting g/kg to % or vice-versa
    # There are also complexities that could come from situation where composition has been measured using different methods
    if(!is.null(Animal.Diet.Comp)){
    mergedat<-unique(Animal.Diet.Comp[,.(B.Code,D.Item,DN.Variable,DN.Unit,DN.Value)])
    }
    # !!!TO DO!!! Add general validation for unit amounts vs unit type #####
  # 3.12) Agroforestry #####
    # 3.12.1) AF.Out######
  table_name<-"AF.Out"
  AF.Out<-excel_dat[[table_name]][,c(1:5,7,8)]
  template_cols<-c(master_template_cols[[table_name]][c(1:5,7,8)],"B.Code")
  AF.Out$B.Code<-Pub.Out$B.Code
  
  setnames(AF.Out,"AF.Level.Name...1","AF.Level.Name")
  template_cols[template_cols=="AF.Level.Name...1"]<-"AF.Level.Name"
  
  # Temp fix for whitespace issue
  colnames(AF.Out)<-trimws(colnames(AF.Out))
  
  AF.Out<-AF.Out[!is.na(AF.Level.Name)]
  AF.Out<-AF.Out[!(AF.Level.Name=="Base" & is.na(AF.Silvopasture) & is.na(AF.Boundary) & is.na(AF.Aquasilviculture) & is.na(AF.Other) & is.na(AF.Notes))]
  
  results<-validator(data=AF.Out,
                     tabname=table_name,
                     unique_cols = c("AF.Level.Name"),
                     template_cols = template_cols,
                     trim_ws = T)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  AF.Out<-results$data
  
    # 3.12.2) AF.Trees ######
  table_name<-"AF.Out"
  AF.Trees<-excel_dat[[table_name]][,c(10:13)]
  template_cols<-c(master_template_cols[[table_name]][c(10:13)],"B.Code")
  AF.Trees$B.Code<-Pub.Out$B.Code
  table_name<-"AF.Trees"
  
  colnames(AF.Trees)<-unlist(tstrsplit(colnames(AF.Trees),"[.][.][.]",keep=1))
  template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
  AF.Trees<-AF.Trees[!is.na(AF.Level.Name)]
  
  results<-validator(data=AF.Trees,
                     tabname=table_name,
                     character_cols = NULL,
                     compulsory_cols = c(AF.Tree="AF.Level.Name"),
                     numeric_cols = "AF.Tree.N",
                     template_cols = template_cols,
                     unit_pairs<-data.table(unit="AF.Tree.Unit",
                                            var="AF.Tree.N",
                                            name_field="AF.Tree.Unit"),
                     allowed_values = data.table(allowed_values=list(master_codes$trees[,unique(Tree.Latin.Name)]),
                                                 parent_tab_name="master_codes$trees",
                                                 field="AF.Tree"),
                     trim_ws = T)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  AF.Trees<-results$data
  
  # 3.13) Chemicals #####
    # 3.13.1) Chems.Code ####
    table_name<-"Chems.Out"
    Chems.Code<-excel_dat[[table_name]][,c(1:3)]
    template_cols<-c(master_template_cols[[table_name]][c(1:3)],"B.Code")
    Chems.Code$B.Code<-Pub.Out$B.Code
    
    setnames(Chems.Code,"C.Name","C.Level.Name")
    Chems.Code<-Chems.Code[!is.na(C.Level.Name)]
    
    # Enforce class character for columns
    Chems.Code[, (names(Chems.Code)) := lapply(.SD, as.character)]
    
    error_dat<-validator(data=Chems.Code,
                       unique_cols = "C.Level.Name",
                       ignore_values = "Unspecified",
                       tabname="Chems.Code")$errors
    
    errors<-c(errors,list(error_dat))
  
    # 3.13.2) Chems.Out ####
    table_name<-"Chems.Out"
    Chems.Out<-excel_dat[[table_name]][,c(5:18)]
    template_cols<-c(master_template_cols[[table_name]][c(5:18)],"B.Code")
    Chems.Out$B.Code<-Pub.Out$B.Code
    
    # Remove empty rows
    Chems.Out<-Chems.Out[!is.na(C.Level.Name)]
    
    colnames(Chems.Out)<-unlist(tstrsplit(colnames(Chems.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
    setnames(Chems.Out,c("C.Brand","C.Notes1","M.Year"),c("C.Name","C.Notes","Time"))
    template_cols[template_cols=="C.Brand"]<-"C.Name"
    template_cols[template_cols=="C.Notes1"]<-"C.Notes"
    template_cols[template_cols=="M.Year"]<-"Time"
    
    # Enforce character
    numeric_cols <- c("C.Amount", "C.Date.Start", "C.Date.End", "C.Date.DAP")
    Chems.Out[, (numeric_cols) := lapply(.SD, as.numeric), .SDcols = numeric_cols]
    other_cols<-colnames(Chems.Out)[!colnames(Chems.Out) %in% numeric_cols]
    Chems.Out[, (other_cols) := lapply(.SD, as.character), .SDcols = other_cols]
    
    allowed_values<-data.table(allowed_values=list(unique(c(master_codes$chem[,C.Name],master_codes$chem[,C.Name.AI...16])),
                                                   master_codes$lookup_levels[Field=="C.App.Method",Values_New],
                                                   master_codes$lookup_levels[Field=="C.Unit",Values_New],
                                                   c(Times.Out$Time,"All Times")),
                               parent_tab_name=c("master_codes$chem","master_codes$lookup_levels","master_codes$lookup_levels","Times.Out"),
                               field=c("C.Name","C.App.Method","C.Unit","Time"))
    
    results<-validator(data=Chems.Out,
                     numeric_cols=c("C.Amount","C.Applications","C.Date.DAP"),
                     numeric_ignore_vals="Unspecified",
                     unit_pairs = data.table(unit=c("C.Unit"),
                                             var=c("C.Amount"),
                                             name_field="C.Level.Name"),
                     check_keyfields=data.table(parent_tab=list(Chems.Code),
                                                parent_tab_name="Chems.Code",
                                                keyfield="C.Level.Name"),
                     hilo_pairs = data.table(low_col="C.Date.Start",high_col="C.Date.End",name_field="C.Level.Name"),
                     allowed_values=allowed_values,
                     ignore_values = "Unspecified",
                     do_time = F,
                     site_data =  Site.Out,
                     valid_start = valid_start,
                     valid_end = valid_end,
                     date_cols = c("C.Date.Start","C.Date.End"),
                      tabname=table_name,
                     duplicate_field="C.Level.Name",
                     trim_ws = T)
  
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    Chems.Out<-results$data
  
      # 3.13.2.1) !!TO DO: Harmonization #######
  if(F & !is.null(Chems.Out)){
    # Update C.Names
    Chems.Out<-Chems.Out[C.Name %in% c("Unspecified","unspecified")|is.na(C.Name),C.Name:=paste0("unspecified ",tolower(C.Type))]
    
    merge_dat<-master_codes$chem[!is.na(C.Name.Raw),.(C.Name.Raw,C.Name)]
    colnames(merge_dat)<-c("C.Name","C.Name.New")
    merge_dat<-split_syn(data=merge_dat,split_col="C.Name",delim=";")
    
    Chems.Out<-merge(Chems.Out,merge_dat,by="C.Name",all.x=T,sort=F)
    
    error_dat<-unique(Chems.Out[!C.Name %in% c("Unspecified") & 
                                  !is.na(C.Name) & 
                                  is.na(C.Name.New) & 
                                  !tolower(C.Name) %in% merge_dat$C.Name.New,.(C.Type,C.Name,B.Code)
                                ][,value:=paste0(C.Type,"-",C.Name)
                                  ][,.(B.Code=paste(B.Code,collapse="/")),by=value
                                    ][,table:=table_name
                                      ][,field:="C.Name"
                                        ][,issue:="No match with master codes chems tab."])
    
    errors[["chems_nomatch"]]<-error_dat
    
    Chems.Out[,C.Name_raw:=C.Name][!is.na(C.Name.New),C.Name:=C.Name.New][,C.Name.New:=NULL]
    
    # Merge in chemical information
    merge_dat<-master_codes$chem[!is.na(C.Name.Raw),.(C.Name,C.Type.AI,C.Is.Name.Commercial,C.AI)]
    merge_dat[is.na(C.AI),C.AI:=C.Name]
    
    Chems.Out<-merge(Chems.Out,merge_dat,by="C.Name",all.x=T,sort=F)
  
  # Units
  h_params<-data.table(h_table="Chems.Out",
                       h_field=c("C.Unit","C.AI.Unit"),
                       h_table_alt=c(NA,NA),
                       h_field_alt=c(NA,NA))
  
  results<-harmonizer_wrap(data=Chems.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks<-list(results$h_tasks)
  Chems.Out<-results$data
  }
  
    # 3.13.3) Chems.AI ####
  table_name<-"Chems.Out"
  Chems.AI<-excel_dat[[table_name]][,c(21:26)]
  template_cols<-c(master_template_cols[[table_name]][c(21:26)],"B.Code")
  Chems.AI$B.Code<-Pub.Out$B.Code
  table_name<-"Chems.AI"
  
  colnames(Chems.AI)<-unlist(tstrsplit(colnames(Chems.AI),"[.][.][.]",keep=1))
  template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
  colnames(Chems.AI)[1]<-"C.Name"
  table_name[1]<-"C.Name"
  
  setnames(Chems.AI,"C.Notes2","C.Notes")
  template_cols[template_cols=="C.Notes2"]<-"C.Notes"
  
  # Remove NA rows
  Chems.AI<-Chems.AI[!is.na(C.Name)]
  
  # Enforce character
  Chems.AI[, (names(Chems.AI)) := lapply(.SD, as.character)]
  
  allowed_values<-data.table(allowed_values=list(unique(master_codes$chem[,C.Name.AI...16]),
                                                 master_codes$lookup_levels[Field=="C.AI.Unit",Values_New]),
                             parent_tab_name=c("master_codes$chem","master_codes$lookup_levels"),
                             field=c("C.Name.AI","C.AI.Unit"))
  
  results<-validator(data=Chems.AI,
                     numeric_cols=c("C.AI.Amount"),
                     numeric_ignore_vals="Unspecified",
                     unit_pairs = data.table(unit=c("C.AI.Unit"),
                                             var=c("C.AI.Amount"),
                                             name_field="C.Name"),
                     check_keyfields=data.table(parent_tab=list(Chems.Out),
                                                parent_tab_name="Chems.Out",
                                                keyfield="C.Name"),
                     allowed_values=allowed_values,
                     tabname=table_name,
                     duplicate_field="C.Name",
                     trim_ws = T)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  Chems.AI<-results$data
  
      # 3.13.3.1) TO DO !!!! Harmonization #######
      if(F){
        # Potential harmonizatio for units and AI
      }
  
    # 3.13.4) Update Chems.Code Base ######
    # IF there is no information in the chems.out tab remove base practice in Chems.Code tab
  if(nrow(Chems.Out)==0){
    Chems.Code<-Chems.Code[!(C.Level.Name=="Base" & is.na(C.Structure) & is.na(C.Notes))]
  }
  # 3.14) Grazing Management ####
    # 3.14.1) GM.Out #####
    table_name<-"GM.Out"
    GM.Out<-excel_dat[[table_name]][,c(1:10)]
    template_cols<-c(master_template_cols[[table_name]][c(1:10)],"B.Code")
    GM.Out$B.Code<-Pub.Out$B.Code
    
    # Temporary field name fix
    #setnames(GM.Out,"Pasture.Shade","GM.Shade",skip_absent = T)
    #template_cols[template_cols=="Pasture.Shade"]<-"GM.Shade"
    
    # Deal with duplicate colnames between tables stored in same excel sheet
    colnames(GM.Out)<-unlist(tstrsplit(colnames(GM.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty rows
    GM.Out<-GM.Out[!is.na(GM.Level.Name)]
    # Remove base practice where all columns are NA
    base_check<-all(is.na(GM.Out[GM.Level.Name=="Base",2:8]))
    if(base_check){
      GM.Out<-GM.Out[GM.Level.Name!="Base"]
    }
    
      # Set allowed values
      vals<-c("Yes","No","Unspecified")
      allowed_values = data.table(allowed_values=list(vals,vals,vals,vals,
                                                      master_codes$lookup_levels[Field=="GM.Stock.Meth",Values_New],
                                                      master_codes$lookup_levels[Field=="GM.Stock.Adapt.Meth",Values_New],
                                                      vals,
                                                      master_codes$lookup_levels[Field %in% c("Pasture.Shade","GM.Shade"),Values_New]),
                                  parent_tab_name=c(NA,NA,NA,NA,
                                                    "master_codes$lookup_levels",
                                                    "master_codes$lookup_levels",
                                                    NA,
                                                    "master_code$lookup_levels"),
                                  field=c("GM.Graz.Pres","GM.Graz.Cont","GM.Graz.Rot","GM.Graz.Strip","GM.Stock.Meth","GM.Stock.Adapt.Meth",
                                          "GM.Burn","GM.Shade"))
      
      results<-validator(data=GM.Out,
                         unique_cols = "GM.Level.Name",
                         tabname=table_name,
                         allowed_values = allowed_values,
                         template_cols = template_cols,
                         check_keyfields=data.table(parent_tab=list(GM.Out),
                                                    parent_tab_name=c("GM.Out"),
                                                    keyfield=c("GM.Level.Name")),
                         trim_ws = T)
      
      error_dat<-results$errors
      errors<-c(errors,list(error_dat))
      
      GM.Out<-results$data
      # 3.14.1.2) TO DO !!! Add Codes #######
      
      GM.Out[,GM.Codes:=NA]
    # 3.14.2) GM.Method ##### 
    table_name<-"GM.Out"
    GM.Method<-excel_dat[[table_name]][,c(11:33)]
    template_cols<-c(master_template_cols[[table_name]][c(11:33)],"B.Code")
    GM.Method$B.Code<-Pub.Out$B.Code
    table_name<-"GM.Method"
    
    setnames(GM.Method,"M.Year","Time",skip_absent = T)
    template_cols[template_cols=="M.Year"]<-"Time"
    
    # Tidy column names
    colnames(GM.Method)<-unlist(tstrsplit(colnames(GM.Method),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty columns with [.][.][.] delim
    col_names<-colnames(GM.Method)
    col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
    GM.Method<-GM.Method[,..col_names]
    template_cols<-template_cols[template_cols %in% col_names]
    
    # Remove any empty rows
    GM.Method<-GM.Method[!is.na(GM.Level.Name)]
    
    if(nrow(GM.Method)>0){
    # Note the num_cols and unit_cols here must be in the same order
    num_cols<-c("GM.Tot.Area","GM.Tot.Stock.Rate","GM.Tot.Graz.Time",
                "GM.Subdiv.Area","GM.Subdiv.Stock.Rate","GM.Subdiv.Time.Cyc",
                "GM.Subdiv.Time.Rest","GM.Subdiv.Time.Len","GM.Biomass.Target")
    
    unit_cols<-c("GM.Tot.Area.Unit","GM.Tot.Stock.Rate.Unit","GM.Tot.Graz.Time.Unit",
                 "GM.Subdiv.Area.Unit","GM.Subdiv.Stock.Rate.Unit",rep("GM.Subdiv.Time.Unit",3),
                 "GM.Biomass.Unit")
    
    # Copy down units
    copy_down_cols<-unique(unit_cols)
    GM.Method <- GM.Method[, (copy_down_cols) := lapply(.SD,function(x){x[1]}), .SDcols = copy_down_cols]
    
    
    # Create unit pairings
    unit_pairs<-data.table(unit=unit_cols,
                           var=num_cols,
                           name_field="B.Code")
    
    # Allowed values for units 
    allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field=="GM.Tot.Area.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Tot.Stock.Rate.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Tot.Graz.Time.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Subdiv.Area.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Subdiv.Stock.Rate.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Subdiv.Time.Unit",Values_New],
                                                   master_codes$lookup_levels[Field=="GM.Biomass.Unit",Values_New],
                                                   c(Times.Out$Time,"All Times")),
                               parent_tab_name=c(rep("master_codes$lookup_levels",7),"Times.Out"),
                               field=c("GM.Tot.Area.Unit","GM.Tot.Stock.Rate.Unit","GM.Tot.Graz.Time.Unit",
                                       "GM.Subdiv.Area.Unit","GM.Subdiv.Stock.Rate.Unit","GM.Subdiv.Time.Unit",
                                       "GM.Biomass.Unit","Time"))
    
    results<-validator(data=GM.Method,
                       tabname=table_name,
                       zero_cols =unique(unit_cols),
                       numeric_cols = c(num_cols,"GM.Sward.Target","GM.Sward.Max"),
                       unit_pairs=unit_pairs,
                       allowed_values = allowed_values,
                       template_cols = template_cols,
                       do_time =  F,
                       site_data = Site.Out,
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    GM.Method<-results$data
    }
    
  # 3.15) Pasture Planting ####
    # 3.15.1) Plant.Out ######
    table_name<-"Plant.Out"
    Plant.Out<-excel_dat[[table_name]][,c(1:5)]
    template_cols<-c(master_template_cols[[table_name]][c(1:5)],"B.Code")
    Plant.Out$B.Code<-Pub.Out$B.Code
    
    colnames(Plant.Out)<-unlist(tstrsplit(colnames(Plant.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty rows
    Plant.Out<-Plant.Out[!is.na(P.Level.Name)]
    
    # Make list of allowed values
    vals<-c("Yes","No","Unspecified")
    
    allowed_values<-data.table(allowed_values=list(aom[grepl("Pasture Management/Improved|Pasture Management/Unimpro",Path),Edge_Value],
                                                   vals,
                                                   c("Yes","No")),
                               parent_tab_name=c("master_codes$AOM",NA,NA),
                               field=c("A.Pasture.Man","A.Pasture.Inter","P.Structure"))
    
    results<-validator(data=Plant.Out,
                       unique_cols = "P.Level.Name",
                       tabname=table_name,
                       allowed_values = allowed_values,
                       template_cols = template_cols,
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Plant.Out<-results$data
    
     # 3.15.1.1) TO DO!!! Add codes #######
    Plant.Out[,P.Codes:=NA]
    # 3.15.2) Plant.Method ######
    table_name<-"Plant.Out"
    Plant.Method<-excel_dat[[table_name]][,c(8:14)]
    template_cols<-c(master_template_cols[[table_name]][c(8:14)],"B.Code")
    Plant.Method$B.Code<-Pub.Out$B.Code
    
    table_name<-"Plant.Method"
    
    colnames(Plant.Method)<-unlist(tstrsplit(colnames(Plant.Method),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    Plant.Method<-Plant.Method[!is.na(P.Level.Name)]
  
    # Create unit pairings
    unit_pairs <- data.table(unit=c("Plant.Density.Unit"),
                            var=c("Plant.Density"),
                            name_field="P.Level.Name")
    
    # Set allowed values
    vals<-c("Yes","No","Unspecified")
    allowed_values = data.table(allowed_values=list(master_codes$lookup_levels[Field == "Plant.Method",Values_New],
                                                    vals,
                                                    master_codes$lookup_levels[Field == "F.Mechanization",Values_New]),
                                parent_tab_name=c("master_codes$lookup_levels",NA,"master_codes$lookup_levels"),
                                field=c("P.Method","Plant.Overseeding","P.Mechanization"))
    
    results<-validator(data=Plant.Method,
                       tabname=table_name,
                       numeric_cols = "Plant.Density",
                       compulsory_cols = c(P.Level.Name="P.Species"),
                       allowed_values = allowed_values,
                       template_cols = template_cols,
                       check_keyfields=data.table(parent_tab=list(Plant.Out),
                                                  parent_tab_name=c("Plant.Out"),
                                                  keyfield=c("P.Level.Name")),
                       trim_ws = T)
    
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Plant.Method<-results$data
    
  # 3.16) Tillage ####
    # 3.16.1) Till.Out #####
    table_name<-"Till.Out"
    Till.Out<-excel_dat[[table_name]][,c(1:4)]
    template_cols<-c(master_template_cols[[table_name]][c(1:4)],"B.Code")
    Till.Out$B.Code<-Pub.Out$B.Code
    
    colnames(Till.Out)<-unlist(tstrsplit(colnames(Till.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty rows
    Till.Out<-Till.Out[!is.na(Till.Level.Name)]
    
    # Set allowed values
    allowed_values <- data.table(allowed_values=list(master_codes$prac[grepl("Tillage",Practice),Subpractice]),
                                parent_tab_name=c("master_codes$prac"),
                                field=c("Till.Practice"))
    
    results<-validator(data=Till.Out,
                       unique_cols = "Till.Level.Name",
                       tabname=table_name,
                       allowed_values = allowed_values,
                       template_cols = template_cols,
                       trim_ws = T)
    
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Till.Out<-results$data
    
    # 3.16.2) Till.Method ####
    table_name<-"Till.Out"
    Till.Method<-excel_dat[[table_name]][,c(6:18)]
    template_cols<-c(master_template_cols[[table_name]][c(6:18)],"B.Code")
    Till.Method$B.Code<-Pub.Out$B.Code
    
    table_name<-"Till.Method"
    
    colnames(Till.Method)<-unlist(tstrsplit(colnames(Till.Method),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    colnames(Till.Method)[1]<-"Till.Level.Name"
    template_cols[1]<-"Till.Level.Name"
    
    Till.Method<-Till.Method[!is.na(Till.Level.Name)]
    
    # Set allowed values
    allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field == "T.Method",Values_New],
                                                    master_codes$lookup_levels[Field == "Till.Other",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Mechanization",Values_New]),
                                parent_tab_name=c("master_codes$lookup_levels",
                                                  "master_codes$lookup_levels",
                                                  "master_codes$lookup_levels"),
                                field=c("T.Method","T.Method.Other","T.Mechanization"))
    
    results<-validator(data=Till.Method,
                       tabname=table_name,
                       numeric_cols = c("T.Depth","T.Strip.P","T.Strip.WT","T.Strip.WU","T.Freq"),
                       date_cols = c("T.Date.Start","T.Date.End"),
                       hilo_pairs = data.table(low_col="T.Date.Start",high_col="T.Date.End",name_field="Till.Level.Name"),
                       valid_start = valid_start,
                       valid_end = valid_end,
                       allowed_values = allowed_values,
                       template_cols = template_cols,
                       site_data = Site.Out,
                       time_data = Times.Out,
                       check_keyfields=data.table(parent_tab=list(Till.Out),
                                                  parent_tab_name=c("Till.Out"),
                                                  keyfield=c("Till.Level.Name")),
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Till.Method<-results$data
    
  # 3.17 Fertilizer ####
    # 3.17.1) Fert.Out ######
    table_name<-"Fert.Out"
    Fert.Out<-excel_dat[[table_name]][,c(1:7,9:28)]
    template_cols<-c(master_template_cols[[table_name]][c(1:7,9:28)],"B.Code")
    Fert.Out$B.Code<-Pub.Out$B.Code
    
    colnames(Fert.Out)<-unlist(tstrsplit(colnames(Fert.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty rows
    Fert.Out<-Fert.Out[!is.na(F.Level.Name)]
    # Remove base practice where all columns are NA
    base_check<-all(is.na(Fert.Out[F.Level.Name=="Base",2:26]))
    if(base_check){
      Fert.Out<-Fert.Out[F.Level.Name!="Base"]
    }
    
    # Copy down units
    unit_cols<-grep("Unit",colnames(Fert.Out),value=T)
    Fert.Out <- Fert.Out[, (unit_cols) := lapply(.SD,function(x){x[1]}), .SDcols = unit_cols]
    
    # Set allowed values
    allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field == "F.Rate.Pracs",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Timing.Pracs",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Precision.Pracs",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Info.Pracs",Values_New]),
                               parent_tab_name=c("master_codes$lookup_levels",
                                                 "master_codes$lookup_levels",
                                                 "master_codes$lookup_levels",
                                                 "master_codes$lookup_levels"),
                               field=c("F.Prac.Rate","F.Prac.Timing","F.Prac.Precision","F.Prac.Info"))
    
    # Set unit pairss
    num_cols<-c("F.NO","F.PO","F.KO","F.NI","F.PI","F.P2O5","F.KI","F.K2O")
    unit_pairs<-data.table(unit=c(rep("F.O.Unit",3),rep("F.I.Unit",5)),
                           var=num_cols,
                           name_field="F.Level.Name")
    
    results<-validator(data=Fert.Out,
                       unique_cols = "F.Level.Name",
                       numeric_cols = num_cols,
                       zero_cols = c("F.O.Unit","F.I.Unit"),
                       tabname=table_name,
                       allowed_values = allowed_values,
                       unit_pairs=unit_pairs,
                       template_cols = template_cols,
                       trim_ws = T)
    
      error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Fert.Out<-results$data
      # 3.17.1.1) TO DO!!! Add Codes #######
    # Code for adding fertilizer code should exist in industrious elephant import script
    Fert.Out$F.Codes<-NA
    # 3.17.2) Fert.Method ######
    table_name<-"Fert.Out"
    Fert.Method<-excel_dat[[table_name]][,c(30:49)]
    template_cols<-c(master_template_cols[[table_name]][c(30:49)],"B.Code")
    Fert.Method$B.Code<-Pub.Out$B.Code
    
    table_name<-"Fert.Method"
    
    colnames(Fert.Method)<-unlist(tstrsplit(colnames(Fert.Method),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    colnames(Fert.Method)[1]<-"F.Level.Name"
    template_cols[1]<-"F.Level.Name"
    
    setnames(Fert.Method,c("M.Year","Times"),c("Time","Time"),skip_absent = T)
    template_cols[template_cols %in% c("M.Year","Times")]<-"Time"
    
    Fert.Method<-Fert.Method[!is.na(F.Level.Name)]
    
    # Set allowed values
    allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field == "F.Unit",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Method",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Physical",Values_New],
                                                   master_codes$lookup_levels[Field == "F.Mechanization",Values_New],
                                                   master_codes$lookup_levels[Field == "M.Source",Values_New],
                                                   master_codes$lookup_levels[Field == "M.Fate",Values_New],
                                                   c(Times.Out$Time,"All Times")),
                               parent_tab_name=c("master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",
                                                 "master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",
                                                 "Times.Out"),
                               field=c("F.Unit","F.Method","F.Physical","F.Mechanization","F.Source","F.Fate","Time"))
    
    # Set unit pairs
    unit_pairs<-data.table(unit=c("F.Unit"),
                           var="F.Amount",
                           name_field="F.Level.Name")
    
    results<-validator(data=Fert.Method,
                       compulsory_cols = c(F.Level.Name="F.Type"),
                       numeric_cols = c("F.Amount","F.Date.DAP","F.Date.DAE"),
                       date_cols = c("F.Date.Start","F.Date.End"),
                       valid_start = valid_start,
                       valid_end = valid_end,
                       hilo_pairs = data.table(low_col="F.Date.Start",high_col="F.Date.End",name_field="F.Level.Name"),
                       tabname=table_name,
                       allowed_values = allowed_values,
                       site_data = Site.Out,
                       do_time =  F,
                       unit_pairs=unit_pairs,
                       template_cols = template_cols,
                       check_keyfields=data.table(parent_tab=list(Fert.Out),
                                                  parent_tab_name=c("Fert.Out"),
                                                  keyfield=c("F.Level.Name")),
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Fert.Out<-results$data
    
     # 3.17.2.1) TO DO!!! Harmonization #####
    
    # 3.17.3) Fert.Composition ######
    table_name<-"Fert.Out"
    Fert.Comp<-excel_dat[[table_name]][,c(50:62,64:73)]
    template_cols<-c(master_template_cols[[table_name]][c(50:62,64:73)],"B.Code")
    Fert.Comp$B.Code<-Pub.Out$B.Code
    
    table_name<-"Fert.Comp"
    
    colnames(Fert.Comp)<-unlist(tstrsplit(colnames(Fert.Comp),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
    Fert.Comp<-Fert.Comp[!is.na(F.Type)]
    
    # Set allowed values
    unit_vals<-master_codes$lookup_levels[Field == "Soil.SOM.Unit",Values_New]
    unit_vals2<-master_codes$lookup_levels[Field == "Soil.N.Unit",Values_New]
    ph_vals<-master_codes$lookup_levels[Field == "Soil.pH.Method",Values_New]
    vals<-c("Yes","No","Unspecified","NA")
    
    allowed_values<-data.table(allowed_values=list(unit_vals,unit_vals,unit_vals2,unit_vals2,unit_vals2,
                                                   unit_vals2,unit_vals2,unit_vals2,unit_vals2,ph_vals,vals),
                               parent_tab_name=rep("master_codes$lookup_levels",11),
                               field=c("F.DM.Unit","F.OC.Unit","F.N.Unit","F.TN.Unit","F.AN.Unit","F.P.Unit","F.TP.Unit",
                                       "F.AP.Unit","F.K.Unit","F.pH.Method","F.DW"))
    
    # Set unit pairs
    unit_cols<-grep("Unit",colnames(Fert.Comp),value=T)
    val_cols<-c(unlist(tstrsplit(unit_cols,"[.]Unit",keep=1)),"F.pH")
    unit_cols<-c(unit_cols,"F.pH.Method")
      unit_pairs<-data.table(unit=unit_cols,
                           var=val_cols,
                           name_field="F.Type")
    
    results<-validator(data=Fert.Comp,
                       compulsory_cols = c(F.Type="F.DW"),
                       numeric_cols = val_cols,
                       tabname=table_name,
                       allowed_values = allowed_values,
                       unit_pairs=unit_pairs,
                       template_cols = template_cols,
                       check_keyfields=data.table(parent_tab=list(Fert.Method),
                                                  parent_tab_name=c("Fert.Method"),
                                                  keyfield=c("F.Type")),
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Fert.Comp<-results$data
    
      # 3.17.3.1) TO DO!!! Harmonization #####
  # 3.18) Pasture Description ####
    # 3.18.1) Pasture.Out #####
    table_name<-"Pasture.Out"
    Pasture.Out<-excel_dat[[table_name]][,c(1:12)]
    template_cols<-c(master_template_cols[[table_name]][c(1:12)],"B.Code")
    Pasture.Out$B.Code<-Pub.Out$B.Code
    
    colnames(Pasture.Out)<-unlist(tstrsplit(colnames(Pasture.Out),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    setnames(Pasture.Out,"M.Year","Time",skip_absent = T)
    template_cols[template_cols=="M.Year"]<-"Time"
    
    # Remove empty rows
    Pasture.Out<-Pasture.Out[!is.na(Pasture.Level.Name)]
    
    # Set unit pairs
    unit_pairs<-data.table(unit=c("Pasture.Biom.Unit"),
                           var="Pasture.Biom",
                           name_field="Pasture.Level.Name")
    
    # Allowed values
    vals<-c("Yes","No","Unspecified")
      allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field == "Pasture.Biom.Unit",Values_New],vals),
                               parent_tab_name=c("master_codes$lookup_levels",NA),
                               field=c("Pasture.Biom.Unit","Pasture.Start.Cond"))
    
    
    results<-validator(data=Pasture.Out,
                       unique_cols = "Pasture.Level.Name",
                       compulsory_cols = c(Pasture.Level.Name="Pasture.Start.Cond"),
                       numeric_cols = c("Pasture.Veg","Pasture.Bare","Pasture.Biom"),
                       date_cols = c("Pasture.Time.Start","Pasture.Time.End"),
                       valid_start = valid_start,
                       valid_end = valid_end,
                       hilo_pairs = data.table(low_col="Pasture.Time.Start",high_col="Pasture.Time.End",name_field="Pasture.Level.Name"),
                       tabname=table_name,
                       allowed_values = allowed_values,
                       site_data = Site.Out,
                       time_data = Times.Out,
                       unit_pairs=unit_pairs,
                       template_cols = template_cols,
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Pasture.Out<-results$data
    
    # Enforce P.Level.Name to be character
    Pasture.Out[,Pasture.Level.Name:=as.character(Pasture.Level.Name)]
    
    # 3.18.2) Pasture.Comp #####
    table_name<-"Pasture.Out"
    Pasture.Comp<-excel_dat[[table_name]][,c(14:18)]
    template_cols<-c(master_template_cols[[table_name]][c(14:18)],"B.Code")
    Pasture.Comp$B.Code<-Pub.Out$B.Code
    
    setnames(Pasture.Comp,"Times","Time")
    template_cols[template_cols=="Times"]<-"Time"
    
    table_name<-"Pasture.Comp"
    colnames(Pasture.Comp)<-unlist(tstrsplit(colnames(Pasture.Comp),"[.][.][.]",keep=1))
    template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
    
    # Remove empty rows
    Pasture.Comp<-Pasture.Comp[!is.na(Pasture.Level.Name)]
    
    # Allowed values
    vals<-aom[grepl("Ingredient/Forage Plants/",Path) & !grepl("Forage Tree|Aquatic Plants",Path),unique(`Scientific Name`)]
    
    allowed_values<-data.table(allowed_values=list(vals,c(Times.Out$Time,"All Times")),
                               parent_tab_name=c("master_codes$AOM","Times.Out$Time"),
                               field=c("Pasture.Species","Time"))
  
    results<-validator(data=Pasture.Comp,
                       numeric_cols = c("Pasture.Cover"),
                       tabname=table_name,
                       allowed_values = allowed_values,
                       do_time = F,
                       template_cols = template_cols,
                       check_keyfields=data.table(parent_tab=list(Pasture.Out),
                                                  parent_tab_name=c("Pasture.Out"),
                                                  keyfield=c("Pasture.Level.Name")),
                       trim_ws = T)
    
    error_dat<-results$errors
    errors<-c(errors,list(error_dat))
    
    Pasture.Comp<-results$data
    
  # 3.19) Other ######
    table_name<-"Other.Out"
    Other.Out<-excel_dat[[table_name]]
    template_cols<-c(master_template_cols[[table_name]],"B.Code")
    Other.Out$B.Code<-Pub.Out$B.Code
    
    setnames(Other.Out,c("O.Feed.Prev","O.O"),c("O.Other","O.Feed.Pres"))
    template_cols[template_cols=="O.O"]<-"O.Other"
    template_cols[template_cols=="O.Feed.Prev"]<-"O.Feed.Pres"
    
    # Remove empty rows
    Other.Out<-Other.Out[!is.na(O.Level.Name)]
    base_check<-all(is.na(Other.Out[O.Level.Name=="Base",2:7]))
    if(base_check){
      Other.Out<-Other.Out[O.Level.Name!="Base"]
    }
    
    # Set allowed values
    vals<-c("Yes","No","Unspecified")
    
    allowed_values<-data.table(allowed_values=list(vals,vals,vals,vals,vals),
                               parent_tab_name=rep(NA,5),
                               field=c("O.Fodder.Banks","O.Feed.Pres","O.Manure.Man","O.CC","O.Other"))
    
    results<-validator(data=Other.Out,
                       unique_cols = "O.Level.Name",
                       allowed_values = allowed_values,
                       trim_ws = T,
                       tabname="Other.Out")
    
    errors<-c(errors,list(results$errors))
    Other.Out<-results$data
    
    Other.Out[,N:=.N,by=B.Code][,No_struc:=any(O.Structure=="Yes"),by=B.Code]
  
  # If Base  practice set W.Structure to NA
  Other.Out[O.Level.Name=="Base",O.Structure:=NA]
  
  error_dat<-Other.Out[N>1 & No_struc==T
  ][,N:=NULL
  ][,No_struc:=NULL
  ][,list(B.Code,O.Level.Name)
  ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
  ][,value:=O.Level.Name
  ][,O.Level.Name:=NULL
  ][,table:=table_name
  ][,field:="O.Level.Name"
  ][,issue:=">1 other practice exists and comparison IS allowed, check comparisons field is correct."]
  errors<-c(errors,list(error_dat))
  
  error_dat<-Other.Out[N>1 & No_struc==F
  ][,N:=NULL
  ][,No_struc:=NULL
  ][,list(B.Code,O.Level.Name)
  ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
  ][,value:=O.Level.Name
  ][,O.Level.Name:=NULL
  ][,table:=table_name
  ][,field:="O.Level.Name"
  ][,issue:=">1 other practice exists and comparison IS NOT allowed, check comparisons field is correct."]
  errors<-c(errors,list(error_dat))
  
  error_dat<-Other.Out[N==1][,N:=NULL
  ][,list(B.Code,O.Level.Name)
  ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
  ][,value:=O.Level.Name
  ][,O.Level.Name:=NULL
  ][,table:=table_name
  ][,field:="O.Level.Name"
  ][,issue:="One other practice exists, please check that the comparisons field is correctly assigned."]
  errors<-c(errors,list(error_dat))
  
  Other.Out[,N:=NULL][,No_struc:=NULL]
  
    # 3.19.1) TO DO!!! Add Codes #####
      Other.Out[,O.Codes:=NA]
  # 3.20) Base Practices (Base.Out) #####
  Base.Out<-list(
    AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
    Animals.Out[A.Level.Name=="Base"& !is.na(A.Codes),c("B.Code","A.Codes")],
    GM.Out[GM.Level.Name=="Base",.(B.Code,GM.Codes)],
    Plant.Out[P.Level.Name=="Base",.(B.Code,P.Codes)],
    Till.Out[Till.Level.Name=="Base",.(B.Code, Till.Codes)],
    #Fert.Out[F.Level.Name=="Base",.(B.Code,F.Codes)],
    Other.Out[O.Level.Name=="Base",.(B.Code,O.Codes)]
  )
  
  Base.Out<-rbindlist(Base.Out[unlist(lapply(Base.Out,nrow))>0],use.names = F)
  
# 4) Treatments (MT.Out)  #####
table_name<-"MT.Out"
MT.Out<-excel_dat[[table_name]][,c(1:16)]
template_cols<-c(master_template_cols[[table_name]][c(1:16)],"B.Code")
MT.Out$B.Code<-Pub.Out$B.Code

setnames(MT.Out,c("C.Name","T.Herd","T.Subgroup"),c("C.Level.Name","Herd.Level.Name","Herd.Sublevel.Name"))
template_cols[template_cols=="C.Name"]<-"C.Level.Name"
template_cols[template_cols=="T.Herd"]<-"Herd.Level.Name"
template_cols[template_cols=="T.Subgroup"]<-"Herd.Sublevel.Name"

# Temporary fix for missing O.Level.Name
if(!"O.Level.Name" %in% colnames(MT.Out)){
  colnames(MT.Out)[14]<-"O.Level.Name"
}

# Remove empty rows
MT.Out<-MT.Out[!is.na(T.Name)]

# Enforce herd sublevel to be character
MT.Out[,Herd.Sublevel.Name:=as.character(Herd.Sublevel.Name)]

# !!!TO DO!!!! Add feed intake item to keyfields or allowed values ####

keyfields<-data.table(parent_tab=list(Pasture.Out, 
                                      Plant.Out,
                                      Fert.Out,
                                      Till.Out,
                                      GM.Out,
                                      Animals.Out,
                                      AF.Out,
                                      Chems.Code,
                                      Other.Out,
                                      Herd.Out,
                                      Herd.Out),
                      parent_tab_name=c("Pasture.Out",
                                        "Plant.Out",
                                        "Fert.Out",
                                        "Till.Out",
                                        "GM.Out",
                                        "Animals.Out",
                                        "AF.Out",
                                        "Chems.Code",
                                        "Other.Out",
                                        "Herd.Out",
                                        "Herd.Out"),
                      keyfield=c("Pasture.Level.Name",
                                 "P.Level.Name",
                                 "F.Level.Name",
                                 "Till.Level.Name",
                                 "GM.Level.Name",
                                 "A.Level.Name",
                                 "AF.Level.Name",
                                 "C.Level.Name",
                                 "O.Level.Name",
                                 "Herd.Level.Name",
                                 "Herd.Level.Name/Herd.Sublevel.Name"))


# Combine T.Name and sublevel to make unique name
MT.Out[,T.Name2:=paste0(c(T.Name,Herd.Sublevel.Name),collapse = "||"),by=.(T.Name,Herd.Sublevel.Name)][is.na(T.Name2)]

results<-validator(data=MT.Out,
                   trim_ws = T,
                   unique_cols = c(T.Name2="T.Name2"),
                   check_keyfields=keyfields,
                   compulsory_cols = c(T.Name="Herd.Level.Name"),
                   duplicate_field = "T.Name",
                   duplicate_ignore_fields = c("T.Name"),
                   rm_duplicates=F,
                   tabname=table_name)

error_dat<-results$errors
errors<-c(errors,list(error_dat))

MT.Out<-results$data

# Create vector of combined names for T.Name and subherd
t_levels<-c(MT.Out$T.Name,MT.Out[!is.na(T.Name2),T.Name2])

# TO DO!!!! Add check for 1 herd per treatment ####

  # 4.1) TO DO !!! Merge in practice data #####
  if(F){
  unique(MT.Out[grepl("[.][.]",P.Product) & !grepl("[.][.]",T.Name),.(B.Code,P.Product,T.Name)])
  # Create list of data table to merge with MT.Out treatment table
  mergedat<-list(V.Level.Name=copy(Var.Out),
                 AF.Level.Name=AF.Out,
                 C.Level.Name=Chems.Code,
                 A.Level.Name=Animals.Out,
                 O.Level.Name=Other.Out)
  
  data<-MT.Out
  for(i in 1:length(mergedat)){
    keyfield<-names(mergedat)[i]
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("Merging table", i, "/", length(mergedat),keyfield)
    flush.console()
    
    if(keyfield=="V.Level.Name"){
      data<-merge(data,mergedat[[i]],
                  by.x=c("B.Code","P.Product",keyfield),
                  by.y=c("B.Code","V.Product",keyfield),
                  all.x=T,
                  sort=F)
      data[,V.Level.Name:=V.Level.Name_new][,V.Level.Name_new:=NULL]
      
    }else{
      data<-merge(data,mergedat[[i]],by=c("B.Code",keyfield),all.x=T)
    }
    
    
    if(nrow(data)!=nrow(MT.Out)){
      cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
    }
  }
  
  # Update Var.Out now that merge has been done with MT.Out table
  Var.Out[,V.Level.Name_raw:=V.Level.Name][,V.Level.Name:=V.Level.Name_new][,V.Level.Name_new:=NULL]

  # Add in Base.Out
  data<-merge(data,Base.Out,by="B.Code",all.x=T)
  if(nrow(data)!=nrow(MT.Out)){
    cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
  }
  
  MT.Out<-data
  }
  
  # 4.2) TO DO !!! Combine practice codes to create T.Codes ######
  if(F){
  code_cols<-c(AF.Level.Name="AF.Codes",
               A.Level.Name="A.Codes",
               V.Level.Name="V.Codes")
  
  t_codes<-apply(MT.Out[,..code_cols],1,FUN=function(x){
    x<-paste(sort(unique(unlist(strsplit(x,"-")))),collapse="-")
    x[x==""]<-NA
    x
  })
  
  MT.Out[,T.Codes:=t_codes]
  }
  # 4.3) MOVE TO ENTERDATA? Combine aggregated treatments #####
if(F){
  N<-grep("[.][.]",MT.Out$T.Name)
  
  Fields<-data.table(Levels=c(names(code_cols),"O.Level.Name","C.Level.Name"),
                     Codes =c(code_cols,NA,NA))
  
  results<-pblapply(N,FUN=function(i){
    if(F){
      # Display progress
      cat('\r', strrep(' ', 150), '\r')
      cat("Processing row", i)
      flush.console()
    }
    # Deal with ".." delim used in Fert tab and Varieties tab that matches ".." delim used to aggregate treatments in MT.Out tab
    # Above should not be required anyone as Var delim changed to "$$" and combined fertilizers disaggregated.
    
    t_name<-MT.Out$T.Name[i] 
    Trts<-unlist(strsplit(t_name,"[.][.]"))
    Study<-MT.Out[i,B.Code]
    
    Y<-MT.Out[T.Name %in% Trts & B.Code == Study]
    
    if(nrow(Y)==length(Trts)){
      # Aggregated Treatments: Split T.Codes & Level.Names into those that are the same and those that differ between treatments
      # This might need some more nuance for fertilizer treatments?
      
      # Exclude Other, Chemical, Weeding or Tilling Practice Levels if they do no structure outcomes.
      Exclude<-c("O.Level.Name","C.Level.Name")[apply(Y[,c("O.Structure","C.Structure")],2,unique)!="Yes" | is.na(apply(Y[,c("O.Structure","C.Structure")],2,unique))]
      Fields1<-Fields[!Levels %in% Exclude]
      
      
      COLS<-Fields1$Levels
      Levels<-apply(Y[,..COLS],2,FUN=function(X){
        X[as.vector(is.na(X))]<-""
        length(unique(X))>1
      })
      
      Agg.Levels<-paste0(COLS[Levels],collapse = "-")
      
      COLS<-COLS[Levels]
      
      CODES.IN<-Fields1$Codes[Levels]
      CODES.IN<-CODES.IN[!is.na(CODES.IN)]
      CODES.IN<-apply(Y[,..CODES.IN],1,FUN=function(X){
        X<-X[!is.na(X)]
        X<-X[order(X)]
        if(length(X)==0){"NA"}else{paste(X,collapse="-")}
      })
      
      CODES.OUT<-Fields1$Codes[!Levels]
      CODES.OUT<-CODES.OUT[!is.na(CODES.OUT)]
      CODES.OUT<-apply(Y[,..CODES.OUT],2,FUN=function(X){
        X<-unlist(X)
        X<-unique(X[!is.na(X)]) 
        X<-X[order(X)]
        if(length(X)>1){ # these codes cannot vary
          "ERROR"
        }else{
          if(length(X)==0){
            NA
          }else{
            if(X==""){NA}else{X}
          }
        }}) 
      CODES.OUT<-CODES.OUT[!is.na(CODES.OUT)]
      
      if(length(CODES.OUT)==0){CODES.OUT<-NA}else{CODES.OUT<-paste(CODES.OUT[order(CODES.OUT)],collapse="-")}
      if(length(CODES.IN)==0){CODES.IN<-NA}else{
        CODES.IN<-CODES.IN[order(CODES.IN)]
        CODES.IN<-paste0(CODES.IN,collapse="...")
      }
      
      # Collapse into a single row using "..." delim to indicate a treatment aggregation
      Y<-apply(Y,2,FUN=function(X){
        X<-unlist(X)
        Z<-unique(X)
        if(length(Z)==1 | length(Z)==0){
          if(Z=="NA" | is.na(Z) | length(Z)==0){
            NA
          }else{
            Z
          }
        }else{
          X<-paste0(X,collapse = "...")
          if(X=="NA"){
            NA
          }else{
            X
          }
        }
      })
      
      Y<-data.table(t(data.frame(list(Y))))
      
      # Do not combine the Treatment names, keep this consistent with Enter.Data tab
      Y$T.Agg.Levels<-Agg.Levels
      Y$T.Codes.No.Agg<-CODES.OUT
      Y$T.Codes.Agg<-CODES.IN
      
      # Retain order of or
      Y$T.Name<-gsub("[.][.]","...",t_name)
      
      list(data=Y)
    }else{
      error<-data.table(B.Code=Study,
                        value=MT.Out[i,T.Name],
                        table="MT.Out",
                        field="T.Name",
                        issue="One or more component names of aggregated treatment name are missing.")  
      list(error=error)
    }
    
    
  })
  
  error_dat<-rbindlist(lapply(results,"[[","error"))
  error_dat<-error_dat[,list(value=paste0(value,collapse = "/")),by=.(B.Code,table,field,issue)]
  errors<-c(errors,list(error_dat))
  
  MT.Out.agg<-rbindlist(lapply(results,"[[","data"))
  
  # Update final treatment codes (T.Codes) for aggregrated treatments
  MT.Out.agg[,T.Codes:=T.Codes.No.Agg]
  
  T.Agg.Levels_name<-unlist(pblapply(1:nrow(MT.Out.agg),FUN=function(i){
    x_cols<-MT.Out.agg[i,unlist(strsplit(T.Agg.Levels,"-"))]
    paste(unlist(MT.Out.agg[i,..x_cols]),collapse = "-")
    }))
  
  MT.Out.agg[,T.Agg.Levels_name:=T.Agg.Levels_name]
  
  error_dat<-MT.Out.agg[is.na(T.Agg.Levels)|T.Agg.Levels=="",.(value=paste0(T.Name,collapse = "/")),by=B.Code
                        ][,table:=table_name
                          ][,field:="T.Name"
                            ][,issue:="Aggregated treatments appear to be not aggregated across any named practice level. Typically this is male/female or age stage groupings, which is acceptable. Check if names indicate the aggregation should be across practice levels indicating a issue. Errors can occur if structuring practices are not correctly defined."]
  errors<-c(errors,list(error_dat))
  
  MT.Out.noagg<-MT.Out[-N]
  MT.Out.noagg[,c("T.Agg.Levels","T.Agg.Levels_name","T.Codes.No.Agg","T.Codes.Agg"):=NA]
  
  MT.Out<-rbind(MT.Out.agg,MT.Out.noagg)
}
  
  # 4.4) MOVE TO ENTERDATA? Update structure for aggregated treatments ####
  if(F){
  col_names<-grep("Structure",colnames(MT.Out),value=T)
  MT.Out <- MT.Out[, (col_names) := lapply(.SD, FUN=function(x){
    x[grepl("Yes",x,ignore.case = T)]<-"Yes"
    x[!grepl("Yes",x,ignore.case = T)]<-NA
    x
  }), .SDcols = col_names]
  }
  
  # TO DO ADD BASE PRACTICE DATA? #####
# 5) Outcomes ####
table_name<-"Out.Out"
Out.Out<-excel_dat[[table_name]][,1:12]
template_cols<-c(master_template_cols[[table_name]][1:12],"B.Code")

# Merge feed intake unit with regular units
Out.Out[Out.Subind=="Feed Intake",Out.Unit:=Out.FI.Unit]

# Remove NA rows
Out.Out<-Out.Out[!rowSums(is.na(Out.Out)) == ncol(Out.Out)]

# Add publication code
Out.Out$B.Code<-Pub.Out$B.Code

# Allowed values
allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field=="Out.FI.Unit",Values_New]),
                           parent_tab_name=c("master_Codes$lookup_levels"),
                           field=c("Out.FI.Unit"))

results<-validator(data=Out.Out,
                   allowed_values = allowed_values,
                   template_cols = template_cols,
                   numeric_cols=c("Out.Depth.Upper","Out.Depth.Lower","Out.NPV.Rate","Out.NPV.Time"),
                   hilo_pairs = data.table(low_col="Out.Depth.Upper",high_col="Out.Depth.Lower",name_field="Out.Code.Joined"),
                   compulsory_cols = c(Out.Code.Joined="Out.Unit"),
                   unique_cols = "Out.Code.Joined",
                   duplicate_field = "Out.Code.Joined",
                   rm_duplicates = T,
                   trim_ws = T,
                   do_site = F,
                   do_time = F,
                   tabname=table_name)

error_dat<-results$errors

# Remove unit errors for ratio outcomes
error_dat<-error_dat[issue=="Missing value in compulsory field Out.Unit.",value:=paste0(unlist(strsplit(value,"/"))[!grepl("Ratio",unlist(strsplit(value,"/")))],collapse = "/")][value!=""]

errors<-c(errors,list(error_dat))

Out.Out<-results$data

Out.Out[,Out.FI.Unit:=NULL]

error_dat<-Out.Out[grepl("Meat Yield",Out.Subind) & is.na(Out.WG.Days),.(value=paste(Out.Code.Joined,collapse = "/")),by=B.Code
                                                                                ][,table:=table_name
                                                                                  ][,field:="Out.Code.Joined"
                                                                                    ][,issue:="Meat yield outcome without experimental duration."]
errors<-c(errors,list(error_dat))

# TO DO: Check outcome names match master codes (these are now AOM outcomes) ####
if(F){
  out_name_changes<-data.table(old_values=c("Nitrogen [(]Apparent Efficiency[)]","Carbon dioxide emissions","Aboveground Biomass"),
                               new_values=c('Nitrogen (Apparent Efficiency Animals Feed)',"Carbon Dioxide Emissions","Aboveground Carbon Biomass"))
  
  parent<-master_codes$out[,list(Subindicator)][,Out.Subind:=Subindicator]
  error_dat<-Out.Out[!Out.Out$Out.Subind %in% parent$Out.Subind & !Out.Subind %in% out_name_changes$old_values
  ][,value:=Out.Subind
  ][,list(B.Code=paste(B.Code,collapse = "/")),by=value
  ][,table:="Out.Out"
  ][,field:="Out.Subind"
  ][,issue:="Outcome not found in master codes."]
  
  errors<-c(errors,list(error_dat))
}

  # 5.1) Out.Econ #####
  table_name<-"Out.Out"
  Out.Econ<-excel_dat[[table_name]][,13:20]
  template_cols<-c(master_template_cols[[table_name]],"B.Code")[12:20]
  
  table_name<-"Out.Econ"
  
  # Remove NA rows
  Out.Econ<-Out.Econ[!rowSums(is.na(Out.Econ)) == ncol(Out.Econ)]
  
  # Add publication code
  Out.Econ$B.Code<-Pub.Out$B.Code
  

  # 5.2) Harmonize Feed Intake Units ####
  intake_units<-unique(master_codes$lookup_levels[Field=="Out.FI.Unit" & !is.na(Values_Old),.(Values_Old,Values_New)
                                           ][,.(Out.Unit=unlist(strsplit(Values_Old,";"))),by=Values_New])

  Out.Out<-merge(Out.Out,intake_units,by="Out.Unit",all.x=T,sort=F)

  Out.Out[Out.Subind=="Feed Intake" & !is.na(Values_New),Out.Unit:=Values_New][,c("Values_New","N"):=NULL]  
    
# 6) Time Sequence ####
  # 6.1) TS.Out #####
  table_name<-"Rot.Out"
  TS.Out<-excel_dat[[table_name]][,1:7]
  template_cols<-c(master_template_cols[[table_name]][1:7],"B.Code")
  TS.Out$B.Code<-Pub.Out$B.Code
  table_name<-"TS.Out"
  
  colnames(TS.Out)<-unlist(tstrsplit(colnames(TS.Out),"[.][.][.]",keep=1))
  template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
  # Remove empty rows
  TS.Out<-TS.Out[!is.na(R.Level.Name)]
  
  colnames(TS.Out)[1]<-"R.Prac"
  template_cols[1]<-"R.Prac"
  
  # Allowed values
  vals<-c("Yes","No","Unspecified")
  allowed_values<-data.table(allowed_values=list(vals,vals),
                             parent_tab_name=c(NA,NA),
                             field=c("R.Control","R.Phases"))
  
  results<-validator(data=TS.Out,
                     allowed_values = allowed_values,
                     numeric_cols=c("R.Reps","Start.Year"),
                     compulsory_cols = c(R.Level.Name="R.Prac"),
                     unique_cols = "R.Level.Name",
                     trim_ws = T,
                     template_cols = template_cols,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  TS.Out<-results$data
  
  # 6.2) TS.Seq #####
  table_name<-"Rot.Out"
  TS.Seq<-excel_dat[[table_name]][,9:11]
  template_cols<-c(master_template_cols[[table_name]][9:11],"B.Code")
  TS.Seq$B.Code<-Pub.Out$B.Code
  table_name<-"TS.Seq"
  
  colnames(TS.Seq)<-unlist(tstrsplit(colnames(TS.Seq),"[.][.][.]",keep=1))
  template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))
  
  setnames(TS.Seq,c("M.Year","R.Treatment "),c("Time","T.Name"),skip_absent = T)
  template_cols[template_cols=="R.Treatment"]<-"T.Name"
  
  TS.Seq<-TS.Seq[!is.na(R.Level.Name)]
  
  results<-validator(data=TS.Seq,
                     compulsory_cols = c(R.Level.Name="Time",R.Level.Name="T.Name"),
                     unique_cols = "R.Level.Name",
                     check_keyfields=data.table(parent_tab=list(MT.Out,TS.Out),
                                                parent_tab_name=c("MT.Out","TS.Out"),
                                                keyfield=c("T.Name","R.Level.Name")),
                     time_data = Times.Out,
                     do_site = FALSE,
                     trim_ws = T,
                     template_cols = template_cols,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  TS.Seq<-results$data
  
  # 6.1) TO DO!!! - build rotation data #####
  
# 7) Enter Data (Data.Out) ####
  table_name<-"Data.Out"
  Data.Out<-excel_dat[[table_name]][,-12]

  # Omit all NA rows
  remove_rows<-rowSums(is.na(Data.Out)) == ncol(Data.Out)
  Data.Out[,row_index:=1:.N]
  Data.Out <- Data.Out[!remove_rows]
  Data.Out$B.Code<-Pub.Out$B.Code

  # 7.2) Validator #####
  setnames(Data.Out,
           c("ED.Site.ID","ED.M.Year","ED.Treatment","ED.Rot","ED.Outcome","ED.Animlas"),
           c("Site.ID","Time","T.Name","R.Level.Name","Out.Code.Joined","ED.Animals"),
           skip_absent = T)
  
  # Bug fix for older versions of sheet - replace ";" delim used to join T.Name and herd subgroup with "||"
  Data.Out[,T.Name:=stringi::stri_replace_all_fixed(str=T.Name, pattern =  gsub("[|][|]",";",t_levels), replacement=t_levels, vectorize_all = FALSE)]
  
  # Add feed intake flag
  Data.Out[grepl("Feed Intake",Out.Code.Joined),Feed.Intake:=T]
  
  # Create keyfield mappings
  keyfields<-data.table(parent_tab=list(Site.Out,Times.Out,TS.Out,Out.Out),
             parent_tab_name=c("Site.Out","Times.Out","TS.Out","Out.Out"),
             keyfield=c("Site.ID","Time","R.Level.Name","Out.Code.Joined"))
  
  # Allowed values
  allowed_values<-data.table(allowed_values=list(master_codes$lookup_levels[Field == "Error.Type",Values_New],
                                                 t_levels),
                           parent_tab_name=c("master_codes$lookup_levels",
                                             "MT.Out"),
                           field=c("ED.Error.Type",
                                   "T.Name"))
  

  results<-validator(data=Data.Out,
                     compulsory_cols = c(row_index="T.Name",row_index="Site.ID",row_index="Out.Code.Joined",
                                         row_index="ED.Mean.T",row_index="ED.Data.Loc"),
                     numeric_cols = c("ED.Mean.T","ED.Error","ED.Reps","ED.Sample.DAS","ED.Animals","ED.Start.Year"),
                     date_cols = c("ED.Sample.Start","ED.Sample.End"),
                     valid_start = valid_start,
                     valid_end = valid_end,
                     hilo_pairs = data.table(low_col="ED.Sample.Start",high_col="ED.Sample.End",name_field="row_index"),
                     check_keyfields=keyfields,
                     allowed_values=allowed_values,
                     unit_pairs = data.table(unit=c("ED.Error.Type","ED.Intake.Item"),var=c("ED.Error","Feed.Intake"),name_field="row_index"),
                     trim_ws = T,
                     do_site = F,
                     do_time = F,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Data.Out<-results$data
  Data.Out[,row_index:=NULL]
  
    # TO DO!!! Update Site.ID ####
  if(F){
  Data.Out[,Site.ID_new:=Site.Out$Site.ID[match(gsub("[.][.] | [.][.]|  [.][.]|   [.][.]","..",trimws(tolower(Data.Out$Site.ID))),tolower(Site.Out$Site.ID_raw))]] 
  Data.Out[is.na(Site.ID_new),Site.ID_new:=Site.ID][,Site.ID:=Site.ID_new][,Site.ID_new:=NULL]
  }
  
  # Update error value used
  Data.Out[ED.Error.Type=="SEM (Standard Error of the Mean)",ED.Error.Type:="SE (Standard Error)"]

  #Check for ratio outcomes that are missing the comparison "control" 
  error_dat<-Data.Out[grepl(master_codes$out[TC.Ratio=="Y",paste0(Subindicator,collapse = "|")],Out.Code.Joined) & is.na(ED.Comparison1)
  ][,list(value=paste(unique(Out.Code.Joined),collapse = "/")),by=B.Code
  ][,table:="Data.Out"
  ][,field:="Out.Code.Joined"
  ][,issue:="Outcome derived from T vs C (e.g. LER) does not have comparison specified."
  ][order(B.Code)]
  errors<-c(errors,list(error_dat))
  
  h_task<-list(harmonizer(data=Data.Out, 
                       master_codes,
                       master_tab="lookup_levels",
                       h_table="Data.Out", 
                       h_field="ED.Error.Type",
                       h_table_alt="Data.Out", 
                       h_field_alt="Error.Type")$h_tasks[,issue:="Non-standard error value used."][value!="Unspecified"])
  
  h_tasks<-c(h_tasks,list(h_task))
  
  # 7.3) Merge data from linked tables ####
  n_rows<-Data.Out[,.N]
    # 7.3.1) !!! TO DO!!! Merge MT.Out ######
    # !!!!Needs to accommodate aggregated treatments #### 
    if(F){
    Data.Out<-merge(Data.Out,unique(MT.Out),by=c("B.Code","T.Name"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    }
    # 7.3.2) Merge Publication  ######
  Data.Out<-merge(Data.Out,Pub.Out,by=c("B.Code"),all.x=T,sort=F)
  stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    # 7.3.3) Merge Outcomes  ####
    # Temporary fix to deal with duplicated Out.Code.Joined, should be resolved when Out.WG.Days is included
    Out.Out<-Out.Out[!duplicated(Out.Out[,.(B.Code,Out.Code.Joined)])]
    
    Data.Out<-merge(Data.Out,unique(Out.Out),by=c("B.Code","Out.Code.Joined"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      # 7.3.3.1) Merge in outcome codes  #######
    merge_dat<-setnames(unique(master_codes$out[,.(Subindicator,Code)]),c("Subindicator","Code"),c("Out.Subind","Out.Code"))
    merge_dat[,N:=.N,by=Out.Subind][N>1]
    merge_dat[,N:=NULL]
    Data.Out<-merge(Data.Out,merge_dat,by=c("Out.Subind"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    # 7.3.4) !!TO DO!!! Add Product Codes ######
    # Needs herd tab to be merged with MT.Out tab #####
    if(F){
    # Add component level 1 to Data.Out (used to match component + product to EU list)
    comp_codes<-master_codes$prod_comp
    prod_master<-master_codes$prod
    
    X<-Data.Out[,c("B.Code","P.Product","ED.Product.Comp","Out.Subind")
    ][,EU.Comp.L1:=comp_codes$Comp.Level1[match(ED.Product.Comp,comp_codes$Component)]]
    
    # Add code field based on product x component combined to ERA data and product (EU) MASTERCODES
    X[,Code:=paste(P.Product,EU.Comp.L1)]
    prod_master[,Code:=paste(Product.Simple,Component)]
    
    # Check product components that do not match component column in EU.Comp tab of MASTERCODES
    error_dat<-unique(X[is.na(EU.Comp.L1)& (grepl("Yield",Out.Subind)|grepl("Efficiency",Out.Subind)) & !is.na(ED.Product.Comp) & !grepl("[.][.]",P.Product)])
    
    # Validation: Check Product + Component Combinations Not in EU2 tab of MASTERCODES
    error_dat<-unique(X[!Code %in% prod_master$Code & 
                          P.Product %in% prod_master$Product.Simple &
                          !is.na(EU.Comp.L1) & 
                          !is.na(P.Product) &
                          !is.na(ED.Product.Comp) & 
                          !grepl("Efficiency",Out.Subind) &
                          !Out.Subind %in% c("Biomass Yield","Aboveground Carbon Biomass","Aboveground Biomass","Belowground Biomass","Crop Residue Yield") & !grepl("[.][.]",P.Product)])
    
    error_dat<-error_dat[,.(value=paste(P.Product,"-",ED.Product.Comp)),by=B.Code
    ][,.(value=paste(value,collapse="/")),by=B.Code
    ][,table:="Data.Out"
    ][,field:="P.Product-ED.Product.Comp"
    ][,issue:="Weird product + component combination, given outcome."]
    
    errors<-c(errors,list(error_dat))
    }
    
     # 7.3.4.1) Merge using product x component code  #######
    if(F){
      # NOTE DOES NOT DEAL WITH AGGREGATE PRODUCTS #####
    merge_prods<-function(P.Product,Component,master_prods,master_prod_codes,prod_tab,tree_tab){
      prods<-unlist(strsplit(P.Product,"-"))
      if(!is.na(Component)){
        prods_comp<-paste(prods,Component)
        N<-match(prods_comp,master_prod_codes)
        N[is.na(N)]<-match(prods[is.na(N)],master_prods)
      }else{
        N<-match(prods,master_prods)
      }
      
      if(any(is.na(N))){
        
      }
      
      p_cols<-c("EU","Product.Type","Product.Subtype","Product","Product.Simple","Latin.Name")
      result<-prod_tab[N,..p_cols]
      
      result <- result[, lapply(.SD, function(x) {
        paste(x, collapse = "**")
      }), .SDcols = p_cols]
      
      return(result)
    }
    
    Data.Out[,ED.Product.Comp.L1:=comp_codes$Comp.Level1[match(ED.Product.Comp,comp_codes$Component)]]
    dat<-unique(Data.Out[!is.na(P.Product),.(P.Product,ED.Product.Comp.L1)])
    
    master_prods<-master_codes$prod$Product.Simple
    master_prod_codes<-master_codes$prod[,paste(Product.Simple,Component)]
    prod_tab<-master_codes$prod
    tree_tab<-master_codes$trees
    
    merge_dat<-rbindlist(pblapply(1:nrow(dat),FUN=function(i){
      merge_prods(P.Product=dat$P.Product[i],
                  Component=dat$ED.Product.Comp.L1[i],
                  master_prods=master_prods,
                  master_prod_codes=master_prod_codes,
                  prod_tab=prod_tab)[,P.Product:=dat$P.Product[i]
                  ][,ED.Product.Comp.L1:=dat$ED.Product.Comp.L1[i]]
    }))[order(P.Product)]
    
    Data.Out<-merge(Data.Out,merge_dat,by=c("P.Product","ED.Product.Comp.L1"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    
    # Check NA & "No Product Specified" against outcome (should not be a productivity outcome)
    error_dat<-unique(Data.Out[is.na(P.Product) | P.Product == "No Product Specified",c("P.Product","B.Code","T.Name","Out.Subind")]
                      [grep("Yield",Out.Subind)])
    }
  
    # 7.3.5) Merge Experimental Design  ######
  Data.Out<-merge(Data.Out,ExpD.Out,by=c("B.Code"),all.x=T,sort=F)
  stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    # 7.3.6) Merge Site  ######
    Data.Out<-merge(Data.Out,unique(Site.Out[,!c("Site.ID_raw","check")]),by=c("B.Code","Site.ID"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    # Make Sure of match between Data.Out and Site.Out
    (error_dat<-unique(Data.Out[is.na(Country),.(B.Code,Site.ID)])[!B.Code %in% errors$site_mismatches$B.Code])
    
    # 7.3.7) !!TO DO !!!Update Structure Fields to reflect Level name rather than "Yes" or "No" ####
    if(F){
      grep("[.]Structure$",colnames(Data.Out),value=T)
      Data.Out[O.Structure!="Yes",O.Structure:=NA][O.Structure=="Yes",O.Structure:=O.Level.Name]
      Data.Out[C.Structure!="Yes",C.Structure:=NA][C.Structure=="Yes",C.Structure:=C.Level.Name]
    }
    
    # 8) Save results ####
    Tables<-list(
      Pub.Out=Pub.Out, 
      Site.Out=Site.Out, 
      Soil.Out=Soil.Out,
      ExpD.Out=ExpD.Out,
      System.Out=System.Out,
      Times.Out=Times.Out,
      Herd.Out=Herd.Out,
      Chems.Code=Chems.Code,
      Chems.Out=Chems.Out,
      Chems.AI=Chems.AI,
      AF.Out=AF.Out,
      AF.Trees=AF.Trees,
      Animals.Out=Animals.Out,
      Animal.Diet=Animal.Diet,
      Animal.Diet.Comp=Animal.Diet.Comp,
      Animal.Diet.Digest=Animal.Diet.Digest,
      Pasture.Out=Pasture.Out,
      Pasture.Comp=Pasture.Comp,
      GM.Out=GM.Out,
      GM.Method=GM.Method,
      Till.Out=Till.Out,
      Till.Method=Till.Method,
      Fert.Out=Fert.Out,
      Fert.Method=Fert.Method,
      Fert.Comp=Fert.Comp,
      Other.Out=Other.Out,
      MT.Out=MT.Out,
      Out.Out=Out.Out,
      Out.Econ=Out.Econ,
      TS.Out=TS.Out,
      TS.Seq=TS.Seq,
      Data.Out=Data.Out,
      Base.Out=Base.Out
    )
    
    save(Tables,file=save_file)
    
# 9) Save errors #####
    errors<-rbindlist(errors,use.names = T)
    errors<-error_tracker(errors=errors,
                  filename =filename_new,
                  error_dir=dirname(File),
                  error_list = NULL)

    return(if(length(errors)==0){NULL}else{errors[[1]]})
  }else{
      if(file.exists(filepath_new)){
      return(fread(filepath_new))
      }else{
        return(NULL)
      }
  }
    
  })

})
 
# 10) Compile errors ####

errors<-rbindlist(results)
errors<-merge(errors,excel_files[,.(filename,era_code2)],by.x="B.Code",by.y="era_code2",all.x=T,sort=F)[,filename:=basename(filename)]
fwrite(errors,file.path(excel_dir,"compiled_auto_errors.csv"),bom=T)

# 11) Compile saved tables ####

files<-list.files(extracted_dir,"RData$",full.names = T)

output<-basename(files)
input<-gsub("[.]xlsm",".RData",basename(excel_files$filename))
missing<-input[!input %in% output]
which_missing<-which(!input %in% output)

if(length(missing)>0){
  stop(length(missing)," excel files have not been extracted and saved")
}

tabs<-names(miceadds::load.Rdata2(basename(files[1]),path=dirname(files[1])))

# Soils need to be dealt with separately as the table structure needs to be changed from wide to long
tabs<-tabs[tabs != "Soil.Out"]

tabs<-tabs[tabs != "Base.Out"]

data_list<-lapply(files,FUN=function(file){
  miceadds::load.Rdata2(basename(file),path=dirname(file))
                        })

data<-lapply(tabs,FUN=function(tab){
  cat(tab,"\n")
  rbindlist(lapply(data_list,"[[",tab),fill = T,use.names = T)
})

names(data)<-tabs
data$Animal.Diet.Comp[,`0`:=NULL]
data$Animal.Diet.Digest[,`0`:=NULL]

# 12) Check & harmonize feed item names ####

  Animal.Diet<-data$Animal.Diet
  Animals.Out<-data$Animals.Out
  Animal.Diet.Comp<-data$Animal.Diet.Comp
  Animal.Diet.Digest<-data$Animal.Diet.Digest
  Data.Out<-data$Data.Out
  MT.Out<-data$MT.Out
  
  # Correct any ";" delimiters to "||"
  Animal.Diet[grep(";",D.ItemxProcess),D.ItemxProcess]
  
  
  # 12.0) Check integrity of key fields ####
  Animal.Diet_key<-unique(Animal.Diet[,.(B.Code,D.ItemxProcess)])[,check:=T]
  A.Level.Name_key<-Animals.Out[,paste(B.Code,A.Level.Name),by=.I][,unique(V1)]
  Animal.Diet_groupkey<-unique(Animal.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)])[,check2:=T]
  
    # 12.0.1) Animal.Diet.Comp ####
    
    # Add group flag for ; separated values
    Animal.Diet.Comp[grep(";",D.Item),is_group:=T]
  
    # Update entire diet flag
    Animal.Diet.Comp[,is_entire_diet:=F
                     ][,key:=paste(B.Code,D.Item),by=.I
                       ][key %in% A.Level.Name_key,is_entire_diet:=T
                           ][,key:=NULL]
    
    error_dat<-Animal.Diet.Comp[is_entire_diet==F,.(D.Item=unlist(strsplit(D.Item,";"))),by=.(B.Code)]
    error_dat<-merge(error_dat,Animal.Diet_key,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.ItemxProcess"),all.x=T,sort=F)
    error_dat<-merge(error_dat,Animal.Diet_groupkey,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.Item.Group"),all.x=T,sort=F)
    error_dat[check2==T,check:=T][,check2:=NULL]
    
    error_dat<-error_dat[is.na(check),.(value=paste(sort(unique(D.Item)),collapse=" --- ")),by=B.Code]
    
    error_dat<-error_dat[,table:="Animal.Diet.Comp (Nutrition)"
    ][,field:="D.Item (multiple items shown by ---)"
    ][,issue:="No match to Ingredients tab D.Item x Processes (D.ItemxProcess)"
      ][order(B.Code)]
    
    errors<-list(error_dat)
    
    # 12.0.2) Animal.Diet.Digest ####
    
    # Add group flag
    Animal.Diet.Digest[grep(";",D.Item),is_group:=T]
    
    # Update entire diet flag
    Animal.Diet.Digest[,is_entire_diet:=F
                       ][,key:=paste(B.Code,D.Item),by=.I
                         ][key %in% A.Level.Name_key,is_entire_diet:=T
                             ][,key:=NULL]
    
    error_dat<-Animal.Diet.Digest[is_entire_diet==F,.(D.Item=unlist(strsplit(D.Item,";"))),by=.(B.Code)]
    error_dat<-merge(error_dat,Animal.Diet_key,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.ItemxProcess"),all.x=T,sort=F)
    error_dat<-merge(error_dat,Animal.Diet_groupkey,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.Item.Group"),all.x=T,sort=F)
    error_dat[check2==T,check:=T][,check2:=NULL]
    
    error_dat<-error_dat[is.na(check),.(value=paste(sort(unique(D.Item)),collapse=" --- ")),by=B.Code]
    
    error_dat<-error_dat[,table:="Animal.Diet.Digest (Digestibility)"
    ][,field:="D.Item (multiple items shown by ---)"
    ][,issue:="No match to Ingredients tab D.Item x Processes (D.ItemxProcess)"
    ][order(B.Code)]
    
    errors<-c(errors,list(error_dat))
    
    # 12.0.3) Data.Out: Feed Intake Item ####
    
    # Add group flag
    Data.Out[,ED.Intake.Item.is_group:=F][grep(";",ED.Intake.Item),ED.Intake.Item.is_group:=T]
    
    # Update entire diet flag
    Data.Out[,ED.Intake.Item.is_entire_diet:=F
             ][ED.Intake.Item=="Entire Diet",ED.Intake.Item.is_entire_diet:=T]
    
    error_dat<-Data.Out[ED.Intake.Item.is_entire_diet==F & !is.na(ED.Intake.Item),.(D.Item=unlist(strsplit(ED.Intake.Item,";"))),by=B.Code]
    error_dat<-merge(error_dat,Animal.Diet_key,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.ItemxProcess"),all.x=T,sort=F)
    error_dat<-merge(error_dat,Animal.Diet_groupkey,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.Item.Group"),all.x=T,sort=F)
    error_dat[check2==T,check:=T][,check2:=NULL]
    
    error_dat<-error_dat[is.na(check),.(value=paste(sort(unique(D.Item)),collapse=" --- ")),by=B.Code]
    
    error_dat<-error_dat[,table:="Data.Out (EnterData)"
    ][,field:="ED.Intake.Item (multiple items split by ---)"
    ][,issue:="No match to Ingredients tab D.Item x Processes (D.ItemxProcess)"
    ][order(B.Code)]
    
    errors<-c(errors,list(error_dat))
    
    if(nrow(error_dat)>0){
      error_tracker(rbindlist(errors)[order(B.Code)],filename = "Diet Item does not have a match",error_dir = error_dir)
    }
    
    # 12.0.4) Animal Diet
    # Update entire diet flag
    
    Animal.Diet[,is_entire_diet:=F
                ][,key:=paste(B.Code,D.Item),by=.I
                  ][key %in% A.Level.Name_key,is_entire_diet:=T
                    ][,key:=NULL
                      ]
    
    Animal.Diet<-merge(Animal.Diet,Animal.Diet_groupkey,by.x=c("B.Code","D.Item"),by.y=c("B.Code","D.Item.Group"),all.x=T,sort=F)
    Animal.Diet[,is_group:=F
                ][!is.na(check2),is_group:=T
                          ][,D.Is.Group:=NULL
                            ][,check2:=NULL]
    
  # 12.1) Animal Diet Harmonization ####
table_name<-"Ingredients.Out"

    # 12.1.1) Harmonize Units/Methods ####
      # 12.1.1.1) Animal.Diet ####
      h_params<-data.table(h_table="Animals.Diet",
                           h_field=c("D.Unit.Amount","D.Unit.Time","D.Unit.Animals"),
                           ignore_vals=c("unspecified","unspecified","unspecified"))[,c("h_field_alt","h_table_alt"):=NA]
      
      
      results<-harmonizer_wrap(data=Animal.Diet,
                               h_params=h_params,
                               master_codes = master_codes)
      
      Animal.Diet<-results$data
      h_tasks<-list(results$h_tasks)
      
      # 12.1.1.2) Animal.Diet.Digest ####
      table_name<-"Animal.Diet.Digest"
      # Units
      h_params<-data.table(h_table=table_name,
                           h_field=c("DD.Unit","DD.Method"),
                           h_field_alt=c("DC.Unit","DD.Method"),
                           h_table_alt=c("Animals.Diet.Comp","Animals.Diet.Digest"),
                           ignore_vals=rep("unspecified",2))
      
      results<-harmonizer_wrap(data=Animal.Diet.Digest,
                               h_params=h_params,
                               master_codes = master_codes)
      
      h_tasks<-c(h_tasks,list(results$h_tasks))
  
      Animal.Diet.Digest<-results$data
      
      # 12.1.1.3) Animal.Diet.Comp ####
      table_name<-"Animal.Diet.Comp"
      # Units
      h_params<-data.table(h_table=table_name,
                           h_field=c("DN.Unit"),
                           h_field_alt=c("DC.Unit"),
                           h_table_alt=c("Animals.Diet.Comp"),
                           ignore_vals=rep("unspecified",1))
      
      results<-harmonizer_wrap(data=Animal.Diet.Comp,
                               h_params=h_params,
                               master_codes = master_codes)
      
      h_tasks<-c(h_tasks,list(results$h_tasks))
  
      Animal.Diet.Comp<-results$data
      
      error_tracker(rbindlist(h_tasks)[order(B.Code)],filename = "Diet Method and Unit Harmonization",error_dir = error_dir)
      
    # 12.1.2) Animal.Diet/Diet Items ####
      # 12.1.2.1) Harmonize Processes ####
      
      # Update process names to match AOM/Ani_Diet
      aom<-master_codes$AOM
      aom<-aom[grep("Feed Process",Path)][!is.na(Ani_Diet_Spp_Syn)
                                          ][,.(Edge_Value,Ani_Diet_Spp_Syn)
                                            ][,major:=T
                                              ][grep("minor",Ani_Diet_Spp_Syn),major:=F
                                                ][,Ani_Diet_Spp_Syn:=unlist(tstrsplit(Ani_Diet_Spp_Syn,"_",keep=1))]
      
      colnames(aom)[1:2]<-c("old","new")
      
    
      # Order replacements by length of 'old' (descending)
      aom<-aom[order(nchar(old),decreasing = T)]
      
      
      # Check processes are present in aom
      process_cols<-grep("D.Process",colnames(Animal.Diet),value=T)
      process_cols<-process_cols[!grepl("Other",process_cols)]
      
      error_dat<-rbindlist(lapply(1:length(process_cols),FUN=function(i){
        p_col<-process_cols[i]
        dat<-Animal.Diet[,c("B.Code",p_col),with=F][,process_col:=p_col]
        colnames(dat)[2]<-"value"
        dat<-dat[!is.na(value)]
        dat <-unique(dat[, .(value = unlist(strsplit(value, "--|[|][|]"))), by = .(B.Code, process_col)])
        dat<-dat[!value %in% aom$old]
      
        dat<-dat[,.(B.Code=paste(unique(B.Code),collapse="/")),by=value
        ][,table:=table_name
        ][,field:=p_col
        ][,issue:="No match for this value in AOM process cols"]
        
        dat
      }))
      
      error_tracker(error_dat,filename = "Unmatched Diet Processes",error_dir = error_dir)
      
      # Apply gsub in sequence (longest first)
      for (i in aom[,which(major)]) {
        Animal.Diet[,D.Process.Mech:=gsub(aom[i,old], aom[i,new], D.Process.Mech, fixed = TRUE)]
        Animal.Diet[,D.Process.Chem:=gsub(aom[i,old], aom[i,new], D.Process.Chem, fixed = TRUE)]
        Animal.Diet[,D.Process.Bio:=gsub(aom[i,old], aom[i,new], D.Process.Bio, fixed = TRUE)]
        Animal.Diet[,D.Process.Therm:=gsub(aom[i,old], aom[i,new], D.Process.Therm, fixed = TRUE)]
        Animal.Diet[,D.Process.Dehy:=gsub(aom[i,old], aom[i,new], D.Process.Dehy, fixed = TRUE)]
        
        #Animal.Diet[,D.Item.Proc_Major:=gsub(aom[i,old], aom[i,new], D.Item.Proc_Major, fixed = TRUE)]
      }
      
      for (i in aom[,which(!major)]) {
        Animal.Diet[, D.Process.Mech := gsub(aom[i, old], "", D.Process.Mech, fixed = TRUE)]
        Animal.Diet[, D.Process.Chem := gsub(aom[i, old], "", D.Process.Chem, fixed = TRUE)]
        Animal.Diet[, D.Process.Bio := gsub(aom[i, old], "", D.Process.Bio, fixed = TRUE)]
        Animal.Diet[, D.Process.Therm := gsub(aom[i, old], "", D.Process.Therm, fixed = TRUE)]
        Animal.Diet[, D.Process.Dehy := gsub(aom[i, old], "", D.Process.Dehy, fixed = TRUE)]
                    
        #Animal.Diet[,D.Process.Chem:=gsub(aom[i,old], "", D.Process.Chem, fixed = TRUE)][,D.Process.Chem:=paste(unique(unlist(strsplit(D.Process.Chem,"[|][|]"))),collapse = "||"),by=.I]
        #Animal.Diet[,D.Process.Bio:=gsub(aom[i,old],"", D.Process.Bio, fixed = TRUE)][,D.Process.Bio:=paste(unique(unlist(strsplit(D.Process.Bio,"[|][|]"))),collapse = "||"),by=.I]
        #Animal.Diet[,D.Process.Therm:=gsub(aom[i,old],"", D.Process.Therm, fixed = TRUE)][,D.Process.Therm:=paste(unique(unlist(strsplit(D.Process.Therm,"[|][|]"))),collapse = "||"),by=.I]
        #Animal.Diet[,D.Process.Dehy:=gsub(aom[i,old], "", D.Process.Dehy, fixed = TRUE)][,D.Process.Dehy:=paste(unique(unlist(strsplit(D.Process.Dehy,"[|][|]"))),collapse = "||"),by=.I]
        #Animal.Diet[,D.Item.Proc_Major:=gsub("  "," ",trimws(gsub(aom[i,old],"", D.Item.Proc_Major, fixed = TRUE)))]
      }
      
      Animal.Diet[, D.Process.Mech := paste(unique(Filter(nzchar, unlist(strsplit(D.Process.Mech, "[|][|]|--")))),collapse = "--"),by = .I]  
      Animal.Diet[, D.Process.Chem := paste(unique(Filter(nzchar, unlist(strsplit(D.Process.Chem, "[|][|]|--")))),collapse = "--"),by = .I]  
      Animal.Diet[, D.Process.Bio := paste(unique(Filter(nzchar, unlist(strsplit(D.Process.Bio, "[|][|]|--")))),collapse = "--"),by = .I]  
      Animal.Diet[, D.Process.Therm := paste(unique(Filter(nzchar, unlist(strsplit(D.Process.Therm, "[|][|]|--")))),collapse = "--"),by = .I]  
      Animal.Diet[, D.Process.Dehy := paste(unique(Filter(nzchar, unlist(strsplit(D.Process.Dehy, "[|][|]|--")))),collapse = "--"),by = .I]  
        

      Animal.Diet[D.Process.Mech=="NA",D.Process.Mech:=NA]
      Animal.Diet[D.Process.Chem=="NA",D.Process.Chem:=NA]
      Animal.Diet[D.Process.Bio=="NA",D.Process.Bio:=NA]
      Animal.Diet[D.Process.Therm=="NA",D.Process.Therm:=NA]
      Animal.Diet[D.Process.Dehy=="NA",D.Process.Dehy:=NA]
      
      
      Animal.Diet[,D.Item.Proc_Major:=paste(na.omit(sort(unique(unlist(strsplit(c(D.Process.Mech,D.Process.Chem,D.Process.Bio,D.Process.Therm,D.Process.Dehy),"--"))))),collapse=" "),by=.I]
  
      Animal.Diet[,D.Item.Proc_All:=paste(na.omit(c(D.Process.Other,D.Item.Proc_Major)),collapse=" "),by=.I]
      
      # Harmonize all ItemxProcess to same delim
      Animal.Diet[,D.ItemxProcess:=gsub("--","||",D.ItemxProcess)]
      
      # 12.1.2.2) Harmonize D.Item Names ####
      
      # Update root and component of diet items
      
      # A) Merge using D.Item.Root.Comp
      merge_dat <-unique(master_codes$ani_diet[, .(D.Item = unlist(strsplit(D.Item, ";", fixed = TRUE))), by = D.Item.Root.Comp])
      setnames(merge_dat,"D.Item.Root.Comp","D.Item.Root.Comp_ani_diet")
      Animal.Diet<-merge(Animal.Diet,merge_dat,by="D.Item",all.x=T,sort=F)
      
      # Update D.Item.Root.Comp.Proc_Major
      Animal.Diet[,D.Item.Root.Comp.Proc_Major:=trimws(paste(D.Item,D.Item.Proc_Major))
                  ][,D.Item.Root.Comp.Proc_Major:=gsub("  |   "," ",D.Item.Root.Comp.Proc_Major) # Remove any residual double or triple spaces
                    ][D.Item!=D.Item.Root.Comp_ani_diet & !is.na(D.Item.Root.Comp_ani_diet),D.Item.Root.Comp.Proc_Major:=trimws(paste(D.Item.Root.Comp_ani_diet,D.Item.Proc_Major))]
      
 
      # B) Merge using D.Item.Root.Comp.Proc_Major
      merge_dat <-unique(master_codes$ani_diet[, .(D.Item = unlist(strsplit(D.Item, ";", fixed = TRUE))), by = .(D.Item.Root.Comp,D.Item.Root.Comp.Proc_Major)
                                               ][,D.Item:=trimws(tolower(D.Item))])
      setnames(merge_dat,c("D.Item.Root.Comp","D.Item.Root.Comp.Proc_Major"),c("D.Item.Root.Comp_ani_diet2","D.Item.Root.Comp.Proc_Major_ani_diet2"))
      
      Animal.Diet[,D.ItemxProcess_low:=tolower(D.ItemxProcess)]
      Animal.Diet<-merge(Animal.Diet,merge_dat,by.x="D.ItemxProcess_low",by.y="D.Item",all.x=T,sort=F)
      
      # Update D.Item.Root.Comp & D.Item.Root.Comp.Proc_Major
      Animal.Diet[D.Item.Root.Comp.Proc_Major!= D.Item.Root.Comp.Proc_Major_ani_diet2,D.Item.Root.Comp.Proc_Major:=D.Item.Root.Comp.Proc_Major_ani_diet2]
      
      # C) Create D.Item.Root.Other.Comp.Proc_All field
      Animal.Diet[,D.Item.Root.Other.Comp.Proc_All:=trimws(paste(na.omit(c(D.Item,D.Process.Other,D.Item.Proc_Major)),collapse = " ")),by=.I]
      Animal.Diet[D.Item!=D.Item.Root.Comp_ani_diet & !is.na(D.Item.Root.Comp_ani_diet),D.Item.Root.Other.Comp.Proc_All:=trimws(paste(c(D.Item.Root.Comp_ani_diet,na.omit(D.Process.Other),D.Item.Proc_Major),collapse=" ")),by=.I]
      Animal.Diet[D.Item!=D.Item.Root.Comp_ani_diet2 & !is.na(D.Item.Root.Comp_ani_diet2),D.Item.Root.Other.Comp.Proc_All:=trimws(paste(c(D.Item.Root.Comp_ani_diet2,na.omit(D.Process.Other),D.Item.Proc_Major),collapse=" ")),by=.I]
      
      # D) Tidy up
      Animal.Diet[,c("D.Item.Root.Comp_ani_diet","D.Item.Root.Comp_ani_diet2","D.Item.Root.Comp.Proc_Major_ani_diet2","D.ItemxProcess_low"):=NULL]
      
      # 12.1.2.3) Match to AOM/ani_diet ####

      # Get harmonization table from master_codes
      
      ani_diet<-master_codes$ani_diet[!is.na(D.Item),.(D.Item=unlist(strsplit(D.Item,";"))),by=.(D.Item.Root.Comp,
                                                                                       D.Item.Root.Comp.Proc_Major,
                                                                                       D.Item.Root.Other.Comp.Proc_All,
                                                                                       D.Item.Other,
                                                                                       D.Item.Proc_All,
                                                                                       D.Item.Proc_Major,
                                                                                       D.Item.Proc_Minor,
                                                                                       D.Item.Comp,
                                                                                       D.Item.AOM)
                                      ][,D.Item.Root.Comp.Proc_Major:=trimws(tolower(D.Item.Root.Comp.Proc_Major))]

        
      # Set to unique values (i.e. remove Diet.Items for now, as there are duplicate rows for D.Item.Root.Comp.Proc_Major)
        mergedat<-unique(ani_diet[,.(D.Item.Root.Comp,D.Item.Root.Comp.Proc_Major,D.Item.Proc_Major,D.Item.Comp,D.Item.AOM)])
        
        # Find non-unique diet items that will cause matching issues
        error_dat<-mergedat[,.(N=.N),by=D.Item.Root.Comp.Proc_Major][N>1]
        excluded_items<-error_dat[,unique(D.Item.Root.Comp.Proc_Major)]
        
        error_dat<-error_dat[,D.Item.Root.Comp.Proc_Major2:=D.Item.Root.Comp.Proc_Major
        ][,B.Code:=Animal.Diet[D.Item.Root.Comp.Proc_Major==D.Item.Root.Comp.Proc_Major2[1],paste(unique(B.Code),collapse = "/")],by=D.Item.Root.Comp.Proc_Major2
        ][,value:=D.Item.Root.Comp.Proc_Major
        ][,.(B.Code,value)
        ][,table:="era_master_sheet/ani_diets"
        ][,field:="D.Item.Root.Comp.Proc_Major"
        ][,issue:="Multiple rows for unique value of D.Item.Root.Comp.Proc_Major exist."]
        
        if(nrow(error_dat)>0){
        error_tracker(error_dat,filename = "Duplicate entries in ani_diets",error_dir = error_dir)
        }
        
        # Remove non-unique items
        mergedat<-unique(mergedat)[,N:=.N,by=D.Item.Root.Comp.Proc_Major][N==1][,N:=NULL]
        
        # Add a field to y which when merged shows the merge has worked for a particular row in x
        mergedat[,check:=T]
        
        # Make fields in x lower case to improve odds of matching
        Animal.Diet[,D.Item.Root.Comp.Proc_Major_low:=tolower(D.Item.Root.Comp.Proc_Major)]
        
        # Merge new names - using D.Item.Proc_Major 
        ani_diet_cols<-c(colnames(Animal.Diet),"index")
        Animal.Diet<-merge(Animal.Diet,mergedat[,!c("D.Item.Proc_Major")],by.x="D.Item.Root.Comp.Proc_Major_low",
                           by.y="D.Item.Root.Comp.Proc_Major",all.x=T,sort=F)
        Animal.Diet[is.na(check),check:=F]
        
        # Record non-matches
        error_dat<-Animal.Diet[!is.na(D.ItemxProcess) & 
                                 !is_entire_diet &
                                 !is_group &
                                 check==F & 
                                 !D.Item.Root.Comp.Proc_Major %in% excluded_items,
                               .(B.Code=paste0(unique(B.Code),collapse = "/")),
                               by=.(D.ItemxProcess,D.Item.Root.Comp.Proc_Major)
        ][,table:=table_name
        ][,issue:="No-match between D.Item.Root.Comp.Proc_Major and era_master_sheet/ani_diet/D.Item.Proc_Major"
          ][order(D.ItemxProcess)]
        
        if(nrow(error_dat)>0){
          error_tracker(error_dat,filename = "Non-matches to ani_diet and AOM",error_dir = error_dir)
        }    
        
        Animal.Diet[,check:=NULL]
        
    # 12.1.3) Other checks ####
      # 12.1.3.1) Animal.Diet - check same group is not split across multiple diets ####
      error_dat<-Animal.Diet[!is.na(D.Item.Group),.(No.Diets=length(unique(A.Level.Name[A.Level.Name!="Base"]))),by=.(B.Code,D.Item.Group)][No.Diets>1]  
      
      for(i in 1:nrow(error_dat)){
        error_dat1<-Animal.Diet[D.Item.Group==error_dat$D.Item.Group[i] & B.Code==error_dat$B.Code[i],.(A.Level.Name,D.Item,D.ItemxProcess,Time,D.Amount,D.Unit.Amount,D.Unit.Time,D.Unit.Animals)]
        
                # Split into a list of data.tables by A.Level.Name
        split_chunks <- split(error_dat1, by = "A.Level.Name", keep.by = FALSE)
      
        # Check pairwise identity of all chunks
        chunk_names <- names(split_chunks)
        combinations <- combn(chunk_names, 2, simplify = FALSE)
        
        comparison_results <- sapply(combinations, function(pair) {
          identical(split_chunks[[pair[1]]], split_chunks[[pair[2]]])
        }, USE.NAMES = TRUE)
        
        names(comparison_results) <- sapply(combinations, function(pair) paste(pair, collapse = " vs "))
        
        # Display comparison results
        
        if(all(comparison_results)){
          base<-error_dat1[A.Level.Name=="Base", D.ItemxProcess]
          trt<-error_dat1[A.Level.Name!="Base"][A.Level.Name==A.Level.Name[1],paste(unique(D.ItemxProcess),collapse=";")]
          base_trt<-paste(c(base,trt),collapse=";")
        }else{
          base_trt<-NA
        }
        
        error_dat[i,identical:=all(comparison_results)]
        error_dat[i,ingredients:=base_trt]
      }
        
      error_dat_e<-error_dat[,.(value=paste0(D.Item.Group,collapse = " --- ")),by=B.Code][,table:="Animal.Diet"
      ][,field:="D.Item.Group (multiple vals separated with ---)"
      ][,issue:="Group differs between diets levels (sometimes this can be due to a process being missed)."
      ][order(B.Code)]
      
      # 12.1.3.2) Check compound items do not have multiple match in the Animal.Diet tab ####
      
      # Animal.Diet.Comp
      error_dat<-unique(Animal.Diet.Comp[is_group==T & grepl(";",D.Item),.(B.Code,D.Item)])
      
      for(i in 1:nrow(error_dat)){
        d_items<-trimws(error_dat[i,unlist(strsplit(D.Item,";"))])
        error_dat1<-Animal.Diet[B.Code==error_dat[i,B.Code] & trimws(D.ItemxProcess) %in% d_items,.(A.Level.Name,D.Item,D.ItemxProcess,Time,D.Amount,D.Unit.Amount,D.Unit.Time,D.Unit.Animals)]
        
        # Split into a list of data.tables by A.Level.Name
        split_chunks <- split(error_dat1, by = "A.Level.Name", keep.by = FALSE)
        
        if(length(split_chunks)>1){
        # Check pairwise identity of all chunks
        chunk_names <- names(split_chunks)
        combinations <- combn(chunk_names, 2, simplify = FALSE)
        
        comparison_results <- sapply(combinations, function(pair) {
          identical(split_chunks[[pair[1]]], split_chunks[[pair[2]]])
        }, USE.NAMES = TRUE)
        
        names(comparison_results) <- sapply(combinations, function(pair) paste(pair, collapse = " vs "))
        
        # Display comparison results
        
  
        error_dat[i,identical:=all(comparison_results)]
        }else{
          error_dat[i,identical:=as.logical(NA)]
        }
        
        error_dat[i,No.Diets:=length(split_chunks)]
      }
      
      error_dat_a<-error_dat[identical==F,.(value=paste0(D.Item,collapse = " --- ")),by=B.Code][,table:="Animal.Diet.Comp"
      ][,field:="D.Item (multiple vals separated with ---)"
      ][,issue:="Combination of ingredients differs between diets levels (sometimes this can be due to a process being missed). Use diet group instead?"
      ][order(B.Code)]
      
      error_dat_b<-error_dat[No.Diets==0,.(value=paste0(D.Item,collapse = " --- ")),by=B.Code][,table:="Animal.Diet.Comp"
      ][,field:="D.Item (multiple vals separated with ---)"
      ][,issue:="Diet description does not match Diet.Out table (this should also be flagged in 'Diet Item does have match' table."
      ][order(B.Code)]
      
      # Animal.Diet.Digest
      error_dat<-unique(Animal.Diet.Digest[grepl(";",D.Item),.(B.Code,D.Item)])
      
      for(i in 1:nrow(error_dat)){
        d_items<-trimws(error_dat[i,unlist(strsplit(D.Item,";"))])
        error_dat1<-Animal.Diet[B.Code==error_dat[i,B.Code] & trimws(D.ItemxProcess) %in% d_items,.(A.Level.Name,D.Item,D.ItemxProcess,Time,D.Amount,D.Unit.Amount,D.Unit.Time,D.Unit.Animals)]
        
        # Split into a list of data.tables by A.Level.Name
        split_chunks <- split(error_dat1, by = "A.Level.Name", keep.by = FALSE)
        
        if(length(split_chunks)>1){
          # Check pairwise identity of all chunks
          chunk_names <- names(split_chunks)
          combinations <- combn(chunk_names, 2, simplify = FALSE)
          
          comparison_results <- sapply(combinations, function(pair) {
            identical(split_chunks[[pair[1]]], split_chunks[[pair[2]]])
          }, USE.NAMES = TRUE)
          
          names(comparison_results) <- sapply(combinations, function(pair) paste(pair, collapse = " vs "))
          
          # Display comparison results
          
          
          error_dat[i,identical:=all(comparison_results)]
        }else{
          error_dat[i,identical:=as.logical(NA)]
        }
        
        error_dat[i,No.Diets:=length(split_chunks)]
      }
      
      error_dat_c<-error_dat[identical==F,.(value=paste0(D.Item,collapse = " --- ")),by=B.Code][,table:="Animal.Diet.Digest"
      ][,field:="D.Item (multiple vals separated with ---)"
      ][,issue:="Combination of ingredients differs between diets levels (sometimes this can be due to a process being missed). Use diet group instead?"
      ][order(B.Code)]
      
      error_dat_d<-error_dat[No.Diets==0,.(value=paste0(D.Item,collapse = " --- ")),by=B.Code][,table:="Animal.Diet.Digest"
      ][,field:="D.Item (multiple vals separated with ---)"
      ][,issue:="Diet description does not match Diet.Out table (this should also be flagged in 'Diet Item does have match' table."
      ][order(B.Code)]
      
      
      # Merge and save errors
      errors<-rbindlist(list(error_dat_e,error_dat_a,error_dat_b,error_dat_c,error_dat_d))[order(B.Code)]
      
      error_tracker(errors,filename = "Potential Issues with Diet Groups",error_dir = error_dir)
      
  # 12.2) Animals.Out: Merge AOM Diet Summary with Animals.Out (.inc Trees)  ####
  
    # Merge relevant AOM columns
    cols<-c("AOM","Scientific Name",paste0("L",1:10))
    merge_dat<-master_codes$AOM[AOM %in% Animal.Diet$D.Item.AOM &!is.na(AOM),..cols]
    setnames(merge_dat,"Scientific Name","AOM.Scientific.Name")
    merge_dat[,AOM.Terms:=apply(merge_dat[,!"AOM"],1,FUN=function(x){
      x<-as.vector(na.omit(x))
      paste(x,collapse="/")})
    ][,AOM.Terms:=unlist(tstrsplit(AOM.Terms,"Feed Ingredient/",keep=2))]
    
    merge_dat<-unique(merge_dat[,.(AOM,AOM.Terms,AOM.Scientific.Name)])
    
    Animal.Diet<-merge(Animal.Diet,merge_dat,by.x="D.Item.AOM",by.y="AOM",all.x=T,sort=F)
    
    Animal.Diet[,D.Item.Is.Tree:=F][grepl("Forage Trees",AOM.Terms),D.Item.Is.Tree:=T]
    
    # Summarize 
    Animal.Diet.Summary<-Animal.Diet[,.(A.Diet.Trees=paste0(sort(unique(AOM.Scientific.Name[D.Item.Is.Tree & !is_group])),collapse=";"),
                                        A.Diet.Other=paste0(sort(unique(basename(AOM.Terms)[!D.Item.Is.Tree & !is_group])),collapse=";")),
                                     by=.(B.Code,A.Level.Name)]
    
    Animals.Out<-merge(Animals.Out,Animal.Diet.Summary,by=c("B.Code","A.Level.Name"),all.x=T,sort=F)
    Animals.Out[A.Diet.Trees=="",A.Diet.Trees:=NA][A.Diet.Other=="",A.Diet.Other:=NA]
    
  # 12.3) To DO: Data.Out: Update Feed Item Names  ####
  if(F){
  # Is intake item a group? (contains more than 1 diet item) 
  Data.Out[grepl(";",ED.Intake.Item),ED.Intake.Item.is_group:=F]
  
  # Dev Note: Need to make this work with lists of items when harmonizing the names
  
  #  Harmonize Semicolon-Separated ED.Intake.Item Strings
  lookup_dt <- unique(Animal.Diet[,.(B.Code,D.ItemxProcess,D.Item.Root.Other.Comp.Proc_All)])
  
  # Expand Data.Out Rows by Semicolon
  Data.Out[, row_id := .I]  # Preserve row identity
  split_dt <- Data.Out[!is.na(ED.Intake.Item) & !ED.Intake.Item %in% c("Entire Diet","Unspecified")
                       ][, .(Item = trimws(unlist(strsplit(ED.Intake.Item, ";", fixed = TRUE)))), 
                       by = .(row_id, B.Code)]
  
  # Merge with Lookup Table
  split_dt <- merge(split_dt, lookup_dt, 
                    by.x = c("B.Code", "Item"), 
                    by.y = c("B.Code", "D.Item.Raw"), 
                    all.x = TRUE, sort = FALSE)
  
  # Replace with Harmonized or Flagged Name
  split_dt[, Harmonized := ifelse(!is.na(D.Item), D.Item, paste0(Item, " [no match]"))]
  
  # Collapse Back into Semicolon-Separated String 
  harmonized_dt <- split_dt[, .(ED.Intake.Item = paste(Harmonized, collapse = ";")), by = .(row_id,B.Code)]
  
  # Merge Back to Data.Out
  Data.Out <- merge(Data.Out, harmonized_dt, by = "row_id", all.x = TRUE, sort = FALSE)
  Data.Out[, `:=`(
    ED.Intake.Item.Raw = ED.Intake.Item,
    ED.Intake.Item = ED.Intake.Item.y
  )][, c("row_id", "ED.Intake.Item.y") := NULL]
  
  
  if(F){
  merge_dat<-unique(Animal.Diet[,.(B.Code,D.Item.Raw,D.Item)])
  Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Intake.Item"),by.y=c("B.Code","D.Item.Raw"),all.x=T,sort=F)
  Data.Out[,ED.Intake.Item.Raw:=ED.Intake.Item
  ][!is.na(D.Item),ED.Intake.Item:=D.Item]
  }
  
  # Is intake item entire diet?
  merge_dat<-unique(Animals.Out[,.(B.Code,A.Level.Name)][,ED.Intake.Item.is_entire_diet:=T])
  Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Intake.Item"),by.y=c("B.Code","A.Level.Name"),all.x=T,sort=F)
  Data.Out[is.na(ED.Intake.Item.is_entire_diet),ED.Intake.Item.is_entire_diet:=F][ED.Intake.Item.Raw=="Entire Diet",ED.Intake.Item.is_entire_diet:=T]
  
  # If entire diet substitute the A.Level.Name for the diet
  merge_dat<-MT.Out[,.(B.Code,T.Name,A.Level.Name)]
  Data.Out<-merge(Data.Out,merge_dat,by=c("B.Code","T.Name"),all.x=T,sort=F)
  Data.Out[ED.Intake.Item.Raw=="Entire Diet",ED.Intake.Item:=A.Level.Name]
  
  # Check for any instances were we cannot match the intake item back to the diet table
  error_dat<-Data.Out[!is.na(ED.Intake.Item) & is.na(D.Item) & ED.Intake.Item.is_group==F & ED.Intake.Item.is_entire_diet==F
  ][,.(value=paste(unique(ED.Intake.Item.Raw),collapse = "/")),by=B.Code
  ][,table:="Data.Out"
  ][,field:="ED.Intake.Item.Raw"
  ][,issue:="Feed intake item cannot be matched to diet, diet group, or diet ingredient."]
  

  # Tidy up
  Data.Out[,c("A.Level.Name","D.Item"):=NULL]
  
  }
    # 12.3.1) List papers with no-entire diet and many feed items in a group ####
    error_dat<-Data.Out[!is.na(ED.Intake.Item),.(B.Code,ED.Intake.Item,ED.Intake.Item.is_entire_diet,ED.Intake.Item.is_group)]
    error_dat<-error_dat[,any_entire_diet:=F
                         ][,any_entire_diet:=any(ED.Intake.Item.is_entire_diet),by=B.Code
                           ][any_entire_diet==F
                             ][,n_items:=str_count(ED.Intake.Item,";")+1,by=ED.Intake.Item
                               ][n_items>2]
    error_dat<-unique(error_dat[,.(B.Code,ED.Intake.Item)])[,.(value=paste(ED.Intake.Item,collapse=" --- ")),by=B.Code
                                                          ][,table:="Data.Out (Enter Data)"
                                                          ][,field:="ED.Intake.Item"
                                                          ][,issue:="No entire diets reported and grouped intake items of 3 or more values. Check to see if group is entire diet."]
    if(nrow(error_dat)>0){
      error_tracker(error_dat[order(B.Code)],filename = "Check if Entire Diet can used in Data.Out",error_dir = error_dir)
    }
    
  # 12.4) Update Tables ####
    
    data$Animal.Diet<-Animal.Diet
    data$Animals.Out<-Animals.Out
    data$Animal.Diet.Comp<-Animal.Diet.Comp
    data$Animal.Diet.Digest<-Animal.Diet.Digest
    data$Data.Out<-Data.Out
    
# 13) Save results ####
save_file<-paste0(project,"_draft_",as.character(Sys.Date()))
n<-sum(grepl(basename(save_file),list.files(era_dirs$era_masterdata_dir,".RData")))                                   

save(data,file=file.path(era_dirs$era_masterdata_dir,paste0(save_file,".",n+1,".RData")))
jsonlite::write_json(data,path=file.path(era_dirs$era_masterdata_dir,paste0(save_file,".",n+1,".json")))

