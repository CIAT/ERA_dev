# First run R/0_set_env.R
# 0.0) Install and load packages ####
if (!require(pacman)) install.packages("pacman")  # Install pacman if not already installed
pacman::p_load(data.table, 
               readxl,
               future, 
               future.apply,
               parallel,
               miceadds,
               pbapply,
               soiltexture,
               httr,
               stringr,
               rnaturalearth,
               rnaturalearthhires,
               sf,
               dplyr,
               progressr)
# 0.1) Define the valid range for date checking #####
valid_start <- as.Date("1950-01-01")
valid_end <- as.Date("2023-12-01")

# 0.2) Set project, directories and parallel cores ####

# Set cores for parallel processing
workers<-parallel::detectCores()-2

# Set the project name, this should usually refer to the ERA extraction template used
project<-era_projects$skinny_cow_2022

# Where extraction excel files are stored
excel_dir<-file.path(era_dirs$era_dataentry_dir,project,"excel_files")
if(!dir.exists(excel_dir)){
  dir.create(excel_dir,recursive=T)
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

error_dir_master<-file.path(error_dir,"master_codes")
if(!dir.exists(error_dir_master)){
  dir.create(error_dir_master)
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
    options(timeout = 60*60*2) # 2.6 gb file & 2hr timehour 
    if(download){
      download.file(s3_file, destfile = local_file)
    }
    unzip(local_file, exdir = excel_dir,overwrite=T,junkpaths=T)
    unlink(local_file)
  }
}

# 2) Load data ####
  # 2.1) Load era vocab #####
  
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)
  
  # 2.2) Load excel data entry template #####
  Master<-list.files(paste0(project_dir,"/data_entry/",project,"/excel_data_extraction_template"),"xlsm$",full.names = T)
  
  # List sheet names that we need to extract
  SheetNames<-excel_sheets(Master)
  SheetNames<-grep(".Out",SheetNames,fixed = T,value=T)
  
  # Remove non-livestock sheets
  SheetNames<-SheetNames[!SheetNames %in% c("Till.Out","Plant.Out","Fert.Out","Residues.Out",
                                            "Weed.Out","Harvest.Out","pH.Out","WH.Out","Irrig.Out",
                                            "PH.E.Out","Int.Out","Rot.Out")]
  
  SheetNames<-c(SheetNames,"Times")
  
  # List column names for the sheets to be extracted
  XL.M<-sapply(SheetNames,FUN=function(SName){
    cat('\r                                                                                                                                          ')
    cat('\r',paste0("Importing Sheet = ",SName))
    flush.console()
    colnames(data.table(suppressWarnings(suppressMessages(readxl::read_excel(Master,sheet = SName)))))
  },USE.NAMES = T)
  
  # 2.3) List extraction excel files #####
  Files<-list.files(excel_dir,".xlsm$",full.names=T)
  
  # 2.4) Check for duplicate files #####
  FNames<-unlist(tail(tstrsplit(Files,"/"),1))
  FNames<-gsub(" ","",FNames)
  FNames<-unlist(tstrsplit(FNames,"-",keep=2))
  FNames<-gsub("[(]1[])]|[(]2[])]","",FNames)
  FNames<-gsub("_1|_2|_3|_4",".1|.2|.3|.4",FNames,fixed=T)
  FNames<-gsub("..",".",FNames,fixed=T)
  
  excel_files<-data.table(filename=Files,era_code=FNames)
  excel_files[,status:="qced"][grepl("/Extracted/",filename),status:="not_qced"]
  
  # Flag any naming issues
  excel_files[grepl("_",era_code)]
  excel_files<-excel_files[!grepl("_",era_code)]
  
  # Look for duplicate files
  excel_files[,N:=.N,by=era_code]
  excel_files[N>1][order(era_code)]
  
  # Remove not qced duplicates
  excel_files<-excel_files[!(N==2 & status=="not_qced")][,N:=.N,by=era_code]
  excel_files[N>1][order(era_code)]
  
  excel_files<-excel_files[!N>1][,N:=NULL]
  excel_files[, era_code2:=gsub(".xlsm", "", era_code)]
  
  # 2.5) Read in data from excel files #####
  
  # If files have already been imported and converted to list form should the important process be run again?
  overwrite<-F
  
  # Delete existing files if overwrite =T
  if(overwrite){
    unlink(extracted_dir,recursive = T)
    dir.create(extracted_dir)
  }
  
  # Set up parallel back-end
  future::plan(multisession, workers = workers)
  
  # Run future apply loop to read in data from each excel file in parallel
  XL <- future.apply::future_lapply(1:nrow(excel_files), FUN=function(i){
    File <- excel_files$filename[i]
    era_code <- excel_files$era_code2[i]
    save_name <- file.path(extracted_dir, paste0(era_code, ".RData"))
    
    if (overwrite == TRUE || !file.exists(save_name)) {
      X <- tryCatch({
        lapply(SheetNames, FUN=function(SName){
          cat('\r', "Importing File ", i, "/", nrow(excel_files), " - ", era_code, " | Sheet = ", SName,"               ")
          flush.console()
          data.table(suppressMessages(suppressWarnings(readxl::read_excel(File, sheet = SName, trim_ws = FALSE))))
        })
      }, error=function(e){
        cat("Error reading file: ", File, "\nError Message: ", e$message, "\n")
        return(NULL)  # Return NULL if there was an error
      })
      
      if (!is.null(X)) {
        names(X) <- SheetNames
        X$file.info<-file.info(File)
        save(X, file=save_name)
      }
    } else {
      miceadds::load.Rdata(filename=save_name, objname="X")
    }
    
    X
  })
  
  # Reset plan to default setting
  future::plan(sequential)
  
  # Add names
  names(XL)<-excel_files$filename
  
  # Filter out any missing data
  XL <- Filter(Negate(is.null), XL)
  
  rm(Files,SheetNames,XL.M,Master)
  
  # List any files that did not load
  errors<-excel_files[!filename %in% names(XL)
  ][,c("status","era_code"):=NULL
  ][,issue:="Excel import failed"]
  setnames(errors,"era_code2","B.Code")
  error_list<-error_tracker(errors=errors,filename = "excel_import_failures",error_dir=error_dir,error_list = NULL)
  
    # 2.5.1) Read in feed intake from user input sheet (error in Data.Out) ######
  future::plan(multisession, workers = workers)
  
  enter_data_raw <- future.apply::future_lapply(1:nrow(excel_files), FUN=function(i){
    File <- excel_files$filename[i]
    era_code <- excel_files$era_code2[i]
    
    tryCatch({
      cat('\r', "Importing File ", i, "/", nrow(excel_files), " - ", era_code, "               ")
      flush.console()
      X<-data.table(suppressMessages(suppressWarnings(readxl::read_excel(File, sheet = "EnterData", trim_ws = FALSE))))
      
      
      X<-X[,1:14]
      x_cols<-unlist(X[1])
      X<-X[-1]
      colnames(X)<-x_cols
      X<-X[!is.na(ED.Treatment) & !is.na(ED.Site.ID)]
      X<-X[,-13]
      X$B.Code<-excel_files[i,era_code2]
      X
      
    }, error=function(e){
      cat("Error reading file: ", File, "\nError Message: ", e$message, "\n")
      return(NULL)  # Return NULL if there was an error
    })
    
  })
  
  enter_data_raw<-rbindlist(enter_data_raw)
  
# 3) Process imported data ####
  # 3.1) Publication (Pub.Out) #####
table_name<-"Pub.Out"
data<-lapply(XL,"[[",table_name)

Pub.Out<-rbindlist(pblapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  X$filename<-names(XL)[i]
  X
}))

# Replace zeros with NAs
Pub.Out<-validator(data=Pub.Out,
                   zero_cols=c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4"),
                   trim_ws = T,
                   tabname="Pub.Out")$data

# Pub.Out: Validation: Duplicate or mismatched B.Codes
Pub.Out<-merge(Pub.Out,excel_files[,list(filename,era_code2)],all.x=T)
Pub.Out[,N:=.N,by=B.Code][,code_issue:=B.Code!=era_code2][,B.Code:=trimws(B.Code)][,era_code2:=trimws(era_code2)]

# Save any errors
errors<-Pub.Out[N>1|code_issue,list(B.Code,era_code2,filename,N,code_issue)
                ][,value:=paste0("Pub.Out = ",B.Code," Filename = ",era_code2)
                  ][,list(B.Code,value)
                    ][,table:="Pub.Out"
                      ][,field:="B.Code"
                        ][,issue:="Potential issues with study code, it does not match the filename."]
error_list<-error_tracker(errors=errors,filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

# Set B.Code to match filename
Pub.Out[,B.Code:=era_code2]
# Tidy up
Pub.Out[,c("era_code2","filename","N","code_issue"):=NULL]

    # 3.1.1) Harmonization ######
results<-val_checker(data=Pub.Out,
                     tabname=table_name,
                     master_codes=master_codes,
                     master_tab="journals",
                     h_field="B.Journal",
                     h_field_alt=NA,
                     exact=F)

Pub.Out<-results$data

harmonization_list<-error_tracker(errors=results$h_task[order(value)],filename = paste0(table_name,"_harmonization"),error_dir=harmonization_dir,error_list = NULL)

# 3.2) Site.Out #####
table_name<-"Site.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[100]])

Site.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]][-1]
  
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with site out structure"))
  }else{
    X<-X[,..col_names]
    X<-X[!is.na(Site.ID)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field="Site.ID",issue="No Site.ID exists"))
    }
  }})

error_dat<-rbindlist(lapply(Site.Out,"[[","error"))

errors<-list(error_dat)

Site.Out<-rbindlist(lapply(Site.Out,"[[","data"))
setnames(Site.Out,c("MSP...20","MSP...21","LatD...8","LatD...10"),
         c("Site.MSP.S1","Site.MSP.S2","LatD","LonD"),skip_absent = T)

# Add "Site" prefix to site fields
c_names<-colnames(Site.Out)
c_names<-c_names[!grepl("Site|B.Code|Country|Buffer",c_names)]
setnames(Site.Out,c_names,paste0("Site.",c_names))

# Fix LatD/LonD 0's that should be NAs
Site.Out[Site.LatD==0 & Site.LonD==0,c("Site.LatD","Site.LonD"):=NA]

# Read in data excluding files with non-match structure
results<-validator(data=Site.Out,
                   zero_cols=colnames(Site.Out)[!colnames(Site.Out) %in% c("Site.LonD","Site.LatD","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree")],
                   numeric_cols=c("Site.LonD","Site.LatD","Site.Lat.Unc","Site.Lon.Unc","Buffer.Manual","Site.Rain.Seasons","Site.MAP","Site.MAT","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree","Site.MSP.S1","Site.MSP.S2"),
                   compulsory_cols = c(Site.ID="Site.Type",Site.ID="Country",Site.ID="Site.LatD",Site.ID="Site.LonD"),
                   extreme_cols=list(Site.MAT=c(10,35),
                                     Site.MAP=c(40,4000),
                                     Site.MSP.S1=c(20,3000),
                                     Site.MSP.S2=c(20,3000),
                                     Site.Elevation=c(0,4000),
                                     Site.Slope.Perc=c(0,50),
                                     Site.Slope.Degree=c(0,45)),
                   unique_cols = "Site.ID",
                   date_cols=NULL,
                   trim_ws=T,
                   tabname=table_name)

Site.Out<-results$data
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
][,table:="Site.Out"
][,field:="Site.LonD/Site.LatD"
][,issue:="Co-ordinates may not be in the country specified."]

  # 3.2.1) Harmonization ######
  h_params<-data.table(h_table=table_name,
                       h_field=c("Site.Admin","Site.Start.S1","Site.End.S1","Site.Start.S2","Site.End.S2","Site.Type","Site.Soil.Texture"),
                       h_table_alt=c(NA,"Site.Out","Site.Out","Site.Out","Site.Out",NA,"Site.Out"),
                       h_field_alt=c(NA,"Site.Seasons","Site.Seasons","Site.Seasons","Site.Seasons",NA,"Soil.Texture"))
  
  results<-harmonizer_wrap(data=Site.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  
  results2<-val_checker(data=Site.Out[grepl("Station",Site.Type) & !grepl("[.][.]",Site.ID)],
                        tabname=table_name,
                        master_codes=master_codes,
                        master_tab="site_list",
                        h_field="Site.ID",
                        h_field_alt=NA)
  
  results3<-val_checker(data=Site.Out,
                        tabname="Site.Out",
                        master_codes=master_codes,
                        master_tab="countries",
                        h_field="Country",
                        h_field_alt=NA)
  
  harmonization_list<-error_tracker(errors=rbindlist(list(results$h_tasks,results2,results3),fill=T),
                                    filename = paste0(table_name,"_harmonization"),
                                    error_dir=harmonization_dir,
                                    error_list = harmonization_list)
  
  Site.Out<-results$data
  
  
  # 3.2.2) Harmonize Site.ID field #######
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
  
  # 3.2.3) Create Aggregated Site Rows #######
  mergedat<-Site.Out[grep("[.][.]",Site.ID)]
  
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
                            table="Site.Out",
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
  
  # 3.2.4) Save errors ######
  error_list<-error_tracker(errors=rbindlist(errors,fill=T)[order(B.Code)],
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
# 3.3) Soil (Soil.Out) #####
table_name<-"Soils.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[100]])

Soil.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
  }else{
    X<-X[,..col_names]
    X<-X[!is.na(Site.ID)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }
  }})

errors<-list(rbindlist(lapply(Soil.Out,"[[","error")))

Soil.Out<-rbindlist(lapply(Soil.Out,"[[","data"))

# Correct duplicate column names
colnames(Soil.Out)[grep("Soil.N.Unit",colnames(Soil.Out))]<-c("Soil.TN.Unit","Soil.AN.Unit")

# Structure errors: Check for case where only one of upper or lower is present
error_dat<-rbindlist(lapply(1:length(data),FUN=function(i){
  dt<-data[[i]]
  
  if(colnames(dt)[1]!="...1"){
    # Filter out columns that are all NA
    dt <- dt[, .SD, .SDcols = colSums(is.na(dt)) != nrow(dt)]
    Xcols<-colnames(dt)
    
    if(("Soil.Upper" %in% Xcols + "Soil.Lower" %in% Xcols)==1){
      Y<-data.table(B.Code=Pub.Out$B.Code[i],
                    value=NA,
                    table=table_name,
                    field="Soil.Upper/Soil.Lower",
                    issue="Only one of upper or lower depth has a value")
      Y
    }
  }
}))
errors<-c(errors,list(error_dat))

zero_cols<-c("SND","SLT","CLY","Soil.Lower","Soil.BD","Soil.TC","Soil.SOC","Soil.SOM","Soil.pH","Soil.CEC","Soil.EC","Soil.FC","Soil.TN","Soil.NH4","Soil.NO3","Soil.AN",
             "SND.Unit","SLT.Unit","CLY.Unit","Soil.BD.Unit","Soil.TC.Unit","Soil.FC.Unit","Soil.SOC.Unit","Soil.SOM.Unit","Soil.CEC.Unit","Soil.EC.Unit","Soil.TN.Unit","Soil.NH4.Unit","Soil.NO3.Unit",
             "Soil.AN.Unit","Soil.SOC.Method","Soil.SOM.Method","Soil.pH.Method")

# Update Site ID
Soil.Out[,Site.ID_new:=Site.Out$Site.ID[match(Soil.Out$Site.ID,Site.Out$Site.ID_raw)]
][is.na(Site.ID_new),Site.ID_new:=Site.ID
][,Site.ID:=Site.ID_new
][,Site.ID_new:=NULL]

results<-validator(data=Soil.Out,
                   zero_cols =zero_cols,
                   hilo_pairs = data.table(low_col="Soil.Upper",high_col="Soil.Lower",name_field="Site.ID"),
                   numeric_cols=c("Soil.Upper","Soil.Lower","Soil.BD","Soil.TC","Soil.SOC","Soil.SOM","Soil.pH","Soil.CEC","Soil.EC","Soil.FC","Soil.TN","Soil.NH4","Soil.NO3","Soil.AN"),
                   tabname=table_name,
                   trim_ws = T,
                   site_data=Site.Out)

error_dat<-results$errors
errors<-c(errors,list(error_dat))

Soil.Out<-results$data
Soil.Out[is.na(Soil.Lower),Soil.Upper:=NA]

  # 3.3.1) Soil.Out: Calculate USDA Soil Texture from Sand, Silt & Clay ####

  # Keep rows with 2 or more observations
  Soil.Out.Texture<-copy(Soil.Out)[,N:=is.na(CLY)+is.na(SLT)+is.na(SND)][N<2]
  
  # Add missing values where 2/3 are present
  Soil.Out.Texture[,x:=1:.N][,val:=sum(c(CLY,SLT,SND),na.rm=T),by=x][,x:=NULL]
  
  Soil.Out.Texture[is.na(SLT),SLT:=100-val
  ][is.na(CLY),CLY:=100-val
  ][is.na(SND),SND:=100-val]
  
  # Any values not 100
  error_dat<-unique(Soil.Out.Texture[N!=1 & (val>102|val<98),list(B.Code,Site.ID)])
  
  if(nrow(error_dat)>0){
    setnames(error_dat,"Site.ID","value")
    error_dat[,table:=table_name][,field:="Site.ID"][,issue:="Sand, silt, clay sum to beyond 2% different to 100%"]
  }
  errors<-c(errors,list(error_dat))
  
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

  # 3.3.2) Save errors ######
  errors<-rbindlist(errors,fill = T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  # ***!!!TO DO!!!***  harmonize methods, units and variables ####
# 3.3) Time periods #####
  table_name<-"Times.Out"
  
  data<-lapply(XL,"[[","Times")

  Times.Out<-rbindlist(pblapply(1:length(data),FUN=function(i){
    X<-data[[i]][7:30,10:18]
    names(X)<-c("Time","Site.ID","TSP","TAP","Plant.Start","Plant.End","Harvest.Start","Harvest.End","Harvest.DAP")
    X<-X[!is.na(Time)]
    X$B.Code<-Pub.Out$B.Code[i]
    X
    }))
  
  
  results<-validator(data=Times.Out,
                     numeric_cols=c("TSP","TAP"),
                     site_data = Site.Out,
                     compulsory_cols = c(Site.ID="Time",Time="Site.ID"),
                     trim_ws = T,
                     duplicate_field="Time",
                     duplicate_ignore_fields=colnames(Times.Out)[!colnames(Times.Out) %in% c("Time","Site.ID","B.Code")],
                     rm_duplicates=T,
                     do_time=F,
                     tabname=table_name)
  
   errors<-results$errors
   
   Times.Out<-results$data
    
    error_list<-error_tracker(errors=errors,
                              filename = paste0(table_name,"_errors"),
                              error_dir=error_dir,
                              error_list = error_list)
  
# 3.4) Experimental Design (ExpD.Out) ####
table_name<-"ExpD.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[1]])

ExpD.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out[,B.Code[i]]
  Filename<-basename(names(XL)[i])
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
  }else{
    X[,B.Code:=B.Code]
    list(data=X)
  }
})

errors<-list(rbindlist(lapply(ExpD.Out,"[[","error")))

ExpD.Out<-rbindlist(lapply(ExpD.Out,"[[","data"))

results<-validator(data=ExpD.Out,
                   numeric_cols=c("EX.Plot.Size","EX.HPlot.Size"),
                   zero_cols = c("EX.Design","EX.Plot.Size","EX.HPlot.Size","EX.Notes"),
                   trim_ws = T,
                   tabname="ExpD.Out")

ExpD.Out<-unique(results$data)

errors<-c(errors,list(results$errors))
error_list<-error_tracker(errors=rbindlist(errors,fill = T)[order(B.Code)],filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

# 3.5) Products (Prod.Out) ####
table_name<-"Prod.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[100]])

Prod.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out[,B.Code[i]]
  Filename<-basename(names(XL)[i])
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
  }else{
    X<-X[,..col_names]
    X[,B.Code:=B.Code]
    X<-X[!is.na(P.Product)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="No products exists."))
    }
    
    list(data=X)
  }
})

errors<-list(rbindlist(lapply(Prod.Out,"[[","error")))
Prod.Out<-rbindlist(lapply(Prod.Out,"[[","data"))

Prod.Out<-validator(data=Prod.Out,
                    tabname="Prod.Out",
                    trim_ws = T)$data

# Check products exists (non-tree)
mprod<-unique(master_codes$prod[,list(Product.Simple)])
colnames(mprod)<-c("P.Product")
mprod[,check:=T]
mergedat<-merge(Prod.Out,mprod,all.x=T)

mergedat<-mergedat[is.na(check),list(B.Code,P.Product)]
error_dat<-mergedat[,value:=P.Product
][,table:=table_name
][,field:="P.Product"
][,issue:="No match for product in master codes Product tab"
][order(B.Code)][,P.Product:=NULL]

errors<-c(errors,list(error_dat))

error_list<-error_tracker(errors=rbindlist(errors)[order(B.Code)],filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

  # TO DO - updated mulched and incorporated codes, check product is in master codes ####
# 3.6) Var (Var.Out) ####
table_name<-"Var.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[100]])

Var.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  X<-X[!grepl("ERROR",V.Product)]
  B.Code<-Pub.Out[,B.Code[i]]
  Filename<-basename(names(XL)[i])
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
  }else{
    X<-X[,..col_names]
    X[,B.Code:=B.Code]
    X<-X[!is.na(V.Var)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
    
    list(data=X)
  }
})

errors<-list(rbindlist(lapply(Var.Out,"[[","error")))

Var.Out<-rbindlist(lapply(Var.Out,"[[","data"))
setnames(Var.Out,"V.Subpecies","V.Subspecies")
Var.Out[,c("V.New.Var","V.New.Crop","V.New.Species","V.New.SubSpecies","...20","V.Trait1","V.Trait2","V.Trait3"):=NULL]

results<-validator(data=Var.Out,
                   numeric_cols=c("V.Maturity"),
                   zero_cols = c("V.Var","V.Species","V.Subspecies","V.Animal.Practice","V.Base",
                                 "V.Type","V.Maturity","V.Code.Crop","V.Code.Animal","V.Crop.Practice",
                                 "V.Codes"),
                   tabname=table_name,
                   duplicate_field = "V.Var",
                   trim_ws=T)

errors<-c(errors,list(results$errors))

Var.Out<-results$data

 # 3.6.1) Harmonization #####
# Save original variety name as this is a keyfield used in the MT.Out tab, if it is changed then this causes issues
Var.Out[,V.Level.Name:=Join]
setnames(Var.Out,"Join","V.Level.Name_raw")

# Check & fix for where Animal Practices has been entered in Crop Practice, remove crop related cols
Var.Out[!is.na(V.Crop.Practice) & V.Crop.Practice!=V.Animal.Practice]
Var.Out<-Var.Out[!is.na(V.Crop.Practice) & V.Crop.Practice!=V.Animal.Practice,V.Animal.Practice:=V.Crop.Practice
][,c("V.Crop.Practice","V.Code.Crop","V.Code.Animal","V.Maturity"):=NULL]

# Update Variety Naming and Codes
merge_dat<-master_codes$vars_animals[,list(V.Product,V.Var,V.Var1,V.Animal.Practice)]
setnames(merge_dat,"V.Animal.Practice","V.Animal.Practice1")

# Check for duplicate names in codes
error_dat<-copy(merge_dat)[,N:=.N,by=list(V.Product,V.Var)][N>1][,.(value=paste0(V.Product,":",V.Var)),by=.(V.Product,V.Var)
][,B.Code:=NA
][,table:=table_name
][,field:="V.Product:V.Var"
][,issue:="More than one description of same variety in master codes/var tab."
][,c("V.Product","V.Var"):=NULL]
errors<-c(errors,list(error_dat))

# Merge codes to Var.out
Var.Out<-merge(Var.Out,merge_dat,all.x=T,by.x=c("V.Product","V.Level.Name"),by.y=c("V.Product","V.Var"))
Var.Out[!is.na(V.Var1),V.Level.Name:=V.Var1
][!is.na(V.Animal.Practice1),V.Animal.Practice:=V.Animal.Practice1][,V.Animal.Practice1:=NULL]

# Update V.Codes
Var.Out[,unique(V.Animal.Practice)]
Var.Out[,V.Codes:=master_codes$prac[match(Var.Out$V.Animal.Practice,master_codes$prac$Subpractice),Code]]

# Errors
error_dat<-unique(Var.Out[(V.Type %in% c("Local","Landrace")) & V.Animal.Practice != "Unimproved Breed",!c("V.Base","V.Codes")])
error_dat<-error_dat[,.(value=paste(unique(V.Var),collapse = "/")),by=B.Code
][,table:=table_name
][,field:="V.Var"
][,issue:="V.Type is local, but V.Animal.Practice is not Unimproved Breed"]

errors<-c(errors,list(error_dat))

error_dat<-unique(Var.Out[V.Type %in% c("Hybrid","Crossbreed") & V.Animal.Practice != "Hybridization or Cross Breeding",!c("V.Base","V.Codes")])
error_dat<-error_dat[,.(value=paste(unique(V.Var),collapse = "/")),by=B.Code
][,table:=table_name
][,field:="V.Var"
][,issue:="V.Type is hybrid/crossbreed, but V.Animal.Practice is not Hybridization or Cross Breeding"]

errors<-c(errors,list(error_dat))

# Update fields associated with varieties (this is not a duplicate of the above, here we are match on the V.Var1 not V.Var column in the Mastercode vars tab)
mvars<-unique(master_codes$vars[!is.na(V.Animal.Practice),
                                list(V.Product,V.Var1,V.Animal.Practice,V.Type)])

mvars[,N:=.N,by=list(V.Product,V.Var1)
      ][N==1
        ][,N:=NULL]

mergedat<-merge(Var.Out[,list(V.Product,V.Level.Name)],mvars,by.x=c("V.Product","V.Level.Name"),by.y=c("V.Product","V.Var1"),all.x=T,sort=F)

Var.Out[!is.na(mergedat$V.Animal.Practice),V.Animal.Practice:=mergedat[!is.na(V.Animal.Practice),V.Animal.Practice]
][!is.na(mergedat$V.Type),V.Type:=mergedat[!is.na(V.Type),V.Type]]

# Non-matching varieties
error_dat<- Var.Out[is.na(V.Var1) & !is.na(V.Var) & !grepl("local|unspecified|unimproved|[*][*][*]",V.Var,ignore.case=T),
][,.(value=paste0(V.Product,":",V.Var)),by=B.Code
][,table:="Var.Out"
][,field:="V.Var"
][,issue:="No match for variety in era_master_sheet vars tab."]

mvars[,value:=paste0(V.Product,":",V.Var1)][,check:=T]
error_dat<-merge(error_dat,mvars[,.(value,check)],by="value",all.x=T,sort=F)
error_dat<-error_dat[is.na(check)][,check:=NULL]

errors<-c(errors,list(error_dat))

 # 3.6.2) Save errors #######
  errors<-rbindlist(errors,use.names = T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

# 3.7) Diet ####
  # 3.7.1) Animals.Out ######
  table_name<-"Animals.Out"
  data<-lapply(XL,"[[",table_name)
  col_names<-colnames(data[[1]][,1:19])
  
  Animals.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    setnames(X,"A.Level.Name...1","A.Level.Name",skip_absent = T)
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
    }else{
      X<-X[,..col_names]
      X<-X[!is.na(A.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }
    }})
  
  errors<-list(rbindlist(lapply(Animals.Out,"[[","error")))
  
  Animals.Out<-rbindlist(lapply(Animals.Out,"[[","data"))
  
  # Rename cols
  p_names_new<- c("A.Feed.Add.1","A.Feed.Add.2","A.Feed.Add.3", "A.Feed.Add.C", "A.Feed.Sub.1","A.Feed.Sub.2","A.Feed.Sub.3",
              "A.Feed.Sub.C","A.Feed.Pro.1","A.Feed.Pro.2","A.Feed.Pro.3","A.Manure.Man","A.Pasture.Man","A.Aquasilvaculture")
  
  p_names_old<-paste0("P",1:14)
  setnames(Animals.Out,p_names_old,p_names_new)
  
  zero_cols<-c(p_names_new,"A.Notes","A.Grazing","A.Hay")
  
  results<-validator(data=Animals.Out,
                     zero_cols =zero_cols,
                     unique_cols = "A.Level.Name",
                     trim_ws = T,
                     tabname=table_name)
  
  errors<-c(errors,list(results$errors))
  Animals.Out<-results$data

  # All NA rows 
  N<-apply(Animals.Out[,!c("A.Level.Name","B.Code")],1,FUN = function(x){all(is.na(x))})
  error_dat<-Animals.Out[N,.(value=paste(A.Level.Name,collapse = "/")),by=B.Code
  ][,table:=table_name
  ][,field:="A.Level.Name"
  ][,issue:="Possible error, row is entirely NA."
  ][value!="Base"]
  
  errors<-c(errors,list(error_dat))
  
  # Errors -  Feed Add Code but no control or >1 control
  Animals.Out<-Animals.Out[A.Level.Name!="Base",feed_add_prac:=sum(!is.na(A.Feed.Add.1),!is.na(A.Feed.Add.2),!is.na(A.Feed.Add.3))>1,by=.(B.Code,A.Level.Name)
                 ][A.Level.Name!="Base",feed_add_cont:=sum(A.Feed.Add.C=="Yes",na.rm=T),by=.(B.Code)]
  
  error_dat<-Animals.Out[feed_add_cont>1 & feed_add_prac==T,list(value=paste0(A.Level.Name,collapse = "/")),by=B.Code
                                            ][,table:=table_name
                                            ][,field:="A.Level.Name"
                                            ][,issue:="Possible error, there is more than 1 control present feed addition practice(s)."]
  
  errors<-c(errors,list(error_dat))

    # Errors - Feed Sub Code but no control
  Animals.Out<-Animals.Out[A.Level.Name!="Base",feed_sub_prac:=sum(!is.na(A.Feed.Sub.1),!is.na(A.Feed.Sub.2),!is.na(A.Feed.Sub.3))>1,by=.(B.Code,A.Level.Name)
    ][A.Level.Name!="Base",feed_sub_cont:=sum(A.Feed.Sub.C=="Yes",na.rm=T),by=.(B.Code)]
    
    error_dat<-Animals.Out[feed_sub_cont==0 & feed_sub_prac==T,list(value=paste0(A.Level.Name,collapse = "/")),by=B.Code
    ][,table:=table_name
    ][,field:="A.Level.Name"
    ][,issue:="Possible error, there is no control present for feed substitution practice(s)."]
    
    errors<-c(errors,list(error_dat))
    
    # Errors - Animal diet has been specified but there are no practices listed
    cols<-zero_cols[!zero_cols %in% "A.Notes"]
    error_dat <- Animals.Out[, lapply(.SD, function(col) sum(!is.na(col))), by = list(A.Level.Name, B.Code), .SDcols = cols]
    error_dat<-data.table(B.Code=error_dat$B.Code,A.Level.Name=error_dat$A.Level.Name,value=rowSums(error_dat[,..cols]))[value==0 & A.Level.Name!="Base"]
    error_dat<-error_dat[,.(value=paste(A.Level.Name,collapse = "/")),by=B.Code
            ][,table:=table_name
              ][,field:="A.Level.Name"
                ][,issue:="Possible error, an animal diet has no associated practices."]
    
    errors<-c(errors,list(error_dat))
    
    # Tidy control cols
    Animals.Out<-Animals.Out[is.na(A.Feed.Sub.C),A.Feed.Sub.C:="No"][is.na(A.Feed.Add.C),A.Feed.Add.C:="No"]

  # 3.7.2) Animal.Diet ####
  table_name<-"Animals.Diet"
  col_names<-colnames(data[[1]][,21:34])
  
  Animals.Diet<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
    }else{
      X<-X[,..col_names]
      setnames(X,c("A.Level.Name...21","D.Item...23"),c("A.Level.Name","D.Item"),skip_absent = T)
      X<-X[!is.na(A.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }
    }})
  
  error_dat<-rbindlist(lapply(Animals.Diet,"[[","error"))
  errors<-c(errors,list(error_dat))
  
  Animals.Diet<-rbindlist(lapply(Animals.Diet,"[[","data"))
  
  zero_cols<-c("D.Item","D.Item.Group","D.Source","D.Process","D.Amount","D.Ad.lib","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","D.Day.Start","D.Day.End","DC.Is.Dry")
  
  results<-validator(data=Animals.Diet,
                     zero_cols =zero_cols,
                     numeric_cols=c("D.Amount"),
                     numeric_ignore_vals="Unspecified",
                     unit_pairs = data.table(unit="D.Unit.Amount",var="D.Amount",name_field="A.Level.Name"),
                     compulsory_cols = c(A.Level.Name="D.Type"),
                     duplicate_field="D.Item",
                     check_keyfields=data.table(parent_tab=list(Animals.Out),
                                                parent_tab_name="Animals.Out",
                                                keyfield="A.Level.Name"),
                     trim_ws = T,
                     tabname=table_name)

  
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Animals.Diet<-results$data
  Animals.Diet[,D.Item.Raw:=D.Item]
  
    # Remove \n and \r
  Animals.Diet[,D.Item:=gsub("\r\n"," ",D.Item,fixed = T)]
  
  # Change "NA" to NA
  Animals.Diet[D.Item=="NA",D.Item:=NA]
  
  # Tidy diet process field
  Animals.Diet[,D.Process:=trimws(D.Process)
               ][,D.Process:=gsub(" /","/",D.Process)
                 ][,D.Process:=gsub("/ ","/",D.Process)
                   ][D.Process=="",D.Process:=NA]
  
  # Create a merged process and diet item name field for harmonization.
  Animals.Diet[,D.ItemxProcess:=D.Item][!is.na(D.Process) & !is.na(D.Item),D.ItemxProcess:=paste0(D.Item,"||",D.Process)]
  # write.table(Animals.Diet[!is.na(D.Process),.(B.Code=paste0(unique(B.Code),collapse ="/")),by=D.ItemxProcess],"clipboard",row.names = F,sep="\t")
  
  # Convert diet process to list
  Animals.Diet[,D.Process:=strsplit(D.Process,"/")]
  
  # Add logic to indicate if a compound diet item
  Animals.Diet[,D.Is.Group:=D.Type %in% D.Item.Group,by=B.Code]
  
  # Error where the entire diet is not being described and is.na(Diet.Item)
  error_dat<-Animals.Diet[D.Type!="Entire Diet" & is.na(D.Item) & !D.Is.Group,
                        ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
                             ][,table:=table_name
                               ][,field:="A.Level.Name"
                                 ][,issue:="Rows in have no diet item selected and diet type is not Entire Diet or a diet group value."]
  
  errors<-c(errors,list(error_dat))
  
    # 3.7.2.1) Harmonization #######
    # Units
    h_params<-data.table(h_table=table_name,
                         h_field=c("D.Unit.Amount","D.Unit.Time","D.Unit.Animals"),
                         ignore_vals=c("unspecified","unspecified","unspecified"))[,c("h_field_alt","h_table_alt"):=NA]
            
    
    results<-harmonizer_wrap(data=Animals.Diet,
                             h_params=h_params,
                             master_codes = master_codes)
    
    Animals.Diet<-results$data
    h_tasks<-list(results$h_tasks)
    
    # Insert updated diet naming system 
    mergedat<-master_codes$ani_diet[order(D.Item),.(D.Item,
                                               D.Item.Root.Comp,
                                               D.Item.Root.Comp.Proc_Major,
                                               D.Item.Root.Other.Comp.Proc_All,
                                               D.Item.Other,
                                               D.Item.Proc_All,
                                               D.Item.Proc_Major,
                                               D.Item.Proc_Minor,
                                               D.Item.Comp,
                                               D.Item.AOM
                                               )]
    
    vals<-strsplit(mergedat$D.Item,";")
    vals_rep_rows<-rep(1:length(vals),unlist(lapply(vals,length)))
    mergedat<-unique(mergedat[vals_rep_rows
                              ][,D.Item:=unlist(vals)
                                ][,D.Item:=trimws(tolower(D.Item))])
      
    # Find non-unique diet items that will cause matching issues
    error_dat<-mergedat[,N:=.N,by=D.Item][N>1]
    excluded_items<-error_dat[,unique(D.Item)]
    error_dat<-error_dat[,D.Item2:=D.Item
              ][,B.Code:=Animals.Diet[D.Item==D.Item2[1],paste(unique(B.Code),collapse = "/")],by=D.Item2
                ][,value:=paste(D.Item,"-",D.Item.Root.Comp.Proc_Major)
                  ][,.(B.Code,value)
                    ][,table:="era_master_sheet/ani_diets"
                      ][,field:="D.Item-D.Item.Root.Comp.Proc_Major"
                        ][,issue:="Multiple rows for unique value of D.Item exist."]
    
    errors<-c(errors,list(error_dat))
    
    mergedat<-unique(mergedat)[,N:=.N,by=D.Item][N==1][,N:=NULL]
    
    # Make fields lower case to improve odds of matching
    Animals.Diet[,D.ItemxProcess_low:=tolower(D.ItemxProcess)]
    
    mergedat[,check:=T]
    
    # Merge new names
    ani_diet_cols<-c(colnames(Animals.Diet),"index")
    Animals.Diet<-merge(Animals.Diet,mergedat,by.x="D.ItemxProcess_low",by.y="D.Item",all.x=T,sort=F)
    Animals.Diet[,index:=1:.N]#[,D.Item_raw:=D.Item]
    
    # Merge on uncorrected names where no match exists
    Animals.Diet_nomatch<-Animals.Diet[!is.na(D.Item) & is.na(check) & !D.Item %in% excluded_items,..ani_diet_cols]
    Animals.Diet_match<-Animals.Diet[!(!is.na(D.Item) & is.na(check) & !D.Item %in% excluded_items)]
    
    mergedat[,D.Item:=trimws(tolower(D.Item.Root.Other.Comp.Proc_All))]
    Animals.Diet_nomatch<-merge(Animals.Diet_nomatch,mergedat,by.x="D.ItemxProcess_low",by.y="D.Item",all.x=T,sort=F)
    #Animals.Diet_nomatch[,D.Item_raw:=D.Item]
    
    Animals.Diet<-rbind(Animals.Diet_match,Animals.Diet_nomatch)[order(index)][,index:=NULL]
    
    # Use check than D.Item.Root.Other.Comp.Proc_All
    
    error_dat<-Animals.Diet[!is.na(D.Item) & 
                              is.na(check) & 
                              !D.Item %in% excluded_items,.(B.Code=paste0(unique(B.Code),collapse = "/")),
                            by=D.ItemxProcess
              ][,value:=D.ItemxProcess
                ][,D.ItemxProcess:=NULL
                  ][,table:=table_name
                    ][,field:="D.Item"
                      ][,issue:="No-match between excel D.Item and era_master_sheet/ani_diet/D.Item"]
    
    errors<-c(errors,list(error_dat))
    
    # Check for duplicate value issues (where one D.Item links to more than one harmonized name)
    # This is where there is multiple entries of a Diet.Item with different processes in one diet so each D.Item has > 1 D.Item x D.Process
    d.item_dups<-unique(Animals.Diet[,.(D.Item,B.Code,D.Item.Root.Other.Comp.Proc_All)])[,N:=.N,by=.(D.Item,B.Code)][N>1]
    
    # This is not necessarily and error, so withheld from error checking for now
    error_dat<-d.item_dups[,.(value=paste0(D.Item ," = ",paste(D.Item.Root.Other.Comp.Proc_All,collapse = "/"))),by=.(B.Code,D.Item)
                                     ][,.(B.Code=paste(unique(B.Code),collapse = "/")),by=value
                                       ][,table:=table_name
                                         ][,field:="D.Item = D.Item.Root.Other.Comp.Proc_All"
                                           ][,issue:="Multiple matches between D.Item in Composition table and Diet Description table."]
    errors<-c(errors,list(error_dat))
    
    
    # write.table(h_dat[,.(value,B.Code)],"clipboard-256000",row.names = F,sep="\t",col.names = F)
    Animals.Diet[,check:=NULL]
    
    # 3.7.2.2) Merge AOM Diet Summary with Animals.Out (.inc Trees)  #######
    
    # Merge relevant AOM columns
    cols<-c("AOM","Scientific Name",paste0("L",1:10))
    merge_dat<-master_codes$AOM[AOM %in% Animals.Diet$D.Item.AOM &!is.na(AOM),..cols]
    setnames(merge_dat,"Scientific Name","AOM.Scientific.Name")
    merge_dat[,AOM.Terms:=apply(merge_dat[,!"AOM"],1,FUN=function(x){
      x<-as.vector(na.omit(x))
      paste(x,collapse="/")})
    ][,AOM.Terms:=unlist(tstrsplit(AOM.Terms,"Feed Ingredient/",keep=2))]
    
    merge_dat<-unique(merge_dat[,.(AOM,AOM.Terms,AOM.Scientific.Name)])
    
    Animals.Diet<-merge(Animals.Diet,merge_dat,by.x="D.Item.AOM",by.y="AOM",all.x=T,sort=F)
    
    Animals.Diet[,D.Item.Is.Tree:=F][grepl("Forage Trees",AOM.Terms),D.Item.Is.Tree:=T]

    # Summarize 
    Animals.Diet.Summary<-Animals.Diet[,.(A.Diet.Trees=paste0(sort(unique(AOM.Scientific.Name[D.Item.Is.Tree & !D.Is.Group])),collapse=";"),
                                          A.Diet.Other=paste0(sort(unique(basename(AOM.Terms)[!D.Item.Is.Tree & !D.Is.Group])),collapse=";")),
                                       by=.(B.Code,A.Level.Name)]
    
    Animals.Out<-merge(Animals.Out,Animals.Diet.Summary,by=c("B.Code","A.Level.Name"),all.x=T,sort=F)
    Animals.Out[A.Diet.Trees=="",A.Diet.Trees:=NA][A.Diet.Other=="",A.Diet.Other:=NA]
    
  # 3.7.3) Animals.Diet.Comp ######
  table_name<-"Animals.Diet.Comp"
  col_names<-colnames(data[[1]][,36:126])
  
  (numeric_cols<-col_names[2:30])
  unit_cols<-paste0(numeric_cols[numeric_cols!="DC.CN"],".Unit")
  method_cols<-paste0(numeric_cols,".Method")
  copy_down_cols<-c(unit_cols,method_cols,"DC.Unit.Is.Dry")
  
  fun1<-function(x){x[1]}
  
  Animals.Diet.Comp<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
    }else{
      X<-X[,..col_names]
      setnames(X,c("D.Item...36"),c("D.Item"),skip_absent = T)
      X[,c("...66","...95","...125"):=NULL]
      X<-X[!is.na(D.Item)]
      if(nrow(X)>0){
        X <- X[, (copy_down_cols) := lapply(.SD,fun1), .SDcols = copy_down_cols]
        X[,B.Code:=B.Code]
        list(data=X)
      }
    }})
  
  error_dat<-rbindlist(lapply(Animals.Diet.Comp,"[[","error"))
  errors<-c(errors,list(error_dat))
  
  Animals.Diet.Comp<-rbindlist(lapply(Animals.Diet.Comp,"[[","data"))
  
  # Run standard validation
  unit_pairs<-data.table(unit=paste0(numeric_cols,".Unit"),var=numeric_cols,name_field="D.Item")
  unit_pairs<-unit_pairs[var!="DC.CN"]
  
  results<-validator(data=Animals.Diet.Comp,
                     zero_cols =colnames(Animals.Diet.Comp),
                     numeric_cols=numeric_cols,
                     unit_pairs = unit_pairs,
                     compulsory_cols = c(D.Item="D.Item"),
                     duplicate_field="D.Item",
                     trim_ws = T,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Animals.Diet.Comp<-results$data

  # Set non-numerics cols to character
  non_numeric_cols<-colnames(Animals.Diet.Comp)[!colnames(Animals.Diet.Comp) %in% numeric_cols]
  non_numeric_cols<-non_numeric_cols[!non_numeric_cols %in% c("is_group","is_entire_diet")]
  Animals.Diet.Comp <- Animals.Diet.Comp[, (non_numeric_cols) := lapply(.SD, as.character), .SDcols = non_numeric_cols]
  
  # Add columns to indicate if a "compound" diet item described by A.Level.Name or Diet.Group 
  diet_groups<-unique(Animals.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
  setnames(diet_groups,"D.Item.Group","D.Item")
  
  diet_entire<-unique(Animals.Out[,.(B.Code,A.Level.Name)][,is_entire_diet:=T])
  setnames(diet_entire,"A.Level.Name","D.Item")
  
  Animals.Diet.Comp<-merge(Animals.Diet.Comp,diet_groups,all.x=T)[is.na(is_group),is_group:=F]
  Animals.Diet.Comp<-merge(Animals.Diet.Comp,diet_entire,all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
  
    # 3.7.3.1) Harmonization #######

  # Merge in updated name from Animals.Diet table
  merge_dat<-unique(Animals.Diet[,.(D.Item.Raw,B.Code,D.Item.Root.Comp.Proc_Major,D.Item.Root.Other.Comp.Proc_All,D.Item.AOM)])
  # Remove any duplicate rows (see error "Multiple matches between D.Item in Composition table and Diet Description table.")
  merge_dat<-merge_dat[!duplicated(merge_dat[,.(B.Code,D.Item.Raw)])][,check:=T]
  merge_dat[,D.Item.Raw:=tolower(D.Item.Raw)]
  
  Animals.Diet.Comp[,D.Item_lc:=tolower(D.Item)]
  Animals.Diet.Comp<-merge(Animals.Diet.Comp,merge_dat,by.x=c("D.Item_lc","B.Code"),by.y=c("D.Item.Raw","B.Code"),all.x=T,sort=F)
  Animals.Diet.Comp[,D.Item_lc:=NULL]
  
  # Check for non-matches
  error_dat<-Animals.Diet.Comp[!(is_group) & 
                                 !(is_entire_diet) & 
                                 is.na(check),.(B.Code,D.Item)
                               ][,.(value=paste(D.Item,collapse = "/")),by=B.Code
                                 ][,table:=table_name
                                   ][,field:="D.Item"
                                     ][,issue:="No matching value in diet description table."]
  errors<-c(errors,list(error_dat))
  
  Animals.Diet.Comp[,check:=NULL]
  
  # Units
  target_cols<-grep(".Unit",col_names,value=T)
  h_params<-data.table(h_table=table_name,
                       h_field=target_cols,
                       h_field_alt=rep("DC.Unit",length(target_cols)),
                       h_table_alt=rep("Animals.Diet.Comp",length(target_cols)),
                       ignore_vals=rep("unspecified",length(target_cols)))[h_field!="DC.Unit.Is.Dry"]
  
  results<-harmonizer_wrap(data=Animals.Diet.Comp,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks<-c(h_tasks,list(results$h_tasks))
  Animals.Diet.Comp<-results$data
  
  # Methods
  target_cols<-grep(".Method",col_names,value=T)
  h_params<-data.table(h_table=table_name,
                       h_field=method_cols,
                       h_field_alt=rep("DC.Method",length(target_cols)),
                       h_table_alt=rep("Animals.Diet.Comp",length(target_cols)))
  
  
  results<-harmonizer_wrap(data=Animals.Diet.Comp,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks<-c(h_tasks,list(results$h_tasks))
  Animals.Diet.Comp<-results$data
    # 3.7.4.2) Wrangle into long form #####
  if(!is.null(Animals.Diet.Comp)){
    if(nrow(Animals.Diet.Comp)>0){
      col_names<-colnames(Animals.Diet.Comp)
      
      other_cols<-col_names[!col_names %in% c(numeric_cols,method_cols,unit_cols)]
      unit_cols<-append(unit_cols, "DC.CN.Unit", after = 4)
      Animals.Diet.Comp[,DC.CN.Unit:=NA]
      cols<-data.table(numeric_cols,unit_cols,method_cols)
      colnames(cols)<-c("DC.Value","DC.Unit","DC.Method")
      
      Animals.Diet.Comp<-rbindlist(lapply(1:nrow(cols),FUN=function(i){
        target_cols<-unlist(cols[i])
        cols_selected<-c(other_cols,target_cols)
        variable<-gsub("DC.","",target_cols[1])
        dat<-Animals.Diet.Comp[,cols_selected,with=F][,DC.Variable:=variable]
        setnames(dat,target_cols,names(target_cols))
        return(dat)
      }))
    }else{
      Animals.Diet.Comp<-NULL
    }
  }
  
  Animals.Diet.Comp<-Animals.Diet.Comp[!is.na(DC.Value)]

  
  # 3.7.4) Animals.Diet.Digest ######
  table_name<-"Animals.Diet.Digest"
  col_names<-colnames(data[[1]][,127:208])
  col_names[col_names=="D.Item...129"]<-"D.Item"
  col_names<-col_names[!grepl("...",col_names,fixed = T)]
  
  numeric_cols<-col_names[3:21]
  unit_cols<-paste0(numeric_cols,".Unit")
  method_cols<-paste0(numeric_cols,".Method")
  dv_cols<-paste0(numeric_cols,".DorV")
  copy_down_cols<-c(unit_cols,method_cols,dv_cols,"DD.Unit.Is.Dry")
  
  fun1<-function(x){x[1]}
  
  Animals.Diet.Digest<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    setnames(X,c("D.Item...129"),c("D.Item"),skip_absent = T)
    
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
    }else{
      X<-X[,..col_names]
      X<-X[!is.na(D.Item)]
      if(nrow(X)>0){
        X <- X[, (copy_down_cols) := lapply(.SD,fun1), .SDcols = copy_down_cols]
        X[,B.Code:=B.Code]
        list(data=X)
      }
    }})
  
  error_dat<-rbindlist(lapply(Animals.Diet.Digest,"[[","error"))
  errors<-c(errors,list(error_dat))
  
  Animals.Diet.Digest<-rbindlist(lapply(Animals.Diet.Digest,"[[","data"))
  
  # Run standard validation
  unit_pairs<-data.table(unit=unit_cols,var=numeric_cols,name_field="D.Item")
  
  results<-validator(data=Animals.Diet.Digest,
                     zero_cols =colnames(Animals.Diet.Digest),
                     numeric_cols=numeric_cols,
                     unit_pairs = unit_pairs,
                     compulsory_cols = c(D.Item="D.Item"),
                     duplicate_field="D.Item",
                     trim_ws = T,
                     tabname=table_name)
  
  errors<-c(errors,list(results$errors))
  Animals.Diet.Digest<-results$data
  
  # Set non-numerics cols to character
  non_numeric_cols<-colnames(Animals.Diet.Digest)[!colnames(Animals.Diet.Digest) %in% numeric_cols]
  Animals.Diet.Digest <- Animals.Diet.Digest[, (non_numeric_cols) := lapply(.SD, as.character), .SDcols = non_numeric_cols]
  
  # Add columns to indicate if a "compound" diet item described by A.Level.Name or Diet.Group 
  diet_groups<-unique(Animals.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
  setnames(diet_groups,"D.Item.Group","D.Item")
  
  diet_entire<-unique(Animals.Out[,.(B.Code,A.Level.Name)][,is_entire_diet:=T])
  setnames(diet_entire,"A.Level.Name","D.Item")
  
  Animals.Diet.Digest<-merge(Animals.Diet.Digest,diet_groups,all.x=T)[is.na(is_group),is_group:=F]
  Animals.Diet.Digest<-merge(Animals.Diet.Digest,diet_entire,all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
  

    # 3.7.4.1) Harmonization #######

  Animals.Diet.Digest[,D.Item_lc:=tolower(D.Item)]
  Animals.Diet.Digest<-merge(Animals.Diet.Digest,merge_dat,by.x=c("D.Item_lc","B.Code"),by.y=c("D.Item.Raw","B.Code"),all.x=T,sort=F)
  Animals.Diet.Digest[,D.Item_lc:=NULL]
  
  # Check for non-matches
  error_dat<-Animals.Diet.Digest[!(is_group) & !(is_entire_diet) & is.na(check),.(B.Code,D.Item)][,.(value=paste(D.Item,collapse = "/")),by=B.Code
  ][,table:=table_name
  ][,field:="D.Item"
  ][,issue:="No matching value in diet description table."]
  errors<-c(errors,list(error_dat))
  
  Animals.Diet.Digest[,check:=NULL]
  
  # Units
  h_params<-data.table(h_table=table_name,
                       h_field=unit_cols,
                       h_field_alt=rep("DC.Unit",length(unit_cols)),
                       h_table_alt=rep("Animals.Diet.Comp",length(unit_cols)),
                       ignore_vals=rep("unspecified",length(unit_cols)))
  
  results<-harmonizer_wrap(data=Animals.Diet.Digest,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks<-c(h_tasks,list(results$h_tasks))
  
  Animals.Diet.Digest<-results$data
  
  # Methods
  h_params<-data.table(h_table=table_name,
                       h_field=method_cols,
                       h_field_alt=rep("DD.Method",length(method_cols)),
                       h_table_alt=rep("Animals.Diet.Digest",length(method_cols)))
  
  
  results<-harmonizer_wrap(data=Animals.Diet.Digest,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks<-c(h_tasks,list(results$h_tasks))
  Animals.Diet.Digest<-results$data
  
    # 3.7.4.2) Wrangle into long form #####
  if(!is.null(Animals.Diet.Digest)){
    if(nrow(Animals.Diet.Digest)>0){
      col_names<-colnames(Animals.Diet.Digest)
      
      other_cols<-col_names[!col_names %in% c(numeric_cols,method_cols,dv_cols,unit_cols)]
      cols<-data.table(numeric_cols,unit_cols,dv_cols,method_cols)
      colnames(cols)<-c("DD.Value","DD.Unit","DD.Nut.or.Diet","DD.Method")
      
      Animals.Diet.Digest<-rbindlist(lapply(1:nrow(cols),FUN=function(i){
        target_cols<-unlist(cols[i])
        cols_selected<-c(other_cols,target_cols)
        variable<-gsub("DD.","",target_cols[1])
        dat<-Animals.Diet.Digest[,cols_selected,with=F][,DD.Variable:=variable]
        setnames(dat,target_cols,names(target_cols))
        return(dat)
      }))
    }else{
      Animals.Diet.Digest<-NULL
    }
  }
  
  Animals.Diet.Digest<-Animals.Diet.Digest[!is.na(DD.Value)]
  
    # 3.7.4.3) If focus is diet then values cannot add up to more 100% #####
  # Note that DM should not be included
  Animals.Diet.Digest[DD.Nut.or.Diet=="Diet or Item",DD.Nut.or.Diet:="Diet"]
  if(!is.null(Animals.Diet.Digest)){
    Animals.Diet.Digest[,DD.Value_sum:=sum(DD.Value[!DD.Variable %in% c("DM","OM")],na.rm=T),by=.(D.Item,DD.Unit,DD.Nut.or.Diet)]
    
    error_dat<-unique(Animals.Diet.Digest[DD.Nut.or.Diet=="Diet" & ((DD.Value_sum>100 & DD.Unit=="%")|(DD.Value_sum>1 & DD.Unit=="g/kg|mg/g")),
                                         .(B.Code,D.Item,DD.Unit)])
    
    error_dat<-error_dat[,.(value=paste(unique(D.Item),collapse = "/")),by=B.Code
    ][,table:=table_name
    ][,field:="D.Item"
    ][,issue:="Focus of digestibility is diet, but values across the diet item or diet sum to more than 100 for % units or more than 1 for g/kg units"]
    
    errors<-c(errors,list(error_dat))
    
    Animals.Diet.Digest[,DD.Value_sum:=NULL]
  }
  
  # 3.7.5) Update D.Item field with harmonized names ######
  Animals.Diet[,D.Item.raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
  Animals.Diet.Comp[,D.Item.raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
  Animals.Diet.Digest[,D.Item.raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
  
  # 3.7.6) Save errors & harmonization #######
  error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                            filename = paste0("diet_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
  harmonization_list<-error_tracker(errors=rbindlist(h_tasks),
                                    filename = "diet",
                                    error_dir=harmonization_dir,
                                    error_list = harmonization_list)
  
# 3.8) Agroforestry #####
table_name<-"AF.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[1]])

  # 3.8.1) AF.Out######
  col_names2<-col_names[c(1:4)]
  
  AF.Out<-pblapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      Filename<-basename(names(XL)[i])
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(errors=data.table(B.Code=B.Code,
                               value=NA,
                               table=table_name,
                               field=NA,
                               issue="Problem with tab structure"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[1]<-"AF.Level.Name"
        X<-X[!is.na(AF.Level.Name)][,B.Code:=B.Code]
        list(data=X)
        }
    })
  
  error_dat<-rbindlist(lapply(AF.Out,"[[","errors"))
  errors<-list(error_dat)

  AF.Out<-rbindlist(lapply(AF.Out,"[[","data"))
  
  # 3.8.3) Save errors
  error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
# 3.9) Chemicals #####
data<-lapply(XL,"[[","Chems.Out")
col_names<-colnames(data[[1]])
table_name<-"Chems.Code"

  # 3.9.1) Chems.Code ####
col_names2<-col_names[1:2]

Chems.Code<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names2 %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[,..col_names2]
    colnames(X)[1]<-"C.Level.Name"
    X<-X[!is.na(C.Level.Name)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors<-list(rbindlist(lapply(Chems.Code,"[[","errors")))
Chems.Code<-rbindlist(lapply(Chems.Code,"[[","data"))

error_dat<-validator(data=Chems.Code,
                   unique_cols = "C.Level.Name",
                   tabname="Chems.Code")$errors

errors<-c(errors,list(error_dat))

  # 3.9.2) Chem    
    # 3.9.2.1) Harmonization #######
col_names2<-col_names[4:17]
table_name<-"Chems.Out"

Chems.Out<-pblapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]

  if(!all(col_names2 %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[,..col_names2]
    colnames(X)[1]<-"C.Level.Name"
    X<-X[!is.na(C.Level.Name)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

error_dat<-rbindlist(lapply(Chems.Out,"[[","errors"))
errors<-c(errors,list(error_dat))

Chems.Out<-rbindlist(lapply(Chems.Out,"[[","data"))
setnames(Chems.Out,"C.Brand","C.Name")
Chems.Out[,C.Type:=gsub("Animal - ","",C.Type)]

results<-validator(data=Chems.Out,
                   zero_cols = colnames(Chems.Out),
                   compulsory_cols = c(C.Level.Name="C.Type",C.Level.Name="C.Name"),
                   numeric_cols=c("C.Amount","C.AI.Amount","C.Applications"),
                   numeric_ignore_vals="Unspecified",
                   unit_pairs = data.table(unit=c("C.AI.Unit","C.Unit"),
                                           var=c("C.AI.Amount","C.Amount"),
                                           name_field="C.Level.Name"),
                   check_keyfields=data.table(parent_tab=list(Chems.Code),
                                              parent_tab_name="Chems.Code",
                                              keyfield="C.Level.Name"),
                    tabname=table_name,
                   duplicate_field="C.Level.Name",
                   trim_ws = T)

error_dat<-results$errors
errors<-c(errors,list(error_dat))
Chems.Out<-results$data


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
  # 3.9.3) Save errors and harmonization tasks ######
  error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                            filename = paste0("chems_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
  harmonization_list<-error_tracker(errors=rbindlist(h_tasks),
                                    filename = "chems",
                                    error_dir=harmonization_dir,
                                    error_list = harmonization_list)

# 3.10) Other ######
table_name<-"Other.Out"
data<-lapply(XL,"[[","Other.Out")
col_names<-colnames(data[[1]])

Other.Out<-pblapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[!is.na(O.Level.Name)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors<-list(rbindlist(lapply(Other.Out,"[[","errors")))

Other.Out<-rbindlist(lapply(Other.Out,"[[","data"))  

Other.Out<-Other.Out[!(is.na(O.Notes) & is.na(O.Structure) & O.Level.Name=="Base")]
Other.Out<-Other.Out[!(O.Level.Name=="Base" & O.Structure=="No")]

results<-validator(data=Other.Out,
                   unique_cols = "O.Level.Name",
                   zero_cols = "O.Notes",
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
][,table:="Other.Out"
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
][,table:="Other.Out"
][,field:="O.Level.Name"
][,issue:=">1 other practice exists and comparison IS NOT allowed, check comparisons field is correct."]
errors<-c(errors,list(error_dat))

error_dat<-Other.Out[N==1][,N:=NULL
][,list(B.Code,O.Level.Name)
][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
][,value:=O.Level.Name
][,O.Level.Name:=NULL
][,table:="Other.Out"
][,field:="O.Level.Name"
][,issue:="One other practice exists, please check that the comparisons field is correctly assigned."]
errors<-c(errors,list(error_dat))

error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                          filename = paste0(table_name,"_errors"),
                          error_dir=error_dir,
                          error_list = error_list)

Other.Out[,N:=NULL][,No_struc:=NULL]

# 3.1) Base Practices (Base.Out) #####
Base.Out<-list(
  Var.Out[V.Base=="Yes" & !is.na(V.Codes),c("B.Code","V.Codes")],
  AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
  Animals.Out[A.Level.Name=="Base"& !is.na(A.Codes),c("B.Code","A.Codes")]
)

Base.Out<-rbindlist(Base.Out[unlist(lapply(Base.Out,nrow))>0],use.names = F)
Base.Out<-Base.Out[,list(Base.Codes=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

# 4) Treatments (MT.Out)  #####
table_name<-"MT.Out"
data<-lapply(XL,"[[",table_name)
col_names<-c("T.Name","T.Comp","T.Control","T.Reps","T.Animals","T.Start.Year","T.Start.Season",
             "A.Level.Name","AF.Level.Name...12","V.Level.Name","O.Level.Name","C.Level.Name")

MT.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[,..col_names]
    X<-X[!is.na(T.Name)]
    setnames(X,"AF.Level.Name...12","AF.Level.Name")
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors<-list(rbindlist(lapply(MT.Out,"[[","errors")))
MT.Out<-rbindlist(lapply(MT.Out,"[[","data"))

setnames(MT.Out,c("T.Comp"),c("P.Product"))

MT.Out[,T.Name:=gsub("[.][.] | [.][.]|  [.][.]|   [.][.]","..",T.Name)]

# Update V.Level.Name
merge_dat<-Var.Out[,.(B.Code,V.Level.Name,V.Level.Name_raw)]
colnames(merge_dat)[2:3]<-c("V.Level.Name_new","V.Level.Name")
MT.Out<-merge(MT.Out,merge_dat,by=c("B.Code","V.Level.Name"),all.x=T,sort=F)
MT.Out[!is.na(V.Level.Name_new),V.Level.Name:=V.Level.Name_new][,V.Level.Name_new:=NULL]


results<-validator(data=MT.Out,
                   numeric_cols = c("T.Reps","T.Animals","T.Start.Year"),
                   unique_cols = "T.Name",
                   zero_cols=c("T.Start.Year","T.Start.Season","A.Level.Name","AF.Level.Name","C.Level.Name","O.Level.Name","V.Level.Name","T.Animals","T.Reps"),
                   trim_ws = T,
                   check_keyfields=data.table(parent_tab=list(Animals.Out,AF.Out,Chems.Code,Other.Out,Var.Out),
                                              parent_tab_name=c("Animals.Out","AF.Out","Chems.Code","Other.Out","Var.Out"),
                                              keyfield=c("A.Level.Name","AF.Level.Name","C.Level.Name","O.Level.Name","V.Level.Name")),
                   compulsory_cols = c(T.Name="T.Name",T.Name="P.Product"),
                   duplicate_field = "T.Name",
                   duplicate_ignore_fields = c("T.Name"),
                   rm_duplicates=F,
                   tabname="MT.Out")

error_dat<-results$errors[!(grepl("[.][.]",value) & grepl("Duplicate rows",issue))]
errors<-c(errors,list(error_dat))

MT.Out<-results$data

# Convert T.Start.Year and T.Reps fields to integers
MT.Out[,T.Start.Year:=as.integer(T.Start.Year)][,T.Reps:=as.integer(T.Reps)][,T.Animals:=as.integer(T.Animals)]

# Explore missing replicate and/or animals per rep data
error_dat<-MT.Out[is.na(T.Animals),.(T.Name,B.Code,T.Reps,T.Animals)
                   ][,.(value=paste(unique(T.Name),collapse = "/")),by=B.Code
                     ][,table:=table_name
                       ][,field:="T.Rep/T/Animal"
                         ][,issue:= "Missing value in compulsory fields T.Reps/T.Animals"]

errors<-c(errors,list(error_dat))

  # 4.1) Merge in practice data #####
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
    }else{
      data<-merge(data,mergedat[[i]],by=c("B.Code",keyfield),all.x=T)
    }
    
    
    if(nrow(data)!=nrow(MT.Out)){
      cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
    }
  }

  # Add in Base.Out
  data<-merge(data,Base.Out,by="B.Code",all.x=T)
  if(nrow(data)!=nrow(MT.Out)){
    cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
  }
  
  MT.Out<-data
  
  # 4.2) Combine practice codes to create T.Codes ######
  code_cols<-c(AF.Level.Name="AF.Codes",
               A.Level.Name="A.Codes",
               V.Level.Name="V.Codes")
  
  t_codes<-apply(MT.Out[,..code_cols],1,FUN=function(x){
    x<-paste(sort(unique(unlist(strsplit(x,"-")))),collapse="-")
    x[x==""]<-NA
    x
  })
  
  MT.Out[,T.Codes:=t_codes]
  
  # 4.3) Combine aggregated treatments #####
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
      
      # Exclude Other, Chemical, Weeding or Planting Practice Levels if they do no structure outcomes.
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
  
  # 4.4) Update structure for aggregated treatments ####
  col_names<-grep("Structure",colnames(MT.Out),value=T)
  
  MT.Out <- MT.Out[, (col_names) := lapply(.SD, FUN=function(x){
    x[grepl("Yes",x,ignore.case = T)]<-"Yes"
    x[!grepl("Yes",x,ignore.case = T)]<-NA
    x
  }), .SDcols = col_names]
  
  # TO DO ADD BASE PRACTICE DATA? #####
  # 4.5) Save errors  ######
  error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)

# 5) Outcomes ####
table_name<-"Out.Out"
data<-lapply(XL,"[[","Out.Out")
col_names<-colnames(data[[1]])

Out.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[,..col_names]
    X<-X[!is.na(Out.Subind)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors<-list(rbindlist(lapply(Out.Out,"[[","error")))
Out.Out<-rbindlist(lapply(Out.Out,"[[","data"))


# Check outcome names match master codes
out_name_changes<-data.table(old_values=c("Nitrogen [(]Apparent Efficiency[)]","Carbon dioxide emissions","Aboveground Biomass"),
                             new_values=c('Nitrogen (Apparent Efficiency Animals Feed)',"Carbon Dioxide Emissions","Aboveground Carbon Biomass"))

parent<-master_codes$out[,list(Subindicator)][,Out.Subind:=Subindicator]
error_dat<-Out.Out[!Out.Out$Out.Subind %in% parent$Out.Subind & !Out.Subind %in% out_name_changes$old_values
][,value:=Out.Subind
][,list(B.Code=paste(B.Code,collapse = "/")),by=value
][,table:="Out.Out"
][,field:="Out.Subind"
][,issue:="Outcome not found in master codes."]
errors<-list(error_dat)

results<-validator(data=Out.Out,
                   numeric_cols=c("Out.Depth.Upper","Out.Depth.Lower","Out.NPV.Rate","Out.NPV.Time","Out.WG.Start","Out.WG.Days"),
                   ignore_values = c("Unspecified","Unpecified"),
                   zero_cols = colnames(Out.Out)[2:13],
                   compulsory_cols = c(Out.Code.Joined="Out.Unit"),
                   unit_pairs = data.table(unit="Out.WG.Unit",var="Out.WG.Start",name_field="Out.Code.Joined"),
                   hilo_pairs = data.table(low_col="Out.Depth.Upper",high_col="Out.Depth.Lower",name_field="Out.Code.Joined"),
                   unique_cols = "Out.Code.Joined",
                   duplicate_field = "Out.Code.Joined",
                   rm_duplicates = T,
                   trim_ws = T,
                   tabname="Out.Out")

error_dat<-results$errors
errors<-c(errors,list(error_dat))

Out.Out<-results$data

# Identical Out.Code.Joined due to Out.WG.Days missing from the text concatenation
error_dat<-Out.Out[,x:=paste(B.Code,Out.Code.Joined)][,N:=.N,by=x][N>1][,.(value=paste(unique(Out.Code.Joined),collapse="/")),by=B.Code][,table:=table_name][,field:="Out.Code.Joined"][,issue:="Non-unique Out.Code.Joined values due to Out.WG.Days values being missed in Out.Code.Joined name generation."]
errors<-c(errors,list(error_dat))

# Use setNames to map old values to new values
replacement_map <- setNames(out_name_changes$new_values, out_name_changes$old_values)
# Apply str_replace_all to the entire Out.Subind column using the named vector
Out.Out[, Out.Subind := stringr::str_replace_all(Out.Subind, replacement_map)]
Out.Out[, Out.Code.Joined := stringr::str_replace_all(Out.Code.Joined, replacement_map)]

Out.Out[,c("x","N"):=NULL]
  
  # 5.1) Harmonization #####
  # Harmonize Out.WG.Days 
  Out.Out[Out.WG.Days==488]  

  # 5.1.1) Intake Units ######
  Out.Out.Intake<-Out.Out[Out.Subind=="Feed Intake"]
  Out.Out.Not.Intake<-Out.Out[Out.Subind!="Feed Intake"]

  h_params<-data.table(h_table=table_name,
                       h_field=c("Out.Unit"),
                       ignore_vals=c("Unspecified","unspecified"),
                       h_field_alt="Out.FI.Unit",
                       h_table_alt=table_name)
  
  results<-harmonizer_wrap(data=Out.Out.Intake,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Out.Out.Intake<-results$data
  
  Out.Out.Intake[,table(Out.Unit)]
  
  error_dat<-unique(results$h_tasks)[order(value),.(B.Code,value,table,field)
                                     ][,issue:="Unexpected units, perhaps error or incomplete, used with feed intake outcome."]
  
  errors<-c(errors,list(error_dat))
  
  # 5.1.2) Update units in for non feed-intake ######
  h_params<-data.table(h_table=table_name,
                       h_field=c("Out.Unit"),
                       ignore_vals=c("Unspecified","unspecified"),
                       h_field_alt="Out.Unit.Correct",
                       h_table_alt=NA,
                       master_tab="unit_harmonization")

  results<-harmonizer_wrap(data=Out.Out.Not.Intake,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Out.Out.Not.Intake<-results$data
  
  # Copy results to clipboard to add to unit_harmonization table of mater codes
  if(F){
  cat(results$h_tasks[,paste0(unique(value),collapse="\t")], file = pipe("pbcopy"))
  }
  # Check results
  if(F){
  unique(data.table(new=results$data$Out.Code.Joined,
                    old=Out.Out.Not.Intake$Out.Code.Joined,
                    new1=results$data$Out.Unit,
                    old1=Out.Out.Not.Intake$Out.Unit))
    
    unique(data.table(new1=results$data$Out.Unit,
                      old1=Out.Out.Not.Intake$Out.Unit))
  }
    
  # Recombine datasets
  Out.Out<-rbind(Out.Out.Intake,Out.Out.Not.Intake)

  # 5.2) Save errors  ######
  error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)

# 6) Enter Data (Data.Out) ####
table_name<-"Data.Out"
data<-lapply(XL,"[[",table_name)
col_names<-colnames(data[[100]])
col_names<-col_names[!col_names %in% c("ED.Int","ED.Rot","ED.I.Amount","ED.I.Unit","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start","ED.Harvest.End","ED.Harvest.DAS")]

Data.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(errors=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with tab structure"))
  }else{
    X<-X[,..col_names]
    X <- X[rowSums(is.na(X)) != ncol(X)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors<-list(rbindlist(lapply(Data.Out,"[[","errors")))
Data.Out<-rbindlist(lapply(Data.Out,"[[","data"),use.names = T)

# Update M.Year & Product 0 to NA
Data.Out[ED.M.Year=="0",ED.M.Year:=NA][ED.Product.Simple=="0",ED.Product.Simple:=NA]

# Convert mean T to numeric (there are floating point issues in the excel import that are resulting in non-matches)
Data.Out[,ED.Mean.T_raw:=ED.Mean.T][,ED.Mean.T:=round(as.numeric(ED.Mean.T),2)]
Data.Out[,ED.Mean.T:=sprintf("%.4f",ED.Mean.T)]
Data.Out[,ED.M.Year_raw:=ED.M.Year][,ED.M.Year:=round(as.numeric(ED.M.Year),1)]
Data.Out[,ED.M.Year:=sprintf("%.4f",ED.M.Year)]

merge_dat<-copy(enter_data_raw)
merge_dat[,ED.Mean.T:=round(as.numeric(ED.Mean.T),2)]
merge_dat[,ED.Mean.T:=sprintf("%.4f",ED.Mean.T)]
merge_dat[,ED.M.Year:=round(as.numeric(ED.M.Year),1)]
merge_dat[,ED.M.Year:=sprintf("%.4f",ED.M.Year)]

# Remove empty intake item column
Data.Out<-Data.Out[,ED.Intake.Item:=NULL]

# Merge in ED.Feed.Item from raw user data
merge_cols<-c("B.Code","ED.Site.ID","ED.Treatment","ED.Product.Simple","ED.M.Year","ED.Outcome","ED.Mean.T")
merge_dat<-unique(merge_dat[!is.na(ED.Intake.Item),!c("ED.Error","ED.Error.Type","ED.Data.Loc","ED.Int","ED.Rot","ED.Product.Comp")])
Data.Out<-merge(Data.Out,merge_dat,by=merge_cols,all.x=T,sort=F)
Data.Out[,ED.Intake.Item:=trimws(ED.Intake.Item)]

# Check for non-matches
error_dat<-Data.Out[grepl("Feed Intake",ED.Outcome) & is.na(ED.Intake.Item),.(B.Code,ED.Treatment,ED.Outcome,ED.Intake.Item)
                ][,(value=paste(unique(ED.Treatment),collapse="/")),by=B.Code
                  ][,table:=table_name
                    ][,field:="ED.Intake.Item"
                      ][,issue:="Feed intake outcome without feed intake item specified."]

if(F){
  # Investigate mismatches
  check<-"LM0114"
  merge_dat[B.Code==check & grepl("Feed",ED.Outcome),..merge_cols]
  Data.Out[B.Code==check & grepl("Feed In",ED.Outcome),..merge_cols]
  enter_data_raw[B.Code==check & grepl("Feed",ED.Outcome),..merge_cols]
}

# Reset Data.Out fields
Data.Out[,c("ED.Mean.T","ED.M.Year"):=NULL]
setnames(Data.Out,c("ED.Mean.T_raw","ED.M.Year_raw"),c("ED.Mean.T","ED.M.Year"))

# TO DO - add logical check to see if Feed Items are present for the A.Level.Name ####

  # 6.0) Clean, rename, validate ####
    # 6.0.1) Update Feed Item Names, Indicate if whole diet, diet group or single ingredient #####
    merge_dat<-unique(Animals.Diet[,.(B.Code,D.Item.Raw,D.Item)])
    Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Intake.Item"),by.y=c("B.Code","D.Item.Raw"),all.x=T,sort=F)
    Data.Out[,ED.Intake.Item.Raw:=ED.Intake.Item
             ][!is.na(D.Item),ED.Intake.Item:=D.Item]
    
    # Is intake item a group?
    merge_dat<-unique(Animals.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
    Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Intake.Item"),by.y=c("B.Code","D.Item.Group"),all.x=T,sort=F)
    Data.Out[is.na(is_group),is_group:=F]
    
    # Is intake item entire diet?
    merge_dat<-unique(Animals.Out[,.(B.Code,A.Level.Name)][,is_entire_diet:=T])
    Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Intake.Item"),by.y=c("B.Code","A.Level.Name"),all.x=T,sort=F)
    Data.Out[is.na(is_entire_diet),is_entire_diet:=F][ED.Intake.Item.Raw=="Entire Diet",is_entire_diet:=T]
    
    # If entire diet substitute the A.Level.Name for the diet
    merge_dat<-MT.Out[,.(B.Code,T.Name,A.Level.Name)]
    Data.Out<-merge(Data.Out,merge_dat,by.x=c("B.Code","ED.Treatment"),by.y=c("B.Code","T.Name"),all.x=T,sort=F)
    Data.Out[ED.Intake.Item.Raw=="Entire Diet",ED.Intake.Item:=A.Level.Name]
    
    # Check for any instances were we cannot match the intake item back to the diet table
    error_dat<-Data.Out[!is.na(ED.Intake.Item) & is.na(D.Item) & is_group==F & is_entire_diet==F
                        ][,.(value=paste(unique(ED.Intake.Item.Raw),collapse = "/")),by=B.Code
                          ][,table:=table_name
                            ][,field:="ED.Intake.Item.Raw"
                              ][,issue:="Feed intake item cannot be matched to diet, diet group, or diet ingredient."]
    
    errors<-c(errors,list(error_dat))
    
    # Tidy up
    Data.Out[,c("A.Level.Name","D.Item"):=NULL]
    
    # 6.0.2) Refine name, update key fields, run validation #####
    # Update column names 
    setnames(Data.Out,c("ED.Treatment","ED.Site.ID","ED.Outcome","ED.M.Year","ED.Product.Simple"),
             c("T.Name","Site.ID","Out.Code.Joined","Time","P.Product"))
    # Update outcome subind names
    Data.Out[, Out.Code.Joined := stringr::str_replace_all(Out.Code.Joined, replacement_map)]
    
    # Update Site.ID
    Data.Out[,Site.ID_new:=Site.Out$Site.ID[match(gsub("[.][.] | [.][.]|  [.][.]|   [.][.]","..",trimws(tolower(Data.Out$Site.ID))),tolower(Site.Out$Site.ID_raw))]] 
    Data.Out[is.na(Site.ID_new),Site.ID_new:=Site.ID][,Site.ID:=Site.ID_new][,Site.ID_new:=NULL]
    
    # Remove whitespace from aggregated treatment names
    Data.Out[,T.Name:=gsub("[.][.] | [.][.]|  [.][.]|   [.][.]","..",T.Name)]
    
    # Update aggregation delimiter
    Data.Out[,T.Name:=gsub("[.][.]","...",T.Name)]
    
    # Update error value used
    Data.Out[ED.Error.Type=="SEM (Standard Error of the Mean)",ED.Error.Type:="SE (Standard Error)"]
    
    # Check that sites match
    results<-validator(data=Data.Out,
                       zero_cols=c("Site.ID","P.Product","Time","T.Name","ED.Error","ED.Product.Comp","ED.Error.Type","ED.Data.Loc","ED.Intake.Item","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Reps","ED.Animals","ED.Variety","ED.Start.Year","ED.Start.Season","ED.Comparison"),
                       numeric_cols = c("ED.Mean.T","ED.Error","ED.Reps","ED.Sample.DAS","ED.Animals","ED.Start.Year"),
                       date_cols = c("ED.Sample.Start","ED.Sample.End"),
                       compulsory_cols = c(T.Name="Site.ID",T.Name="Out.Code.Joined",T.Name="ED.Mean.T",T.Name="P.Product"),
                       valid_start = valid_start,
                       valid_end = valid_end,
                       unit_pairs = data.table(unit="ED.Error.Type",var="ED.Error",name_field="Out.Code.Joined"),
                       check_keyfields=data.table(parent_tab=list(Out.Out,MT.Out),
                                                  parent_tab_name=c("Out.Out","MT.Out"),
                                                  keyfield=c("Out.Code.Joined","T.Name")),
                       ignore_values = "Unspecified",
                       site_data = Site.Out,
                       tabname=table_name,
                       trim_ws = T,
                       do_time=F,
                       convert_NA_strings=T)
    
    error_dat<-results$errors[!grepl("Unspecified",value)]
    errors[["validator"]]<-error_dat
    
    Data.Out<-results$data
    
    # Check component names used
    error_dat<-Data.Out[!ED.Product.Comp %in% master_codes$prod_comp$Component & !is.na(ED.Product.Comp)
    ][,value:=ED.Product.Comp
    ][,list(B.Code=paste(unique(B.Code),collapse = "/")),by=value
    ][,table:="Data.Out"
    ][,field:="ED.Product.Comp"
    ][,issue:="Product component value used does not match master codes prod_comp table."
    ][order(B.Code)]
    errors<-c(errors,list(error_dat))
    
    #Check for ratio outcomes that are missing the comparison "control" 
    error_dat<-Data.Out[grepl(master_codes$out[TC.Ratio=="Y",paste0(Subindicator,collapse = "|")],Out.Code.Joined) & is.na(ED.Comparison)
    ][,list(value=paste(unique(Out.Code.Joined),collapse = "/")),by=B.Code
    ][,table:="Data.Out"
    ][,field:="Out.Code.Joined"
    ][,issue:="Outcome derived from T vs C (e.g. LER) does not have comparison specified."
    ][order(B.Code)]
    errors<-c(errors,list(error_dat))
    
    h_tasks<-list(harmonizer(data=Data.Out, 
                         master_codes,
                         master_tab="lookup_levels",
                         h_table="Data.Out", 
                         h_field="ED.Error.Type",
                         h_table_alt="Data.Out", 
                         h_field_alt="Error.Type")$h_tasks[,issue:="Non-standard error value used."][value!="Unspecified"])
    
  # 6.1) Merge data from linked tables ####
  n_rows<-Data.Out[,.N]
    # 6.1.1) Merge MT.Out ######
    Data.Out<-merge(Data.Out,unique(MT.Out[,!c("P.Product")]),by=c("B.Code","T.Name"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    # 6.1.2) Merge Publication  ######
  Data.Out<-merge(Data.Out,Pub.Out,by=c("B.Code"),all.x=T,sort=F)
  stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    # 6.1.3) Merge Outcomes  ####
    # Temporary fix to deal with duplicated Out.Code.Joined, should be resolved when Out.WG.Days is included
    Out.Out<-Out.Out[!duplicated(Out.Out[,.(B.Code,Out.Code.Joined)])]
    
    Data.Out<-merge(Data.Out,unique(Out.Out),by=c("B.Code","Out.Code.Joined"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      # 6.1.3.1) Merge in outcome codes  #######
    merge_dat<-setnames(unique(master_codes$out[,.(Subindicator,Code)]),c("Subindicator","Code"),c("Out.Subind","Out.Code"))
    merge_dat[,N:=.N,by=Out.Subind][N>1]
    merge_dat[,N:=NULL]
    Data.Out<-merge(Data.Out,merge_dat,by=c("Out.Subind"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
  unique(Data.Out[is.na(Out.Code) & !B.Code %in% errors$validator[field=="Out.Code.Joined" & grepl("Mismatch in field value",issue),B.Code],.(B.Code,Out.Code.Joined)])

    # 6.1.4) Add Product Codes ######
    
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
    
     # 6.1.4.1) Merge using product x component code  #######
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
  
    # 6.1.5) Merge Experimental Design  ######
  Data.Out<-merge(Data.Out,ExpD.Out,by=c("B.Code"),all.x=T,sort=F)
  stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    # 6.1.6) Merge Site  ######
    Data.Out<-merge(Data.Out,unique(Site.Out[,!c("Site.ID_raw","check")]),by=c("B.Code","Site.ID"),all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    # Make Sure of match between Data.Out and Site.Out
    (error_dat<-unique(Data.Out[is.na(Country),.(B.Code,Site.ID)])[!B.Code %in% errors$site_mismatches$B.Code])
    
    # 6.1.7) Update Structure Fields to reflect Level name rather than "Yes" or "No" ####
    grep("[.]Structure$",colnames(Data.Out),value=T)
    Data.Out[O.Structure!="Yes",O.Structure:=NA][O.Structure=="Yes",O.Structure:=O.Level.Name]
    Data.Out[C.Structure!="Yes",C.Structure:=NA][C.Structure=="Yes",C.Structure:=C.Level.Name]

  # 6.2) Save errors #####
    error_list<-error_tracker(errors=rbindlist(errors,use.names = T),
                              filename = paste0(table_name,"_errors"),
                              error_dir=error_dir,
                              error_list = error_list)
    
  # 7) Save tables as a list  ####
  Tables<-list(
    Pub.Out=Pub.Out, 
    Site.Out=Site.Out, 
    Soil.Out=Soil.Out,
    ExpD.Out=ExpD.Out,
    Times.Out=Times.Out,
    Prod.Out=Prod.Out,
    Var.Out=Var.Out,
    Chems.Code=Chems.Code,
    Chems.Out=Chems.Out,
    AF.Out=AF.Out,
    Animals.Out=Animals.Out,
    Animals.Diet=Animals.Diet,
    Animals.Diet.Comp=Animals.Diet.Comp,
    Animals.Diet.Digest=Animals.Diet.Digest,
    Other.Out=Other.Out,
    MT.Out=MT.Out,
    Out.Out=Out.Out,
    Data.Out=Data.Out
  )
  
  save(Tables,file=file.path(data_dir,paste0(project,"-",Sys.Date(),".RData")))
