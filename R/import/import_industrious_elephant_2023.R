# Make sure you have set the era working directory using the 0_set_env.R script ####
## 0.0) Install and load packages ####
pacman::p_load(data.table, 
               readxl,
               future, 
               future.apply,
               progressr,
               parallel,
               miceadds,
               pbapply,
               soiltexture,
               httr,
               stringr,
               rnaturalearth,
               rnaturalearthhires,
               sf,
               dplyr)
  ## 0.1) Define the valid range for date checking #####
  valid_start <- as.Date("1950-01-01")
  valid_end <- as.Date("2023-12-01")
  
  ## 0.2) Set directories and parallel cores ####
  
  # Set cores for parallel processing
  worker_n<-parallel::detectCores()-2
  
  # Set the project name, this should usually refer to the ERA extraction template used
  project<-era_projects$industrious_elephant_2023
  
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
# If working locally from the "old" one-drive directory then you will to run 1_setup_s3.R section 1.2.1.
download<-F
update<-F

s3_file<-paste0("https://digital-atlas.s3.amazonaws.com/era/data_entry/",project,"/",project,".zip")

# Check if the file exist
if(update){
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
}

# 2) Load data ####
  ## 2.1) Load era vocab (era_master_sheet.xlsx) #####
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)
  
  ## 2.2) Load excel data entry template #####
  Master<-list.files(paste0(project_dir,"/data_entry/",project,"/excel_data_extraction_template"),"xlsm$",full.names = T)
  
  # List sheet names that we need to extract
  SheetNames<-excel_sheets(Master)
  SheetNames<-grep(".Out",SheetNames,fixed = T,value=T)
  
  # List column names for the sheets to be extracted
  XL.M<-sapply(SheetNames,FUN=function(SName){
    cat('\r                                                                                                                                          ')
    cat('\r',paste0("Importing Sheet = ",SName))
    flush.console()
    colnames(data.table(suppressWarnings(suppressMessages(readxl::read_excel(Master,sheet = SName)))))
  },USE.NAMES = T)
  
  # Subset Cols
  XL.M[["AF.Out"]]<-XL.M[["AF.Out"]][1:13] # Subset Agroforesty out tab to needed columns only
  
  ## 2.3) List extraction excel files #####
  Files<-list.files(excel_dir,".xlsm$",full.names=T)
  
  ## 2.4) Check for duplicate files #####
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
  
  ## 2.5) Read in data from excel files #####
  
  # If files have already been imported and converted to list form should the import process be run again?
  overwrite<-T
  
  # Delete existing files if overwrite =T
  if(overwrite){
    unlink(extracted_dir,recursive = T)
    dir.create(extracted_dir)
  }
  
  # Set up parallel back-end
  plan(multisession, workers = worker_n)

  # Enable progressr and set up handlers
  progressr::handlers("progress")
  progressr::handlers(global = TRUE)
  
  # Wrap the parallel processing in a with_progress call
  XL <- with_progress({
    # Define the progressor based on the number of files
    p <- progressr::progressor(along = 1:nrow(excel_files))
    
    # Run future apply loop to read in data from each Excel file in parallel
    future.apply::future_lapply(1:nrow(excel_files), FUN = function(i) {
      
      File <- excel_files$filename[i]
      era_code <- excel_files$era_code2[i]
      save_name <- file.path(extracted_dir, paste0(era_code, ".RData"))
      
      # Update the progress bar
      p(sprintf("Processing file %d of %d: %s", i, nrow(excel_files), era_code))

      

      if (overwrite == TRUE || !file.exists(save_name)) {
        X <- tryCatch({
          lapply(SheetNames, FUN = function(SName) {
            #cat('\r', "Importing File ", i, "/", nrow(excel_files), " - ", era_code, " | Sheet = ", SName, "               ")
            #flush.console()
            data.table::data.table(suppressMessages(suppressWarnings(readxl::read_excel(File, sheet = SName, trim_ws = FALSE))))
          })
        }, error = function(e) {
          #. cat("Error reading file: ", File, "\nError Message: ", e$message, "\n")
          return(NULL)  # Return NULL if there was an error
        })
        
        if (!is.null(X)) {
          names(X) <- SheetNames
          X$file.info <- file.info(File)
          save(X, file = save_name)
        }
      } else {
        miceadds::load.Rdata(filename = save_name, objname = "X")
      }
      
      X
    })
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
  
# 3) Process imported data ####
  ## 3.1) Publication (Pub.Out) #####
  data<-lapply(XL,"[[","Pub.Out")
  
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
  errors<-Pub.Out[N>1|code_issue,list(B.Code,era_code2,filename,N,code_issue)][order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "pub_code_errors",error_dir=error_dir,error_list = error_list)
  
  # Reset B.Codes to filename
  Pub.Out[,B.Code:=era_code2][,c("era_code2","filename","N","code_issue","...7"):=NULL]
  
    ### 3.1.1) Harmonization ######
    results<-val_checker(data=Pub.Out,
                          tabname="Pub.Out",
                          master_codes=master_codes,
                          master_tab="journals",
                          h_field="B.Journal",
                          h_field_alt=NA,
                          exact=F)
    
    Pub.Out<-results$data
    
    harmonization_list<-error_tracker(errors=results$h_task[order(value)],filename = "pub_harmonization",error_dir=harmonization_dir,error_list = NULL)
  
  ## 3.2) Site.Out #####
  data<-lapply(XL,"[[","Site.Out")
  col_names<-colnames(data[[800]])
  
  Site.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
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
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="No Site.ID exists"))
      }
    }})
  
  errors_a<-rbindlist(lapply(Site.Out,"[[","error"))
  Site.Out<-rbindlist(lapply(Site.Out,"[[","data"))
  
  error_list<-error_tracker(errors=errors_a,filename = "site_structure_errors",error_dir=error_dir,error_list = error_list)
  
  # Read in data excluding files with non-match structure
  results<-validator(data=Site.Out,
                     zero_cols=colnames(Site.Out)[!colnames(Site.Out) %in% c("Site.LonD","Site.LatD","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree")],
                     numeric_cols=c("Site.LonD","Site.LatD","Site.Lat.Unc","Site.Lon.Unc","Buffer.Manual","Site.Rain.Seasons","Site.MAP","Site.MAT","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree","Site.MSP.S1","Site.MSP.S2"),
                     compulsory_cols = c(Site.ID="Site.Type",Site.ID="Country",Site.ID="Site.LatD",Site.ID="Site.LonD"),
                     extreme_cols=list(Site.MAT=c(10,34),
                                       Site.MAP=c(40,4000),
                                       Site.MSP.S1=c(20,3000),
                                       Site.MSP.S2=c(20,3000),
                                       Site.Elevation=c(0,4000),
                                       Site.Slope.Perc=c(0,50),
                                       Site.Slope.Degree=c(0,45)),
                     unique_cols = "Site.ID",
                     date_cols=NULL,
                     trim_ws = T,
                     tabname="Site.Out")
  
  
  Site.Out<-results$data
  errors1<-results$errors
  # remove aggregated sites from error list
  errors1<-errors1[!grepl("[.][.]",value)]
  
  errors2<-Site.Out[(is.na(Site.Lat.Unc)|is.na(Site.Lon.Unc)) & is.na(Buffer.Manual) & !grepl("[.][.]",Site.ID)
                    ][,list(value=paste0(unique(Site.ID),collapse = "/")),by=list(B.Code)
                      ][,tabname:="Site.Out"
                        ][,field:="Site.ID"
                          ][,issue:="Missing value in compulsory field location uncertainty."]
  
  dat<-Site.Out[!(is.na(Site.LatD)|is.na(Site.LonD)|is.na(ISO.3166.1.alpha.3))]
  errors3<-check_coordinates(data=dat[,list(Site.LatD,Site.LonD,ISO.3166.1.alpha.3)])
  errors3<-dat[!errors3][,list(value=paste(unique(Site.ID),collapse="/")),by=list(B.Code)
                ][,table:="Site.Out"
                  ][,field:="Site.LonD/Site.LatD"
                    ][,issue:="Co-ordinates are not in the country specified?"]
  
  
    ## 3.2.1) Harmonization ######
      h_params<-data.table(h_table="Site.Out",
                           h_field=c("Site.Admin","Site.Start.S1","Site.End.S1","Site.Start.S2","Site.End.S2","Site.Type","Site.Soil.Texture"),
                           h_table_alt=c(NA,"Site.Out","Site.Out","Site.Out","Site.Out",NA,"Site.Out"),
                           h_field_alt=c(NA,"Site.Seasons","Site.Seasons","Site.Seasons","Site.Seasons",NA,"Soil.Texture"))
      
      results<-harmonizer_wrap(data=Site.Out,
                               h_params=h_params,
                               master_codes = master_codes)
      
      
      results2<-val_checker(data=Site.Out[Site.Type %in% c("Researcher Managed & Research Facility") & !grepl("[.][.]",Site.ID)],
                   tabname="Site.Out",
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
      
      harmonization_list<-error_tracker(errors=rbindlist(list(results$h_tasks,results2,results3),fill=T),filename = "site_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
      
      Site.Out<-results$data
    
    
    ## 3.2.2) Harmonize Site.ID field #######
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
    
    errors5<-Site.Out[,.(B.Code,Country,Site.ID_raw,Site.Type)][,Code:=paste0(Country,"|||",Site.ID_raw)]
    errors5<-errors5[!Code %in% master_site_list & 
                           Site.Type == "Researcher Managed & Research Facility" &
                           !grepl("[.][.]",Site.ID_raw)
    ][,.(value=paste(Site.ID_raw,collapse = "/")),by=B.Code
    ][,table:="Site.Out"
    ][,field:="Site.ID"
    ][,issue:="No match for facility in era_master_sheet site_list tab (inc. synonyms or harmonization fields)."]
    
    ## 3.2.3) Create Aggregated Site Rows #######
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
        
        # If only 1 value present for this column across all rows, then use this one value. Otherwise we concatenate all the values.
        x_u<-unique(x)
        if(length(x_u)==1){
         x<-x_u 
        }
        
        paste(x, collapse = "..")
          
      }), .SDcols = colnames(agg_dat)]
      
      list(data=result)
      }
    })
    
    mergedat<-rbindlist(lapply(result,"[[","data"))
    errors4<-rbindlist(lapply(result,"[[","error"))
    
    # Replace aggregated site rows
    Site.Out<-rbind(Site.Out[!grepl("[.][.]",Site.ID)],mergedat)
   
    ## 3.2.4) Save errors #####
    error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4,errors5),fill=T),
                              filename = "site_other_errors",
                              error_dir=error_dir,
                              error_list = error_list)    
  ## 3.3) Times periods #####
  data<-lapply(XL,"[[","Times.Out")
  col_names<-colnames(data[[800]])
      
    ### 3.3.1) Times.Out ###### 
    col_names2<-col_names[1:5]
    Times.Out<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with time tab structure,corruption of excel file?"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[1]<-"Time"
        X<-X[!is.na(Time)]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }
    })
    
    errors_a<-rbindlist(lapply(Times.Out,"[[","error"))
    Times.Out<-rbindlist(lapply(Times.Out,"[[","data"))
    
    setnames(Times.Out,c("Time.Year.Start","Time.Year.End"),c("Time.Start.Year","Time.End.Year"))
    
    results<-validator(data=Times.Out,
                       unique_cols = c("Time"),
                       numeric_cols = c("Time.Start.Year","Time.End.Year"),
                       ignore_values = c("Unspecified","unspecified","UnspecifiedTime"),
                       tabname="Times.Out",
                       duplicate_field="Time",
                       duplicate_ignore_fields=colnames(Times.Out)[!colnames(Times.Out) %in% c("Time","B.Code")],
                       rm_duplicates=T,
                       trim_ws = T,
                       do_time = F)
    
    errors5<-results$errors
    Times.Out<-results$data
    
    # Error checking non-numeric values in Time.Start.Year or Time.End.Year (could be moved to validator?)
    errors5<-Times.Out[is.na(as.numeric(Time.End.Year))|is.na(as.numeric(Time.Start.Year))|nchar(Time.Start.Year)>4|nchar(Time.End.Year)>4
                      ][!(is.na(Time.Start.Year)|is.na(Time.End.Year)|grepl("unspecified",Time.Start.Year,ignore.case = T))
                        ][,list(value=paste(unique(c(Time.Start.Year,Time.End.Year)),collapse="/")),by=.(B.Code)
                            ][,table:="Time.Out"
                              ][,field:="Time.Start.Year & Time.End.Year"
                                ][,issue:="Character values for year start or end. Please convert unknown years to 9999.1, 9999.2, etc. conveying the ascending temporal sequence."
                                  ][order(B.Code)]
    
    # Years should be integer values
    Times.Out[,Time.Start.Year:=as.integer(Time.Start.Year)][,Time.End.Year:=as.integer(Time.End.Year)]
    
    Times.Out[,c("Time.Start.Year","Time.End.Year"):=.(as.numeric(Time.Start.Year),as.numeric(Time.End.Year))]
  
    # Check for incorrect use of season field (should be something simple like dry/wet and not include the year)
    
    # Remove season if it the same as the Time
    Times.Out[Time.Season==Time.Start.Year,Time.Season:=NA]
    
    errors7<-Times.Out[grep(paste(seq(1960,2024,1),collapse="|"),Time.Season),.(B.Code,Time.Season)]
    errors7<-errors7[,.(value=paste0(unique(Time.Season),collapse = "/")),by=B.Code
                     ][,table:="Time.Out"
                       ][,field:="Time.Season"
                         ][,issue:="Season conflated with year (e.g. 2020 SR instead of SR)"
                           ][order(B.Code)]
    
    errors8<-Times.Out[Time.Start.Year<1930|Time.End.Year<1930,.(value=paste0(unique(Time.Start.Year),collapse = "/")),by=B.Code
    ][,table:="Time.Out"
    ][,field:="Time.Start.Year"
    ][,issue:="Start year is less than 1930."
    ][order(B.Code)]
    
    Times.Out[Time.Start.Year<1930|Time.End.Year<1930,c("Time.Start.Year","Time.End.Year"):=NA]
    
      #### 3.3.1.1) Enforce time ordering ######
    # An oversight in the extraction protocol means that times were not necessarily entered in order
    Times.Out<-Times.Out[order(B.Code,Time.Start.Year,Time.End.Year)]
    
    # Check for any instance where we have one year an several seasons, these will need to be manually ordered
    Times.Out[,N:=.N,by=.(B.Code,Time.Start.Year,Time.End.Year)]
    # Check if Time names can be turned numeric and if these are in ascending order
    
    is_ascending <- function(x) {
      all(diff(x) >= 0)
    }
    
    errors6<-Times.Out[N>1
                       ][,time.num:=as.numeric(Time) # Convert time to numeric
                         ][,time.num.all:=!any(is.na(time.num)),by=B.Code # Were all times converted to numeric?
                           ][time.num.all==T,in.order:=is_ascending(time.num),by=B.Code # Are converted times in ascending order?
                           ][in.order==F|is.na(in.order)
                             ][,Time:=paste(Time,"-",Time.Season)]
    
    errors6<-errors6[,list(value=paste(Time,collapse="/")),by=B.Code
            ][!B.Code %in% errors5$B.Code # Remove issues to do with unknown years being entered as character values
            ][,table:="Time.Out"
            ][,field:="Time-Time.Season"
            ][,issue:="Check that the time sequence has been entered in ascending temporal order, if not reorder it."]
    
    errors6<-merge(errors6,Site.Out[,.(B.Code,Country)][,.(Country=paste(unique(Country),collapse="/")),by=B.Code],by="B.Code",all.x=T)
    errors6[,value:=paste0(Country,": ",value)][,Country:=NULL]
    
    # Add seq number to Times.Out
    Times.Out[,N:=NULL][,seq_n:=1:.N,by=B.Code]

    
    ### 3.3.2) Times.Clim ###### 
    col_names2<-col_names[7:14]
    Times.Clim<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with time tab structure,corruption of excel file?"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[2]<-"Time"
        X<-X[!is.na(Time)]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }
    })
    
    errors_b<-rbindlist(lapply(Times.Clim,"[[","error"))
    Times.Clim<-rbindlist(lapply(Times.Clim,"[[","data"))
    
    # Update Site.ID
    Times.Clim[,Site.ID_new:=Site.Out$Site.ID[match(Times.Clim$Site.ID,Site.Out$Site.ID_raw)]
    ][is.na(Site.ID_new),Site.ID_new:=Site.ID
    ][,Site.ID:=Site.ID_new
    ][,Site.ID_new:=NULL]
    
    results<-validator(data=Times.Clim,
                       numeric_cols=c("Time.Clim.SP","Time.Clim.TAP","Time.Clim.Temp.Mean","Time.Clim.Temp.Max","Time.Clim.Temp.Min"),
                       time_data = Times.Out,
                       site_data = Site.Out,
                       compulsory_cols = c(Site.ID="Time",Time="Site.ID"),
                       trim_ws = T,
                       duplicate_field="Time",
                       duplicate_ignore_fields=colnames(Times.Clim)[!colnames(Times.Clim) %in% c("Time","Site.ID","B.Code")],
                       rm_duplicates=T,
                       tabname="Times.Clim")
    
    errors4<-results$errors
    Times.Clim<-results$data
    
    # note errors1-errors3 can be moved to the validator function, but some QAQC already exists that would need to be merged wiht the error tracking
    errors1<-Times.Clim[Time.Clim.Temp.Mean>45|Time.Clim.Temp.Max>50|Time.Clim.Temp.Min>30|
                 Time.Clim.Temp.Min>Time.Clim.Temp.Max|Time.Clim.Temp.Min>Time.Clim.Temp.Mean|Time.Clim.Temp.Mean>Time.Clim.Temp.Max
                 ][,issue:="Extremely high temperature or min>mean,min>max,mean>max"]
    
    errors2<-Times.Clim[Time.Clim.Temp.Mean<5|Time.Clim.Temp.Max<10|Time.Clim.Temp.Min<0
                        ][,issue:="Extremely low temperature or min>mean,min>max,mean>max"]
    
    errors3<-Times.Clim[Time.Clim.SP>Time.Clim.TAP
                        ][,issue:="Seasonal > annual precip"]
    
    Times.Clim<-unique(Times.Clim[!is.na(Site.ID) & !is.na(Time)])[,N:=.N,by=.(B.Code,Site.ID,Time)]
    
    errors6<-Times.Clim[N>1
                         ][,.(value=paste(unique(paste(Site.ID,"-",Time)),collapse = "/")),by=B.Code
                              ][,table:="Times.Clim"
                                ][,field:="Site.ID-Time"
                                  ][,issue:=">1 combination of Site.ID and Time is present in the time climate table."]
    
    Times.Clim<-Times.Clim[N==1][,N:=NULL]
    
    ### 3.3.3) Save errors #####
    errors<-rbind(errors1,errors2,errors3)[,Time.Clim.Notes:=NULL][order(B.Code)]
    errors[,Time:=as.numeric(Time)]
    error_list<-error_tracker(errors=errors,filename = "time_climate_errors",error_dir=error_dir,error_list = error_list)
    
    error_list<-error_tracker(errors=rbindlist(list(errors4,errors5,errors6,errors7,errors8),fill=T),filename = "time_other_errors",error_dir=error_dir,error_list = error_list)
    error_list<-error_tracker(errors=errors6,filename = "time_order_check",error_dir=error_dir,error_list = error_list)
    
  ## 3.4) Soil (Soil.Out) #####
  data<-lapply(XL,"[[","Soils.Out")
  
  # Structure errors: Check for malformed column names
  errors1<-rbindlist(lapply(1:length(data),FUN=function(i){
    dt<-data[[i]]
    if(colnames(dt)[1]=="...1"|!any(grepl("Unit",colnames(dt)))){
      Y<-data.table(B.Code=Pub.Out$B.Code[i],filename=basename(names(XL)[i]))
      Y[,issue:="Problem with structure of Soil.Out tab, first col has incorrect name or no unit colnames present"]
      Y
    }
  }))
  
  # Structure errors: Check for case where only one of upper or lower is present
  errors2<-rbindlist(lapply(1:length(data),FUN=function(i){
    dt<-data[[i]]
  
    if(colnames(dt)[1]!="...1"){
      # Filter out columns that are all NA
      dt <- dt[, .SD, .SDcols = colSums(is.na(dt)) != nrow(dt)]
      Xcols<-colnames(dt)
      
      if(("Soil.Upper" %in% Xcols + "Soil.Lower" %in% Xcols)==1){
        Y<-data.table(B.Code=Pub.Out$B.Code[i],filename=basename(names(XL)[i]))
        Y[,issue:="Only one of upper or lower depth has value"]#
        Y
      }
    }
  }))
  
  # Combine soil data into a table
  fun1<-function(x){x[1]}
  
  Soil.Out<-rbindlist(lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
  
    if(colnames(X)[1]=="...1"|!any(grepl("Unit",colnames(X)))){
      cat("Issue with soil table structure:",Pub.Out$B.Code[i],"\n")
      NULL
    }else{
      X<-X[!is.na(Site.ID)]
    
      if(nrow(X)>0){
      # Filter out columns that are all NA
      X <- X[, .SD, .SDcols = colSums(is.na(X)) != nrow(X)]
      copy_down_cols<-grep("Unit|Method",colnames(X),value=T)
      X <- X[, (copy_down_cols) := lapply(.SD,fun1), .SDcols = copy_down_cols]
      
      # Make table long with cols for value, variable, unit and method
      Xcols<-colnames(X)
      
      # Determine variable column start position
      N<-"Soil.Upper" %in% Xcols + "Soil.Lower" %in% Xcols
      if(N==1){
        start.pos<-3
      }else{
        if(N==2){
          start.pos<-4
        }else{
          start.pos<-2
        }
      }
      
      fixed.cols<-Xcols[1:(start.pos-1)]
      var.cols<-c(fixed.cols,Xcols[start.pos:(grep("Unit",Xcols)[1]-1)])
      unit.cols<-c(grep("Unit",Xcols,value=T))
      method.cols<-c(grep("Method",Xcols,value=T))
      
      # Deal with instances where more than one pH method is present
      N<-grep("Soil.pH",var.cols,value=T)
      if(length(N)>=2){
       colnames(X)[colnames(X) %in% N]<-paste0("Soil.pH_",1:length(N))
       var.cols[var.cols %in% N]<-paste0("Soil.pH_",1:length(N))
      }
      
      N<-grep("Soil.pH",method.cols,value=T)
      if(length(N)>=2){
        colnames(X)[colnames(X) %in% N]<-paste0("Soil.pH.Method_",1:length(N))
        method.cols[method.cols %in% N]<-paste0("Soil.pH.Method_",1:length(N))
      }
      
      # Deal with instances where more than one N Unit is present
      N<-grep("Soil.TN",var.cols,value=T)
      if(length(N)>=2){
        colnames(X)[colnames(X) %in% N]<-paste0("Soil.TN_",1:length(N))
        var.cols[var.cols %in% N]<-paste0("Soil.TN_",1:length(N))
      }
      
      N<-grep("Soil.TN",unit.cols,value=T)
      if(length(N)>=2){
        colnames(X)[colnames(X) %in% N]<-paste0("Soil.TN.Unit_",1:length(N))
        unit.cols[unit.cols %in% N]<-paste0("Soil.TN.Unit_",1:length(N))
      }
      
      # Melt values and units into long form
      var_tab<-melt(X[,..var.cols],id.vars = fixed.cols)
      unit_tab<-data.table(variable=row.names(t(X[1,..unit.cols])),Unit=unlist(X[1,..unit.cols]))
      unit_tab<-unit_tab[,variable:=gsub("[.]Unit","",variable)][Unit!="NA"]
      
      if(var_tab[,any(grepl("[.][.][.]",variable))]){
        cat("... present in variables",i,Pub.Out$B.Code[i],"\n")
      }
      
      
      if(unit_tab[,any(grepl("[.][.][.]",variable))]){
        cat("... present in units",i,Pub.Out$B.Code[i],"\n")
      }
  
      # Merge values and units
      Y<-merge(var_tab,unit_tab,all.x=T)
      
      # If methods are present melt into long form and merge with values and units
      if(length(method.cols)>1){
        method_tab<-data.table(variable=row.names(t(X[1,..method.cols])),Method=unlist(X[1,..method.cols]))
        method_tab<-method_tab[,variable:=gsub("[.]Method","",variable)][Method!="NA"]
        if(method_tab[,any(grepl("[.][.][.]",variable))]){
          cat("... present in methods",i,Pub.Out$B.Code[i],"\n")
        }
        
        Y<-merge(Y,method_tab,all.x=T)
      }else{
        Y[,Method:=NA]
      }
      
      # Remove delimiters used to align multiple pH methods
      Y[,variable:=unlist(tstrsplit(variable,"_",keep=1))][,Method:=unlist(tstrsplit(Method,"_",keep=1))]
   
      Y$B.Code<-Pub.Out$B.Code[i]
      Y$filename<-basename(names(XL)[i])
      Y
      }else{
        NULL
      }
    }
    
  }),fill=T)
  
  # Add unit for sand, silt and clay
  Soil.Out[variable %in% c("CLY","SND","SLT"),Unit:="%"]
  
  # Update Site ID
  Soil.Out[,Site.ID_new:=Site.Out$Site.ID[match(Soil.Out$Site.ID,Site.Out$Site.ID_raw)]
           ][is.na(Site.ID_new),Site.ID_new:=Site.ID
             ][,Site.ID:=Site.ID_new
               ][,Site.ID_new:=NULL]

  
    results<-validator(data=Soil.Out,
                      numeric_cols=c("Soil.Upper","Soil.Lower","value"),
                      compulsory_cols=c(Site.ID="value"),
                      tabname="Soil.Out",
                      trim_ws = T,
                      site_data=Site.Out)
  
  Soil.Out<-results$data
  errors<-results$errors
  
  # Check for "..." in column names
  errors3<-unique(Soil.Out[grep("[.][.][.]",variable),list(variable,B.Code)])
  if(nrow(errors3)>0){
    setnames(errors3,"variable","value")
    errors3[,table:="Soil.Out"][,field:="variable"][,issue:="Potential duplicate soil variable recorded"]
  }
  
  # Check for multiple instance of same depth for same site
  errors4<-unique(Soil.Out[,.N,by=list(B.Code,Site.ID,Soil.Upper,Soil.Lower,variable)][,variable:=NULL][N>1][,c("N","Soil.Upper","Soil.Lower"):=NULL])
  if(nrow(errors4)>0){
    setnames(errors4,"Site.ID","value")
    errors4[,table:="Soil.Out"][,field:="Site.ID"][,issue:="Check for multiple instance of same depth for same site"]
  }
  
  # Check for depth lower > depth upper
  errors5<-unique(Soil.Out[Soil.Upper>Soil.Lower,list(variable,B.Code)])
  if(nrow(errors5)>0){
    setnames(errors5,"variable","value")
    errors5[,table:="Soil.Out"][,field:="variable"][,issue:="Upper depth value is greater than lower depth value"]
  }
  
  errors7<-Soil.Out[variable!="Soil.pH" & is.na(Unit) & !is.na(value),list(value=paste0(unique(variable),collapse="/")),by=B.Code]
  errors7[,table:="Soil.Out"][,field:="variable"][,issue:="Amount is present, but unit is missing for value."]
  
    ### 3.4.1) Soil.Out: Calculate USDA Soil Texture from Sand, Silt & Clay ####
    Soil.Out.Texture<-Soil.Out[variable %in% c("SND","SLT","CLY"),list(B.Code,filename,Site.ID,Soil.Upper,Soil.Lower,variable,value)]
    
    # Remove studies with issues
    Soil.Out.Texture<-Soil.Out.Texture[!B.Code %in% errors4$B.Code]
    
    # Make dataset wide
    Soil.Out.Texture<-dcast(Soil.Out.Texture,B.Code+filename+Site.ID+Soil.Upper+Soil.Lower~variable,value.var = "value")
    
    # Keep rows with 2 or more observations
    Soil.Out.Texture<-Soil.Out.Texture[,N:=is.na(CLY)+is.na(SLT)+is.na(SND)][N<2]
    
    # Add missing values where 2/3 are present
    Soil.Out.Texture[,val:=sum(c(CLY,SLT,SND),na.rm=T),by=list(B.Code,filename,Site.ID,Soil.Upper,Soil.Lower)]
    
    Soil.Out.Texture[is.na(SLT),SLT:=100-val
                     ][is.na(CLY),CLY:=100-val
                       ][is.na(SND),SND:=100-val]
    
    # Any values not 100
    errors6<-unique(Soil.Out.Texture[N!=1 & (val>102|val<98),list(B.Code,Site.ID)])
    
    if(nrow(errors6)>0){
      setnames(errors6,"Site.ID","value")
      errors6[,table:="Soil.Out"][,field:="Site.ID"][,issue:="Sand, silt, clay sum to beyond 2% different to 100%"]
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
    Soil.Out<-merge(Soil.Out,Soil.Out.Texture[,list(B.Code,filename,Site.ID,Soil.Upper,Soil.Lower)])
    
    # Merge missing texture values
    Soil.Out.Texture<-melt(Soil.Out.Texture[,!"Site.Soil.Texture"],id.vars = c("B.Code","filename","Site.ID","Soil.Upper","Soil.Lower"))
    X<-merge(Soil.Out.Texture,Soil.Out[,list(B.Code,filename,Site.ID,Soil.Upper,Soil.Lower,variable)][,check:=T],all.x=T)
    Soil.Out.Texture<-rbind(Soil.Out.Texture,X[is.na(check)][,check:=NULL])
    
    # Combine & save errors
    errors<-rbindlist(list(errors,errors1,errors2,errors3,errors4,errors5,errors6,errors7),fill = T)[order(B.Code)]
    error_list<-error_tracker(errors=errors,filename = "soil_errors",error_dir=error_dir,error_list = error_list)
  
    ### 3.4.3) ***!!!TO DO!!!***  harmonize methods, units and variables ####
    Soil.Out[,filename:=NULL]
  ## 3.5) Experimental Design (ExpD.Out) ####
  data<-lapply(XL,"[[","ExpD.Out")
  col_names<-colnames(data[[1]])
  ExpD.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out[,B.Code[i]]
    Filename<-basename(names(XL)[i])
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=Filename,issue="Problem with ExpD.Out tab structure"))
    }else{
      X[,B.Code:=B.Code]
      list(data=X)
    }
  })
  
  errors<-rbindlist(lapply(ExpD.Out,"[[","error"))
  error_list<-error_tracker(errors=errors,filename = "expd_structure_errors",error_dir=error_dir,error_list = error_list)
  
  ExpD.Out<-rbindlist(lapply(ExpD.Out,"[[","data"))
  
  results<-validator(data=ExpD.Out,
                     numeric_cols=c("EX.Plot.Size","EX.HPlot.Size"),
                     zero_cols = c("EX.Design","EX.Plot.Size","EX.HPlot.Size","EX.Notes"),
                     trim_ws = T,
                     tabname="ExpD.Out")
  
  ExpD.Out<-results$data
  
  error_list<-error_tracker(errors=results$errors,filename = "expd_other_errors",error_dir=error_dir,error_list = error_list)
  
  
  ## 3.6) Products (Prod.Out) ####
  data<-lapply(XL,"[[","Prod.Out")
  col_names<-colnames(data[[800]])
  
  Prod.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out[,B.Code[i]]
    Filename<-basename(names(XL)[i])
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=Filename,issue="Problem with product tab structure"))
    }else{
      X<-X[,..col_names]
      X[,B.Code:=B.Code]
      X<-X[!is.na(P.Product)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="No products exists"))
      }
      
      list(data=X)
    }
  })
  
  
  errors_a<-rbindlist(lapply(Prod.Out,"[[","error"))
  error_list<-error_tracker(errors=errors_a,filename = "prod_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Prod.Out<-rbindlist(lapply(Prod.Out,"[[","data"))
  
  Prod.Out<-validator(data=Prod.Out,
                    tabname="Prod.Out",
                    trim_ws = T)$data
  
  # Check products exists (non-tree)
  mprod<-unique(master_codes$prod[,list(Product.Subtype,Product.Simple)])
  colnames(mprod)<-c("P.Product.Subtype","P.Product")
  mprod[,check:=T]
  mergedat<-merge(Prod.Out,mprod,all.x=T)
  
  mergedat<-mergedat[is.na(check),list(B.Code,P.Product.Subtype,P.Product)]
  errors1<-mergedat[P.Product.Subtype!="Tree"
                    ][,value:=paste0(P.Product.Subtype[1],"/",P.Product[1]),by=list(P.Product.Subtype,P.Product)
                     ][,list(B.Code=paste(B.Code,collapse = "/")),by=value
                       ][,table:="Prod.Out"
                       ][,master_table:="prod"
                         ][,field:="P.Product.Subtype/P.Product"
                           ][,issue:="No match for crop product in master codes Product tab"
                             ][order(B.Code)]
  
  # Check tree product exists
  mprod<-unique(master_codes$trees[,"Tree.Latin.Name"])
  colnames(mprod)<-c("P.Product")
  mprod[,check:=T]
  mergedat<-merge(Prod.Out,mprod,all.x=T)
  
  mergedat<-mergedat[is.na(check),list(B.Code,P.Product.Subtype,P.Product)]
  errors2<-mergedat[P.Product.Subtype=="Tree"
                    ][,value:=paste0(P.Product.Subtype[1],"/",P.Product[1]),by=list(P.Product.Subtype,P.Product)
                      ][,list(B.Code=paste(B.Code,collapse = "/")),by=value
                       ][,table:="Prod.Out"
                          ][,master_table:="trees"
                           ][,field:="P.Product"
                             ][,issue:="No match for tree in master codes tree tab"
                              ][order(B.Code)]
  
  error_list<-error_tracker(errors=rbind(errors1,errors2),filename = "prod_other_errors",error_dir=error_dir,error_list = error_list)
  
    ### 3.6.1) Harmonization ######
    # Update mulch and incorporated codes, check product is in master codes
    # Crops 
    prod_master<-unique(master_codes$prod[,.(Product.Subtype,Product.Simple,Mulched,Incorp,Unknown.Fate)])
    
    Prod.Out.Crop<-merge(Prod.Out[P.Product.Subtype!="Tree",.(B.Code,P.Product.Subtype,P.Product)],prod_master,
                    by.x=c("P.Product.Subtype","P.Product"),
                    by.y=c("Product.Subtype","Product.Simple"),all.x=T)

    
    # Trees
    tree_master<-master_codes$trees[,.(Tree.Latin.Name, Mulched,Incorp,Unknown.Fate)]
    
    Prod.Out.Trees<-merge(Prod.Out[P.Product.Subtype=="Tree",.(B.Code,P.Product.Subtype,P.Product)],tree_master,
                         by.x=c("P.Product"),
                         by.y=c("Tree.Latin.Name"),all.x=T)
    
    Prod.Out.Trees[is.na(Mulched)]
    
    Prod.Out<-rbind(Prod.Out.Crop,Prod.Out.Trees)
    
    setnames(Prod.Out,
             c("Mulched","Incorp","Unknown.Fate"),
             c("P.Mulched","P.Incorp","P.Unknown.Fate"))

  ## 3.7) Var (Var.Out) ####
  data<-lapply(XL,"[[","Var.Out")
  col_names<-colnames(data[[800]])
  
  Var.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    X<-X[!grepl("ERROR",V.Product)]
    B.Code<-Pub.Out[,B.Code[i]]
    Filename<-basename(names(XL)[i])
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=Filename,issue="Problem with varaety tab structure"))
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
  
  errors_a<-rbindlist(lapply(Var.Out,"[[","error"))
  error_list<-error_tracker(errors=errors_a,filename = "var_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Var.Out<-rbindlist(lapply(Var.Out,"[[","data"))
  
  Var.Out<-rbindlist(lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    X<-X[!grepl("ERROR",V.Product)]
    X<-na.omit(X, cols=c("V.Var"))
    if(nrow(X)>0){
      X$B.Code<-Pub.Out$B.Code[i]
      X
    }
  }))
  
  results<-validator(data=Var.Out,
                     numeric_cols=c("V.Maturity"),
                     zero_cols = c("V.Var","V.Accession","V.Crop.Practice","V.Base","V.Type","V.Trait1","V.Trait2","V.Trait3","V.Maturity"),
                     tabname="Var.Out",
                     duplicate_field = "V.Var",
                     trim_ws=T)
  
  errors1<-results$errors
  Var.Out<-results$data
  
  # Update V.Codes
  Var.Out[,V.Codes:=master_codes$prac[match(Var.Out$V.Crop.Practice,master_codes$prac$Subpractice),Code]]
  
  # Errors
  errors2<-unique(Var.Out[(V.Type %in% c("Local","Landrace")) & V.Crop.Practice != "Unimproved Variety",!c("V.Base","V.Codes")])
  errors2<-errors2[,value:=paste(V.Product,"-",V.Var)
                 ][,list(B.Code=paste(B.Code,collapse = "/")),by=value
                 ][,table:="Var.Out"
                 ][,field:="V.Product - V.Var"
                 ][,issue:="Type is local, but not unimproved, possible error (but could be correct)."]
  
  errors3<-unique(Var.Out[V.Type %in% c("Hybrid") & V.Crop.Practice == "Unimproved Variety",!c("V.Base","V.Codes")])
  errors3<-errors3[,value:=paste(V.Product,"-",V.Var)
                   ][,list(B.Code=paste(B.Code,collapse = "/")),by=value
                     ][,table:="Var.Out"
                       ][,field:="V.Product - V.Var" 
                         ][,issue:="Type is Hybrid, but practice is improved, probable error."]
  
  # Check for any remaining duplicates
  errors4<-Var.Out[,.(B.Code,V.Product,V.Var)]
  # Remove duplicates as these cause merge issues with the treatments tab
  Var.Out<-Var.Out[!duplicated(errors4)]
  
  errors4<-errors4[duplicated(errors4)][,Code:=paste(V.Product,"-",V.Var)
                                ][,list(value=paste(Code,collapse = "/")),by=B.Code
                                  ][,table:="Var.Out"
                                  ][,field:="V.Product - V.Var"
                                  ][,issue:="Duplicate varieties present"]
  
  # Check for multiple base practices for the same crop
  errors5<-Var.Out[V.Base=="Yes",.(V.Product,V.Var,V.Base,B.Code)
  ][,N:=length(V.Base),by=.(B.Code,V.Product)
  ][N>1
  ][,.(value=paste(paste0(V.Product,":",V.Var),collapse="/")),by=.(B.Code)
    ][,table:="Var.Out"
      ][,field:="V.Product:V.Var"
        ][,issue:="Multiple base practices present."]
  
    ### 3.7.1) Var.Out: Harmonize Variety Naming and Codes #####
  
  # Save original variety name as this is a keyfield used in the MT.Out tab, if it is changed then this causes issues
  Var.Out[,V.Level.Name:=V.Var]
  
  # Update fields associated with varieties
  mvars<-unique(master_codes$vars[,list(V.Product,V.Var,V.Crop.Practice,V.Type)])[,N:=.N,by=list(V.Product,V.Var)]
  h_tasks1<-mvars[N>1][,master_tab:="vars"][,issue:="More than one description of same variety in master codes"]
  mvars<-mvars[N==1][,N:=NULL]
  
  mergedat<-merge(Var.Out[,list(V.Product,V.Var)],mvars,all.x=T)
  
  Var.Out[!is.na(mergedat$V.Crop.Practice),V.Crop.Practice:=mergedat[!is.na(V.Crop.Practice),V.Crop.Practice]
  ][!is.na(mergedat$V.Type),V.Type:=mergedat[!is.na(V.Type),V.Type]]
  
  # Update varietal by matching to master_codes
  mvars<-unique(master_codes$vars[!is.na(V.Var1) & !is.na(V.Var),list(V.Product,V.Var,V.Var1)])[,N:=.N,by=list(V.Product,V.Var)]
  h_tasks2<-mvars[N>1][,master_tab:="vars"][,issue:="More than one description of same variety in master codes"]
  mvars<-mvars[N==1][,N:=NULL]
  
  mergedat<-merge(Var.Out[,list(V.Product,V.Var)],mvars,all.x=T)
  
  Var.Out[!is.na(mergedat$V.Var1),V.Var:=mergedat[!is.na(V.Var1),V.Var1]]
  
  
  harmonization_list<-error_tracker(errors=h_tasks1,filename = "var_master_dups1_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  harmonization_list<-error_tracker(errors=h_tasks2,filename = "var_master_dups2_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  # Non-matching varieties
  h_tasks3<- Var.Out[is.na(mergedat$V.Var1) & !is.na(V.Var) & !grepl("local|unspecified|unimproved",V.Var,ignore.case=T),list(V.Product,V.Var,B.Code)
  ][,list(B.Code=paste(B.Code,collapse = "/")),by=list(V.Product,V.Var)
  ][,master_tab:="vars"
  ][,table:="Var.Out"
  ][,field:="V.Var"]
  setnames(h_tasks3,"V.Var","value")
  
  harmonization_list<-error_tracker(errors=errors3,filename = "var_varieties_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
    #### Q: Should we update traits and maturity too? ####
    ### 3.7.2) Save errors #######
    error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4,errors5),use.names = T)[order(B.Code)],filename = "var_other_errors",error_dir=error_dir,error_list = error_list)
  
  ## 3.8) Agroforestry #####
  data<-lapply(XL,"[[","AF.Out")
  col_names<-colnames(data[[800]])
  
    ### 3.8.1) AF.Out ######
    col_names2<-col_names[1:8]
    
    AF.Out<-pblapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      Filename<-basename(names(XL)[i])
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(errors2=data.table(B.Code=B.Code,filename=Filename,issue="Problem with agroforestry tab structure"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[1]<-"AF.Level.Name"
        X<-X[!is.na(AF.Level.Name)]
        
        # Remove any all NA rows
        na_matrix <- is.na(X[, !("AF.Level.Name"), with = FALSE])
        rows_with_na <- rowSums(na_matrix) == ncol(na_matrix)
        errors<-X[!is.na(AF.Level.Name) & AF.Level.Name!="Base" & rows_with_na==T][,B.Code:=B.Code]
        X <- X[!rows_with_na]
        
        if(nrow(X)>0){
          X$B.Code<-B.Code
          if(nrow(errors)>0){
            list(data=X,errors=errors)
          }else{
            list(data=X)
          }
        }else{
          if(nrow(errors)>0){
            list(errors=errors)
          }
        }
      }
      })
    
    errors_a<-rbindlist(lapply(AF.Out,"[[","errors2"))
    errors1<-rbindlist(lapply(AF.Out,"[[","errors"))
    AF.Out<-rbindlist(lapply(AF.Out,"[[","data"))
    
    AF.Out<-validator(data=AF.Out,
                      tabname="AF.Out",
                      trim_ws = T)$data
    
    
    X<-unique(AF.Out[AF.Level.Name=="Base",c("AF.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
    
    # Check for multiple base practices
    errors5<-AF.Out[AF.Level.Name=="Yes",.(AF.Level.Name,B.Code)
    ][,N:=length(AF.Level.Name),by=.(B.Code)
    ][N>1
    ][,.(value=paste(AF.Level.Name,collapse="/")),by=.(B.Code)
    ][,table:="AF.Out"
    ][,field:="AF.Level.Name"
    ][,issue:="Multiple base practices present."]
    
    ### 3.8.2) AF.Trees ######
    col_names2<-col_names[10:13]
    AF.Trees<-pblapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      Filename<-basename(names(XL)[i])
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=Filename,issue="Problem with agroforestry tab structure - trees"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[1]<-"AF.Level.Name"
        X<-X[!is.na(AF.Level.Name)]
        # Remove any all NA rows
        na_matrix <- is.na(X[, !("AF.Level.Name"), with = FALSE])
        rows_with_na <- rowSums(na_matrix) == ncol(na_matrix)
        X <- X[!rows_with_na]
        
        if(nrow(X)>0){
          X$B.Code<-B.Code
          list(data=X)
    
        }else{
          NULL
          }
        }
    })
    errors_b<-rbindlist(lapply(AF.Trees,"[[","errors2"))
    AF.Trees<-rbindlist(lapply(AF.Trees,"[[","data"))
    
    results<-validator(data=AF.Trees,
                       numeric_cols=c("AF.Tree.N"),
                       tabname="AF.Trees",
                       trim_ws = T)
    
    AF.Trees<-results$data
    errors2<-results$errors
    
    # Key field matches
    errors3<-check_key(parent=AF.Out,child=AF.Trees,tabname="AF.Trees",keyfield="AF.Level.Name")
    
    # Check for missing units
    errors4<-AF.Trees[!is.na(AF.Tree.N) & is.na(AF.Tree.Unit)
                    ][,list(value=paste0(unique(AF.Level.Name))),by=B.Code
                      ][,table:="AF.Tree"
                        ][,field:="AF.Level.Name"
                          ][,issue:="Trees number is present without unit."]
    
    ### 3.8.3) Save errors ######
    errors<-rbindlist(list(errors2,errors3,errors1,errors4,errors5),fill=T)
    error_list<-error_tracker(errors=errors,filename = "af_errors",error_dir=error_dir,error_list = error_list)
    error_list<-error_tracker(errors=rbind(errors_a,errors_b),filename = "af_structure_errors",error_dir=error_dir,error_list = error_list)
    
    
    ### 3.8.x) Harmonization - TO DO !!! ######
    # AF.Tree and AF.Tree.Unit
  
  ## 3.9) Tillage (Till.Out) #####
  data<-lapply(XL,"[[","Till.Out")
  col_names<-colnames(data[[1]])
  
    ### 3.9.1) Till.Codes #######
  
  col_names2<-col_names[1:4]
  
  Till.Codes<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with tillage tab structure"))
    }else{
      X<-X[,..col_names2]
      X<-X[!is.na(Till.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Till.Codes,"[[","errors"))
  Till.Codes<-rbindlist(lapply(Till.Codes,"[[","data"))  
  
  Till.Codes<-validator(data=Till.Codes,
                      tabname="Till.Codes",
                      trim_ws = T)$data
  
  # Update codes
  Till.Codes$Till.Code<-master_codes$prac$Code[match(Till.Codes$Till.Practice,master_codes$prac$Subpractice)]
  Till.Codes[,Till.Codes:=NULL]
  
  errors1<-Till.Codes[is.na(Till.Code),list(B.Code,Till.Practice)
             ][,value:=Till.Practice
             ][,Till.Practice:=NULL
               ][,table:="Till.Code"
                 ][,field:="T.Practice"
                   ][,issue:="Tillage practice selected does not match a value in the master codes."]
  
  
  errors4<-validator(data=Till.Codes,
                     unique_cols = "Till.Level.Name",
                     tabname="Till.Codes")$errors
    
    ### 3.9.2) Till.Out #######
  col_names2<-col_names[6:18]
  
  Till.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with tillage tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"Till.Level.Name"
      X<-X[!is.na(Till.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_b<-rbindlist(lapply(Till.Out,"[[","errors"))
  error_list<-error_tracker(errors=unique(rbind(errors_a,errors_b)),filename = "till_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Till.Out<-rbindlist(lapply(Till.Out,"[[","data"))  
  
    # Correct naming of Time col to match Time Tab
    setnames(Till.Out,c("Times","T.Method.Other"),c("Time","Till.Other"))
    
    # Update Site.ID
    Till.Out[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(Till.Out[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]
    ][is.na(Site.ID_new),Site.ID_new:=Site.ID
    ][,Site.ID:=Site.ID_new
    ][,Site.ID_new:=NULL]
    
    results<-validator(data=Till.Out,
                       numeric_cols=c("T.Depth","T.Strip.P","T.Strip.WT","T.Strip.WU","T.Freq"),
                       date_cols=c("T.Date.End", "T.Date.Start"),
                       tabname="Till.Out",
                       valid_start=valid_start,
                       valid_end=valid_end,
                       site_data=Site.Out,
                       time_data=Times.Out,
                       trim_ws = T,
                       ignore_values= c("All Times","Unspecified","Not specified","All Sites"))
    
    errors2<-results$errors
    Till.Out<-results$data
    
    # Check for multiple base practices
    errors5<-Till.Out[Till.Level.Name=="Yes",.(Till.Level.Name,B.Code)
    ][,N:=length(Till.Level.Name),by=.(B.Code)
    ][N>1
    ][,.(value=paste(Till.Level.Name,collapse="/")),by=.(B.Code)
    ][,table:="Till.Out"
    ][,field:="Till.Level.Name"
    ][,issue:="Multiple base practices present."]

      #### 3.9.2.2) Harmonization ######
      h_params<-data.table(h_table="Till.Out",
                           h_field=c("T.Method","Till.Other","T.Mechanization"),
                           h_table_alt=c(NA,NA,"Fert.Out"),
                           h_field_alt=c(NA,NA,"F.Mechanization"))
      
      results<-harmonizer_wrap(data=Till.Out,
                               h_params=h_params,
                               master_codes = master_codes)
      
      harmonization_list<-error_tracker(errors=results$h_tasks,filename = "till_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
      
      Till.Out<-results$data
      
    ### 3.9.3) Save errors ######
    # Error checking: look for non-matches in keyfield
    errors3<-check_key(parent = Till.Codes,child = Till.Out,tabname="Till.Out",keyfield="Till.Level.Name")[,issue:="A practice has some description in the tillage (typically base), but no practice has been associated with it."]
    
    # save errors
    errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5),fill=T)[order(B.Code)]
    error_list<-error_tracker(errors=errors,filename = "till_errors",error_dir=error_dir,error_list = error_list)
    
  ## 3.10) Planting  #####
  data<-lapply(XL,"[[","Plant.Out")
  col_names<-colnames(data[[800]])
  
    ### 3.10.1) Plant.Out ####
  col_names2<-col_names[1:6]
    Plant.Out<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Plant.Out tab structure,corruption of excel file?"))
      }else{
        X<-X[,..col_names2]
        colnames(X)[2]<-"P.Level.Name"
        X<-X[!is.na(P.Level.Name)]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }
    })
    
    errors_a<-rbindlist(lapply(Plant.Out,"[[","error"))
    Plant.Out<-rbindlist(lapply(Plant.Out,"[[","data"))
    
    # Name added but p-structure is missing
    errors<-unique(Plant.Out[P.Level.Name!="Base" & !is.na(P.Level.Name) & is.na(P.Structure)
    ][,list(value=paste(P.Level.Name,collapse = "/")),by=B.Code
    ][,table:="Plant.Out"
    ][,field:="P.Level.Name"
    ][,issue:="Practice name is specified but the does it P.Structure outcomes row is blank."])[order(B.Code)]
    
    error_list<-error_tracker(errors=errors,filename = "plant_comparison_allowed_issue",error_dir=error_dir,error_list = error_list)
    
    results<-validator(data=Plant.Out,
                       unique_cols = "P.Level.Name",
                       tabname="Plant.Out",
                       trim_ws = T)
    errors1<-results$errors
    
    Plant.Out<-results$data
    
    # Check for multiple base practices
    errors5<-Plant.Out[P.Level.Name=="Yes",.(P.Level.Name,B.Code)
    ][,N:=length(P.Level.Name),by=.(B.Code)
    ][N>1
    ][,.(value=paste(P.Level.Name,collapse="/")),by=.(B.Code)
    ][,table:="Plant.Out"
    ][,field:="P.Level.Name"
    ][,issue:="Multiple base practices present."]
    
    ### 3.10.2) Plant.Method ####
    col_names2<-col_names[8:23]
    
    Plant.Method<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      if(!"Plant.Relay" %in% colnames(X)){
        X[,Plant.Relay:="Not in template"]
      }
      
        if(!all(col_names2 %in% colnames(X))){
          cat("Structural issue with file",i,B.Code,"\n")
          list(errors=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Plant.Out tab structure"))
        }else{
          X<-X[,..col_names2]
          colnames(X)[1]<-"P.Level.Name"
          X<-X[!is.na(P.Level.Name)]
          if(nrow(X)>0){
            X[,B.Code:=B.Code]
            list(data=X)
          }else{
            NULL
          }
        }
    })
    
    errors_b<-rbindlist(lapply(Plant.Method,"[[","errors"),use.names = T)
    Plant.Method<-rbindlist(lapply(Plant.Method,"[[","data"),use.names = T)
    setnames(Plant.Method,"P.Method","Plant.Method")
    
    error_list<-error_tracker(errors=rbind(errors_a,errors_b),filename = "plant_structure_errors",error_dir=error_dir,error_list = error_list)
    
    # Standardization and basic error checks
    results<-validator(data=Plant.Method,
                       unique_cols="P.Level.Name",
                       numeric_cols=c("Plant.Density","Plant.Row","Plant.Station","Plant.Seeds","Plant.Thin","Plant.Tram.Row","Plant.Tram.N","Plant.Block.Rows","Plant.Block.Perc","Plant.Block.Width"),
                       tabname="Plant.Method",
                       trim_ws = T)
    
    errors2<-results$errors
    Plant.Method<-results$data
    
    # Non match of name between tables
    errors3<-check_key(parent = Plant.Out,child = Plant.Method,tabname="Plant.Method",keyfield="P.Level.Name")
    
    # Row vs station spacing issue?
    errors4<-unique(Plant.Method[Plant.Row<Plant.Station
                          ][,value:=paste("row = ",Plant.Row[1],", station =",Plant.Station[1]),by=list(Plant.Row,Plant.Station)
                            ][,list(B.Code,value)
                              ][,table:="Plant.Method"
                                ][,field:="P.Level.Name"
                                  ][,issue:="Row spacing is less than station spacing, possible error."])
    
    
    # Planting density without unit
    errors6<-unique(Plant.Method[is.na(Plant.Density.Unit) & !is.na(Plant.Density)
    ][,list(value=paste0(P.Level.Name,collapse="/")),by=B.Code
    ][,table:="Plant.Method"
    ][,field:="P.Level.Name"
    ][,issue:="Missing density unit."])
      #### 3.10.2.1) Harmonization ######
    
    h_params<-data.table(h_table="Plant.Out",
                         h_field=c("Plant.Method","Plant.Density.Unit","Plant.Mechanization","Plant.Intercrop"),
                         h_table_alt=c(NA,NA,"Fert.Out","Plant.Out"),
                         h_field_alt=c(NA,NA,"F.Mechanization","Intercrop.Types"),
                         ignore_vals=c("unspecified","unspecified","unspecified","unspecified"))
    
    results<-harmonizer_wrap(data=Plant.Method,
                             h_params=h_params,
                             master_codes = master_codes)
    
    harmonization_list<-error_tracker(errors=results$h_tasks,filename = "plant_method_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
    
    Plant.Method<-results$data
    
    
    ### 3.10.3) Save errors ######
    error_list<-error_tracker(errors=rbind(errors1,errors2,errors3,errors4,errors5,errors6)[order(B.Code)],filename = "plant_method_value_errors",error_dir=error_dir,error_list = error_list)
    
    # errors x
    errors_bcodes<-Pub.Out[B.Version %in% paste0("V2.0.",22:25),B.Code]
    errors_bcodes<-Pub.Out$B.Code
    
    errorsx<-Plant.Out[B.Code %in% errors_bcodes,list(N_all=length(unique(P.Level.Name)),N_nobase=length(unique(P.Level.Name[P.Level.Name!="Base"]))),by=list(B.Code)]
    errorsy<-Plant.Method[B.Code %in% errors_bcodes,list(N_all_m=length(unique(P.Level.Name)),N_nobase_m=length(unique(P.Level.Name[P.Level.Name!="Base"]))),by=list(B.Code)]
    errorsxy<-merge(errorsx,errorsy,all.x=T)[,check:=F][N_all!=N_all_m,check:=T]
    errorsxy<-merge(errorsxy,Pub.Out[,list(B.Code,B.Version)],all.x=T)
    errorsxy<-errorsxy[check==T | is.na(N_all_m)
    ][,check:=NULL
    ][,table:="Plant.Method"
    ][,field:=NA][,issue:="Either structural issue with Plant.Out tab from debugging task or formulae in cells H2 and J2 needs copying down."]
    
    
    error_list<-error_tracker(errors= errorsxy,filename = "plant_debugging_errors",error_dir=error_dir,error_list = error_list)
    
    
  ## 3.11) Fertilizer (Fert.Out) #####
  data<-lapply(XL,"[[","Fert.Out")
  col_names<-colnames(data[[1]])
  
    ### 3.11.1) Fert.Out ######
  col_names2<-col_names[c(1:7,9:18)]
  Fert.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!"F.Int" %in% colnames(X)){
      X[,F.Int:="Not in template"]
    }
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Plant.Out tab structure,corruption of excel file?"))
    }else{
      X<-X[,..col_names2]
      X<-X[!is.na(F.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }
  })
  
  errors_a<-rbindlist(lapply(Fert.Out,"[[","errors"))
  Fert.Out<-rbindlist(lapply(Fert.Out,"[[","data"))
  setnames(Fert.Out,c("F.Prac.Rate","F.Prac.Timing","F.Prac.Precision","F.Prac.Info"),c("F.Rate.Pracs","F.Timing.Pracs","F.Precision.Pracs","F.Info.Pracs"))
  
  results<-validator(data=Fert.Out,
                     unique_cols="F.Level.Name",
                     numeric_cols=c("F.NO","F.PO","F.KO","F.NI","F.PI","F.P2O5","F.KI","F.K2O"),
                     zero_cols=c("F.O.Unit","F.I.Unit"),
                     trim_ws = T,
                     tabname="Fert.Out")
  
  errors1<-results$errors
  Fert.Out<-results$data
  
  # Check for errors in Fert.Out table
  
  # Missing units
  errors2<-unique(Fert.Out[(!is.na(F.NO)|!is.na(F.PO)|!is.na(F.KO)) & is.na(F.O.Unit),"B.Code"
                    ][,value:=NA
                      ][,table:="Fert.Out"
                        ][,field:="F.O.Unit"
                          ][,issue:="Organic fertilizer unit missing"])
  
  errors3<-unique(Fert.Out[(!is.na(F.NI)|!is.na(F.PI)|!is.na(F.KI)|!is.na(F.K2O)|!is.na(F.P2O5)) & is.na(F.I.Unit),"B.Code"
                    ][,value:=NA
                      ][,table:="Fert.Out"
                        ][,field:="F.I.Unit"
                          ][,issue:="Inorganic fertilizer unit missing"])
  
  # Check for multiple base practices
  errors11<-Fert.Out[F.Level.Name=="Yes",.(F.Level.Name,B.Code)
  ][,N:=length(F.Level.Name),by=.(B.Code)
  ][N>1
  ][,.(value=paste(F.Level.Name,collapse="/")),by=.(B.Code)
  ][,table:="Fert.Out"
  ][,field:="F.Level.Name"
  ][,issue:="Multiple base practices present."]
  
      #### 3.11.1.1) Harmonization #######
  h_params<-data.table(h_table="Fert.Out",
                       h_field=c("F.I.Unit","F.O.Unit","F.Rate.Pracs","F.Timing.Pracs","F.Precision.Pracs","F.Info.Pracs"),
                       h_table_alt=c("Fert.Out","Fert.Out",NA,NA,NA,NA),
                       h_field_alt=c("F.Unit","F.Unit",NA,NA,NA,NA))
  
  results<-harmonizer_wrap(data=Fert.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks1<-results$h_tasks[,issue:="Issue with units of inorganic or organic fertilizer, or practices selected."]
  
  Fert.Out<-results$data
  
  ### 3.11.2) Fert.Composition ######
  col_names2<-col_names[c(50:62)]
  col_names2[1]<-"F.Type2"
  col_names3<-col_names[c(64:73)]
  
  Fert.Comp<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    colnames(X)[grep("F.Type",colnames(X))]<-paste0("F.Type",1:length(grep("F.Type",colnames(X))))
    
    
    if(!all(c(col_names2,col_names3) %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(errors=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Fert.Composition tab structure"))
    }else{
      Y<-X[1,..col_names3]
      X<-X[,..col_names2]
      colnames(X)[1]<-"F.Type"
      X<-X[!is.na(F.Type)]
      
      if(nrow(X)>0){
        X<-cbind(X,Y[rep(1,nrow(X))])
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }
  })
  
  errors_c<-rbindlist(lapply(Fert.Comp,"[[","errors"))
  Fert.Comp<-rbindlist(lapply(Fert.Comp,"[[","data"))
  
  vars<-c("F.DM","F.OC","F.N","F.TN","F.AN","F.P","F.TP","F.AP","F.K")
  pairs<-data.table(var=vars,unit=paste0(vars,".Unit"),name_field="F.Type")
  
  results<-validator(data=Fert.Comp,
                     unique_cols = c("F.Type"),
                     numeric_cols=c("F.DM","F.OC","F.N","F.TN","F.AN","F.CN","F.P","F.TP","F.AP","F.K","F.pH"),
                     unit_pairs = pairs,
                     trim_ws = T,
                     tabname="Fert.Comp")
  
  errors8<-results$errors
  Fert.Comp<-results$data

  # Add indicator of whether a line refers to a fertilizer item or the whole fertilizer treatment
  match_dat<-Fert.Out[,.(B.Code,F.Level.Name)][,Is.F.Level.Name:=T]
  
  Fert.Comp<-merge(Fert.Comp,match_dat,all.x=T,by.x=c("B.Code","F.Type"),by.y=c("B.Code","F.Level.Name"))
  Fert.Comp[is.na(Is.F.Level.Name),Is.F.Level.Name:=F]
  
    #### 3.11.2.1) Harmonization #######
    h_params<-data.table(h_table="Fert.Comp",
                         h_field=c("F.DM.Unit","F.OC.Unit","F.N.Unit","F.TN.Unit","F.AN.Unit","F.P.Unit","F.TP.Unit","F.AP.Unit","F.K.Unit","F.pH.Method"),
                         h_table_alt=c("Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out"),
                         h_field_alt=c("Soil.SOM.Unit","Soil.SOM.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.K.Unit","Soil.pH.Method"))
    
    results<-harmonizer_wrap(data=Fert.Comp,
                             h_params=h_params,
                             master_codes = master_codes)
    
    h_tasks4<-results$h_tasks
    
    Fert.Comp<-results$data
    
  ### 3.11.3) Fert.Method ####
  col_names2<-col_names[c(30:49)]
  col_names2[3]<-"F.Type1"
  Fert.Method<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    colnames(X)[grep("F.Type",colnames(X))]<-paste0("F.Type",1:length(grep("F.Type",colnames(X))))
    
    col_names3<-col_names2
    if("F.NP2O5K2O" %in% colnames(X)){
      col_names3[4]<-"F.NP2O5K2O"
    }
    
    if(!all(col_names3 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(errors=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Fert.Method tab structure. Could be that NPK is missing"))
    }else{
      X<-X[,..col_names3]
      colnames(X)[1]<-"F.Level.Name"
      X<-X[!is.na(F.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        setnames(X,"F.Type1","F.Type")
        list(data=X)
      }else{
        NULL
      }
    }
  })
  
  errors_b<-rbindlist(lapply(Fert.Method,"[[","errors"))
  Fert.Method<-rbindlist(lapply(Fert.Method,"[[","data"),fill=T)
  setnames(Fert.Method,"Times","Time")
  
  # Update Site.ID
  Fert.Method[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(Fert.Method[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]
  ][is.na(Site.ID_new),Site.ID_new:=Site.ID
  ][,Site.ID:=Site.ID_new
  ][,Site.ID_new:=NULL]

  results<-validator(data=Fert.Method,
                     numeric_cols=c("F.Amount","F.Date.DAP","F.Date.DAE"),
                     date_cols=c("F.Date.End", "F.Date.Start", "F.Date"),
                     compulsory_cols = c(F.Level.Name="F.Category",F.Level.Name="F.Type"),
                     hilo_pairs = data.table(low_col="F.Date.Start",high_col="F.Date.End",name_field="F.Level.Name"),
                     unit_pairs = data.table(unit="F.Amount",var="F.Unit",name_field="F.Level.Name"),
                     valid_start=valid_start,
                     valid_end=valid_end,
                     site_data=Site.Out,
                     time_data=Times.Out,
                     tabname="Fert.Method",
                     trim_ws = T,
                     ignore_values = c("All Times","Unspecified","Not specified","All Sites"))
  
  errors6<-results$errors
  Fert.Method<-results$data
  
  # Check key fields
  errors7<-check_key(parent = Fert.Out,child = Fert.Method,tabname="Fert.Method",keyfield="F.Level.Name")
  
  # Fert Level Name not used in Fert Methods and Fert Methods table is not blank
  errors5<-Fert.Out[!grepl("control",F.Level.Name,ignore.case = T)][!F.Level.Name %in% Fert.Method$F.Level.Name,.(value=paste0(unique(F.Level.Name),collapse = "/")),by=B.Code]
  errors5<-errors5[B.Code %in% Fert.Method[F.Level.Name!="Base",B.Code]
                                           ][,table:="Fert.Method"
                                             ][,field:="F.Level.Name"  
                                               ][,issue:="Name is not used in Fert.Method table and fert method table is not blank, could indicate an error."]

   #### 3.11.3.2) Harmonization #######
  h_params<-data.table(h_table="Fert.Method",
                       h_field=c("F.Unit","F.Method","F.Physical","F.Mechanization","F.Source","F.Fate"),
                       h_table_alt=c("Fert.Out",NA,NA,"Fert.Out","Fert.Method","Res.Method"),
                       h_field_alt=c("F.Unit",NA,NA,"F.Mechanization","M.Source","M.Fate"))
  
  results<-harmonizer_wrap(data=Fert.Method,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks2<-results$h_tasks
  
  Fert.Method<-results$data
  
  # Update fertilizer names using fert tab of master_codes and merge associated f.codes
  fert_master<-unique(master_codes$fert[,list(F.Category,F.Type,F.Category_New,F.Type_New,Fert.Code1,Fert.Code2,Fert.Code3,Fert.Code4)][,match:=T])
  
  # Convert multiple old names using ; delimiter to new rows
  fert_master<-rbindlist(pblapply(1:nrow(fert_master),FUN=function(i){
    data<-fert_master[i]
    N<-data[,str_count(F.Type,";")]
    if(!is.na(N)){
      data<-fert_master[rep(i,N+1) ]
      data[,F.Type:=unlist(str_split(F.Type[1],";"))]
    }
    data
  }))
  
  # We also need to update the Fert.Composition Table, so merge in the F.Category
  # Note that some values are the F.Level.Name
  Fert.Comp<-merge(Fert.Comp,unique(Fert.Method[,list(B.Code,F.Type,F.Category)]),by=c("B.Code","F.Type"),all.x=T)
  Fert.Comp[!is.na(F.Category),F.Type.Codex:=paste(c(F.Category[1],F.Type[1]),collapse = "-"),by=.(F.Category,F.Type)
            ][,F.Type.Code:=tolower(F.Type.Codex),by=F.Type.Codex]
  
  # Create a code for fertilizers in the excels
  Fert.Method[,F.Type.Codex:=paste(c(F.Category[1],F.Type[1]),collapse = "-"),by=.(F.Category,F.Type)
              ][,F.Type.Code:=tolower(F.Type.Codex),by=F.Type.Codex]
  
  # Create codes the master file to match again
  fert_master[,F.Type.Codex:=paste(c(F.Category[1],F.Type[1]),collapse = "-"),by=.(F.Category,F.Type)
              ][,F.Type.Code:=tolower(F.Type.Codex),by=F.Type.Codex
              ][,F.Type.Code2x:=paste(c(F.Category_New[1],F.Type_New[1]),collapse = "-"),by=.(F.Category_New,F.Type_New)
              ][,F.Type.Code2:=tolower(F.Type.Code2x),by=F.Type.Code2x
              ][,F.Codes:=paste0(unique(sort(c(Fert.Code1,Fert.Code2,Fert.Code3,Fert.Code4))),collapse = "-"),by=F.Type.Code2x
                ][,F.Codes:=gsub("-NA","",F.Codes),by=F.Codes]
  
  # Match on "old names"
  Fert.Method<-merge(Fert.Method,fert_master[,list(F.Type.Code,F.Type.Code2x,F.Codes)],all.x=T,by="F.Type.Code")
  Fert.Comp<-merge(Fert.Comp,fert_master[,list(F.Type.Code,F.Type.Code2x)],all.x=T,by="F.Type.Code")
  
  # Match on "new names"
  fert_master2<-unique(fert_master[,.(F.Type.Code2x,F.Type.Code2,F.Codes)])
  names(fert_master2)<-c("F.Type.Code2x2","F.Type.Code","F.Codes2")
  Fert.Method<-merge(Fert.Method,fert_master2,
                     all.x=T,by="F.Type.Code")
  
  # Combine merged codes from old and new names
  Fert.Method[is.na(F.Type.Code2x),F.Codes:=F.Codes2
              ][is.na(F.Type.Code2x),F.Type.Code2x:=F.Type.Code2x2
                ][,c("F.Codes2","F.Type.Code2x2"):=NULL]
  
  # Expose fertilizers with no match
  h_tasks3<-Fert.Method[is.na(F.Type.Code2x),list(B.Code=paste(unique(B.Code),collapse="/")),by=F.Type.Codex
                        ][,table:="Fert.Method"
                          ][,field:="F.Category-F.Type"
                            ][,table_alt:=NA
                              ][,field_alt:="F.Type_New"
                                ][,master_tab:="fert"]
  setnames(h_tasks3,"F.Type.Codex","value")
  
  write.table(h_tasks3,"clipboard-256000",row.names = F,sep="\t")
  
  # Replace values
  Fert.Method[!is.na(F.Type.Code2x),F.Type:=unlist(tstrsplit(F.Type.Code2x,"-",keep=2))
              ][!is.na(F.Type.Code2x),F.Category:=unlist(tstrsplit(F.Type.Code2x,"-",keep=1))]
  
  Fert.Method[,c("F.Type.Code2x","F.Type.Code","F.Type.Codex"):=NULL]
  
  Fert.Comp[!is.na(F.Type.Code2x),F.Type:=unlist(tstrsplit(F.Type.Code2x,"-",keep=2))
              ][!is.na(F.Type.Code2x),F.Category:=unlist(tstrsplit(F.Type.Code2x,"-",keep=1))]
  
  Fert.Comp[,c("F.Type.Code2x","F.Type.Code","F.Type.Codex"):=NULL]
  
  # Check Fert.Comp vs Fert.Method key fields
  errors9<-check_key(parent = Fert.Method,child = Fert.Comp,tabname="Fert.Comp",keyfield="F.Type")
  
  # Use of unspecified fertilizer type, need to check if should have been NPK or unspecified P, N, K etc.
  errors10<-unique(Fert.Method[F.Type=="Unspecified",list(B.Code=unique(B.Code)),.(F.Level.Name,F.Category,F.Type)])
  errors10<-errors10[,code:=paste0(F.Category,"-",F.Type)
           ][,.(value=paste0(unique(F.Level.Name),collapse="/")),by=.(B.Code,code)
             ][,value:=paste0(code,": ",value)
               ][,code:=NULL
                 ][,table:="Fert.Method"
                   ][,field:="F.Category-F.Type:F.Level.Name"
                     ][,issue:="Use of Unspecified as fertilizer type name. We need to check that this truly should have been unspecfied (i.e. we know nothing about the fertilizer applied at all) or if unspecified N, P or K could have been used."]
  

  ### 3.11.4) Save harmonization and error tasks #####
  errors<-rbindlist(list(errors_a,errors_b,errors_c))
  error_list<-error_tracker(errors=errors,filename = "fert_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors5,errors6,errors7,errors8,errors9,errors10,errors11),fill=T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "fert_other_errors",error_dir=error_dir,error_list = error_list)
  
  h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4),fill=T)
  harmonization_list<-error_tracker(errors=h_tasks,filename = "fert_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  ### 3.11.5) Fert.Out: Update Fertilizer codes   #######
     # Fert.Method: Combine F.Codes across ingredients and merge with Fert.Out
      Fert.Method[,F.Codes.Level:=paste(sort(unique(unlist(strsplit(F.Codes,"-")))),collapse = "-"),by=.(B.Code,F.Level.Name)
                  ][,F.Codes.Level:=gsub("-NA","",F.Codes.Level)]
      
      Fert.Out<-merge(Fert.Out,unique(Fert.Method[,.(B.Code,F.Level.Name,F.Codes.Level)]),all.x=T,by=c("B.Code","F.Level.Name")) 
      
      # Fert_Out: Add Fertilizer Codes from NPK rating
      Fert.Out[(!is.na(F.NI) & F.NI!=0) & !grepl("b17|b23",F.Codes.Level),F.NI.Code:="b17",by=.(B.Code,F.Level.Name)
               ][((!is.na(F.PI) & F.PI!=0)|(!is.na(F.P2O5) & F.P2O5!=0)) & !grepl("b21",F.Codes.Level),F.PI.Code:="b21",by=.(B.Code,F.Level.Name)
                 ][((!is.na(F.KI) & F.KI!=0)|(!is.na(F.K2O) & F.K2O!=0)) & !grepl("b16",F.Codes.Level),F.KI.Code:="b16",by=.(B.Code,F.Level.Name)]
      
      Fert.Out[,F.Codes:=paste(sort(unique(c(F.NI.Code,F.NI.Code,F.KI.Code,unlist(strsplit(F.Codes.Level,"-"))))),collapse = "-"),by=.(B.Code,F.Level.Name)
               ][,F.Codes:=gsub("-NA|NA","",F.Codes)
                 ][F.Codes=="",F.Codes:=NA
                   ][,c("F.Codes.Level","F.NI.Code","F.KI.Code","F.PI.Code"):=NULL]
  
      # Fert.Out: Add in h10.1 Codes for no Fertilizer use
        Fert.Out[is.na(F.Codes) & !F.Level.Name=="Base",F.Codes:="h10.1"]
        
        # TO DO: Consider codes for urea in Fert.Method and NI listed in Fert.Out
        # TO DO: Add check in case organic nutrients are listed in Fert.Out, but nothing corresponding in Fert.Method tab.
    
  ### 3.11.6) Add F.Level.Name2 field ######
      ReName.Fun<-function(X,Y,F.NO,F.PO,F.KO,F.NI,F.PI,F.P2O5,F.KI,F.K2O){
        NPK<-data.table(F.NO=F.NO,F.PO=F.NO,F.KO=F.KO,F.NI=F.NI,F.PI=F.PI,F.P2O5=F.P2O5,F.KI=F.KI,F.K2O=F.K2O)
        N<-colnames(NPK)[which(!apply(NPK,2,is.na))]
        
        
        F.Level<-Fert.Method[F.Level.Name==X & B.Code ==Y,list(F.Type,F.Amount)]
        
        F.Level<-rbind(F.Level[is.na(F.Amount)],F.Level[!is.na(F.Amount),list(F.Amount=sum(F.Amount)),by=F.Type])
        
        # Deal with rounding issues   
        F.Level[,F.Amount:=round(F.Amount*10*round(log(F.Amount,base=10)),0)/(10*round(log(F.Amount,base=10)))]
        F.Level<-F.Level[,paste(F.Type,F.Amount)]
        
        F.Level<-paste(F.Level[order(F.Level)],collapse = "|")
        
        
        if(length(F.Level)==0 | F.Level==""){
          F.Level<-"No Fert Control"
        }else{
          if(length(N)>0){
            NPK<-NPK[,..N]
            NPK<-paste(paste(colnames(NPK),unlist(NPK),sep="-"),collapse=" ")
            F.Level<-paste(NPK,F.Level,sep = "||")
          }
        }
        return(F.Level)
      }
      
      Fert.Out[,N:=1:.N]
      Fert.Out[,F.Level.Name2:=ReName.Fun(F.Level.Name,B.Code,F.NO,F.PO,F.KO,F.NI,F.PI,F.P2O5,F.KI,F.K2O),by="N"]
      
      # Tidy up
      Fert.Out[,N:=NULL]
      rm(ReName.Fun)
      
      Fert.Method<-merge(Fert.Method,Fert.Out[,.(B.Code,F.Level.Name,F.Level.Name2)],by=c("B.Code","F.Level.Name"),all.x=T,sort=F)
      
  ### 3.11.x) ***!!!TO DO!!!*** Add in h10 code where there are fertilizer treatments, but fertilizer column is blank #######
  if(F){
    # Old Code
  MT.Out[,Count.F.NA:=sum(is.na(F.Level.Name)),by="B.Code"][,Count.F:=sum(!is.na(F.Level.Name)),by="B.Code"]
  MT.Out[Count.F.NA>0 & Count.F>0 & is.na(F.Level.Name) & !(N %in% grep("[.][.]",T.Name)),F.Level.Name:="No Fertilizer Based On MT.Out"]
  MT.Out[,Count.F.NA:=NULL][,Count.F:=NULL]
  
  # Add No Fertilizer Practice to Fert.Out Tab
  # Studies With
  Y<-unique(MT.Out[F.Level.Name=="No Fertilizer Based On MT.Out",B.Code])
  X<-Fert.Out[1]
  X[1:ncol(X)]<-NA
  X<-X[rep(1,length(Y))][,B.Code:=Y][,F.Level.Name:="No Fertilizer Based On MT.Out"][,F.Codes:="h10"][,F.Codes2:="h10"]
  Fert.Out<-rbind(Fert.Out,X)
  rm(X,Y)
  
  # Check for instances where T.Codes do not contain "h10" but F.Codes do, this will indicate an error (e.g. ".." delim still being used in Fert.Out tab)
  MT.Out[F.Codes=="h10"][-grep("h10",T.Codes)]
  unique(Fert.Out[grep("[.][.]",F.Level.Name),"B.Code"])
  }
  ### 3.12) Chemicals #####
  data<-lapply(XL,"[[","Chems.Out")
  col_names<-colnames(data[[1]])
  
   ### 3.12.1) Chems.Code ####
  col_names2<-col_names[1:3]
  
  Chems.Code<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Chems.Out tab structure (first 3 cols A:C in excel)"))
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
  
  errors_a<-rbindlist(lapply(Chems.Code,"[[","errors"))
  Chems.Code<-rbindlist(lapply(Chems.Code,"[[","data"))
  
  results<-validator(data=Chems.Code,
                     unique_cols = "C.Level.Name",
                     tabname="Chems.Code",
                     trim_ws = T)
  
  errors8<-results$errors
  Chems.Code<-results$data
  
    ### 3.12.2) Chems.Out ####
  col_names2<-col_names[5:23]
  col_names2[grep("C.Type",col_names2)]<-"C.Type1"
  col_names2[grep("C.Brand",col_names2)]<-"C.Brand1"
  
  Chems.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!"C.Date.DAE" %in% colnames(X)){
      X[,C.Date.DAE:=NA]
    }
    
    colnames(X)[grep("C.Type",colnames(X))]<-paste0("C.Type",1:length(grep("C.Type",colnames(X))))
    colnames(X)[grep("C.Brand",colnames(X))]<-paste0("C.Brand",1:length(grep("C.Brand",colnames(X))))
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Chems.Out tab structure (first 3 cols A:C in excel)"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"C.Level.Name"
      X<-X[!is.na(C.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        setnames(X,c("C.Type1","C.Brand1"),c("C.Type","C.Brand"))
        list(data=X)
      }else{
        NULL
      }
    }})
  errors_b<-rbindlist(lapply(Chems.Out,"[[","errors"))
  Chems.Out<-rbindlist(lapply(Chems.Out,"[[","data"))
  setnames(Chems.Out,"C.Brand","C.Name")
  Chems.Out[,C.Type:=gsub("Animal - ","",C.Type)]
  
  # Update Site.ID
  Chems.Out[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(Chems.Out[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]    
  ][is.na(Site.ID_new),Site.ID_new:=Site.ID
  ][,Site.ID:=Site.ID_new
  ][,Site.ID_new:=NULL]

  results<-validator(data=Chems.Out,
                     compulsory_cols = c(C.Level.Name="C.Type",C.Level.Name="C.Name"),
                     numeric_cols=c("C.Amount","C.Date.DAP","C.Date.DAE"),
                     date_cols=c("C.Date.Start", "C.Date.End", "C.Date"),
                     valid_start=valid_start,
                     valid_end=valid_end,
                     site_data=Site.Out,
                     time_data=Times.Out,
                     tabname="Chems.Out",
                     ignore_values = c("All Times","Unspecified","Not specified","All Sites"),
                     trim_ws = T)
  
  errors1<-results$errors
  Chems.Out<-results$data
  
  # Dates are dd/mm format, (year agnostic)
  Chems.Out[,C.Date:=format(C.Date,"%m-%d")]
  errors1<-errors1[field!="C.Date"]
  
  errors2<-unique(Chems.Out[C.Date.End<C.Date.Start,list(B.Code,C.Date.End,C.Date.Start)])
  errors2[,value:=paste0("C.Date.Start =",C.Date.Start," & C.Date.End = ",C.Date.End)
          ][,c("C.Date.Start","C.Date.End"):=NULL
            ][,table:="Chems.Out"
              ][,field:="C.Date.Start/C.Date.End"
                ][,issue:="End date before start date."
                  ][order(B.Code)]
  
  # Check key_fields
  errors3<-check_key(parent = Chems.Code,child = Chems.Out,tabname="Chems.Out",keyfield="C.Level.Name")
  
  # Check units
  errors9<-Chems.Out[is.na(C.Unit) & !is.na(C.Amount),list(value=paste(C.Level.Name,collapse = "/")),by=B.Code
                       ][,table:="Chems.Out"
                         ][,field:="C.Level.Name"
                           ][,issue:="Amount is present, but unit is missing."
                             ][order(B.Code)]
  
  
  # Add Code for Herbicide
  Chems.Out[C.Type=="Herbicide",C.Code:="h66.1"]
  
  # Check for multiple base practices
  errors11<-Chems.Out[C.Level.Name=="Yes",.(C.Level.Name,B.Code)
  ][,N:=length(C.Level.Name),by=.(B.Code)
  ][N>1
  ][,.(value=paste(C.Level.Name,collapse="/")),by=.(B.Code)
  ][,table:="Chems.Out"
  ][,field:="C.Level.Name"
  ][,issue:="Multiple base practices present."]
  
      #### 3.12.2.1) Harmonization #######
  h_params<-data.table(h_table="Chems.Out",
                       h_field=c("C.App.Method","C.Mechanization","C.Unit"),
                       h_table_alt=c(NA,"Fert.Out",NA),
                       h_field_alt=c(NA,"F.Mechanization",NA))
  
  results<-harmonizer_wrap(data=Chems.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks1<-results$h_tasks
  Chems.Out<-results$data
  
      ##### NEEDS UPDATING ACCORDING TO NEW CHEMS STRUCTURE ######
  if(F){
  mergedat<-setnames(master_codes$chem[,4:5],"C.Name.2020...5","C.Name")[!is.na(C.Type)][,check:=T]
  
  h_tasks2<-setnames(merge(Chems.Out[,list(B.Code,C.Type,C.Name)],mergedat,all.x = T)[is.na(check)][,check:=NULL],"C.Name","value")[,field:="C.Name"][,field_alt:="C.Name.2020"][,table:="Chem.Out"][,master_tab:="chem"]
  
  errors7<-unique(h_tasks2[!C.Type %in% mergedat$C.Type][,issue:="C.Type may be incorrect or simply missing from chem tab in master sheet."][,value:=C.Type][,field:="C.Type"][,C.Type:=NULL])
  h_tasks2<-h_tasks2[C.Type %in% mergedat$C.Type]
  }
    ### 3.12.3) Chems.AI ####
  col_names2<-col_names[25:31]
  col_names2[grep("C.Type",col_names2)]<-"C.Type2"
  col_names2[grep("C.Brand",col_names2)]<-"C.Brand2"
  
  Chems.AI<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!"C.Date.DAE" %in% colnames(X)){
      X[,C.Date.DAE:=NA]
    }
    
    colnames(X)[grep("C.Type",colnames(X))]<-paste0("C.Type",1:length(grep("C.Type",colnames(X))))
    colnames(X)[grep("C.Brand",colnames(X))]<-paste0("C.Brand",1:length(grep("C.Brand",colnames(X))))
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Chems.Out tab structure (first 3 cols A:C in excel)"))
    }else{
      X<-X[,..col_names2]
      X<-X[!is.na(C.Brand2)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        setnames(X,c("C.Type2","C.Brand2"),c("C.Type","C.Brand"))
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_c<-rbindlist(lapply(Chems.AI,"[[","errors"))
  Chems.AI<-rbindlist(lapply(Chems.AI,"[[","data"))
  setnames(Chems.AI,"C.Brand","C.Name")
  Chems.AI[,C.Type:=gsub("Animal - ","",C.Type)]
  
  results<-validator(data=Chems.AI,
                     numeric_cols=c("C.AI.Amount"),
                     tabname="Chems.AI",
                     trim_ws = T)
  
  errors4<-results$errors
  Chems.AI<-results$data
  
  errors5<-check_key(parent = Chems.Out,child = Chems.AI,tabname="Chems.AI",keyfield="C.Name")[,issue:="Mismatch in Product Name (C.Brand or C.Name) between description and AI table."]
  
  # Check units
  errors10<-Chems.AI[is.na(C.AI.Unit) & !is.na(C.AI.Amount),list(value=paste(C.Name,collapse = "/")),by=B.Code
  ][,table:="Chems.AI"
  ][,field:="C.Name"
  ][,issue:="Amount is present, but unit is missing (active ingredient section)."
  ][order(B.Code)]
  
      #### 3.12.3.1) Harmonization #######
  h_params<-data.table(h_table="Chems.AI",
                       h_field="C.AI.Unit",
                       h_table_alt="Chems.Out",
                       h_field_alt="C.AI.Unit")
  
  results<-harmonizer_wrap(data=Chems.AI,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks3<-results$h_tasks
  Chems.AI<-results$data
      ##### NEEDS UPDATING ACCORDING TO NEW CHEMS STRUCTURE ######
  if(F){
    mergedat<-setnames(master_codes$chem[,4:5],"C.Name.2020...5","C.Name.AI")[!is.na(C.Type)][,check:=T][,C.Name.AI:=tolower(C.Name.AI)]
    h_tasks4<-setnames(merge(Chems.AI[!is.na(C.Name.AI),list(B.Code,C.Type,C.Name.AI)][,C.Name.AI:=tolower(C.Name.AI)],mergedat,all.x = T)[is.na(check)][,check:=NULL],"C.Name.AI","value")
    h_tasks4<-h_tasks4[,list(B.Code=paste0(unique(B.Code),collapse="/")),by=list(C.Type,value)
                       ][,field:="C.Name.AI"
                         ][,field_alt:="C.Name.2020"
                           ][,table:="Chems.AI"
                             ][,master_tab:="chems"]
                             
    errors6<-unique(h_tasks4[!C.Type %in% mergedat$C.Type][,issue:="C.Type may be incorrect, or simply missing from Master Sheet."][,value:=C.Type][,field:="C.Type"][,C.Type:=NULL])
    h_tasks4<-h_tasks4[C.Type %in% mergedat$C.Type]
  }
    ### 3.12.4) Add/check herbicide codes ######
    
    # Add C.Code to Chems.Code tab
    Chems.Code<-merge(Chems.Code,
                      Chems.Out[,list(C.Codes=unique(C.Code[!is.na(C.Code)])),by=list(C.Level.Name,B.Code)],
                      by=c("B.Code","C.Level.Name"),
                      all.x=T)
  
    ### 3.12.5) Save errors and harmonization ######
  errors<-rbindlist(list(errors_a,errors_b,errors_c))
  error_list<-error_tracker(errors=errors,filename = "chem_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6,errors7,errors8,errors9,errors10,errors11),fill=T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "chem_other_errors",error_dir=error_dir,error_list = error_list)
  
  h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4),fill=T)
  harmonization_list<-error_tracker(errors=h_tasks,filename = "chem_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  ## 3.14) Residues #######
  data<-lapply(XL,"[[","Residues.Out")
  col_names<-colnames(data[[1]])
  
    ### 3.14.1) Res.Out #######
    col_names2<-col_names[1:11]
    col_names2[grep("M.Level.Name",col_names2)]<-"M.Level.Name1"
    col_names2[grep("M.Unit",col_names2)]<-"M.Unit1"
    
    Res.Out<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      colnames(X)[grep("M.Level.Name",colnames(X))]<-paste0("M.Level.Name",1:length(grep("M.Level.Name",colnames(X))))
      colnames(X)[grep("M.Unit",colnames(X))]<-paste0("M.Unit",1:length(grep("M.Unit",colnames(X))))
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Residues.Out tab structure"))
      }else{
        X<-X[,..col_names2]
        setnames(X,c("M.Level.Name1","M.Unit1"),c("M.Level.Name","M.Unit"))
        
        X<-X[!is.na(M.Level.Name)]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }})
    
    errors_a<-rbindlist(lapply(Res.Out,"[[","error"))
    Res.Out<-rbindlist(lapply(Res.Out,"[[","data"))
    
    results<-validator(data=Res.Out,
                       unique_cols = "M.Level.Name",
                       numeric_cols=c("M.NO","M.PO","M.KO"),
                       zero_cols="M.Unit",
                       tabname="Res.Out",
                       duplicate_field = "M.Level.Name",
                       trim_ws = T)
    
    errors1<-results$errors
    Res.Out<-results$data
    
    Res.Out<-Res.Out[!(is.na(P1) & is.na(P2) & is.na(P3) & is.na(P4))]
    
    # Check units
    errors2<-Res.Out[is.na(M.Unit) & !(is.na(M.KO) & is.na(M.NO) & is.na(M.PO)),list(value=paste(M.Level.Name,collapse = "/")),by=B.Code
    ][,table:="Res.Out"
    ][,field:="M.Level.Name"
    ][,issue:="Amount is present, but unit is missing."
    ][order(B.Code)]
    
    # Check for multiple base practices
    errors10<-Res.Out[M.Level.Name=="Yes",.(M.Level.Name,B.Code)
    ][,N:=length(M.Level.Name),by=.(B.Code)
    ][N>1
    ][,.(value=paste(M.Level.Name,collapse="/")),by=.(B.Code)
    ][,table:="Res.Out"
    ][,field:="M.Level.Name"
    ][,issue:="Multiple base practices present."]
    
    #### 3.14.1.1) Harmonization ########
    h_params<-data.table(h_table="Res.Out",
                         h_field=c("M.Unit"),
                         h_table_alt=c("Fert.Out"),
                         h_field_alt=c("F.Unit"))
    
    results<-harmonizer_wrap(data=Res.Out,
                             h_params=h_params,
                             master_codes = master_codes)
    
    errors2<-results$h_tasks[,issue:="Issue with units"]
    
    Res.Out<-results$data
    
    ### 3.14.2) Res.Method #######
    col_names2<-col_names[13:31]
    col_names2[grep("M.Level.Name",col_names2)]<-"M.Level.Name2"
    col_names2[grep("M.Unit",col_names2)]<-"M.Unit2"
    col_names2[grep("M.Tree",col_names2)]<-"M.Tree1"
    col_names2[grep("M.Material",col_names2)]<-"M.Material1"
    
    Res.Method<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      if(!"M.Date.DAE" %in% colnames(X)){
        X[,M.Date.DAE:=NA]
      }
      
      if(!"M.Date.Text" %in% colnames(X)){
        X[,M.Date.Text:=NA]
      }
      
      
      colnames(X)[grep("M.Level.Name",colnames(X))]<-paste0("M.Level.Name",1:length(grep("M.Level.Name",colnames(X))))
      colnames(X)[grep("M.Unit",colnames(X))]<-paste0("M.Unit",1:length(grep("M.Unit",colnames(X))))
      colnames(X)[grep("M.Tree",colnames(X))]<-paste0("M.Tree",1:length(grep("M.Tree",colnames(X))))
      colnames(X)[grep("M.Material",colnames(X))]<-paste0("M.Material",1:length(grep("M.Material",colnames(X))))
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Res.Method tab structure"))
      }else{
        X<-X[,..col_names2]
        setnames(X,c("M.Level.Name2","M.Unit2","M.Tree1","M.Material1"),c("M.Level.Name","M.Unit","M.Tree","M.Material"))
        
        X<-X[!is.na(M.Level.Name)]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }})
    
    errors_b<-rbindlist(lapply(Res.Method,"[[","error"))
    Res.Method<-rbindlist(lapply(Res.Method,"[[","data"))
    
    setnames(Res.Method,"Times","Time")
    
    # Update Site.ID
    Res.Method[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(Res.Method[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]  
    ][is.na(Site.ID_new),Site.ID_new:=Site.ID
    ][,Site.ID:=Site.ID_new
    ][,Site.ID_new:=NULL]

    results<-validator(data=Res.Method,
                       numeric_cols=c("M.Amount","M.Cover","M.Date.DAP","M.Date.DAE"),
                       date_cols=c("M.Date.End", "M.Date.Start", "M.Date"),
                       hilo_pairs = data.table(low_col="M.Date.Start",high_col="M.Date.End",name_field="M.Level.Name"),
                       unit_pairs = data.table(unit="M.Unit",var="M.Amount",name_field="M.Level.Name"),
                       valid_start=valid_start,
                       valid_end=valid_end,
                       site_data=Site.Out,
                       time_data=Times.Out,
                       tabname="Res.Method",
                       ignore_values = c("All Times","Unspecified","Not specified","All Sites"),
                       trim_ws = T)
    
    errors3<-results$errors
    Res.Method<-results$data
    
    errors4<-check_key(parent = Res.Out,child = Res.Method,tabname="Res.Method",keyfield="M.Level.Name")
    
    # Check compulsory field
    errors5<-Res.Method[is.na(M.Tree) & is.na(M.Material),list(value=paste(M.Level.Name,collapse = "/")),by=B.Code
    ][,table:="Res.Method"
    ][,field:="M.Level.Name"
    ][,issue:="Compulsory field is NA M.Tree and M.Material"
    ][order(B.Code)]
    
      #### 3.14.2.2) Harmonization ########
    h_params<-data.table(h_table="Res.Method",
                         h_field=c("M.Unit","M.Mechanization","M.Fate","M.Process"),
                         h_table_alt=c(NA,"Fert.Out",NA,"Res.Out"),
                         h_field_alt=c(NA,"F.Mechanization",NA,"M.Process"))
    
    results<-harmonizer_wrap(data=Res.Method,
                             h_params=h_params,
                             master_codes = master_codes)
    
    h_tasks1<-results$h_tasks
    Res.Method<-results$data
    
    h_tasks2<-val_checker(data=Res.Method[!grepl("unspecified",M.Tree,ignore.case = T)],tabname="Res.Method",master_codes,master_tab="trees",h_field="M.Tree",h_field_alt="Tree.Latin.Name",exact=T)
    
    h_tasks3<-val_checker(data=Res.Method[!grepl("unspecified",M.Material,ignore.case = T) & is.na(M.Tree)],tabname="Res.Method",master_codes,master_tab="residues",h_field="M.Material",exact=T)
    
    h_tasks4<-harmonizer(data=Res.Method[!grepl("unspecified",M.Material,ignore.case = T) & !is.na(M.Tree)],h_table="Res.Method",h_field="M.Material",h_table_alt = "Res.Method",h_field_alt = "Tree.Materials",master_codes = master_codes)$h_tasks
    
    
    Res.Method[M.Tree == "Acacia angustissima"]
    
    ### 3.14.3) Res.Composition ######
    col_names2<-col_names[33:56]
    col_names2[grep("M.Tree",col_names2)]<-"M.Tree2"
    col_names2[grep("M.Material",col_names2)]<-"M.Material2"
    
    Res.Comp<-lapply(1:length(data),FUN=function(i){
      X<-data[[i]]
      B.Code<-Pub.Out$B.Code[i]
      
      colnames(X)[grep("M.Tree",colnames(X))]<-paste0("M.Tree",1:length(grep("M.Tree",colnames(X))))
      colnames(X)[grep("M.Material",colnames(X))]<-paste0("M.Material",1:length(grep("M.Material",colnames(X))))
      
      if(!all(col_names2 %in% colnames(X))){
        cat("Structural issue with file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Res.Comp tab structure"))
      }else{
        X<-X[,..col_names2]
        setnames(X,c("M.Tree2","M.Material2"),c("M.Tree","M.Material"))
        
        X<-X[!(is.na(M.Tree) & is.na(M.Material))]
        if(nrow(X)>0){
          X[,B.Code:=B.Code]
          list(data=X)
        }else{
          NULL
        }
      }})
    
    errors_c<-rbindlist(lapply(Res.Comp,"[[","error"))
    Res.Comp<-rbindlist(lapply(Res.Comp,"[[","data"))
    
    results<-validator(data=Res.Comp,
                       numeric_cols=c("M.DM","M.OC","M.N","M.TN","M.AN","M.CN","M.P","M.TP","M.AP","M.K"),
                       tabname="Res.Comp",
                       trim_ws = T)
    
    errors6<-results$errors
    Res.Comp<-results$data
    
    # Check key fields
    errors7<-check_key(parent = Res.Method,child = Res.Comp[!is.na(M.Tree)],tabname="Res.Method",keyfield="M.Tree")[value=="Acacia angustissima/Acacia caerulescens",issue:="Junk data in composition tab"]
    errors8<-check_key(parent = Res.Method,child = Res.Comp[is.na(M.Tree)],tabname="Res.Method",keyfield="M.Material")
    
    vars<-c("M.DM","M.OC","M.N","M.TN","M.AN","M.P","M.TP","M.AP","M.K")
    pairs<-data.table(var=vars,unit=paste0(vars,".Unit"),name_field="M.Material")
    
    errorsx<-check_units(data=Res.Comp,unit_pairs=pairs,tabname="Res.Comp")
    errorsy<-check_units(data=Res.Comp,unit_pairs=pairs[,name_field:="M.Tree"],tabname="Res.Comp")
  
    errors9<-rbind(errorsx,errorsy)[value!=""][order(B.Code)]
    
     #### 3.14.3.1) Harmonization #######
    h_params<-data.table(h_table="Res.Comp",
                         h_field=c("M.DM.Unit","M.OC.Unit","M.N.Unit","M.TN.Unit","M.AN.Unit","M.P.Unit","M.TP.Unit","M.AP.Unit","M.K.Unit","M.pH.Method"),
                         h_table_alt=c("Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out"),
                         h_field_alt=c("Soil.SOM.Unit","Soil.SOM.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.K.Unit","Soil.pH.Method"))
    
    results<-harmonizer_wrap(data=Res.Comp,
                             h_params=h_params,
                             master_codes = master_codes)
    
    h_tasks5<-results$h_tasks
    
    Res.Comp<-results$data
    
    ### 3.14.4) Save errors and harmonization ######
    errors<-rbindlist(list(errors_a,errors_b,errors_c))
    error_list<-error_tracker(errors=errors,filename = "residue_structure_errors",error_dir=error_dir,error_list = error_list)
    
    errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6,errors7,errors8,errors9,errors10),fill=T)[order(B.Code)]
    error_list<-error_tracker(errors=errors,filename = "residues_other_errors",error_dir=error_dir,error_list = error_list)
    
    h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4,h_tasks5),fill=T)
    harmonization_list<-error_tracker(errors=h_tasks,filename = "residue_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
    
  ## 3.15) pH #####
  data<-lapply(XL,"[[","pH.Out")
  col_names<-colnames(data[[1]])
    
    ### 3.15.1) pH.Out ######
  col_names2<-col_names[1:4]
  
  pH.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with pH.Level.Name tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"pH.Level.Name"
      X<-X[!is.na(pH.Level.Name)][!is.na(pH.Prac)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
    
  errors_a<-rbindlist(lapply(pH.Out,"[[","errors"))
  pH.Out<-rbindlist(lapply(pH.Out,"[[","data"))  
  
  # Fix missing pH.Codes
  pH.Out[is.na(pH.Codes) & pH.Prac=="Liming or Calcium Addition",pH.Codes:="b32"]
  
  results<-validator(data=pH.Out,
                     unique_cols = "pH.Level.Name",
                     tabname="pH.Out",
                     trim_ws = T)
  
  errors1<-results$errors
  pH.Out<-results$data
  
  # Check for multiple base practices
  errors5<-pH.Out[pH.Level.Name=="Yes",.(pH.Level.Name,B.Code)
  ][,N:=length(pH.Level.Name),by=.(B.Code)
  ][N>1
  ][,.(value=paste(pH.Level.Name,collapse="/")),by=.(B.Code)
  ][,table:="pH.Out"
  ][,field:="pH.Level.Name"
  ][,issue:="Multiple base practices present."]
  
    ### 3.15.2) pH.Method ######
  col_names2<-col_names[6:13]
  
  pH.Method<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with pH.Level.Name tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"pH.Level.Name"
      X<-X[!is.na(pH.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_b<-rbindlist(lapply(pH.Method,"[[","errors"))
  pH.Method<-rbindlist(lapply(pH.Method,"[[","data"))  
  
  results<-validator(data=pH.Method,
                     numeric_cols=c("pH.CaCO3","pH.ECCE","pH.MgCO3","pH.Amount"),
                     compulsory_cols = c(pH.Level.Name="pH.Material"),
                     unit_pairs = data.table(unit="pH.Unit",var="pH.Amount",name_field="pH.Level.Name"),
                     tabname="pH.Method",
                     trim_ws = T)
  
  errors2<-results$errors
  pH.Method<-results$data
  
  errors3<-check_key(parent = pH.Out,child = pH.Method,tabname="pH.Method",keyfield="pH.Level.Name")
  
  # Check junk data issue (where Liming or Calcium Addition was accidently left in the Master Excel Template)
  # These files have a pH practice present with no notes (notes would indicate it is not junk data)
  check_codes<-pH.Out[pH.Level.Name=="Base" & pH.Prac=="Liming or Calcium Addition" & is.na(pH.Notes),B.Code]
  # Subset to codes that have no corresponding entry in the pH.Method table (i.e. they are not described so probably junk data)
  check_codes<-check_codes[!check_codes %in% pH.Method$B.Code]
  errors4<-data.table(B.Code=check_codes,value="Liming or Calcium Addition",table="pH.Method",field="pH.Prac",issue="Junk data value accidently left in master template?")
  
      #### 3.15.2.1) Harmonization ########
  h_params<-data.table(h_table="pH.Method",
                       h_field=c("pH.Unit","pH.Material"),
                       h_table_alt=c("Fert.Out",NA),
                       h_field_alt=c("F.Unit",NA),
                       ignore_vals=c("unspecified","unspecified"))
  
  results<-harmonizer_wrap(data=pH.Method,
                           h_params=h_params,
                           master_codes = master_codes
                           )
  
  pH.Method<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "pH_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  ### 3.15.3) Save errors ######
  errors<-rbindlist(list(errors_a,errors_b))
  error_list<-error_tracker(errors=errors,filename = "pH_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5))
  error_list<-error_tracker(errors=errors,filename = "pH_other_errors",error_dir=error_dir,error_list = error_list)
  
  ## 3.16) Irrigation #####
  data<-lapply(XL,"[[","Irrig.Out")
  col_names<-colnames(data[[1]])
  
    ### 3.16.1) Irrig.Codes ######
  col_names2<-col_names[1:6]
  
  Irrig.Codes<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Irrig.Out tab structure"))
    }else{
      X<-X[,..col_names2]
      X<-X[!is.na(I.Level.Name)][!(is.na(I.Strategy) & is.na(I.Method) & is.na(I.Precision))]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Irrig.Codes,"[[","errors"))
  Irrig.Codes<-rbindlist(lapply(Irrig.Codes,"[[","data"))  
  colnames(Irrig.Codes)[6]<-"I.Notes"
  
  Irrig.Codes[is.na(I.Method) & I.Strategy=="No Irrigation Control or Rainfed Agriculture",I.Method:="NA"]
  
  errors1<-Irrig.Codes[I.Strategy=="No Irrigation Control or Rainfed Agriculture" & grepl("irrigation",I.Method)
              ][,list(value=paste0(I.Level.Name,collapse = "/")),by=B.Code
                ][,table:="Irrig.Codes"
                  ][,field:="I.Level.Name"
                    ][,issue:="Contradiction: no irrigation strategy, but method states irrigation."]
  
  results<-validator(data=Irrig.Codes,
                     unique_cols = "I.Level.Name",
                     compulsory_cols = c(I.Level.Name="I.Method",I.Level.Name="I.Strategy"),
                     tabname="Irrig.Codes",
                     trim_ws = T)
  
  errors2<-results$errors
  Irrig.Codes<-results$data
  
  errors2<-errors2[!(grepl("Missing value",issue) & value=="Base")]
  
  # Check for multiple base practices
  errors5<-Irrig.Codes[I.Level.Name=="Yes",.(I.Level.Name,B.Code)
  ][,N:=length(I.Level.Name),by=.(B.Code)
  ][N>1
  ][,.(value=paste(I.Level.Name,collapse="/")),by=.(B.Code)
  ][,table:="Irrig.Codes"
  ][,field:="I.Level.Name"
  ][,issue:="Multiple base practices present."]
  
  
    ### 3.16.2) Irrig.Method ######
  col_names2<-col_names[8:21]
  
  Irrig.Method<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Irrig.Out tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"I.Level.Name"
      X<-X[!is.na(I.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_b<-rbindlist(lapply(Irrig.Method,"[[","errors"))
  Irrig.Method<-rbindlist(lapply(Irrig.Method,"[[","data")) 
  
  setnames(Irrig.Method,"Times","Time")
  
  # Update Site.ID
  Irrig.Method[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(Irrig.Method[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]  
  ][is.na(Site.ID_new),Site.ID_new:=Site.ID
  ][,Site.ID:=Site.ID_new
  ][,Site.ID_new:=NULL]

  results<-validator(data=Irrig.Method,
                     numeric_cols=c("I.Amount","I.Date.DAP","I.Date.DAE","I.Date.Inteval"),
                     date_cols=c("I.Date.End", "I.Date.Start", "I.Date.Gen"),
                     unit_pairs = data.table(unit="I.Unit",var="I.Amount",name_field="I.Level.Name"),
                     hilo_pairs = data.table(low_col="I.Date.Start",high_col="I.Date.End",name_field="I.Level.Name"),
                     valid_start=valid_start,
                     valid_end=valid_end,
                     site_data=Site.Out,
                     time_data=Times.Out,
                     tabname="Irrig.Method",
                     trim_ws = T,
                     ignore_values = c("All Times","Unspecified","Not specified","All Sites"))
  
  errors3<-unique(results$errors)
  Irrig.Method<-results$data
  
  errors4<-check_key(parent = Irrig.Codes,child = Irrig.Method,tabname="Irrig.Method",keyfield="I.Level.Name")

        #### 3.16.2.2) Harmonization #######
  h_params<-data.table(h_table="Irrig.Method",
                       h_field=c("I.Unit","I.Water.Type"),
                       h_field_alt=c("I.Unit","I.Water.Type"),
                       h_table_alt=c("Irrig.Out","Irrig.Out"))
  
  results<-harmonizer_wrap(data=Irrig.Method,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Irrig.Method<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "irrigation_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  
   ### 3.16.3) Save errors ######
  errors<-rbindlist(list(errors_a,errors_b))
  error_list<-error_tracker(errors=errors,filename = "irrig_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors4),use.names = T)
  error_list<-error_tracker(errors=errors,filename = "irrig_other_errors",error_dir=error_dir,error_list = error_list)
  ## 3.17) Water Harvesting (WH.Out) #####
  data<-lapply(XL,"[[","WH.Out")
  col_names<-colnames(data[[1]])
  
  WH.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with pH.Level.Name tab structure"))
    }else{
      X<-X[!is.na(WH.Level.Name)][!(is.na(P1) & is.na(P2) & is.na(P3) & is.na(P4))]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(WH.Out,"[[","errors"))
  error_list<-error_tracker(errors=errors,filename = "wh_structure_errors",error_dir=error_dir,error_list = error_list)
  
  WH.Out<-rbindlist(lapply(WH.Out,"[[","data"))  
  
  # update codes
  WH.Out[,N:=sum(c(!is.na(P1),!is.na(P2) ,!is.na(P3) ,!is.na(P4))),by=list(B.Code, WH.Level.Name)]
  wh_codes<-master_codes$prac[Practice=="Water Harvesting",list(Subpractice,Code)]
  WH.Out[,P1c:=wh_codes[match(WH.Out$P1,Subpractice),Code]]
  WH.Out[,P2c:=wh_codes[match(WH.Out$P2,Subpractice),Code]]
  WH.Out[,P3c:=wh_codes[match(WH.Out$P3,Subpractice),Code]]
  WH.Out[,P4c:=wh_codes[match(WH.Out$P4,Subpractice),Code]]
  
  
  WH.Out[is.na(P1c) & !is.na(P1)]
  
  WH.Out[,WH.Codes:=paste0(sort(c(P1c,P2c,P3c,P4c)[!is.na(c(P1c,P2c,P3c,P4c))]),collapse="-"),by=list(B.Code,WH.Level.Name)]
  WH.Out[,c("P1c","P2c","P3c","P4c"):=NULL][,N1:=stringr::str_count(WH.Codes,"-")]
  
  errors1<-WH.Out[N!=N1|WH.Codes=="",list(B.Code,WH.Level.Name)
                 ][,value:=WH.Level.Name
                   ][,WH.Level.Name:=NULL
                     ][,table:="WH.Out"
                       ][,field:="WH.Level.Name"
                         ][,issue:="Number of practice codes do not match number of practices, does the water harvesting practice exist in ERA?"]
  
  results<-validator(data=WH.Out,
                     unique_cols = "WH.Level.Name",
                     trim_ws = T,
                     tabname="WH.Out")
  
  errors2<-results$errors
  WH.Out<-results$data
  
  # Check for multiple base practices
  errors3<-WH.Out[WH.Level.Name=="Yes",.(WH.Level.Name,B.Code)
  ][,N:=length(WH.Level.Name),by=.(B.Code)
  ][N>1
  ][,.(value=paste(WH.Level.Name,collapse="/")),by=.(B.Code)
  ][,table:="WH.Out"
  ][,field:="WH.Level.Name"
  ][,issue:="Multiple base practices present."]
  
  WH.Out[,c("N","N1"):=NULL]
    ### 3.17.1) Save errors ######
    error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3),fill=T),filename = "wh_other_errors",error_dir=error_dir,error_list = error_list)

  
  ## 3.18) Dates #####
  data<-lapply(XL,"[[","PD.Out")
  col_names<-colnames(data[[1]])
  
    ### 3.18.1) PD.Codes ######
  col_names2<-col_names[1:5]
  
  PD.Codes<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Dates tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"PD.Level.Name"
      X<-X[!is.na(PD.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(PD.Codes,"[[","error"))
  PD.Codes<-rbindlist(lapply(PD.Codes,"[[","data"))  
  
  results<-validator(data=PD.Codes,
                     unique_cols = "PD.Level.Name",
                     trim_ws = T,
                     tabname="PD.Codes")
  
  errors1<-results$errors
  PD.Codes<-results$data
  
  
      #### 3.18.1.1) Harmonization #######
  h_params<-data.table(h_table="PD.Codes",
                       h_field=c("PD.Prac","PD.Prac.Info"),
                       h_field_alt=c("PD.Timing.Pracs","PD.Info.Pracs"),
                       h_table_alt=c("PD.Out","PD.Out"))
  
  results<-harmonizer_wrap(data=PD.Codes,
                           h_params=h_params,
                           master_codes = master_codes)
  
  PD.Codes<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "dates_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
    ### 3.18.2) PD.Out ######
  col_names2<-col_names[7:14]
  
  PD.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Dates tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"PD.Level.Name"
      X<-X[!is.na(PD.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_b<-rbindlist(lapply(PD.Out,"[[","error"))

  PD.Out<-rbindlist(lapply(PD.Out,"[[","data"))  
  setnames(PD.Out,"Times","Time")
  
  # Update Site.IDs
  PD.Out[Site.ID!="All Sites",Site.ID_new:=Site.Out$Site.ID[match(PD.Out[Site.ID!="All Sites",Site.ID],Site.Out$Site.ID_raw)]  
  ][is.na(Site.ID_new),Site.ID_new:=Site.ID
  ][,Site.ID:=Site.ID_new
  ][,Site.ID_new:=NULL]

  results<-validator(data=PD.Out,
                     numeric_cols=c("PD.Date.DAS","PD.Date.DAP"),
                     date_cols=c("PD.Date.Start", "PD.Date.End"),
                     hilo_pairs = data.table(low_col="PD.Date.Start",high_col="PD.Date.End",name_field="PD.Level.Name"),
                     valid_start=valid_start,
                     valid_end=valid_end,
                     site_data=Site.Out,
                     time_data=Times.Out,
                     tabname="PD.Out",
                     trim_ws = T,
                     ignore_values = c("All Times","Unspecified","Not specified","All Sites"))
  
  errors2<-results$errors
  PD.Out<-results$data
  
  
  errors3<-unique(PD.Out[is.na(PD.Date.Start) & is.na(PD.Date.End) & is.na(PD.Date.DAS) & is.na(PD.Date.DAP),list(B.Code,PD.Level.Name)
  ][,value:=paste0(PD.Level.Name,collapse="/"),by=B.Code
  ][,PD.Level.Name:=NULL
  ][,table:="PD.Out"
  ][,field:="PD.Level.Name"
  ][,issue:="No date, DAP or DAE specfied for a practice in the Dates tab."])
  
  errors4<-unique(PD.Out[is.na(PD.Date.Start) & !is.na(PD.Date.End)|!is.na(PD.Date.Start) & is.na(PD.Date.End),list(B.Code,PD.Level.Name)
  ][,value:=paste0(PD.Level.Name,collapse="/"),by=B.Code
  ][,PD.Level.Name:=NULL
  ][,table:="PD.Out"
  ][,field:="PD.Level.Name"
  ][,issue:="Only one of start and end date specified in the Dates tab."])
  
    ### 3.18.3) Save errors ######
  errors<-rbindlist(list(errors_a,errors_b))
  error_list<-error_tracker(errors=errors,filename = "dates_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors4),fill=T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "dates_other_errors",error_dir=error_dir,error_list = error_list)
  
  ## 3.19) Harvest (Har.Out) ######
  data<-lapply(XL,"[[","Harvest.Out")
  col_names<-colnames(data[[1]])
  
  Har.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with harvest tab structure"))
    }else{
      X<-X[!is.na(H.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Har.Out,"[[","errors"))
  error_list<-error_tracker(errors=errors,filename = "harvest_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Har.Out<-rbindlist(lapply(Har.Out,"[[","data"))  
  
  # Update codes
  Har.Out$H.Code<-master_codes$prac$Code[match(Har.Out$H.Prac,master_codes$prac$Subpractice)]
  
  results<-validator(data=Har.Out,
                     unique_cols = "H.Level.Name",
                     trim_ws = T,
                     tabname="Har.Out")
  
  errors1<-results$errors
  Har.Out<-results$data
  
  error_list<-error_tracker(errors=errors1,filename = "harvest_other_errors",error_dir=error_dir,error_list = error_list)
  
  
  ## 3.20) Other (Other.Out) ######
  data<-lapply(XL,"[[","Other.Out")
  col_names<-colnames(data[[1]])
  
  Other.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with other tab structure"))
    }else{
      X<-X[!is.na(O.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Other.Out,"[[","errors"))
  error_list<-error_tracker(errors=errors,filename = "other_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Other.Out<-rbindlist(lapply(Other.Out,"[[","data"))  
  
  Other.Out<-Other.Out[!(is.na(O.Notes) & is.na(O.Structure) & O.Level.Name=="Base")]
  Other.Out<-Other.Out[!(O.Level.Name=="Base" & O.Structure=="No")]
  
  results<-validator(data=Other.Out,
                     unique_cols = "O.Level.Name",
                     trim_ws = T,
                     tabname="Other.Out")
  
  errors1<-results$errors
  Other.Out<-results$data
  
  
  Other.Out[,N:=.N,by=B.Code][,No_struc:=any(O.Structure=="Yes"),by=B.Code]
  
  # If Base  practice set W.Structure to NA
  Other.Out[O.Level.Name=="Base",O.Structure:=NA]
  
  errors2<-Other.Out[N>1 & No_struc==T
                       ][,N:=NULL
                         ][,No_struc:=NULL
                           ][,list(B.Code,O.Level.Name)
                             ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
                              ][,value:=O.Level.Name
                              ][,O.Level.Name:=NULL
                                ][,table:="Other.Out"
                                  ][,field:="O.Level.Name"
                                    ][,issue:=">1 other practice exists and comparison IS allowed, check comparisons field is correct."]
  
  errors3<-Other.Out[N>1 & No_struc==F
  ][,N:=NULL
  ][,No_struc:=NULL
  ][,list(B.Code,O.Level.Name)
  ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
  ][,value:=O.Level.Name
  ][,O.Level.Name:=NULL
  ][,table:="Other.Out"
  ][,field:="O.Level.Name"
  ][,issue:=">1 other practice exists and comparison IS NOT allowed, check comparisons field is correct."]
  
  
  errors4<-Other.Out[N==1][,N:=NULL
    ][,list(B.Code,O.Level.Name)
    ][,list(O.Level.Name=paste(O.Level.Name,collapse="/")),by=B.Code
    ][,value:=O.Level.Name
    ][,O.Level.Name:=NULL
    ][,table:="Other.Out"
    ][,field:="O.Level.Name"
    ][,issue:="One other practice exists, please check that the comparisons field is correctly assigned."]
  
  error_list<-error_tracker(errors=rbind(errors1,errors2,errors3,errors4)[order(B.Code)],filename = "other_other_errors",error_dir=error_dir,error_list = error_list)
  
  Other.Out[,N:=NULL][,No_struc:=NULL]
  
  ## 3.21) Weeding (Weed.Out) ######
  data<-lapply(XL,"[[","Weed.Out")
  col_names<-colnames(data[[1]])
  
  Weed.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with weeding tab structure"))
    }else{
      X<-X[!is.na(W.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Weed.Out,"[[","errors"))
  error_list<-error_tracker(errors=errors,filename = "weed_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Weed.Out<-rbindlist(lapply(Weed.Out,"[[","data"))  
  
  # Remove unnecessary rows
  Weed.Out<-Weed.Out[!(W.Level.Name=="Base" & is.na(W.Method) & is.na(W.Freq) & is.na(W.Freq.Time) & is.na(W.Notes))]
  
  # If Base  practice set W.Structure to NA
  Weed.Out[W.Level.Name=="Base",W.Structure:=NA]
  
  results<-validator(data=Weed.Out,
                     unique_cols = "W.Level.Name",
                     trim_ws = T,
                     tabname="Weed.Out")
  errors1<-results$errors
  Weed.Out<-results$data
  
  
  Weed.Out[,N:=.N,by=B.Code][,No_struc:=any(W.Structure=="Yes"),by=B.Code]
  
  errors1<-Weed.Out[N>1 & No_struc==T
  ][,N:=NULL
  ][,No_struc:=NULL
  ][,list(B.Code,W.Level.Name)
  ][,list(W.Level.Name=paste(W.Level.Name,collapse="/")),by=B.Code
  ][,value:=W.Level.Name
  ][,W.Level.Name:=NULL
  ][,table:="Weed.Out"
  ][,field:="W.Level.Name"
  ][,issue:=">1 weeding practices exist and comparison IS allowed, check comparisons field is correct (many conservation ag experiments where this is ok)."]
  
  errors2<-Weed.Out[N>1 & No_struc==F
  ][,N:=NULL
  ][,No_struc:=NULL
  ][,list(B.Code,W.Level.Name)
  ][,list(W.Level.Name=paste(W.Level.Name,collapse="/")),by=B.Code
  ][,value:=W.Level.Name
  ][,W.Level.Name:=NULL
  ][,table:="Weed.Out"
  ][,field:="W.Level.Name"
  ][,issue:=">1 weeding practices exist and comparison IS NOT allowed, check comparisons field is correct."]
  
  errors3<-Weed.Out[N==1 & W.Level.Name!="Base" & No_struc==F][,N:=NULL
  ][,list(B.Code,W.Level.Name)
  ][,list(W.Level.Name=paste(W.Level.Name,collapse="/")),by=B.Code
  ][,value:=W.Level.Name
  ][,W.Level.Name:=NULL
  ][,table:="Weed.Out"
  ][,field:="W.Level.Name"
  ][,issue:="One weeding practice exists and comparison is NOT allowed, please check that the comparisons field is correctly assigned."]
  
  error_list<-error_tracker(errors=rbind(errors1,errors2,errors3)[order(B.Code)],filename = "weed_other_errors",error_dir=error_dir,error_list = error_list)
  
  Weed.Out[,N:=NULL][,No_struc:=NULL]
  
  # Add hand weeding code
  Weed.Out[!W.Method %in% c("Mechanical","Ploughing"),W.Codes:="h66.2"]
  Weed.Code<-Weed.Out[,list(W.Codes=unique(W.Codes[!is.na(W.Codes)]),W.Structure=W.Structure[1]),by=list(B.Code,W.Level.Name)]

    ### 3.21.1) Harmonization #######
  h_params<-data.table(h_table="Weed.Out",
                       h_field=c("W.Method","W.Freq.Time"),
                       h_field_alt=c("W.Type","W.Time.Units"),
                       h_table_alt=c("Weed.Out","Weed.Out"))
  
  results<-harmonizer_wrap(data=Weed.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Weed.Out<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "weed_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  ## 3.22) Base Practices (Base.Out) #####
  Base.Out<-list(
    Var.Out[V.Base=="Yes" & !is.na(V.Codes),c("B.Code","V.Codes")],
    AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
    Till.Codes[Till.Level.Name=="Base"& !is.na(Till.Code),c("B.Code","Till.Code")],
    Fert.Out[F.Level.Name=="Base" & !is.na(F.Codes),c("B.Code","F.Codes")],
    Res.Out[M.Level.Name=="Base" & !is.na(M.Codes),c("B.Code","M.Codes")],
    Har.Out[H.Level.Name=="Base" & !is.na(H.Code),c("B.Code","H.Code")],
    pH.Out[pH.Level.Name=="Base" & !is.na(pH.Codes),c("B.Code","pH.Codes")],
    WH.Out[WH.Level.Name=="Base" & !is.na(WH.Codes),c("B.Code","WH.Codes")],
    Irrig.Codes[I.Level.Name=="Base" & !is.na(I.Codes),c("B.Code","I.Codes")],
    Weed.Code[!is.na(W.Codes),c("B.Code","W.Codes")],
    Chems.Code[!is.na(C.Codes),c("B.Code","C.Codes")]
  )
  
  Base.Out<-rbindlist(Base.Out,use.names = F)
  Base.Out<-Base.Out[,.(Base.Codes=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

# 4) Treatments (MT.Out)  #####
data<-lapply(XL,"[[","MT.Out")
col_names<-colnames(data[[1]])
col_names<-col_names[!grepl("[.][.][.]",col_names)]

MT.Out<-lapply(1:length(data),FUN=function(i){
X<-data[[i]]
setnames(X,c("0","AF.Other"),c("AF.Level.Name","AF.Level.Name"),skip_absent = T)
B.Code<-Pub.Out$B.Code[i]

if(!all(col_names %in% colnames(X))){
  cat("Structural issue with file",i,B.Code,"\n")
  list(errors=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with make treatments tab structure (AF.Level.Name duplicated C.Name missing?)."))
}else{
  X<-X[,..col_names]
  X<-X[!is.na(T.Name)]
  if(nrow(X)>0){
    X[,B.Code:=B.Code]
    list(data=X)
  }else{
    NULL
  }
}})

errors_a<-rbindlist(lapply(MT.Out,"[[","errors"))
error_list<-error_tracker(errors=errors_a,filename = "treatment_structure_errors",error_dir=error_dir,error_list = error_list)

MT.Out<-rbindlist(lapply(MT.Out,"[[","data"))
setnames(MT.Out,c("C.Name","V.Var","T.Comp"),c("C.Level.Name","V.Level.Name","P.Product"))
MT.Out[,V.Level.Name:=trimws(V.Level.Name)]

results<-validator(data=MT.Out,
                 numeric_cols = c("T.Reps","T.Start.Year"),
                 unique_cols = "T.Name",
                 trim_ws = T,
                 compulsory_cols = c(T.Name="T.Name",T.Name="P.Product"),
                 duplicate_field = "T.Name",
                 tabname="MT.Out")

errors1<-results$errors
errors1<-errors1[value!="Unspecified"]

MT.Out<-results$data

# Trimws in aggregated names
# Function to split, trim, and re-collapse strings
split_trimws<-function(x,delim){

  x<-strsplit(x,delim,fixed=T)
  x<-lapply(x,trimws)
  x<-lapply(x,paste,collapse=delim)
  x<-unlist(x)
  x[x=="NA"]<-as.character(NA)

  return(x)
}

MT.Out[,T.Name:=split_trimws(T.Name,delim="..")]

# Error checking: look for non-matches in keyfields
keyfields<-c("AF.Level.Name","C.Level.Name","H.Level.Name","I.Level.Name","M.Level.Name","F.Level.Name",
           "pH.Level.Name","P.Level.Name","PD.Level.Name","Till.Level.Name","WH.Level.Name",
           "O.Level.Name","W.Level.Name")

# List of variable names as strings
prac_tabs <- c("AF.Out", "Chems.Code", "Har.Out", "Irrig.Codes", "Res.Out", "Fert.Out",
             "pH.Out", "Plant.Out", "PD.Codes", "Till.Codes", "WH.Out",
             "Other.Out", "Weed.Code")

# Use mget to get the objects by names and setNames to assign the names
prac_list <- setNames(mget(prac_tabs, .GlobalEnv), prac_tabs)

key_params<-data.table(parent=prac_list,
                     child=list(MT.Out),
                     tabname="MT.Out",
                     tabname_parent=prac_tabs,
                     keyfield=keyfields)

errors2<-rbindlist(lapply(1:nrow(key_params),FUN=function(i){
child<-key_params[i,child][[1]]
keyfield<-key_params[i,keyfield]
N<-!is.na(unlist(child[,..keyfield]))
child<-child[N]

if(key_params[i,parent][[1]][,.N] != unique(key_params[i,parent][[1]])[,.N]){
  cat("Warning: Potential non-unique values in key field for",keyfield, "in the",key_params[i,tabname_parent],"table \n")
}

result<-check_key(parent = unique(key_params[i,parent][[1]]),
          child = child,
          tabname= key_params[i,tabname],
          tabname_parent= key_params[i,tabname_parent],
          keyfield= keyfield)
result[,issue:=paste0("Non-match for name in ",key_params[i,keyfield]," column.")]
result
}))[order(B.Code)]

# Error checking: look for non-matches in keyfields - Var.Out
# Match is made on V.Product and V.Level.Name
mergedat<-Var.Out[,.(B.Code,V.Product,V.Level.Name)][,check:=T]
setnames(mergedat,"V.Product","P.Product")
dat<-MT.Out[!is.na(V.Level.Name)]
errors3<-merge(dat,mergedat,all.x=T)[is.na(check),list(value=paste0(T.Name,collapse = "/")),by=B.Code
                                   ][,table:="MT.Out"
                                   ][,field:="T.Name"
                                   ][,issue:="Variety practice + crop selected does not merge with Var.Out tab."]

  # Check for aggregated products
  errors6<-MT.Out[grepl("[.][.]",P.Product) & !grepl("[.][.]",T.Name),.(value=paste(unique(P.Product),collapse="/")),by=B.Code
           ][,table:="MT.Out"
             ][,field:="P.Product"
               ][,issue:="Product is aggregated, check the use of aggregation is valid."]
      
  # Check for duplicate rows in make treatment table
  error_dat<-cbind(MT.Out[,.(B.Code,T.Name)],code=apply(MT.Out[,!"T.Name"],1,paste,collapse="/"))[,N:=.N,by=.(B.Code,code)][N>1 & !grepl("[.][.]",T.Name)]
  error_dat<-error_dat[,.(value=paste(T.Name,collapse = "==")),by=.(B.Code,code)
                       ][,code:=NULL
                         ][,.(value=paste(value,collapse = "/")),by=B.Code
                           ][,table:="MT.Out"
                             ][,field:="T.Name"
                               ][,issue:="Identical treatments, look at names to see if this could be an error (e.g. a missing field)."]

  write.table(error_dat,"clipboard-256000",row.names = F,sep="\t")
  
  
  error_list<-error_tracker(errors=error_dat,filename = "treatment_duplicates",error_dir=error_dir,error_list = error_list)
  
  
  # Convert T.Start.Year and T.Reps fields to integers
  MT.Out[,T.Start.Year:=as.integer(T.Start.Year)][,T.Reps:=as.integer(T.Reps)]
  
  # Check residue codes
  h_params<-data.table(h_table="MT.Out",
                     h_field=c("T.Residue.Prev"),
                     h_table_alt=c("Res.Method"),
                     h_field_alt=c("M.Fate"))
  
  results<-harmonizer_wrap(data=MT.Out,
                         h_params=h_params,
                         master_codes = master_codes)
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "treatment_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  ## 4.1) Update residue codes ####
  
  # Add residue codes from product sheet
  MT.Out<-merge(MT.Out,Prod.Out[,.(B.Code,P.Product,P.Mulched,P.Incorp,P.Unknown.Fate)],by.x=c("B.Code","P.Product"),by.y=c("B.Code","P.Product"),all.x=T)
  
  # Assign residue code based on residue fate
  MT.Out[,T.Residue.Code:=NA
         ][T.Residue.Prev %in% c("Retained (unknown if mulched/incorp.)"),T.Residue.Code:=P.Unknown.Fate
         ][T.Residue.Prev %in% c("Incorporated"),T.Residue.Code:=P.Incorp
           ][T.Residue.Prev %in% c("Mulched (left on surface)"),T.Residue.Code:=P.Mulched
             ][,c("P.Mulched","P.Incorp","P.Unknown.Fate"):=NULL]
  
  ## 4.2) Merge in practice data #####
  warning("Has issue with V.Level.Name being derived from harmonized field that also needs to be harmonzied in MT.Out been fixed?")
  # Check to see if above issues is addressed
  MT.Out[!V.Level.Name %in%   Var.Out[,V.Level.Name],.(B.Code,V.Level.Name)]
  
  unique(MT.Out[grepl("[.][.]",P.Product) & !grepl("[.][.]",T.Name),.(B.Code,P.Product,T.Name)])
  # Create list of data table to merge with MT.Out treatment table
  mergedat<-list(V.Level.Name=Var.Out,
                 AF.Level.Name=AF.Out,
                 Till.Level.Name=Till.Codes,
                 P.Level.Name=Plant.Out,
                 F.Level.Name=Fert.Out,
                 C.Level.Name=Chems.Code,
                 M.Level.Name=Res.Out[,!c("P1","P2","P3","P4")],
                 pH.Level.Name=pH.Out,
                 I.Level.Name=Irrig.Codes,
                 WH.Level.Name=WH.Out[,!c("P1","P2","P3","P4")],
                 PD.Level.Name=PD.Codes,
                 H.Level.Name=Har.Out,
                 O.Level.Name=Other.Out,
                 W.Level.Name=Weed.Out)
  
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
                  all.x=T)
    }else{
      data<-merge(data,mergedat[[i]],by=c("B.Code",keyfield),all.x=T)
    }
    
  
    if(nrow(data)!=nrow(MT.Out)){
      cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
    }
  }
  
  prod_cols<-c("P.Product.x","P.Product.y")
  N<-apply(data[,.(P.Product.x,P.Product.y)],1,FUN=function(x){length(unique(x[!is.na(x)]))})
  prod_cols<-c("B.Code","T.Name",prod_cols)
  
  errors4<-data[N>2,..prod_cols
                ][,value:=paste0(T.Name,": ",P.Product.x,"/",P.Product.y)
                        ][,(prod_cols[prod_cols!="B.Code"]):=NULL
                          ][,table:="MT.Out"
                            ][,field:="T.Name: Product/Planting"
                              ][,issue:="Planting practice product does not match treatment product."]
  
  data[,P.Product.y:=NULL]
  setnames(data,"P.Product.x","P.Product")
  
  # Add in Base.Out
  data<-merge(data,Base.Out,by="B.Code",all.x=T)
  if(nrow(data)!=nrow(MT.Out)){
    cat(" ! Warning: nrow(output) = ",nrow(data),"vs nrow(input)",nrow(MT.Out),"\n")
  }
  
  MT.Out<-data
  
  ## 4.3) Combine practice codes to create T.Codes ######
  code_cols<-c(AF.Level.Name="AF.Codes",
               H.Level.Name="H.Code",
               I.Level.Name="I.Codes",
               M.Level.Name="M.Codes",
               F.Level.Name="F.Codes",
               pH.Level.Name="pH.Codes",
               Till.Level.Name="Till.Code",
               V.Level.Name="V.Codes",
               WH.Level.Name="WH.Codes",
               W.Level.Name="W.Codes",
               C.Level.Name="C.Codes")
  
  t_codes<-apply(MT.Out[,..code_cols],1,FUN=function(x){
    x<-paste(sort(unique(unlist(strsplit(x,"-")))),collapse="-")
    x[x==""]<-NA
    x
  })
  
  MT.Out[,T.Codes:=t_codes]
  
  ## 4.x) What about structure? #####
  
  ## 4.4) Combine aggregated treatments #####
  
  N<-grep("[.][.]",MT.Out$T.Name)
  
  Fields<-data.table(Levels=c("T.Residue.Prev",names(code_cols),"P.Level.Name","O.Level.Name","PD.Level.Name"),
                     Codes =c("T.Residue.Code",code_cols,NA,NA,NA))
  Fields[Levels %in% c("W.Level.Name","C.Level.Name"),Codes:=NA]
  
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
      Exclude<-c("O.Level.Name","P.Level.Name","C.Level.Name","W.Level.Name")[apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure")],2,unique)!="Yes" | is.na(apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure")],2,unique))]
      Fields1<-Fields[!Levels %in% Exclude]
      
      
      # Exception for residues from experimental crop (but not M.Level.Name as long as multiple products present
      # All residues set the the same code (removing N.fix/Non-N.Fix issue)
      # Fate labels should not require changing
      if(length(unique(Y$P.Product))>1){
        Y[grep("b41",T.Residue.Code),T.Residue.Code:="b41"]
        Y[grep("b40",T.Residue.Code),T.Residue.Code:="b40"]
        Y[grep("b27",T.Residue.Code),T.Residue.Code:="b27"]
        Y[T.Residue.Code %in% c("a16","a17"),T.Residue.Code:="a15"]
        Y[T.Residue.Code %in% c("a16.1","a17.1"),T.Residue.Code:="a15.1"]
        Y[T.Residue.Code %in% c("a16.2","a17.2"),T.Residue.Code:="a15.2"]
      }
      
      
      COLS<-Fields1$Levels
      Levels<-apply(Y[,..COLS],2,FUN=function(X){
        X[as.vector(is.na(X))]<-""
        length(unique(X))>1
      })
      
      Agg.Levels<-paste0(COLS[Levels],collapse = "-")
      
      COLS<-COLS[Levels]
      
      Agg.Levels2<-paste(apply(Y[,..COLS],1,FUN=function(X){
        X[as.vector(is.na(X))]<-"NA"
        paste(X,collapse="---")
      }),collapse="...")
      
      if("F.Level.Name" %in% COLS){
        
        COLS2<-COLS
        COLS2[COLS2=="F.Level.Name"]<-"F.Level.Name2"

        Agg.Levels3<-paste(apply(Y[,..COLS2],1,FUN=function(X){
          X[as.vector(is.na(X))]<-"NA"
          paste(X,collapse="---")
        }),collapse="...")
      }else{
        Agg.Levels3<-Agg.Levels2
      }
      
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
      Y$T.Agg.Levels2<-Agg.Levels2
      Y$T.Agg.Levels3<-Agg.Levels3
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
  
  errors5<-rbindlist(lapply(results,"[[","error"))
  errors5<-errors5[,list(value=paste0(value,collapse = "/")),by=.(B.Code,table,field,issue)]
  
  MT.Out.agg<-rbindlist(lapply(results,"[[","data"))
  
  # Update final treatment codes (T.Codes) for aggregrated treatments
  MT.Out.agg[,T.Codes:=T.Codes.No.Agg]
  
  MT.Out.noagg<-MT.Out[-N]
  MT.Out.noagg[,c("T.Agg.Levels","T.Agg.Levels2","T.Agg.Levels3","T.Codes.No.Agg","T.Codes.Agg"):=NA]
  
  MT.Out<-rbind(MT.Out.agg,MT.Out.noagg)
  
  ## 4.5) Update crop residues for aggregated products  #####
  MT.Out.agg<-MT.Out[grepl("[.][.][.]",P.Product)]
  MT.Out.noagg<-MT.Out[!grepl("[.][.][.]",P.Product)]
  
  MT.Out.agg[,.(P.Product,T.Residue.Prev,T.Residue.Code)]
  
  X<-unlist(pblapply(1:nrow(MT.Out.agg),FUN=function(i){
    
    Fate<-unlist(strsplit(MT.Out.agg$T.Residue.Prev[i],"[.][.][.]"))
    Y<-unlist(strsplit(MT.Out.agg$P.Product[i],"[.][.][.]"))
    Y<-lapply(Y,strsplit,"[.][.]")
    if(length(Y)==1 & length(Fate)>1){
      Y<-rep(Y,length(Fate))
    }
    
    if(length(Fate)==1 & length(Fate)!=length(Y)){
      Fate<-rep(Fate,length(Y))
    }
    
    Y<-unlist(lapply(1:length(Y),FUN=function(j){
      Z<-unlist(Y[[j]])
      fate_j<-Fate[j]
      if(is.na(fate_j)|fate_j=="Unspecified"){
        NA
      }else{
        fate_col<-NA
        if(fate_j == "Retained (unknown if mulched/incorp.)"){
          fate_col<-"Unknown.Fate"
        }
        if(fate_j == "Mulched (left on surface)"){
          fate_col<-"Mulched"  
        }
        if(fate_j== "Incorporated"){
          fate_col<-"Incorp"
        }
        
        if(!is.na(fate_col)){
          A<-unlist(master_codes$prod[match(Z,Product.Simple),..fate_col])
          B<-unlist(master_codes$trees[match(Z,Tree.Latin.Name),..fate_col])
          
          if(is.na(A)&is.na(B)){
            warning(paste(Z,": Does not match a tree or product name."))
          }
        }
        
        if(fate_j=="Removed"){
          A<-"h35"
          B<-NA
        }
        if(fate_j=="Grazed"){
          A<-"h39"
          B<-NA
        }
        if(fate_j=="Burned"){
          A<-"h36"
          B<-NA
        }
        
        A<-c(A,B)
        A<-A[!is.na(A)]
        A
      }
      
    }))
    
    
    if(length(unique(Y))==1){
      unique(Y)
    }else{
      A<-Y
      A[grep("b41",A)]<-"b41"
      A[grep("b40",A)]<-"b40"
      A[grep("b27",A)]<-"b27"
      A[A %in% c("a16","a17")]<-"a15"
      A[A %in% c("a16.1","a17.1")]<-"a15.1"
      A[A %in% c("a16.2","a17.2")]<-"a15.2"
      A<-unique(A)
      if(length(A)==1){
        Y<-A
      }else{
        Y<-paste0(unique(Y[order(Y)]),collapse = "...")
      }
      
      if(Y %in% c("NA","")){NA}else{Y}
      
    }
  }))
  
  MT.Out.agg[,T.Residue.Code:= X]
  
  MT.Out<-rbind(MT.Out.noagg,MT.Out.agg)
  
  
  ## 4.6) Update structure for aggregated treatments ####
  col_names<-grep("Structure",colnames(MT.Out),value=T)
  
  MT.Out <- MT.Out[, (col_names) := lapply(.SD, FUN=function(x){
    x[grepl("Yes",x,ignore.case = T)]<-"Yes"
    x[!grepl("Yes",x,ignore.case = T)]<-NA
    x
    }), .SDcols = col_names]
  
  ## 4.7) Merge and save errors #####
  errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6),fill = T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "treatment_other_errors",error_dir=error_dir,error_list = error_list)
  
  ## 4.x) ***!!!TO DO!!!***MT.Out: Correct Ridge & Furrow #####
  # remove water harvesting code if ridge and furrow is a conventional tillage control
  if(F){
    # Add row index
    MT.Out[,N:=1:nrow(MT.Out)]
    # Is only conventional tillage present in the experiment? (then ridge and furrow must be part of a water harvesting experiment)
    TC.FUN<-function(X){
      X<-unlist(X)
      X<-X[!is.na(X)]
      if(length(X)==0){
        "NA"
      }else{
        paste(unique(X),collapse = "-")
      }
    }
    
    # Which papers have ridge & furrow with conventional tillage where conventional tillage is not the only tillage practice present?
    MT.Out[,Unique.TC:=paste(unique(Till.Codes[!is.na(Till.Codes)]),collapse="-"),by="B.Code"][,N:=1:.N]
    
    (X<-unique(MT.Out[(N %in% grep("b71.2",WH.Codes)) & ((N %in% grep("h6",Till.Codes))) & (Unique.TC != "h6"),c("WH.Level.Name","WH.Codes","B.Code","Unique.TC")]))
    
    MT.Out[(N %in% grep("b71.2",WH.Codes)) & ((N %in% grep("h6",Till.Codes))) & Unique.TC != "h6",WH.Codes:=gsub("-b71.2","",WH.Codes)
    ][(N %in% grep("b71.2",WH.Codes)) & ((N %in% grep("h6",Till.Codes))) & Unique.TC != "h6",WH.Codes:=gsub("b71.2-","",WH.Codes)
    ][(N %in% grep("b71.2",WH.Codes)) & ((N %in% grep("h6",Till.Codes))) & Unique.TC != "h6",WH.Codes:=gsub("-b71.2","",WH.Codes)
    ][,N:=NULL][,Unique.TC:=NULL]
    
    # Correct WH.Out
    WH.Out[B.Code %in% X$B.Code & WH.Level.Name %in% X$WH.Level.Name,WH.Codes:=gsub("-b71.2","",WH.Codes)
    ][B.Code %in% X$B.Code & WH.Level.Name %in% X$WH.Level.Name,WH.Codes:=gsub("b71.2-","",WH.Codes)
    ][B.Code %in% X$B.Code & WH.Level.Name %in% X$WH.Level.Name,WH.Codes:=gsub("b71.2","",WH.Codes)]
    
    rm(X)
  }
  
# 5) Intercropping  ####
data<-lapply(XL,"[[","Int.Out")
col_names<-colnames(data[[1]])
col_names<-col_names[!grepl("[.][.][.]",col_names)]

Int.Out<-lapply(1:length(data),FUN=function(i){
X<-data[[i]]
B.Code<-Pub.Out$B.Code[i]

if(!all(col_names %in% colnames(X))){
  cat("Structural issue with file",i,B.Code,"\n")
  list(errors=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with intercropping tab structure (AF.Level.Name duplicated C.Name missing?)."))
}else{
  X<-X[,..col_names]
  X<-X[!is.na(IN.Level.Name)]
  if(nrow(X)>0){
    X[,B.Code:=B.Code]
    list(data=X)
  }else{
    NULL
  }
}})

errors_a<-rbindlist(lapply(Int.Out,"[[","errors"))
error_list<-error_tracker(errors=errors_a,filename = "intercropping_structure_errors",error_dir=error_dir,error_list = error_list)

Int.Out<-rbindlist(lapply(Int.Out,"[[","data"))
setnames(Int.Out,c("Practice","IN.Prods.Alll"),c("I.Practice","IN.Prods.All"),skip_absent=TRUE)

results<-validator(data=Int.Out,
                 numeric_cols = c("IN.Start.Year","IN.Reps"),
                 zero_cols = c("IN.Residue.Fate"),
                 unique_cols = "IN.Level.Name",
                 compulsory_cols = c(IN.Level.Name="I.Practice"),
                 trim_ws = TRUE,
                 tabname="Int.Out")
errors1<-results$errors
errors<-errors1[value!="Unspecified"]
Int.Out<-results$data

# Remove duplicates
N<-Int.Out[,N:=.N,by=list(B.Code,IN.Level.Name)]
Int.Out<-Int.Out[!N>1][,N:=NULL]


keyfields<-paste0("IN.Comp",1:4)
parent<-MT.Out[,list(B.Code,T.Name)]

errors2<-rbindlist(lapply(1:length(keyfields),FUN=function(i){
keyfield<-keyfields[i]
n_col<-c("B.Code",keyfield)
child<-Int.Out[,..n_col]
colnames(parent)[2]<-keyfield

result<-check_key(parent = parent,
                  child = child,
                  tabname= "Int.Out",
                  tabname_parent = "MT.Out",
                  keyfield= keyfield)
result[,issue:=paste0("Non-match btw intercropping and make treatment for name in ",keyfield," column.")]
result<-result[value!="NA"]
result
}))[order(B.Code)]

# Check papers with 38 Int treatments
Int.Out[,list(N=.N),by=B.Code][N>=35]

errors4<-Int.Out[!grepl("__",IN.Level.Name),.(value=paste(IN.Level.Name)),by=B.Code][,table:="Int.Out"][,field:="IN.Level.Name"][,issue:="Delimiter __ missing from intercrop name."]

# Update order of components
Int.Out[,IN.Comp1:=unlist(tstrsplit(IN.Level.Name,"__",keep = 1))][,IN.Comp2:=unlist(tstrsplit(IN.Level.Name,"__",keep = 2))][,IN.Comp3:=unlist(tstrsplit(IN.Level.Name,"__",keep = 3))][,IN.Comp4:=unlist(tstrsplit(IN.Level.Name,"__",keep = 4))]

  ## 5.1) Update aggregated treatment delimiters #####
  col_names<-c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")
  
  Int.Out[, (col_names) := lapply(.SD, split_trimws, delim = ".."), .SDcols = col_names]
  Int.Out[, IN.Level.Name:= split_trimws(IN.Level.Name,delim = "..")]
  
  # Change agg Treat delim = "..." 
  Int.Out[, (col_names) := lapply(.SD, function(x) gsub("[.][.]", "...", x)), .SDcols = col_names]
  
  ## 5.2) Update residue codes from MT.Out sheet #####
  
    # Update Residue Codes where they are NA
    Int.Out[is.na(IN.Res1),IN.Res1:=MT.Out[match(Int.Out[is.na(IN.Res1),paste(B.Code,IN.Comp1)],MT.Out[,paste(B.Code,T.Name)]),T.Residue.Code]]
    Int.Out[is.na(IN.Res2),IN.Res2:=MT.Out[match(Int.Out[is.na(IN.Res2),paste(B.Code,IN.Comp2)],MT.Out[,paste(B.Code,T.Name)]),T.Residue.Code]]
    Int.Out[is.na(IN.Res3),IN.Res3:=MT.Out[match(Int.Out[is.na(IN.Res3),paste(B.Code,IN.Comp3)],MT.Out[,paste(B.Code,T.Name)]),T.Residue.Code]]
    Int.Out[is.na(IN.Res4),IN.Res4:=MT.Out[match(Int.Out[is.na(IN.Res4),paste(B.Code,IN.Comp4)],MT.Out[,paste(B.Code,T.Name)]),T.Residue.Code]]
    
    Int.Out[,IN.Res1:=as.character(IN.Res1)]
    Int.Out[,IN.Res2:=as.character(IN.Res2)]
    Int.Out[,IN.Res3:=as.character(IN.Res3)]
    Int.Out[,IN.Res4:=as.character(IN.Res4)]
    
    join.fun<-function(A,B,C,D){
      X<-unique(c(A,B,C,D))
      X<-X[!is.na(X)]
      if(length(X)==0){
        as.character(NA)
      }else{
        if(length(X)>1){
          X<-X[!grepl("h",X)]
        }
        paste(X[order(X)],collapse="-")
      }
    }
    
    Int.Out[,N:=1:.N]
    Int.Out[,IN.Residue.Code:=join.fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4),by=N]
    Int.Out[,N:=NULL]
    
    # Simplify residue fate values
    
    Int.Out[IN.Residue.Fate=="Grazed/Mulched",IN.Residue.Fate:="Mulched (left on surface)"]
    Int.Out[IN.Residue.Fate=="Burned/Removed",IN.Residue.Fate:="Removed"]
    
    # Add codes for residue loss practices
    if(F){
      # Check there are not contradictory codes btw MT.Out and Int.Out
            Int.Out[!is.na(IN.Residue.Code) & IN.Residue.Fate %in% c("Removed","Grazed","Burned"),
              list(IN.Residue.Code,IN.Residue.Fate,B.Code)]
      
      unique(Int.Out[is.na(IN.Residue.Code) & !IN.Residue.Fate %in% c("Removed","Grazed","Burned","Unspecified") &!is.na(IN.Residue.Fate),
              list(IN.Residue.Code,IN.Residue.Fate,B.Code)])
      
      View(Int.Out[!is.na(IN.Residue.Code) & IN.Residue.Fate %in% c("Removed","Grazed","Burned","Mulched (left on surface)"),
                   list(IN.Residue.Code,IN.Residue.Fate,B.Code)])
    }
    
    Int.Out[IN.Residue.Fate == "Removed",IN.Residue.Code:="h35"]
    Int.Out[IN.Residue.Fate == "Grazed",IN.Residue.Code:="h39"]
    Int.Out[IN.Residue.Fate == "Burned",IN.Residue.Code:="h36"]
    
  
    # If residues are NA, but a residue fate for the component is specified then we classify the residues based on
    # matching the products to the residue fate specified.
    
    # Int.Out: Update In.Res column codes for residues that are from Tree List
    Int.Out[,N:=1:.N]
    Z<-Int.Out[is.na(IN.Residue.Code) & !IN.Residue.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified") & !is.na(IN.Residue.Fate),]
    
    tree_species<-master_codes$trees$Tree.Latin.Name
    X<-Z[IN.Prod1 %in% tree_species | IN.Prod2 %in% tree_species | IN.Prod3 %in% tree_species |IN.Prod4 %in% tree_species,
         c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4","B.Code","N","IN.Residue.Fate")]
    
    if(nrow(X)>1){
      X<-rbindlist(lapply(c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4"),FUN=function(COL){
        N2<-match(unlist(X[,..COL]), tree_species)
        if(length(N2)!=0){
          X<-cbind(X,master_codes$trees[match(unlist(X[,..COL]), Tree.Latin.Name)])
          X[IN.Residue.Fate=="Mulched (left on surface)",T.Residue.Code:=Mulched]
          X[IN.Residue.Fate=="Incorporated",T.Residue.Code:=Incorp]
          X[IN.Residue.Fate=="Retained (unknown if mulched/incorp.)",T.Residue.Code:=Unknown.Fate]
          
          X[!is.na(Species),c("N","T.Residue.Code")][,In.Prod:=COL]
        }
        
      }))
      
      for(Y in unique(X$In.Prod)){
        X1<-X[In.Prod==Y]
        if(Y=="IN.Prod1"){
          Int.Out[X1$N,IN.Res1:=X1$T.Residue.Code]
        }
        if(Y=="IN.Prod2"){
          Int.Out[X1$N,IN.Res2:=X1$T.Residue.Code]
        }
        if(Y=="IN.Prod3"){
          Int.Out[X1$N,IN.Res3:=X1$T.Residue.Code]
        }
        if(Y=="IN.Prod4"){
          Int.Out[X1$N,IN.Res4:=X1$T.Residue.Code]
        }
        
      }
    }
    
    # Int.Out: Update In.Res column codes for residues that are from Product List
    products<-master_codes$prod$Product.Simple
    X<-Z[IN.Prod1 %in% products | IN.Prod2 %in% products | IN.Prod3 %in% products |IN.Prod4 %in% products,
         c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4","B.Code","N","IN.Residue.Fate")]
    
    X<-rbindlist(lapply(c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4"),FUN=function(COL){
      N2<-match(unlist(X[,..COL]),products)
      if(length(N2)!=0){
        X<-cbind(X,master_codes$prod[match(unlist(X[,..COL]), Product.Simple)])
        X[IN.Residue.Fate=="Mulched (left on surface)",T.Residue.Code:=Mulched]
        X[IN.Residue.Fate=="Incorporated",T.Residue.Code:=Incorp]
        X[IN.Residue.Fate=="Retained (unknown if mulched/incorp.)",T.Residue.Code:=Unknown.Fate]
        
        X[!is.na(Product.Simple),c("N","T.Residue.Code","Product.Simple")][,In.Prod:=COL]
      }
      
    }))
    
    for(Y in unique(X$In.Prod)){
      X1<-X[In.Prod==Y]
      if(Y=="IN.Prod1"){
        Int.Out[X1$N,IN.Res1:=X1$T.Residue.Code]
      }
      if(Y=="IN.Prod2"){
        Int.Out[X1$N,IN.Res2:=X1$T.Residue.Code]
      }
      if(Y=="IN.Prod3"){
        Int.Out[X1$N,IN.Res3:=X1$T.Residue.Code]
      }
      if(Y=="IN.Prod4"){
        Int.Out[X1$N,IN.Res4:=X1$T.Residue.Code]
      }
      
      Int.Out[X1$N,IN.Residue.Code:=join.fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4),by=N]
    }
    
    # Update combined residue codes
    Int.Out[is.na(IN.Residue.Code),IN.Residue.Code:=join.fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4),by=N]
    
    # Determine residues Codes that are common among components vs those that are different 
    Code.Shared.Fun<-function(A,B,C,D,Shared){
      X<-c(unlist(strsplit(A,"-")),unlist(strsplit(B,"-")),unlist(strsplit(C,"-")),unlist(strsplit(D,"-")))
      
      Y<-c(A,B,C,D)
      Y<-length(Y[!is.na(Y)])
      
      X1<-table(X)
      X1<-names(X1)[X1==Y]
      
      
      
      if(Shared){
        if(is.null(X1)){
          NA
        }else{
          X1<-unique(X1)
          paste(X1[order(X1)],collapse="-")
        }
      }else{
        X<-c(strsplit(A,"-"),strsplit(B,"-"),strsplit(C,"-"),strsplit(D,"-"))
        X2<-unlist(lapply(X,FUN=function(Z){
          Z<-Z[!Z %in% X1]
          if(length(Z)==0 | is.na(Z[1])){
            NA
          }else{
            paste(Z[order(Z)],collapse = "-")
          }
          
        }))
        X2<-X2[!is.na(c(A,B,C,D))]
        if(length(X2)!=0){
          paste(X2,collapse = "***")
        }else{
          NA
        }
        
      }
      
    }
    
    Int.Out[,N:=1:.N]
    Int.Out[,IN.R.Codes.Shared:=as.character(Code.Shared.Fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4,Shared=TRUE)),by="N"]
    Int.Out[,IN.R.Codes.Diff:=as.character(Code.Shared.Fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4,Shared=F)),by="N"]
  
    if(F){
      # Check residue codes  
        unique(Int.Out[IN.Residue.Fate=="Removed",.(IN.Prods.All,IN.Residue.Fate,IN.Residue.Code)])
        unique(Int.Out[IN.Residue.Fate=="Burned",.(IN.Prods.All,IN.Residue.Fate,IN.Residue.Code)])
        unique(Int.Out[IN.Residue.Fate=="Grazed",.(IN.Prods.All,IN.Residue.Fate,IN.Residue.Code)])
    }
    
  ## 5.3) Update T.Codes #####
    # Do we need rules for IN.T.Codes column? (keep all practices as per current method? keep common practices? keep if 50% or greater?)
    Code.Comb.Fun<-function(A,B,C,D){
      X<-c(unlist(strsplit(A,"-")),unlist(strsplit(B,"-")),unlist(strsplit(C,"-")),unlist(strsplit(D,"-")))
      X<-unique(X[!is.na(X)])
      if(length(X)==0){
        NA
      }else{
        paste(X[order(X)],collapse="-")
      }
    }
    Code.Shared.Fun<-function(A,B,C,D,Shared){
      X<-c(unlist(strsplit(A,"-")),unlist(strsplit(B,"-")),unlist(strsplit(C,"-")),unlist(strsplit(D,"-")))
      
      Y<-c(A,B,C,D)
      Y<-length(Y[!is.na(Y)])
      
      X1<-table(X)
      X1<-names(X1)[X1==Y]
      
      
      
      if(Shared){
        if(is.null(X1)){
          NA
        }else{
          X1<-unique(X1)
          paste(X1[order(X1)],collapse="-")
        }
      }else{
        X<-c(strsplit(A,"-"),strsplit(B,"-"),strsplit(C,"-"),strsplit(D,"-"))
        X2<-unlist(lapply(X,FUN=function(Z){
          Z<-Z[!Z %in% X1]
          if(length(Z)==0 | is.na(Z[1])){
            NA
          }else{
            paste(Z[order(Z)],collapse = "-")
          }
          
        }))
        X2<-X2[!is.na(c(A,B,C,D))]
        if(length(X2)!=0){
          paste(X2,collapse = "***")
        }else{
          NA
        }
        
      }
      
    }
    
    Int.Out<-pblapply(1:nrow(Int.Out),FUN=function(i){
      # print(i)
      X<-Int.Out[i,]
      Y<-unlist(X[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")])
      Y<-Y[!(Y==0 | is.na(Y))]
      Y<-unique(unlist(strsplit(MT.Out[paste0(MT.Out$T.Name,MT.Out$B.Code) %in% paste0(Y,X$B.Code),T.Codes],"-"))) 
      if(length(Y)!=0){
        Y<-Y[!is.na(Y)]
        Y<-c(Y,X$IN.Code)
        Y<-paste0(Y[order(Y)],collapse = "-")
        X$IN.T.Codes<-Y
      }
      
      Y<-unlist(X[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")])
      Z<-data.table(IN.Comp1.T.Codes=as.character(NA),IN.Comp2.T.Codes=as.character(NA),IN.Comp3.T.Codes=as.character(NA),IN.Comp4.T.Codes=as.character(NA))
      
      N<-MT.Out[match(paste0(Y,X$B.Code),paste0(MT.Out$T.Name,MT.Out$B.Code)),T.Codes]
      
      if(length(N)>0){
        Z[1,1:length(N)]<-as.list(N)
      }
      
      Z<-cbind(X,Z)
      
      # Update Combined T.Codes Column
      Z[,IN.T.Codes:=as.character(Code.Comb.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes))
      ][,IN.T.Codes.Shared:=as.character(Code.Shared.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes,Shared=T))
      ][,IN.T.Codes.Diff:=as.character(Code.Shared.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes,Shared=F))]
      
    })
    Int.Out<-rbindlist(Int.Out,use.names = T)
    
  ## 5.4) Special case for when reduced & zero-tillage are both present ####
  # Where both these practices are present then count the combined practice as reduced only, by removing zero till
    N<-Int.Out[,grepl("b39",IN.T.Codes) & grepl("b38",IN.T.Codes)]
    
    # Remove b38 & b39 codes from difference col
    Int.Out[N,IN.T.Codes.Diff:=paste(unlist(strsplit(gsub("-b38|b38-|b38|-b39|b39-|b39","",IN.T.Codes.Diff[1]),"***",fixed = T)),collapse="***"),by=IN.T.Codes.Diff]
    
    # Remove b39 codes from IN.T.Codes col
    Int.Out[N,IN.T.Codes:=gsub("-b39|b39-|b39","",IN.T.Codes[1]),by=IN.T.Codes]
    
    # Add b38 code to shared col
    Int.Out[N,IN.T.Codes.Shared:=paste(sort(c(unlist(strsplit(IN.T.Codes.Shared[1],"-")),"b38")),collapse="-"),by=IN.T.Codes.Shared]
  
  ## 5.5) Combine products #####
  # A duplicate of In.Prods.All column, but using a "-" delimiter that distinguishes products that contribute to a system outcome from products aggregated 
  # in the Products tab (results aggregated across products) which use a ".." delim.
  
    Int.Out[,IN.Prod:=apply(Int.Out[,c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4")],1,FUN=function(X){
      X<-unlist(X[!is.na(X)])
      paste(X[order(X)],collapse="***")
    })]
    # Int.Out: Combine Products: Validation - Check for NAs
    Int.Out.Missing.Prod<-Int.Out[is.na(IN.Prod)|IN.Prod=="",c("IN.Level.Name","B.Code","IN.Prod")]
    if(nrow(Int.Out.Missing.Prod)>0){
      View(Int.Out.Missing.Prod)
    }
    rm(Int.Out.Missing.Prod)
  
  ## 5.6) Update intercropping delimiters ####
    Int.Out$IN.Level.Name2<-apply(Int.Out[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")],1,FUN=function(X){
      X<-paste0(X[!is.na(X)],collapse = "***")
    })
  
  ## 5.7) Structure ####
    X<-MT.Out[,.(C.Structure,P.Structure,W.Structure,O.Structure,PD.Structure)
              ][grepl("Yes",C.Structure),C.Structure:=MT.Out[grepl("Yes",C.Structure),C.Level.Name]
                ][grepl("Yes",P.Structure),P.Structure:=MT.Out[grepl("Yes",P.Structure),P.Level.Name]
                  ][grepl("Yes",O.Structure),O.Structure:=MT.Out[grepl("Yes",O.Structure),O.Level.Name]
                    ][grepl("Yes",W.Structure),W.Structure:=MT.Out[grepl("Yes",W.Structure),W.Level.Name]
                      ][grepl("Yes",PD.Structure),PD.Structure:=MT.Out[grepl("Yes",PD.Structure),PD.Level.Name]]
    
    X<-pbapply(X,1,FUN=function(X){
      X<-paste(unique(X[!(is.na(X)|X %in% c("","No","no"))]),collapse=":::")
      if(is.null(X)|X==""){
        NA
      }else{
        X
      }
    })
    
    MT.Out[,Structure.Comb:=X][!is.na(Structure.Comb),Structure.Comb:=paste(P.Product,Structure.Comb)]
    
    I.Cols<-c("B.Code","T.Name","P.Product",grep("Structure",colnames(MT.Out),value=T))
    X<-data.table(MT.Out[,..I.Cols],Structure=X)
    
    X[!is.na(Structure),Structure:=paste(P.Product,Structure)]
    
    if(F){
      unique(X[grep("[.][.][.]",Structure),c("B.Code","Structure")])
    }
    
    Z<-MT.Out[,paste(B.Code,T.Name)]
    Z<-pblapply(1:nrow(Int.Out),FUN = function(i){
      
      Y<-paste(Int.Out[i,B.Code],unlist(strsplit(Int.Out[i,IN.Level.Name2],"[*][*][*]")))
      
      N<-match(Y,Z)
  
      if(any(is.na(N))){
        cat("\n No Match: i = ",i," - ",Y[is.na(N)],"\n")
        list(error=Y[is.na(N)],data=NA)
      }else{
        Y<-X[N,unique(Structure)]
        Y<-paste(Y,collapse = "***")
        
        if(Y=="NA"){
          list(data=NA)
        }else{
          list(data=Y)
        }
      }
    })
    
    errors3<-data.table(value=unlist(lapply(Z,"[[","error")))
    errors3[,B.Code:=unlist(tstrsplit(value," ",keep=1))
            ][,value:=gsub(paste0(B.Code," "),"",value),by=.(B.Code,value)
              ][,table:="Int.Out"
                ][,field:="IN.Level.Name"
                  ][,issue:="Component(s) of aggregated treatment name used in intercropping tab do not match a value in the treatments tab."
                    ][,parent_table:="MT.Out"]
    
    Z<-unlist(lapply(Z,"[[","data"))
    
    Int.Out[,IN.Structure:=Z]
    
    rm(Z,X,I.Cols)
    
    if(F){
      Int.Out[!is.na(IN.Structure) & grepl("[:][:][:]",IN.Structure),c("B.Code","IN.Level.Name","IN.Structure")]
    }
  
  
  ## 5.8) Residues in system outcomes ####
    Recode.Res<-function(X){
      A<-unlist(strsplit(unlist(X),"-"))
      if(is.na(A[1])){
        A
      }else{
        A[grep("b41",A)]<-"b41"
        A[grep("b40",A)]<-"b40"
        A[grep("b27",A)]<-"b27"
        A[A %in% c("a16","a17")]<-"a15"
        A[A %in% c("a16.1","a17.1")]<-"a15.1"
        A[A %in% c("a16.2","a17.2")]<-"a15.2"
        A<-unique(A)
        paste(A[order(A)],collapse="-")
      }
    }
    
  Int.Out[,IN.Res.System:=Recode.Res(IN.Residue.Code[1]),by=IN.Residue.Code]
  rm(Recode.Res)
  
  ## 5.9) Save errors #####
    error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4),fill=TRUE)[order(B.Code)],
                              filename = "intercrop_other_errors",
                              error_dir=error_dir,
                              error_list = error_list)
  
  ## X) ***!!!TO DO!!!*** - check is NA start.date or reps after treatments, intercropping and rotation are merged #####
if(F){
  errors4<-MT.Out[(is.na(T.Start.Year)) & !grepl("[.][.]",T.Name),
                  list(B.Code,T.Name)
  ][,list(value=paste0(T.Name,collapse="/")),by=B.Code
  ][,table:="MT.Out"
  ][,field:="T.Name"
  ][,issue:="Start year is NA (change to Unspecified if it is truely unknown)."
  ][order(B.Code)]
  
  
  errors5<-MT.Out[(is.na(T.Reps)) & !grepl("[.][.]",T.Name),
                  list(B.Code,T.Name)
  ][,list(value=paste0(T.Name,collapse="/")),by=B.Code
  ][,table:="MT.Out"
  ][,field:="T.Name"
  ][,issue:="Reps are NA. Change to Unspecified if truely unknown"
  ][order(B.Code)]
}



  
# 6) Time Sequences (Rot.Out) ####
data<-lapply(XL,"[[","Rot.Out")
col_names<-colnames(data[[1]])

  ## 6.1) Rot.Seq #####
  col_names2<-col_names[15:18]
  
  Rot.Seq<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with rotation tab structure"))
    }else{
      X<-X[,..col_names2]
      colnames(X)[1]<-"R.Level.Name"
      X<-X[!is.na( R.Level.Name)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_b<-rbindlist(lapply(Rot.Seq,"[[","error"))
  
  Rot.Seq<-rbindlist(lapply(Rot.Seq,"[[","data"))  
  setnames(Rot.Seq,c("Time Period","Treatment","Fate Crop Residues The Previous Season"),c("Time","R.Treatment","R.Resid.Fate"),skip_absent = T)
  
  results<-validator(data=Rot.Seq,
                     tabname="Rot.Seq",
                     zero_cols = "R.Resid.Fate",
                     trim_ws = T,
                     time_data=Times.Out,
                     duplicate_field="R.Level.Name")
  
  errors1<-results$errors
  Rot.Seq<-results$data
  
  # Update/correct R.Resid.Fate values used
  Rot.Seq[R.Resid.Fate=="Retained (Unknown if mulched/incorp.)",R.Resid.Fate:="Retained (unknown if mulched/incorp.)"]
  Rot.Seq[R.Resid.Fate=="Grazed/Mulched",R.Resid.Fate:="Mulched (left on surface)"]
  
  ## 6.2) Rot.Out #####
  col_names2<-col_names[1:13]
  
  Rot.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with rotation tab structure"))
    }else{
      X<-X[,..col_names2]
      X<-X[!(is.na( R.Level.Name)|R.Level.Name %in% c("#N/A","NA"))]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Rot.Out,"[[","error"))
  Rot.Out<-rbindlist(lapply(Rot.Out,"[[","data"))  
  
  setnames(Rot.Out,"R.User.Code","R.Practice")
  
  results<-validator(data=Rot.Out,
                     tabname="Rot.Out",
                     unique_cols = "R.Level.Name",
                     compulsory_cols = c(R.Level.Name="R.Practice"),
                     trim_ws = T,
                     time_data=Times.Out,
                     duplicate_field="R.Level.Name"
                     )
  errors2<-results$errors
  Rot.Out<-results$data
  
  # Remove duplicated rows
  Rot.Out<-Rot.Out[!duplicated(Rot.Out[,.(B.Code,R.Level.Name)])]
  
  ## 6.3) Rot.Seq processing #####
    ### 6.3.1) Update delimiters #####
    Rot.Seq[, R.Treatment:= split_trimws(R.Treatment,delim = "..")]
    Rot.Seq[,R.Treatment:=gsub("[.][.]","...",R.Treatment)]
    Rot.Seq[,R.Treatment:=gsub("__","***",R.Treatment)]
    
    # Check treatments are in make.trt tab or intercropping tab
    
    # Check rotation names against MT.Out
    dat<-Rot.Seq[!grepl("***",R.Treatment,fixed=T) & R.Treatment!="Natural or Bare Fallow",list(B.Code,R.Treatment)]
    colnames(dat)[2]<-"T.Name"
    errors3.1<-check_key(parent=MT.Out,child=dat,tabname="Rot.Seq",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code = F)
    errors3.1<-errors3.1[,list(value=paste(value,collapse = "/")),by=.(B.Code,table,field,issue,parent_table)]
    
    # Check rotation names against Int.Out
    dat<-Rot.Seq[grepl("***",R.Treatment,fixed=T),list(B.Code,R.Treatment)]
    colnames(dat)[2]<-"IN.Level.Name2"
    errors3.2<-check_key(parent=Int.Out,child=dat,tabname="Rot.Seq",tabname_parent="Int.Out",keyfield="IN.Level.Name2",collapse_on_code = F)
    errors3.2<-errors3.2[,list(value=paste(value,collapse = "/")),by=.(B.Code,table,field,issue,parent_table)]
    
    # Value must be missing in both treatments and intercrops
    errors3<-rbind(errors3.1,errors3.2)[,field:="R.Treatment"
                                        ][,issue:="Treatname name used in time sequence does not match in treatment or intercrop tabs. "
                                          ][,value:=gsub("[.][.][.]","..",value)][,value:=gsub("[*][*][*]","__",value)]
  
    ### 6.3.2) Add in products & structure ######
      # Merge in products
      # Treatments tab
      mergedat<-MT.Out[,.(B.Code,T.Name,P.Product,Structure.Comb)]
      Rot.Seq<-merge(Rot.Seq,mergedat,by.x=c("B.Code","R.Treatment"),by.y=c("B.Code","T.Name"),all.x=T,sort=F)
      
      # Intercrop treatments
      mergedat<-Int.Out[,.(B.Code,IN.Level.Name2,IN.Prod1,IN.Prod2,IN.Prod3,IN.Prod4,IN.Structure)]
      Rot.Seq<-merge(Rot.Seq,mergedat,by.x=c("B.Code","R.Treatment"),by.y=c("B.Code","IN.Level.Name2"),all.x=T,sort=F)
      
      Rot.Seq<-setnames(Rot.Seq,c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4"),c("R.Prod1","R.Prod2","R.Prod3","R.Prod4"))
      Rot.Seq[is.na(R.Prod1),R.Prod1:=P.Product][,P.Product:=NULL]
      
      #  Add in Fallow
      Rot.Seq[R.Treatment=="Natural or Bare Fallow",R.Prod1:="Fallow"]
  
      # Combine Product codes
      Rot.Seq[,R.Prod:=apply(Rot.Seq[,c("R.Prod1","R.Prod2","R.Prod3","R.Prod4")],1,FUN=function(X){
        X<-unlist(X[!is.na(X)])
        paste(X[order(X)],collapse="***") # Intercropping delim
      })]
      Rot.Seq[R.Prod=="",R.Prod:=NA]
      
      # Are there any residual mismatches not highlighted in errors3?
      Rot.Seq[is.na(R.Prod) & !B.Code %in% errors3$B.Code]
      
    ### 6.3.3) Add in treatment codes ######
  
    # Merge in products
    # Treatments tab
    mergedat<-unique(MT.Out[,.(B.Code,T.Name,T.Codes,T.Agg.Levels,T.Codes.No.Agg,T.Codes.Agg)])
    Rot.Seq<-merge(Rot.Seq,mergedat,by.x=c("B.Code","R.Treatment"),by.y=c("B.Code","T.Name"),all.x=T)
    
    # Intercrop treatments
    mergedat<-Int.Out[,.(B.Code,IN.Level.Name2,IN.T.Codes,IN.Code,IN.T.Codes.Shared,IN.T.Codes.Diff)]
    Rot.Seq<-merge(Rot.Seq,mergedat,by.x=c("B.Code","R.Treatment"),by.y=c("B.Code","IN.Level.Name2"),all.x=T)
  
    Rot.Seq[,R.T.Codes:=IN.T.Codes
            ][is.na(R.T.Codes),R.T.Codes:=T.Codes.No.Agg
              ][,R.IN.Codes:=IN.Code]
  
    # Add in fallow codes
    Rot.Seq[R.Treatment == "Natural or Bare Fallow",R.T.Codes:="h24"]
    
    ### 6.3.4) Update residue codes based on previous season ######
  
    # Merge in info from Rot.Out
    Rot.Seq<-merge(Rot.Seq,
                   Rot.Out[,.(B.Code,R.Level.Name,R.Start.Year,R.Start.Season)],
                   by=c("B.Code","R.Level.Name"),
                   all.x=T)
    
    # Merge in data from Time.Out
    Rot.Seq<-merge(Rot.Seq,Times.Out[,.(B.Code,Time,Time.Start.Year,Time.Season,seq_n)],by=c("B.Code","Time"),all.x=T)
    setnames(Rot.Seq,c("Time.Start.Year","Time.Season"),c("Year","Season"))
    
    # Reorder Rot.Seq according the ascending seq of Times provided by the merge with the Time.Out tab
    # NOTE! This requires that the sequence have been entered in ascending temporal order in the Time.Out tab
  
    Rot.Seq<-Rot.Seq[order(B.Code,R.Level.Name,seq_n)]
    
    # Only relevant to Majestic Hippo
    Rot.Seq[grep("-5",Year),Season:="Off"][,Year:=gsub("-5","",Year)]
    
    # Does min start year = min year? (is a complete temporal sequence presented or just part of the sequence)
    # I don't think enough attention was paid to the start season for it to be useful here.
    Rot.Seq[,Full.Seq:=F
            ][,Min.Year:=min(Year),by=.(B.Code,R.Level.Name)
              ][Min.Year<=R.Start.Year,Full.Seq:=T
                ][grep("-",Year),Full.Seq:=NA]
    
    # Add column that shows product of the previous season
    
    # Add Starting Year/Season to Rot.Seq
    Rot.Out[,ID:=paste(B.Code[1],R.Level.Name[1]),by=.(B.Code,R.Level.Name)]
    Rot.Seq[,ID:=paste(B.Code[1],R.Level.Name[1]),by=.(B.Code,R.Level.Name)]
    
    seq_ids<-unique(Rot.Seq$ID)
    
    # Q: Can we assume if fallow and start of sequence assume previous state was also fallow (rather than NA)? #####
    # Does the above matter as fallows at beginning of a time seq will not be receiving any residues?
    
    Rot.Seq<-rbindlist(pblapply(1:length(seq_ids),FUN=function(i){
      
      seq_id<-seq_ids[i]
      #print(X)
      data<-Rot.Seq[ID==seq_id]
      data[Season=="Off",Season:=NA]
  
      seq_rows<-1:(nrow(data)-1)
      
      if(!data$Full.Seq[1]){
        # For sequences that have a starting date before the start of the sequence provided we can assume the residues come from 
        # the previous crop in the sequence (even though is not listed in the Rot.Seq tab)
        Z<-unlist(data[seq_rows,R.Prod])
        # Find what previous crop for first value in sequence should have been
        Z<-c(Z[match(Z[1],Z[2:length(Z)])],Z)

        data[,R.Prod.Prev:=Z]
      }else{
        # If full sequence provided then assume we do not know about the fate of residues
        data[,R.Prod.Prev:=c(NA,unlist(data[seq_rows,R.Prod]))]
      }
      
      data
    }))
    
    # Rot.Seq: Update residue codes cross-referencing to master_codes
    
    # Split into columns to deal with aggregated products or intercropped treatments
    X<- strsplit(gsub("[.][.][.]","***",Rot.Seq$R.Prod.Prev),"[*][*][*]")
    N<-max(unlist(lapply(X,length)))
    
    # Add codes for residues that are from tree and product vocab
    tree_master<-unique(master_codes$trees[,.(Tree.Latin.Name, Mulched,Incorp,Unknown.Fate)])
    colnames(tree_master)[2:4]<-paste0("T.",colnames(tree_master)[2:4])
    colnames(tree_master)[1]<-"product"
    prod_master<-unique(master_codes$prod[!is.na(Mulched),.(Product.Simple, Mulched,Incorp,Unknown.Fate)])
    colnames(prod_master)[2:4]<-paste0("P.",colnames(prod_master)[2:4])
    colnames(prod_master)[1]<-"product"
    prod_master<-rbind(prod_master,data.table("Fallow","b27","b41","b40"),use.names=F)
  
    for(i in 1:N){
      col<-paste0("R.Prod.Prev",i)
      res_col<-paste0("R.Res.Code.",i)
      
      prods<-unlist(lapply(X,"[",i))
      Rot.Seq[,(col):=prods]
      
      res_dat<-data.table(product=prods,res_fate=Rot.Seq[,R.Resid.Fate])
      res_dat<-merge(res_dat,tree_master,by="product",all.x=T,sort=F)
      res_dat<-merge(res_dat,prod_master,by="product",all.x=T,sort=F)
      
      res_dat[res_fate=="Mulched (left on surface)",res_code:=if(!is.na(T.Mulched[1])){T.Mulched[1]}else{P.Mulched[1]},by=product]
      res_dat[res_fate=="Incorporated",res_code:=if(!is.na(T.Incorp[1])){T.Incorp[1]}else{P.Incorp[1]},by=product]
      res_dat[res_fate=="Retained (unknown if mulched/incorp.)",res_code:=if(!is.na(T.Unknown.Fate[1])){T.Unknown.Fate[1]}else{P.Unknown.Fate[1]},by=product]
      
      if(i==1){
        # Update Non ERA residue codes (control codes)
        # Is product is NA (usually 1st time in seq and we do not know previous crop) but a residue fate is present add a code for an unspecified plant
        res_dat[res_fate == "Removed",res_code:="h35"
                ][res_fate == "Grazed",res_code:="h39"
                  ][res_fate == "Burned",res_code:="h36"
                    ][is.na(product) & res_fate=="Mulched (left on surface)",res_code:="b27"
                      ][is.na(product) & res_fate=="Incorporated",res_code:="b41"
                        ][is.na(product) & res_fate=="Retained (unknown if mulched/incorp.)",res_code:="b40"]
        }
                
      Rot.Seq[,(res_col):=res_dat$res_code]
    }
    
    res_cols<-paste0("R.Res.Code.",1:N)
  
    # 2) Join using appropriate delimiter & update Rot.Seq - R.Residues.Codes
    Res.Fun<-function(cols,delim){
      X<-paste0(c(A,B,C,D)[!is.na(c(A,B,C,D))],collapse = delim)
      if(length(X)==0 | X==""){
        NA
      }else{
        X
      }
    }
    
    X1int<-apply(Rot.Seq[,..res_cols],1,FUN=function(x,delim="***"){
      x<-unique(x[!is.na(x) & x!=""])
      if(length(x)==0){
        return(as.character(NA))
      }else{
        return(paste(sort(x),collapse = delim))
      }
    })
    
    X1agg<-apply(Rot.Seq[,..res_cols],1,FUN=function(x,delim="..."){
      x<-unique(x[!is.na(x) & x!=""])
      if(length(x)==0){
        return(as.character(NA))
      }else{
        return(paste(sort(x),collapse = delim))
      }
    })
    
    Rot.Seq[,R.Residues.Codes:=R.Res.Code.1]
    Rot.Seq[grep("[*][*][*]",R.Treatment),R.Residues.Codes:=X1int[grep("[*][*][*]",Rot.Seq$R.Treatment)]]
    Rot.Seq[grep("[.][.][.]",R.Treatment),R.Residues.Codes:=X1agg[grep("[.][.][.]",Rot.Seq$R.Treatment)]]
  
    
    ### 6.3.5) Add rotation codes ######
  
    Rot.Seq<-merge(Rot.Seq,Rot.Out[,.(R.Level.Name,B.Code,R.Code)],by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
    
    # Find first time that product switches 
  
    P.Switch<-function(X){
      Y<-which(X!=X[1])
      if(length(Y)==0){
        rep(F,length(X))
      }else{
        Y<-Y[1]:length(X)  
        c(rep(F,length(1:(Y[1]-1))),rep(T,length(Y)))
      }
    }
    
    Rot.Seq[,Prod.Switch:=P.Switch(R.Prod),by=.(B.Code,R.Level.Name)
                        ][,Prod.Switch.All.F:=if(sum(Prod.Switch)==0){T}else{F},by=.(B.Code,R.Level.Name)
                          ][,min_seq:=seq_n==min(seq_n),by=.(B.Code,R.Level.Name)]
    
    
    # See section 6.5 for how this is used to remove codes for the first year of sequence
    
    ### 6.3.6) Update structure ######
    Join.Fun<-function(X,Y){c(X,Y)[!is.na(c(X,Y))]}
    Rot.Seq[,R.Structure:=Join.Fun(Structure.Comb[1],IN.Structure[1]),by=.(Structure.Comb,IN.Structure)]
    rm(Join.Fun)
  
  ## 6.4) Rot.Out processing #####
    ### 6.4.1) Update/add fields from Rot.Seq ######
      # R.T.Codes.All 
        # Would it be better to have Agg Treats by component split with a "|||" as well as Cmn Trts? and retaining NA values
        # Perhap R.T.Codes.All should be derived from Rot.Seq?
       Rot.Out$R.T.Codes.All<-unlist(pblapply(1:nrow(Rot.Out),FUN=function(i){
          X<-Rot.Out[i,]
          Y<-Rot.Seq[B.Code==X$B.Code & R.Level.Name==X$R.Level.Name,R.Treatment]
  
          N<-Y %in% Int.Out$IN.Level.Name2
          if(sum(N)>0){
            Z<-Int.Out$IN.T.Codes[paste0(Int.Out$IN.Level.Name2,Int.Out$B.Code) %in% paste0(Y,X$B.Code)]
            Z<-unique(unlist(strsplit(Z,"-")))
            Z<-Z[!is.na(Z)]
          }else{
            Z<-NA
          }
          
          N<-Y %in% MT.Out$T.Name
          if(sum(N)>0){
            Z<-MT.Out$T.Codes[paste0(MT.Out$T.Name,MT.Out$B.Code) %in% paste0(Y,X$B.Code)]
            
            if(length(grep("[.][.][.]",Z))>0){
              A<-unique(MT.Out$T.Codes.Agg[paste0(MT.Out$T.Name,MT.Out2$B.Code) %in% paste0(Y,X$B.Code)])
              A<-A[!is.na(A)]
              
              B<-unique(MT.Out$T.Codes.No.Agg[paste0(MT.Out$T.Nam2,MT.Out$B.Code) %in% paste0(Y,X$B.Code)])
              B<-B[!is.na(B)]
              if(length(B)>0){
                Z<-paste0("Agg Trts: ",paste0(A,collapse="|||")," Cmn Trts: ",paste0(B,collapse = "|||"))
              }else{
                Z<-paste0("Agg Trts: ",paste0(A,collapse="|||"))
              }
            }else{
              
              Z<-unique(unlist(strsplit(Z,"-")))
              Z<-Z[!is.na(Z)]
            }
          }else{
            Z<-NA
          }
          
          if(length(Z)>1){
            Z<-paste0(Z[order(Z)],collapse = "-")
          }
          if(length(Z)==0){
            NA
          }else{
            Z
          }
        }))
        
      # R.Residue.Codes.All
        # Consider adding a logical field that indicates if an observation is from a preceeding season.
        mergedat<-Rot.Seq[,list(R.Residues.All2=if(all(is.na(R.Residues.Codes))){as.character(NA)}else{paste(R.Residues.Codes,collapse = "|||")},
                         R.Residue.Codes.All = paste(unique(R.Residues.Codes[!is.na(R.Residues.Codes)])[order(unique(R.Residues.Codes[!is.na(R.Residues.Codes)]))],collapse="-")),by=.(B.Code,R.Level.Name)]
        Rot.Out[,c("R.Residue.Codes.All","R.Residues.All"):=NULL]
        Rot.Out<-merge(Rot.Out,unique(mergedat),by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
        Rot.Out[R.Residue.Codes.All=="",R.Residue.Codes.All:=NA]
      # R.All.Products
        mergedat<-Rot.Seq[,list(R.All.Products=paste0(sort(unique(unlist(strsplit(unlist(strsplit(R.Prod,"[*][*][*]")),"[.][.][.]")))),collapse = "-")),by=.(B.Code,R.Level.Name)]
        Rot.Out[,R.All.Products:=NULL]
        Rot.Out<-merge(Rot.Out,unique(mergedat),by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
        
      # R.All.Structure
        mergedat<-Rot.Seq[,list(R.All.Structure=paste0(sort(unique(R.Structure)),collapse = "-")),by=.(B.Code,R.Level.Name)]
        Rot.Out[,R.All.Structure:=NULL]
        Rot.Out<-merge(Rot.Out,unique(mergedat),by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
        
    ### 6.4.2) Update delimiters in R.T.Level.Names.All ######
        # Is this required ?
        mergedat<-unique(Rot.Seq[,list(R.T.Level.Names.All2=paste0(R.Treatment,collapse="|||")),by=.(B.Code,R.Level.Name)])
        Rot.Out<-merge(Rot.Out,mergedat,by=c("B.Code","R.Level.Name"),all.x=T,sort=F)   
        
    ### 6.4.3) Generate T.Codes & Residue for System Outcomes ####
    # Set Threshold for a practice to be considered present in the sequence (proportion of phases recorded)
    Threshold<-0.5
    
    seq_ids<-Rot.Seq[,unique(ID)]
    mergedat<-rbindlist(pblapply(1:length(seq_ids),FUN=function(i){
      TC<-seq_ids[i]
      X<-Rot.Seq[ID==TC]
      Y<-table(unlist(strsplit(X[,R.T.Codes],"-")))/nrow(X)
      INT<-table(unlist(strsplit(X[,R.IN.Codes],"-")))/nrow(X)
      if(length(Y)==0){
        Y<-NA
      }else{
        Y<-names(Y[Y>=Threshold])
        Y<-paste(Y[order(Y)],collapse="-")
      }
      
      if(length(INT)==0){
        INT<-NA
      }else{
        INT<-names(INT[INT>=Threshold])
        INT<-paste(INT[order(INT)],collapse="-")
      }
      
      A<-unlist(strsplit(X[,R.Residues.Codes],"-"))
      A<-unlist(strsplit(A,"[*][*][*]"))
      A[grep("b41",A)]<-"b41"
      A[grep("b40",A)]<-"b40"
      A[grep("b27",A)]<-"b27"
      A[A %in% c("a16","a17")]<-"a15"
      A[A %in% c("a16.1","a17.1")]<-"a15.1"
      A[A %in% c("a16.2","a17.2")]<-"a15.2"
      A<-table(A)/nrow(X)
      if(length(A)==0){
        A<-NA
      }else{
        A<-as.vector(names(A[A>=Threshold]))
        A<-paste(A[order(A)],collapse="-")
      }
      
      data.table(B.Code=X$B.Code[1],R.Level.Name=X$R.Level.Name[i],R.T.Codes.Sys=Y,R.Res.Codes.Sys=A,R.IN.Codes.Sys=INT)
      
    }))
    
    Rot.Out<-merge(Rot.Out,mergedat,by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
    Rot.Out[R.Res.Codes.Sys=="",R.Res.Codes.Sys:=NA
            ][R.IN.Codes.Sys=="",R.IN.Codes.Sys:=NA]
  
    ### 6.4.4) Add Practice Level Columns ######
    Levels<-Fields[!Levels %in% c("P.Level.Name","O.Level.Name","W.Level.Name","C.Level.Name","PD.Level.Name","T.Residue.Prev")]
    Levels<-c(Levels[,Levels],Levels[,Codes])
    
    Rot.Levels<-rbindlist(pblapply(1:nrow(Rot.Out),FUN=function(i){
      
      X<-Rot.Out[i]
      
      # Note that intercropping uses "***" & aggregated trts "..."
      
      Trts.All<-unlist(strsplit(X[,R.T.Level.Names.All2],"[|][|][|]"))
      Trts.All<-unlist(strsplit(Trts.All,"[*][*][*]"))
      Trts.All<-unlist(strsplit(Trts.All,"[.][.][.]"))
      
      Trts<-MT.Out[B.Code==X[,B.Code]][match(Trts.All,T.Name)]
      
      Levels.Joined<-unlist(lapply(Levels,FUN=function(X){
        Y<-unlist(Trts[,..X])
        paste(Y,collapse = "---")}
      ))
      Levels.Joined<-data.table(matrix(nrow=1,ncol=length(Levels),Levels.Joined,dimnames=list(1,Levels)))
      data.table(X[,c("B.Code","R.Level.Name")],Levels.Joined)
      
    }))
    
    Rot.Levels[,R.ID:=paste(B.Code,R.Level.Name)]
    rm(Levels)
    
    ### 6.4.5) Add crop sequence to Rot.Out #####
    Seq.Fun<-function(A){
      paste(A[!is.na(A)],collapse = "|||")
    }
    Rot.Seq.Summ<-Rot.Seq[,list(R.Prod.Seq=Seq.Fun(R.Prod)),by=.(B.Code,R.Level.Name)]
    Rot.Out<-merge(Rot.Out,Rot.Seq.Summ,by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
    
  ## 6.5) Other validation ######
        # Check for sequences that have a rotation code, but no rotation in the sequence presented
        # Only improved fallow should be able to have a sequence with no change in product.
        Rot.Seq[,n_crops:=length(unique(R.Prod)),by=.(B.Code,R.Level.Name)]
        errors4<-Rot.Seq[n_crops==1 & R.Code!="h24" & !is.na(R.Code) & !B.Code %in% errors3$B.Code
                ][,.(value=paste0(R.Level.Name[1],"-", R.Code[1],":",paste(unique(R.Prod),collapse="/"))),by=.(B.Code,R.Level.Name)
                  ][,list(value=paste(value,collapse = "/")),by=B.Code
                    ][,table:="Rot.Out"
                      ][,field:="R.Level.Name-R.Code: R.Prod"
                        ][,issue:="Rotation practice present but only one crop present in corresponding sequence."]  
        write.table(errors4,"clipboard-256000",row.names = F,sep="\t")
  
        # Update Rotation Codes to NA where crop has not changed (with exception of improved fallows with no change in product)
        # Add Year==Min.Year & min_seq==T if you only want this to apply to the first instance of a sequence
        Rot.Seq[Prod.Switch==F & (Prod.Switch.All.F==F & !(R.Code %in% c("h24","b60.1","b60","b60.2"))),R.Code:=NA]
        
        # Rot.Out: Validation - Where do we have missing residue codes?
        issues<-unique(Rot.Seq[!R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","Burned or Grazed","Other") & 
                  !is.na(R.Prod) & !is.na(R.Resid.Fate) &
                  ((!is.na(R.Prod.Prev1) & is.na(R.Res.Code.1)) |
                     (!is.na(R.Prod.Prev2) & is.na(R.Res.Code.2)) | 
                     (!is.na(R.Prod.Prev3) & is.na(R.Res.Code.3)) | 
                     (!is.na(R.Prod.Prev4) & is.na(R.Res.Code.4))),
                .(B.Code,R.Level.Name, R.Treatment,R.Resid.Fate,
                  R.Prod.Prev1,R.Res.Code.1,
                  R.Prod.Prev2,R.Res.Code.2,
                  R.Prod.Prev3,R.Res.Code.3,
                  R.Prod.Prev4,R.Res.Code.4)])
        
        issues[is.na(R.Res.Code.1),issue_crop:=R.Prod.Prev1]
        issues[is.na(R.Res.Code.2) & !is.na(R.Prod.Prev2),issue_crop:=paste0(c(issue_crop[!is.na(issue_crop)],R.Prod.Prev2),collapse="-"),by=.(B.Code,R.Level.Name)]
        issues[is.na(R.Res.Code.3) & !is.na(R.Prod.Prev3),issue_crop:=paste0(c(issue_crop[!is.na(issue_crop)],R.Prod.Prev3),collapse="-"),by=.(B.Code,R.Level.Name)]
        issues[is.na(R.Res.Code.4) & !is.na(R.Prod.Prev4),issue_crop:=paste0(c(issue_crop[!is.na(issue_crop)],R.Prod.Prev4),collapse="-"),by=.(B.Code,R.Level.Name)]
        
        errors5<-issues[!B.Code %in% error_list$prod_other_errors$B.Code,.(value=paste(unique(issue_crop),collapse="/")),by=B.Code][,table:="Rot.Seq"][,field:="R.Level.Name"][,issue:="Crop name merged to Rot.Seq from MT.Out tab has no match in mastercodes products table (check product names throughout sheet)."]

      
  ## 6.6) Save errors #####
  error_list<-error_tracker(errors=unique(rbind(errors_a,errors_b)),filename = "rot_structure_errors",error_dir=error_dir,error_list = error_list)
  error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4,errors5),fill=T)[order(B.Code)],filename = "rot_other_errors",error_dir=error_dir,error_list = error_list)

# 7) Outcomes ####
data<-lapply(XL,"[[","Out.Out")
col_names<-colnames(data[[800]])

  ## 7.1) Out.Out #####
  col_names2<-col_names[1:10]
  
  Out.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    # Add missing cols to older versions
    if(!"Out.Agg.Stat" %in% colnames(X)){
      X$Out.Agg.Stat<-"Not in template"
    }
    
    if(!"Out.Notes" %in% colnames(X)){
      X$Out.Notes<-"Not in template"
    }
    
    if(!all(col_names2 %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with outcome tab structure"))
    }else{
      X<-X[,..col_names2]
      X<-X[!is.na(Out.Subind)]
      if(nrow(X)>0){
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        cat("No data in file",i,B.Code,"\n")
        list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="No data for outcome name in outcome tab"))
      }
    }})
  
  errors_a<-rbindlist(lapply(Out.Out,"[[","error"))
  Out.Out<-rbindlist(lapply(Out.Out,"[[","data"))
  
  # Check outcome names match master codes
  parent<-master_codes$out[,list(Subindicator)][,Out.Subind:=Subindicator]
  errors1<-Out.Out[!Out.Out$Out.Subind %in% parent$Out.Subind
                   ][,list(value=paste(Out.Subind,collapse = "/")),by=B.Code
                     ][,table:="Out.Out"
                     ][,master_table:="out"
                       ][,field:="Out.Subind"
                         ][,issue:="Outcome name used does not match master codes."
                           ][order(B.Code)]
  
  allowed_values<-data.table(allowed_values=list(master_codes$out[Depreciated==F,unique(Subindicator)]),
                             parent_tab_name=c("master_Codes$out$Subindicator"),
                             field=c("Out.Subind"))
  
  results<-validator(data=Out.Out,
                     numeric_cols=c("Out.Depth.Upper","Out.Depth.Lower","Out.NPV.Rate","Out.NPV.Time"),
                     compulsory_cols = c(Out.Code.Joined="Out.Unit"),
                     allowed_values = allowed_values,
                     unique_cols = "Out.Code.Joined",
                     tabname="Out.Out",
                     trim_ws = T)
  
  errors2<-results$errors
  errors2<-errors2[!(grepl("Ratio|Use Efficiency|Agronomic Efficiency|Rate of Return|Index",value) & issue=="Missing value in compulsory field Out.Unit.")]
  Out.Out<-results$data
  
  # Incorrect depths
  errors3<-Out.Out[Out.Depth.Upper>Out.Depth.Lower
                   ][,list(value=paste0(Out.Code.Joined,collapse="/")),by=B.Code
                     ][,table:="Out.Out"
                       ][,field:="Out.Code.Joined"
                         ][,issue:="Upper depth is greater than lower depth."
                           ][order(B.Code)]
  
    ### 7.1.2) Harmonization ######
  
  h_params<-data.table(master_tab="unit_harmonization",
                       h_field="Out.Unit",
                       h_field_alt="Out.Unit.Correct",
                       h_table="Out.Out")
  
  results<-harmonizer_wrap(data=Out.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Out.Out<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_tasks,filename = "outcome_unit_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
  write.table(results$h_tasks, pipe("pbcopy"), row.names = FALSE, sep = "\t")
  
  ## 7.2) Out.Econ #####
col_names2<-col_names[13:20]

Out.Econ<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  # Add missing cols to older versions
  for(i in 1:length(col_names2)){
    cname<-col_names2[i]
  if(!cname %in% colnames(X)){
    X[,(cname):="Not in template"]
  }
  }
  
  if(!all(col_names2 %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with outcome economics tab structure"))
  }else{
    X<-X[,..col_names2]
    X<-X[!is.na(Econ.Var)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors_b<-rbindlist(lapply(Out.Econ,"[[","error"))
Out.Econ<-rbindlist(lapply(Out.Econ,"[[","data"))

results<-validator(data=Out.Econ,
                   tabname="Out.Econ",
                   site_data = Site.Out,
                   time_data=Times.Out,
                   trim_ws = T,
                   ignore_values = c("unspecified","All times","All sites","Not in template"))

errors4<-results$errors[order(B.Code)]
Out.Econ<-results$data

error_list<-error_tracker(errors=rbind(errors_a,errors_b),filename = "out_structure_errors",error_dir=error_dir,error_list = error_list)
error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4),fill=T),filename = "out_other_errors",error_dir=error_dir,error_list = error_list)

# 8) Enter Data ####
data<-lapply(XL,"[[","Data.Out")
col_names<-colnames(data[[800]])

  ## 8.0) Data.Out #####
  Data.Out<-pblapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    # Add missing cols to older versions
    if(!"ED.Sample.DAE" %in% colnames(X)){
      X$ED.Sample.DAE<-"Not in template"
    }
    
    if(!"ED.Sample.Stage" %in% colnames(X)){
      X$ED.Sample.Stage<-"Not in template"
    }
    
    if(!"ED.Comparison2" %in% colnames(X)){
      X$ED.Comparison2<-"Not in template"
    }
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with enter data tab structure"))
    }else{
      if(nrow(X)>0){
        X<-X[,..col_names]
        X[,B.Code:=B.Code]
        list(data=X)
      }else{
        NULL
      }
    }})
  
  errors_a<-rbindlist(lapply(Data.Out,"[[","errors"))
  error_list<-error_tracker(errors=errors_a,filename = "enterdata_structure_errors",error_dir=error_dir,error_list = error_list)
  
  Data.Out<-rbindlist(lapply(Data.Out,"[[","data"),use.names = T)
  setnames(Data.Out,c("ED.Treatment","ED.Int","ED.Rot","ED.Site.ID","ED.Outcome","ED.M.Year","ED.Product.Simple"),
           c("T.Name","IN.Level.Name","R.Level.Name","Site.ID","Out.Code.Joined","Time","P.Product"))
  
  # remove NA rows
  Data.Out<-Data.Out[!(is.na(Site.ID) & is.na(T.Name) & is.na(P.Product) & is.na(Time) & is.na(Out.Code.Joined) & is.na(ED.Mean.T))]
  
  Data.Out[ED.Product.Comp=="NA",ED.Product.Comp:=NA]
  

    # Check times in treatment tab match time tab (note aggregation too)
  errors2<-Data.Out[,list(B.Code,T.Name,IN.Level.Name,R.Level.Name,Time)]
  
  errors2 <-errors2[, .(Time_split = unlist(tstrsplit(Time, "..", fixed = TRUE))),
                            by = .(B.Code, T.Name,IN.Level.Name,R.Level.Name,Time)]
  
  errors2<-merge(errors2,unique(Times.Out[,list(B.Code,Time)])[,Check:=T],all.x=T,by.x=c("B.Code","Time_split"),by.y=c("B.Code","Time"))
  errors2[,Missing:=paste0(Time_split[is.na(Check)],collapse="/"),by=list(B.Code,T.Name,IN.Level.Name,R.Level.Name,Time)]
  errors2<-unique(errors2[Missing!=""][,list(B.Code,T.Name,IN.Level.Name,R.Level.Name,Time,Missing)])
  errors2<-errors2[,list(T.Name=paste(T.Name,collapse = "/")),by=list(B.Code,IN.Level.Name,R.Level.Name,Time,Missing)]
  errors2<-errors2[,list(IN.Level.Name=paste(IN.Level.Name,collapse = "/")),by=list(B.Code,T.Name,R.Level.Name,Time,Missing)]
  errors2<-errors2[,list(R.Level.Name=paste(R.Level.Name,collapse = "/")),by=list(B.Code,T.Name,IN.Level.Name,Time,Missing)]
  
  errors2[,table:="Data.Out"
          ][,issue:="Time in treatments tab does not match Time tab"]
  
  error_list<-error_tracker(errors2,filename = "enterdata_time_errors",error_dir=error_dir,error_list = error_list)
  
  results<-validator(data=Data.Out,
                     numeric_cols = c("ED.Mean.T","ED.Error","ED.Reps"),
                     date_cols = c("ED.Sample.Start","ED.Sample.End"),
                     compulsory_cols = c(T.Name="Site.ID",T.Name="Time",T.Name="Out.Code.Joined",T.Name="ED.Mean.T"),
                     valid_start = valid_start,
                     valid_end = valid_end,
                     tabname="Data.Out",
                     trim_ws = TRUE,
                     do_time=F,
                     do_site = F,
                     convert_NA_strings=T)
  
  errors<-list(results$errors[!(grepl("[.][.]",value) & field=="Time")])
  
  Data.Out<-results$data
  
  # Update Site.ID
  Data.Out[,Site.ID_new:=Site.Out$Site.ID[match(gsub("[.][.] | [.][.]|  [.][.]|   [.][.]","..",trimws(tolower(Data.Out$Site.ID))),tolower(Site.Out$Site.ID_raw))]] 
  Data.Out[is.na(Site.ID_new),Site.ID_new:=Site.ID][,Site.ID:=Site.ID_new][,Site.ID_new:=NULL]
  
     ### 8.0.1) Check keyfields match ######
  # Check that Outcomes match
  error_dat<-check_key(parent=Out.Out,child=Data.Out[!is.na(Out.Code.Joined)],tabname="Data.Out",tabname_parent="Out.Out",keyfield="Out.Code.Joined",collapse_on_code=T)
  errors[["outcome_mismatches"]]<-error_dat

  errors<-c(errors,list(check_key(parent=MT.Out,child=Data.Out[!is.na(ED.Comparison1)][,T.Name:=ED.Comparison1],tabname="Data.Out",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code=T)[,field:="ED.Comparison1"]))
  errors<-c(errors,list(check_key(parent=MT.Out,child=Data.Out[!is.na(ED.Comparison2) & ED.Comparison2!="Not in template"][,T.Name:=ED.Comparison2],tabname_parent="MT.Out",tabname="Data.Out",keyfield="T.Name",collapse_on_code=T)[,field:="ED.Comparison2"]))
  
  # Check that Rotations match
  error_dat<-check_key(parent=Rot.Out,child=Data.Out[!is.na(R.Level.Name)],tabname="Data.Out",tabname_parent="Rot.Out",keyfield="R.Level.Name",collapse_on_code=T)
  errors[["rotation_mismatches"]]<-error_dat
  
  # Check that Site.IDs match
  error_dat<-check_key(parent=Site.Out,child=Data.Out,tabname="Data.Out",tabname_parent="Site.Out",keyfield="Site.ID",collapse_on_code=T)
  errors[["site_mismatches"]]<-error_dat
  
     ### 8.0.2) Other validation checks ######
  # Check end>start date
  errors<-c(errors,list(Data.Out[ED.Sample.Start>ED.Sample.End
                    ][,list(value=paste0(Out.Code.Joined,collapse="/")),by=B.Code
                      ][,table:="Data.Out"
                        ][,field:="Out.Code.Joined"
                          ][,issue:="Start date is greater than end date."
                            ][order(B.Code)]))
  
  # Check for missing data locations
  errors<-c(errors,list(unique(Data.Out[is.na(ED.Data.Loc),list(B.Code,Out.Code.Joined)
               ])[,list(value=paste(Out.Code.Joined,collapse="/")),by=B.Code
                 ][,tabname:="Data.Out"
                   ][,field:="Out.Code.Joined"
                     ][,issue:="Data location is missing"
                       ][order(B.Code)]))
  
  # Check for missing error type
  errors<-c(errors,list(unique(Data.Out[is.na(ED.Error.Type) &!is.na(ED.Error),list(B.Code,Out.Code.Joined)
                            ])[,list(value=paste(Out.Code.Joined,collapse="/")),by=B.Code
                               ][,tabname:="Data.Out"
                                 ][,field:="Out.Code.Joined"
                                   ][,issue:="Error type is missing"
                                     ][order(B.Code)]))
  
  # Check for missing error type
  errors<-c(errors,list(unique(Data.Out[is.na(T.Name) & grepl("Crop Yield",Out.Code.Joined),list(B.Code,IN.Level.Name)
                            ])[,list(value=paste(IN.Level.Name,collapse="/")),by=B.Code
                               ][,tabname:="Data.Out"
                                 ][,field:="IN.Level.Name"
                                   ][,issue:="Crop yield is associated with a intercropping outcome"
                                     ][order(B.Code)]))
  
  # Check component names used
  results<-val_checker(data=Data.Out,
                       tabname="Data.Out",
                       master_codes=master_codes,
                       master_tab="prod_comp",
                       h_field="ED.Product.Comp",
                       h_field_alt="Component",
                       exact=T)
  
  errors<-c(errors,list(Data.Out[!ED.Product.Comp %in% master_codes$prod_comp$Component & !is.na(ED.Product.Comp)
                   ][,list(value=paste(unique(ED.Product.Comp),collapse = "/")),by=B.Code
                     ][,table:="Data.Out"
                       ][,master_table:="prod_comp"
                         ][,field:="ED.Product.Comp"
                           ][,issue:="Product component value used does not match master codes."
                             ][order(B.Code)]))
  
  
  #Check for ratio outcomes that are missing the comparison "control" 
  errors<-c(errors,list(Data.Out[grepl(master_codes$out[TC.Ratio=="Y",paste0(Subindicator,collapse = "|")],Out.Code.Joined) & is.na(ED.Comparison1)
  ][,list(value=paste(unique(Out.Code.Joined),collapse = "/")),by=B.Code
  ][,table:="Data.Out"
  ][,field:="Out.Code.Joined"
  ][,issue:="Outcome derived from T vs C (e.g. LER) does not have comparison specified."
  ][order(B.Code)]))
  
  ## 8.1) Update delims  #####
  
  # MT.Out
  Data.Out[,T.Name:= split_trimws(T.Name[1],delim = ".."),by=T.Name]
  Data.Out[,T.Name:=gsub("[.][.]","...",T.Name)]
  
  # Check that Treatments match
  error_dat<-check_key(parent=MT.Out,child=Data.Out[!is.na(T.Name)],tabname="Data.Out",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code=T)
  errors[["treatment_mismatches"]]<-error_dat

  # Int.Out
  Data.Out[,IN.Level.Name:=gsub("__","***",IN.Level.Name)][,IN.Level.Name:=gsub("[.][.]","...",IN.Level.Name)]
  
  Data.Out[!grepl("[*][*][*]",IN.Level.Name),IN.Level.Name:=gsub("_","***",IN.Level.Name,fixed=T)]
  Data.Out[,IN.Level.Name:= split_trimws(split_trimws(IN.Level.Name[1],delim = "..."),"***"),by=IN.Level.Name]
  
  # Comparison fields for ratio outcomes
  Data.Out[,ED.Comparison1:= split_trimws(ED.Comparison1[1],delim = ".."),by=ED.Comparison1][,ED.Comparison1:=gsub("[.][.]","...",ED.Comparison1)]
  Data.Out[,ED.Comparison2:= split_trimws(ED.Comparison2[1],delim = ".."),by=ED.Comparison2][,ED.Comparison2:=gsub("[.][.]","...",ED.Comparison2)]
  
  ## 8.2) Index products from other tabs #####
  
  Data.Out[,P.Product:=NA]
  
  # MT.Out
  mergedat<-unique(Data.Out[!is.na(T.Name),list(B.Code,T.Name)][,Index:=1:.N])
  mergedat<-merge(mergedat,unique(MT.Out[,list(B.Code,T.Name,P.Product)]),all.x=T,sort=F)[order(Index)]
  Data.Out[!is.na(T.Name),P.Product:=mergedat$P.Product]
  
  # Int.Out
  mergedat<-unique(Data.Out[is.na(T.Name) &!is.na(IN.Level.Name),list(B.Code,IN.Level.Name)][,Index:=1:.N])
  mergedat<-merge(mergedat,unique(Int.Out[,list(B.Code,IN.Level.Name2,IN.Prods.All)]),by.x=c("B.Code","IN.Level.Name"),by.y=c("B.Code","IN.Level.Name2"),all.x=T,sort=F)[order(Index)]
  Data.Out[is.na(T.Name) &!is.na(IN.Level.Name),P.Product:=gsub("[.][.]","-",mergedat$IN.Prods.All)]
 
  # Rot.Out
  mergedat<-unique(Data.Out[is.na(T.Name) &is.na(IN.Level.Name) &!is.na(R.Level.Name),list(B.Code,R.Level.Name)][,Index:=1:.N])
  mergedat<-merge(mergedat,unique(Rot.Out[,list(B.Code,R.Level.Name,R.All.Products)]),all.x=T,sort=F)[order(Index)]
  Data.Out[is.na(T.Name) &is.na(IN.Level.Name) &!is.na(R.Level.Name),P.Product:=mergedat$R.All.Products]
  
  # Check for missing required fields that don't work with standard validator
  error_dat<-unique(rbind(
    Data.Out[is.na(T.Name) & is.na(IN.Level.Name) & is.na(R.Level.Name)
    ][,list(value=paste0(unique(Out.Code.Joined),collapse="/")),by=B.Code
    ][,field:="Out.Code.Joined"
    ][,issue:="Compulsory Treatment, Intercropping and Rotation fields are all blank (at least one value must be present)."],
    Data.Out[is.na(P.Product) & !(is.na(IN.Level.Name) & is.na(T.Name))
    ][,list(value=paste0(unique(IN.Level.Name),collapse="/")),by=B.Code
    ][,field:="IN.Level.Name/T.Name"
    ][,issue:="Compulsory Product field is blank."]
  ))[,list(value=paste(value,collapse="/")),by=list(B.Code,field,issue)
  ][,table:="Data.Out"
  ][order(B.Code)]
  
  errors[["product_mistmatches"]]<-error_dat
  
  ## 8.3) Harmonization #####
  h_tasks1<-harmonizer(data=Data.Out, 
                       master_codes,
                       master_tab="lookup_levels",
                       h_table="Data.Out", 
                       h_field="ED.Error.Type",
                       h_table_alt="Data.Out", 
                       h_field_alt="Error.Type")$h_tasks[,issue:="Non-standard error value used."]
  
  ## 8.4) Add controls for outcomes that are ratios #####
  Data.Out[,N:=1:.N]
  Data.R<-Data.Out[!is.na(ED.Comparison1) & !is.na(T.Name),c("T.Name","ED.Comparison1","B.Code","N","Out.Code.Joined")]
  
  # Missing practices not the ED.Table as Treatments - these need adding to the Data table
  Data.R[,Missing:=match(Data.R[,paste(B.Code,ED.Comparison1)],Data.Out[,paste(B.Code,T.Name)])]
  M.Treat<-Data.R[is.na(Missing),ED.Comparison1]
  
  Data.R<-Data.Out[Data.R[is.na(Missing),N]
  ][,T.Name:=M.Treat
  ][,ED.Mean.T:=NA
  ][,ED.Error:=NA
  ][,ED.Error.Type:=NA
  ][,ED.Comparison1:=NA
  ][,ED.Ratio.Control:=T]
  
  error_dat<-Data.R[!T.Name %in% MT.Out[,T.Name] & !grepl("__",T.Name),list(value=paste0(unique(T.Name),collapse="/")),by=B.Code
  ][,table:="Data.Out"
  ][,field:="ED.Comparison"
  ][,issue:="Treatment does not match treatment tab"]
  
  errors<-c(errors,list(error_dat))
  
  Data.Out[,ED.Ratio.Control:=F]
  
  Data.Out<-rbind(Data.Out,Data.R)
  
  rm(Data.R,M.Treat)
  
  # These should be NA values for controls of ratio outcomes
  Ratio.Controls<-unique(Data.Out[is.na(ED.Mean.T) & !grepl("Efficiency|ARE",Out.Code.Joined),.(B.Code,Out.Code.Joined)])
  
  Data.Out[,N:=1:nrow(Data.Out)]
  
  ## 8.5) Merge data from linked tables ####
  n_rows<-Data.Out[,.N]
    ### 8.5.2) Add MT.Out ######
    Data.Out<-merge(Data.Out,unique(MT.Out[,!c("P.Product","Base.Codes")]),by=c("B.Code","T.Name"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    ### 8.5.3) Add Int.Out ######
    Data.Out<-merge(Data.Out,Int.Out[,!"IN.Level.Name"],by.x=c("B.Code","IN.Level.Name"),by.y=c("B.Code","IN.Level.Name2"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
      #### 8.5.3.1) Check for non-matches #######
      error_dat<-unique(Data.Out[!is.na(IN.Level.Name) & is.na(IN.Comp.N),.(value=paste(unique(IN.Level.Name),collapse="/")),by=B.Code])
      error_dat<-error_dat[!B.Code %in% unique(c(error_list$intercrop_other_errors[grepl("Non-match btw intercropping and make treatment|Delimiter|Component(s) of aggregated treatment name used in intercropping ta",issue),B.Code],error_list$intercropping_structure_errors$B.Code))
                           ][,table:="Data.Out/Int.Out"
                             ][,field:="IN.Level.Name"
                               ][,issue:="Non-match between intercrop in data entry vs intercropping tab (make sure practice is specified in Int tab)."]
      
      errors[["intercrop_mismatches"]]<-error_dat
    
    # Explore residual non-matches
    if(F){
      i<-1
      error_dat[i]
      Int.Out[B.Code == error_dat$B.Code[i],.(IN.Level.Name,IN.Level.Name2,IN.Comp1,IN.Comp2,IN.Comp3)]
    }
    
    ### 8.5.4) Add Rot.Out ######
    Data.Out<-merge(Data.Out,Rot.Out,by=c("B.Code","R.Level.Name"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
      #### 8.5.4.1) Check for missing rotation codes #######
    # Check for studies where Rot.Out is always NA for a ED.Rot treatment - there should be Rotation code but it is missing
    error_dat<-unique(Data.Out[!is.na(R.Level.Name),list(Len=sum(!is.na(R.Code))),by=c("R.Level.Name","B.Code","R.Code")][Len==0])
    error_dat<-error_dat[,.(value=paste(R.Level.Name,collapse = "/")),by=B.Code
    ][,table:="Data.Out"
    ][,field:="R.Level.Name"
    ][,issue:="Time sequence does not contain a rotation code, if the treatment name suggests a rotation should be present then check, could be raw data or ingestion issue."]
    
    
    errors<-c(errors,list(error_dat))
    
    ### 8.5.5) Add Publication  ######
    Data.Out<-merge(Data.Out,Pub.Out,by=c("B.Code"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    ### 8.5.6) Add Outcomes  ####
    Data.Out<-merge(Data.Out,unique(Out.Out),by=c("B.Code","Out.Code.Joined"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    # Check  Crop Residue->Biomass outcomes based on crops or components reported
    BiomassCrops<-master_codes$prod[Product.Subtype %in% c("Fodders","Cover Crop") & Component=="Biomass",unique(Product.Simple)]
    
    # Find crop residue outcomes that are for biomass crops (fodders and cover crops)
    Data.Out[Out.Subind == "Crop Residue Yield",Is.Biomass.Prod:=all(unlist(strsplit(P.Product,"[.][.]")) %in% BiomassCrops),by=P.Product]
    
    # Check these are not crop residues
    unique(Data.Out[Is.Biomass.Prod==T,.(Out.Subind,P.Product,ED.Product.Comp)])
    error_dat<-Data.Out[Is.Biomass.Prod==T,.(value=paste(unique(P.Product),collapse="/")),by=B.Code
    ][,table:="Data.Out"
    ][,field:="P.Product"
    ][,issue:="Biomass crops that have a Crop Residue Yield outcome (should be Biomass Yield?)."]
    
    errors<-c(errors,list(error_dat))
    
    # Where stover is listed as the component change all outcomes from biomass to crop residue
    Data.Out[!is.na(P.Product) & 
               !P.Product %in% master_codes$prod[Product.Subtype %in% c("Fodders","Cover Crops"),Product.Simple]  & 
               grepl("stover|residue",ED.Product.Comp,ignore.case = T) &
               Out.Subind=="Biomass Yield",Out.Code.Joined:=gsub("Biomass Yield","Crop Residue Yield",Out.Code.Joined)]
    
    Data.Out[!is.na(P.Product) & 
               !P.Product %in% master_codes$prod[Product.Subtype %in% c("Fodders","Cover Crops"),Product.Simple]  & 
               grepl("stover|residue",ED.Product.Comp,ignore.case = T) &
               Out.Subind=="Biomass Yield",Out.Subind:="Crop Residue Yield"]
    
    # Where natural fallow and crop residue yield are listed, change outcome to biomass yield
    Data.Out[Out.Subind=="Crop Residue Yield" & P.Product %in% c("Fallow","Natural Fallow"),Out.Subind:="Biomass Yield"]
    
    # For fodders or cover crops remove stover
    Data.Out[!is.na(P.Product) & 
               grepl("stover|residue",ED.Product.Comp,ignore.case = T) &
               Out.Subind=="Biomass Yield",ED.Product.Comp:="Biomass/Fodder"]
    
    error_dat<-Data.Out[!is.na(P.Product) & grepl("stover|residue",ED.Product.Comp,ignore.case = T) & Out.Subind=="Biomass Yield",.(value=paste(unique(P.Product),collapse="/")),by=B.Code
    ][,table:="Data.Out"
    ][,field:="P.Product"
    ][,issue:="Biomass yield associated with stover or residue crop component."]
    
    errors<-c(errors,list(error_dat))
    
    Data.Out[,Is.Biomass.Prod:=NULL]
    
      #### 8.5.6.1) Merge in outcome codes  #######
    mergedat<-setnames(unique(master_codes$out[,.(Subindicator,Code)]),c("Subindicator","Code"),c("Out.Subind","Out.Code"))
    Data.Out<-merge(Data.Out,mergedat,by=c("Out.Subind"),all.x=T,sort=F)
    
    unique(Data.Out[is.na(Out.Code) & !B.Code %in% errors$outcome_mismatches$B.Code,.(B.Code,Out.Code.Joined,Out.Subind)])
    
    # Check for crop yield associated with multiple products
    error_dat<-unique(Data.Out[grepl("-",P.Product) & is.na(T.Name) & Out.Subind %in% c("Crop Yield"),.(B.Code,IN.Level.Name,R.Level.Name,P.Product,Out.Code.Joined)])
    error_dat[,.(value=paste(unique(P.Product),collapse="/")),by=B.Code][,table:="Data.Out"][,field:="P.Product"][,issue:="Crop yield outcome associated with multiple products."]
    
    errors<-c(errors,list(error_dat))
    
    ### 8.5.7) Add product codes ######
      #### 8.5.7.1) Check product components #######
      
      # Update okra fruits to okra pods
      Data.Out[P.Product %in% c("Okra","Arabica","Robusta","Cocoa") & ED.Product.Comp=="Fruit (Unspecified)",ED.Product.Comp:="Pods"]
      
      # Update production component if legume or cereal and outcome is crop yield
      Data.Out[P.Product %in% master_codes$prod[Product.Subtype %in% c("Cereals","Legumes"),Product.Simple] & Out.Subind=="Crop Yield",ED.Product.Comp:="Grain/Seed"]
      Data.Out[P.Product %in% c("Coffee") & Out.Subind=="Crop Yield",ED.Product.Comp:="Grain/Seed"]
      
      # Add component level 1 to Data.Out (used to match component + product to EU list)
      comp_codes<-master_codes$prod_comp
      prod_master<-master_codes$prod
      
      X<-Data.Out[,c("B.Code","P.Product","ED.Product.Comp","Out.Subind")
      ][,EU.Comp.L1:=comp_codes$Comp.Level1[match(ED.Product.Comp,comp_codes$Component)]]
      
      # Add code field based on product x component combined to ERA data and product (EU) MASTERCODES
      X[,Code:=paste(P.Product,EU.Comp.L1)]
      prod_master[,Code:=paste(Product.Simple,Component)]
      
      # Check Yield Outcomes with no product component specified:
      error_dat<-unique(X[is.na(ED.Product.Comp) & (grepl("Yield",Out.Subind)|grepl("Efficiency",Out.Subind)) & !grepl("[.][.]",P.Product)])
      error_dat<-error_dat[!is.na(P.Product),list(P.Product=paste(unique(P.Product),collapse="/"),Out.Subind=paste(unique(Out.Subind),collapse = "/")),by=B.Code]
      
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
      
      #### 8.5.7.2) Merge using product x component code  #######
      
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
      dat<-unique(Data.Out[,.(P.Product,ED.Product.Comp.L1)])
      
      master_prods<-master_codes$prod$Product.Simple
      master_prod_codes<-master_codes$prod[,paste(Product.Simple,Component)]
      prod_tab<-master_codes$prod
      tree_tab<-master_codes$trees
      
      mergedat<-rbindlist(pblapply(1:nrow(dat),FUN=function(i){
        merge_prods(P.Product=dat$P.Product[i],
                    Component=dat$ED.Product.Comp.L1[i],
                    master_prods=master_prods,
                    master_prod_codes=master_prod_codes,
                    prod_tab=prod_tab)[,P.Product:=dat$P.Product[i]
                    ][,ED.Product.Comp.L1:=dat$ED.Product.Comp.L1[i]]
      }))
      
      Data.Out<-merge(Data.Out,mergedat,by=c("P.Product","ED.Product.Comp.L1"),all.x=T,sort=F)
      
        ##### I THINK THIS IS NO LONGER RELEVANT - 3) Add in Agroforestry Trees ####
        if(F){
          NX<-is.na(X[,EU])  & Data.Out[,ED.Product.Simple] %in% TreeCodes$Product.Simple
          X[NX,Product.Type :="Plant Product"]
          X[NX,Product.Subtype :="Agroforestry Tree"]
          X[NX,Product := Data.Out[NX,ED.Product.Simple]]
          X[NX,Product.Simple := Product]
          X[NX,Component  := Data.Out[NX,ED.Product.Comp.L1]]
          X[NX,Latin.Name := Product]
          
          X[NX,EU:=TreeCodes[match(Data.Out[NX,ED.Product.Simple],TreeCodes[,Product.Simple]),EU]]
          rm(NX)
        }
        ##### I THINK THIS IS NO LONGER RELEVANT -  4) Repeat 1-3 but for aggregated products (SLOW consider parallel) ####
        if(F){
          N<-grep("[.][.]",Data.Out$ED.Product.Simple)
          Z<-Data.Out[N,c("ED.Product.Simple","ED.Product.Comp","ED.Product.Code")]
          
          Z<-rbindlist(pblapply(1:nrow(Z),FUN=function(i){
            X1<-Z[i]
            Product<-unlist(strsplit(X1$ED.Product.Simple,"[.][.]"))
            Comp<-X1$ED.Product.Comp
            
            Code<-paste(Product,Comp)
            N<-match(Code,EUCodes$Code)
            
            X2<-EUCodes[N,c("EU","Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name" )]
            
            if(sum(is.na(X2$EU))>0){
              N<-which(is.na(X2$EU))
              N1<-match(Product[N],EUCodes$Product.Simple)
              Y<-EUCodes[N1,c("EU","Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name" )
              ]
              
              Y[,EU2:=lapply(strsplit(EU,"[.]"),"[",1)
              ][,EU2.Match:=EU2 %in% EUCodes$EU
              ][EU2.Match!=F,EU:=EU2
              ][,Component:=NA
              ][,EU2:=NULL
              ][,EU2.Match:=NULL]
              
              
              if(sum(is.na(Y$EU))>0){
                N<-which(is.na(Y$EU))
                N1<-match(Product[N],TreeCodes[,Product.Simple])
                if(sum(!is.na(N1))>0){
                  
                  Y<-rbind(Y[-N[!is.na(N1)]],data.table(EU=TreeCodes[N1,EU],Product.Type="Plant Product",Product.Subtype = "Agroforestry Tree",Product = Product[N],
                                                        Product.Simple=Product[N],Component = Comp, Latin.Name = Product[N]))
                }
                
              }
              X2<-rbind(X2[!is.na(EU)],Y) 
            }
            
            X2<-data.table(t(apply(X2,2,FUN=function(A){
              paste(unique(A[!(is.na(A)|A=="NA")]),collapse = "**")
            })))
            
            X2
            
          }))
          
          Z[,N:=grep("[.][.]",Data.Out$ED.Product.Simple)]
        }
      #### 8.5.7.3) Product Validation ####
      
      # Check NA & "No Product Specified" against outcome (should not be a productivity outcome)
      error_dat<-unique(Data.Out[is.na(P.Product) | P.Product == "No Product Specified",c("P.Product","B.Code","T.Name","IN.Level.Name","R.Level.Name","Out.Subind")]
                        [grep("Yield",Out.Subind)])
      
      # Often these errors results from mismatches and structural errors in others
      error_dat[is.na(IN.Level.Name) & !B.Code %in% c("JO0088","JO0002","SP0038","SA0059.3"),table(B.Code)]
      
    ### 8.5.8) Add Experimental Design  ######
    Data.Out<-merge(Data.Out,ExpD.Out,by=c("B.Code"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    ### 8.5.9) Update replicates ######
    # Reps specified in EnterData tab > Rotation tab > Intercropping tab > Treatment tab
    Data.Out[,N:=1:.N
             ][,Final.Reps:=if(!is.na(ED.Reps)){ED.Reps}else{
               if(!is.na(R.Reps)){R.Reps}else{
                 if(!is.na(IN.Reps)){IN.Reps}else
                   if(!is.na(T.Reps)){T.Reps}}},by=N
               ][,N:=NULL
                 ][,Final.Reps:=as.numeric(Final.Reps)]
    
    # If there are any reps at all listed for the experiment substitute these in for missing values
    Data.Out[,any_reps:=round(mean(Final.Reps,na.rm=T),0),by=B.Code]
    Data.Out[is.na(Final.Reps),Final.Reps:=any_reps][,any_reps:=NULL]
    
    error_dat<-unique(Data.Out[is.na(Final.Reps) & 
                                 !B.Code %in% c(errors$treatment_mismatches$B.Code,
                                                errors$intercrop_mismatches$B.Code,
                                                errors$rotation_mismatches$B.Code),c("B.Code","T.Name","IN.Level.Name","R.Level.Name","Final.Reps","ED.Reps","R.Reps","IN.Reps","T.Reps")])
    
    error_dat<-data.table(B.Code=error_dat[,unique(B.Code)])[,value:=NA][,table:="Data.Out"][,field:="T.Reps/Int.Reps/R.Reps"][,issue:="No replicates recorded."]
    
    errors<-c(errors,list(error_dat))
    
    ### 8.5.10) Add Site  ######
    Data.Out<-merge(Data.Out,unique(Site.Out[,!c("Site.ID_raw","check")]),by=c("B.Code","Site.ID"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    
    # Make Sure of match between Data.Out and Site.Out
    (error_dat<-unique(Data.Out[is.na(Country),.(B.Code,Site.ID)])[!B.Code %in% errors$site_mismatches$B.Code])
      
    ### 8.5.11) Update residue codes ######
      #### 8.5.11.1) IN.Residue.Code > T.Residue.Code > R.Residue.Codes.All ######
      # Previously was T.Residue.Code > IN.Residue.Code but this caused issues where the intercrop received both crop and agroforesty residues, for example
      # In that case the intercrop would only get the residue code of the component for which the outcome was being reported.
      Data.Out[,N:=1:.N][,Final.Residue.Code:=if(!is.na(IN.Residue.Code)){IN.Residue.Code}else{
        if(!is.na(T.Residue.Code)){T.Residue.Code}else{if(!is.na(R.Residue.Codes.All)){R.Residue.Codes.All}
        }},by=N]
      
      #### 8.5.11.2) If observation is a component of a rotation treatment update residues from the Rot.Seq table #######
      ##### ***ISSUE*** WHAT ABOUT ROTATIONS WITH BOTH PHASES? IS THIS COVERED IN ROT.OUT SECTION (NEED TO DUPLICATE PHASES?) #####
      
      mergedat<-Rot.Seq[,.(B.Code,R.Treatment,R.Level.Name,Time,R.Residues.Codes)][,code:=paste(B.Code,R.Treatment,R.Level.Name,Time)]
      Data.Out[!is.na(R.Level.Name) & !is.na(IN.Level.Name) & is.na(T.Name) & !grepl("[.][.]",Time),Temp.Code.Int:=paste(B.Code,IN.Level.Name,R.Level.Name,Time)]
      Data.Out[!is.na(R.Level.Name) & !is.na(T.Name) & ! grepl("[.][.]",Time),Temp.Code.Treat:=paste(B.Code,T.Name,R.Level.Name,Time)]
      
      # Intercrop present
      X<-match(Data.Out$Temp.Code.Int,mergedat$code)
      Data.Out[!is.na(X),Final.Residue.Code:=Rot.Seq$R.Residues.Codes[X[!is.na(X)]]]
      Data.Out[!is.na(X),R.Code:=Rot.Seq$R.Code[X[!is.na(X)]]]
      
      # No Intercrop present
      X<-match(Data.Out$Temp.Code.Treat,mergedat$code)
      Data.Out[!is.na(X),Final.Residue.Code:=Rot.Seq$R.Residues.Codes[X[!is.na(X)]]]
      Data.Out[!is.na(X),R.Code:=Rot.Seq$R.Code[X[!is.na(X)]]]
      
      # Tidy Up
      Data.Out[,c("Temp.Code.Int","Temp.Code.Treat"):=NULL]
      
      #### 8.5.11.3) Check results are sensible #######
      error_dat<-Data.Out[!grepl("[.][.]",Time) & !T.Residue.Prev=="Unspecified",.(B.Code,Time,T.Residue.Prev,T.Residue.Code,T.Name,IN.Residue.Fate,IN.Level.Name,IN.Residue.Code,R.Residue.Codes.All,R.Level.Name,Final.Residue.Code)
      ][(!is.na(T.Residue.Code) | !is.na(IN.Residue.Code) | !is.na(R.Residue.Codes.All)) & is.na(Final.Residue.Code) & 
          !B.Code %in% c(errors$rotation_mismatches$B.Code,errors$intercrop_mismatches$B.Code,errors$treatment_mismatches$B.Code,unlist(strsplit(error_list$prod_other_errors$B.Code,"/")),error_list$rot_other_errors[grep("Crop name merged to Rot.Seq from MT.Out tab has no match in mastercodes pr",issue),B.Code])]
      
      if(F){
      i<-1
      error_dat[i]
      Rot.Seq[B.Code==error_dat$B.Code[i] & R.Level.Name==error_dat$R.Level.Name[i]]
      Rot.Out[B.Code==error_dat$B.Code[i] & R.Level.Name==error_dat$R.Level.Name[i]]
      }
      
      error_dat<-error_dat[,.(value=paste0(unique(R.Level.Name),collapse="/")),by=B.Code][,table:="Rot.Out vs MT.Out"
      ][,field:="R.Level.Name"
      ][,issue:="Mismatch in residue management between time seq and treatments tab (often treatments tab indicates residue management but time seq tab states Unspecified?)."]
      
      errors<-c(errors,list(error_dat))
      
      # Potential Issue with IN.Residue Code
      error_dat<-Data.Out[!grepl("[.][.]",Time) & !T.Residue.Prev=="Unspecified" & !is.na(IN.Residue.Code) & is.na(Final.Residue.Code),.(B.Code,Time,T.Residue.Prev,T.Residue.Code,T.Name,IN.Residue.Fate,IN.Level.Name,IN.Residue.Code,R.Residue.Codes.All,R.Level.Name,Final.Residue.Code)
      ][!is.na(T.Residue.Code) | !is.na(IN.Residue.Code) | !is.na(R.Residue.Codes.All)
      ][!B.Code %in% c(errors$rotation_mismatches$B.Code,errors$intercrop_mismatches$B.Code,errors$treatment_mismatches$B.Code,unlist(strsplit(error_list$prod_other_errors$B.Code,"/")),error_list$rot_other_errors[grep("Crop name merged to Rot.Seq from MT.Out tab has no match in mastercodes pr",issue),B.Code])]
      
      
      error_dat<-error_dat[,.(value=paste0(unique(IN.Level.Name),collapse="/")),by=B.Code][,table:="Int.Out vs MT.Out"
      ][,field:="IN.Level.Name"
      ][,issue:="Mismatch in residue management between intercropping and treatments tab (often treatments tab indicates residue management but intercropping tab states Unspecified?)."]
      
      errors<-c(errors,list(error_dat))
      
      #### 8.5.11.4) Replace intercropping "***" or aggregation "..." delim in Final.Residue.Code #######
      
      # Intercrops
      PC1<-PracticeCodes[Practice %in% c("Agroforestry Pruning"),Code]
      PC2<-PracticeCodes[Practice %in% c("Mulch","Crop Residue","Crop Residue Incorporation"),Code] 
      
      # This function changes a17/a16 agroforestry n/non-n to a15 agroforestry unspecified
      Recode<-function(PC1,PC2,Final.Codes){
        
        X<-Final.Codes %in% PC1
        
        if(sum(X)>0){
          Final.Codes[X]<-gsub("a17","a15",Final.Codes[X])
          Final.Codes[X]<-gsub("a16","a15",Final.Codes[X])
        }
        
        X<-Final.Codes %in% PC2
        
        if(sum(X)>0){
          Final.Codes[X]<-unlist(tstrsplit(Final.Codes[X],"[.]",keep=1))
        }
        Final.Codes
        
      }
      
      FunX<-function(A){
        A<-unique(unlist(strsplit(unlist(strsplit(A,"[*][*][*]")),"-")))
        A<-unique(Recode(PC1,PC2,A))
        A<-A[!is.na(A)]
        # Cannot have h and b codes present, remove h codes
        if(sum(grepl("h",A))>0 & length(A)>1){
          A<-A[!grepl("h",A)]
        }
        paste(A[order(A)],collapse = "-")
      }
      
      Data.Out[grep("[*][*][*]",Final.Residue.Code),Final.Residue.Code:=FunX(Final.Residue.Code),by=N]
      
      
      FunX<-function(A){
        A<-unlist(strsplit(unlist(strsplit(A,"[.][.][.]")),"-"))
        A<-Recode(PC1,PC2,A)
        A<-table(A[A!="NA"])/length(A)
        # Apply majority rule
        A<-names(A[A>=Threshold])
        # Cannot have h-code and b-code
        if(sum(grepl("h",A))>0 & length(A)>1){
          A<-A[!grepl("h",A)]
        }
        if(length(A)==0){NA}else{paste(A[order(A)],collapse="-")}
      }
      
      Data.Out[grep("[.][.][.]",Final.Residue.Code),Final.Residue.Code:=FunX(Final.Residue.Code),by=N]
      
      rm(FunX,PC1,PC2,Recode)
      
      # Explore results
      if(F){
        results<-Data.Out[!grepl("[.][.]",Time) & !T.Residue.Prev=="Unspecified",.(B.Code,Time,T.Residue.Prev,T.Residue.Code,T.Name,IN.Residue.Fate,IN.Level.Name,IN.Residue.Code,R.Residue.Codes.All,R.Level.Name,Final.Residue.Code)
        ][(!is.na(T.Residue.Code) | !is.na(IN.Residue.Code) | !is.na(R.Residue.Codes.All)) & 
            !is.na(R.Level.Name) &
            !B.Code %in% c(errors$rotation_mismatches$B.Code,
                           errors$intercrop_mismatches$B.Code,
                           errors$treatment_mismatches$B.Code,
                           unlist(strsplit(error_list$prod_other_errors$B.Code,"/")),
                           error_list$rot_other_errors[grep("Crop name merged to Rot.Seq from MT.Out tab has no match in mastercodes pr",issue),B.Code])]
        
        i<-1
        results[i]
        Rot.Seq[B.Code==results$B.Code[i] & R.Level.Name==results$R.Level.Name[i]]
        Rot.Out[B.Code==results$B.Code[i] & R.Level.Name==results$R.Level.Name[i]]
      }
      
    ### 8.4.12) Add climate & time ######
      Data.Out<-merge(Data.Out,unique(Times.Out[,!c("seq_n","check")]),by=c("B.Code","Time"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      
      unique(Times.Clim[!is.na(Site.ID) & !is.na(Time)])[,N:=.N,by=.(B.Code,Site.ID,Time)][N>1]
      
      Data.Out<-merge(Data.Out,unique(Times.Clim[!is.na(Site.ID) & !is.na(Time)]),by=c("B.Code","Site.ID","Time"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      #### 8.4.12.1) Update aggregated times #######
      Data.Out[!is.na(Time.Season),c("Time.Season.Start","Time.Season.End"):=Time.Season]
      
      # Create function to split aggregate times and extract start and end for years and seasons
      agg_time_fun<-function(value,b_code,parent_tab,n_seasons,delim){
        values<-unlist(strsplit(value,delim,fixed=T))
        
        vals<-parent_tab[B.Code==b_code & Time %in% values]
        max_seq_n<-vals[,max(seq_n)]

        if(is.infinite(max_seq_n)){
          max_seq_n<-NA
          seq_n<-NA
          yr_season_ratio<-NA
        }else{
          seq_n<-max_seq_n-vals[,min(seq_n)]
          yr_season_ratio<-parent_tab[B.Code==b_code,(max(Time.Start.Year)-min(Time.Start.Year)+1)/.N]
        }
        
        if(nrow(vals)==length(values)){
        if(any(is.na(vals$Time.Start.Year))){
         seq_years<-NA 
        }else{
        seq_years<-vals[,length(min(Time.Start.Year):max(Time.Start.Year))]
        }
        
        min_val<-vals[Time==values[1],.(Time.Start.Year,Time.Season)]
        max_val<-vals[Time==tail(values,1),.(Time.End.Year,Time.Season)]
        values<-cbind(min_val,max_val)  
        colnames(values)<-c("t_start_yr","t_start_s","t_end_yr","t_end_s")
        values$Time<-value
        values$B.Code<-b_code
        values$n_seasons<-n_seasons
        values$max_seq_n<-max_seq_n
        values$seq_yrs<-seq_years
        values$yr_season_ratio<-yr_season_ratio
        values$seq_n<-seq_n
        return(values)
        }else{
          cat("No match to parent table",b_code,"-",paste0(values[!values %in% vals$Time],collapse="/"),"\n")
          return(NULL)
        }

      }
      
      # Subset to aggregated times
      agg_times<-unique(Data.Out[grepl("[.][.]",Time),.(Time,Time.Start.Year,Time.End.Year,Time.Season,B.Code,Site.Rain.Seasons)])

      # apply function
      agg_times<-rbindlist(lapply(1:nrow(agg_times),FUN=function(i){
        agg_time_fun(value=agg_times$Time[i],b_code=agg_times$B.Code[i],n_seasons=agg_times$Site.Rain.Seasons[i],parent_tab = Times.Out,delim="..")
      }))
      
      agg_times[yr_season_ratio==1,Duration:=seq_n+0.5]
      agg_times[yr_season_ratio>=0.5 & yr_season_ratio<1,Duration:=seq_n*0.5+0.5]
      agg_times[yr_season_ratio>=0.25 & yr_season_ratio<0.4 & !n_seasons %in% c(1,2),Duration:=round(seq_n*1/3+1/3,1)]
      agg_times[is.na(Duration) & n_seasons==2,Duration:=seq_n*0.5+0.5]
      agg_times[is.na(Duration) & seq_yrs==seq_n+1,Duration:=seq_n+0.5]
      agg_times[is.na(Duration) & grepl("[.]1[.][.]|[.]1$|LR|Year 1[.]|year 1[.]",Time) & grepl("[.]2[.][.]|[.]2$|SR|Year 2$|year 2$",Time) & !grepl(".[3]$|[.]3[.][.]",Time),Duration:=seq_n*0.5+0.5]
      agg_times[is.na(Duration) & !is.na(yr_season_ratio),Duration:=seq_n*0.5+0.5]
      setnames(agg_times,"Duration","Time.Agg.Duration")
      
      # merge data back to Data.Out and replace values
      Data.Out<-merge(Data.Out,unique(agg_times[,.(Time,B.Code,t_start_yr,t_end_yr,t_start_s,t_end_s,Time.Agg.Duration)]),by=c("Time","B.Code"),sort=F,all.x=T)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      
      Data.Out[grep("[.][.]",Time),
               c("Time.Start.Year","Time.End.Year","Time.Season.Start","Time.Season.End"):=.(t_start_yr,t_end_yr,t_start_s,t_end_s)]  
      
      Data.Out[,c("t_start_yr","t_end_yr","t_start_s","t_end_s","Time.Season"):=NULL]
      
      #### 8.4.12.2) Estimate experimental duration ####
      Data.Out[,exp_start:=T.Start.Year
               ][,exp_start_s:=T.Start.Season
               ][!is.na(IN.Level.Name) & !is.na(IN.Start.Year),exp_start:=IN.Start.Year    
                 ][!is.na(IN.Level.Name) & !is.na(IN.Start.Year),exp_start_s:=IN.Start.Season
                 ][!is.na(R.Level.Name) & !is.na(R.Start.Year),exp_start:=R.Start.Year
                 ][!is.na(R.Level.Name) & !is.na(R.Start.Year),exp_start_s:=R.Start.Season]
      
      time_seq<-Times.Out[,.(y_max=max(Time.Start.Year),y_min=min(Time.Start.Year),n_seasons=.N,t_start=Time[1],unique_seasons=length(unique(na.omit(Time.Season))),
                             all_years=if(!any(is.na(Time.Start.Year))){all((min(Time.Start.Year):max(Time.Start.Year)) %in% Time.Start.Year)}else{NA}),by=B.Code
                          ][,years:=y_max-y_min+1
                            ][,yr_season_ratio:=years/n_seasons]
      
      mergedat<-copy(Data.Out)
      mergedat<-merge(mergedat,time_seq,by="B.Code",all.x=T)
      
      # Substitute missing start year for first year in Time.Out tab
      mergedat[is.na(exp_start) & !grepl(9999,y_min),exp_start:=y_min]
      
      # Are more years present in experiment that time seq?
      mergedat[,y_flag:=F][exp_start<y_min,y_flag:=T]
      
      x<-unique(mergedat[y_flag==F,.(B.Code,Time,Time.Start.Year,Time.Season.Start,Time.Agg.Duration,yr_season_ratio,t_start,y_min,unique_seasons,n_seasons,all_years,exp_start)])
      x[,Time2:=tail(unlist(strsplit(Time,"[.][.]")),1),by=Time]
      
      # If observation and start time are the same, then it must be first season of experiment
      x[Time2==t_start,Exp.Duration:=0.5]
      
      # If the time period name is in YYYY format assume there is only 1 season
      x[Time2 %in% 1930:2024 & t_start %in% 1930:2024 & is.na(Exp.Duration),Exp.Duration:=as.numeric(Time2)-as.numeric(t_start)+0.5]

      # Work out difference in seq_n between t_start and Time2
      x<-merge(x,Times.Out[,.(B.Code,Time,seq_n,Time.Season)],by.x=c("B.Code","t_start"),by.y=c("B.Code","Time"),all.x=T,sort=F)
      setnames(x,c("seq_n","Time.Season"),c("seq_n_start","seq_season_start"))
      x<-merge(x,Times.Out[,.(B.Code,Time,seq_n)],by.x=c("B.Code","Time2"),by.y=c("B.Code","Time"),all.x=T,sort=F)
      x[,seq_diff:=seq_n-seq_n_start]
      
      # If seasons are the same then difference should be the years +0.5
      x[Time.Season.Start==seq_season_start & is.na(Exp.Duration) & (Time.Start.Year!=y_min),Exp.Duration:=Time.Start.Year-y_min+0.5]
      
      # If there 50% more seasons than years exactly, let's assume there are 2 seasons
      x[is.na(Exp.Duration) & (yr_season_ratio==0.5|unique_seasons==2) & all_years,Exp.Duration:=seq_diff*0.5+0.5]
      
      # Is a third season present?
      x[,s3:=F][,s3:=any(grepl("[.]3[.][.]|[.]3$",Time) & !grepl("9999",Time))|(yr_season_ratio>0.31 & yr_season_ratio<0.36) & grep("SA",B.Code),by=B.Code]
      x[s3==T & is.na(Exp.Duration) & all_years,Exp.Duration:=seq_diff*0.33+0.33]
      
      # Identify 2 seasons
      a<-"[.]2$|S$|SR$|W$|A$|Winter$|-ST|Season 1"
      b<-"[.]1$|L$|LR$|S$|B$|Summer$|-2ND|Season 2"
      
      x[,s2:=(any(grepl(a,Time2)) & (any(grepl(b,Time2))|any(grepl(b,t_start))))|(any(grepl(b,Time2)) & (any(grepl(a,Time2))|any(grepl(a,t_start)))),by=B.Code
        ][,s2:=s2 & grepl(paste0(a,"|",b),t_start) & !grepl("9999",Time2)]
      x[is.na(Exp.Duration) & s2==T & all_years,Exp.Duration:=seq_diff*0.5+0.5]
      
      # Identify 1 season
      x[is.na(Exp.Duration) & (yr_season_ratio==1|unique_seasons==0) & all_years,Exp.Duration:=seq_diff+0.5]
      
      a<-"year1$|year 1$"
      b<-"year2$|year 2$"
      
      x[grepl(a,t_start,ignore.case = T) & grepl(b,Time2,ignore.case = T),Exp.Duration:=1.5]
      
      # Try identifying 2 seasons from season names
      a<-"Major|Rainy|Winter|summer"
      b<-"Minor|Dry|Summer|winter"
      x[is.na(Exp.Duration) ,s2:=(any(grepl(a,Time.Season.Start)) & (any(grepl(b,Time.Season.Start))|any(grepl(b,seq_season_start))))|(any(grepl(b,Time.Season.Start)) & (any(grepl(a,Time.Season.Start))|any(grepl(a,seq_season_start)))),by=B.Code
        ][is.na(Exp.Duration) ,s2:=s2 & grepl(paste0(a,"|",b),seq_season_start)]
      x[is.na(Exp.Duration) & s2==T & all_years,Exp.Duration:=seq_diff*0.5+0.5]
      
      error_dat<-x[is.na(Exp.Duration) & !is.na(seq_n),.(value=paste0(Time2,collapse = "/")),by=B.Code][,table:="Data.Out"][,field:="Time"][,issue:="Cannot estimate season length, probably due to incomplete sequence or error in sequence in Time.Out tab."][order(B.Code)]
      errors<-c(errors,list(error_dat))
      
      x<-x[!is.na(Exp.Duration),.(B.Code,Time,Exp.Duration)]
      
      # Where experimental start is different to start of time seq
      y<-unique(mergedat[y_flag==T,.(B.Code,exp_start,exp_start_s,Time,Time.Start.Year,Time.Season.Start,Time.Agg.Duration,yr_season_ratio,t_start,y_min,unique_seasons,n_seasons,all_years)])
      y[,Time2:=tail(unlist(strsplit(Time,"[.][.]")),1),by=Time]
      # If number of seasons = 0 or 1 we can assume duration is the difference in the years
      y[,exp_start:=as.numeric(exp_start)][yr_season_ratio==1|unique_seasons==0,Exp.Duration:=Time.Start.Year-exp_start+0.5]
    
      y<-merge(y,Times.Out[,.(B.Code,Time,seq_n)],by.x=c("B.Code","Time2"),by.y=c("B.Code","Time"),all.x=T,sort=F)
      
      # Where  we have 2 seasons order on seq_n and give each step 0.5 years
      y<-y[order(B.Code,Time.Start.Year,seq_n,decreasing = F)]
      y[is.na(Exp.Duration) & unique_seasons==2,N:=.N,by=.(B.Code,Time.Start.Year)][N==2,season1:=Time.Season.Start[1],by=.(B.Code,Time.Start.Year)][,season1:=season1[!is.na(season1)][1],by=B.Code]

      y[season1==Time.Season.Start,season1:=0][!is.na(season1) & season1!=0,season1:=0.5]
      
      y[is.na(Exp.Duration) & !is.na(season1),Exp.Duration:=Time.Start.Year-exp_start+0.5+as.numeric(season1)]
      
      # For remainder make do with difference between years
      y[is.na(Exp.Duration),Exp.Duration:=Time.Start.Year-exp_start+0.5]
      
      y<-y[!is.na(Exp.Duration) & Exp.Duration>0,.(B.Code,Time,Exp.Duration)]
      
      xy<-unique(rbind(x,y))
      xy[,N:=.N,by=.(B.Code,Time)]
      error_dat<-xy[N>1,.(value=paste0(unique(Time),collapse = "/")),by=B.Code
                    ][,table:="Data.Out"
                      ][,field:="Time"
                        ][,issue:="Multiple experimental durations calculate for this time, might mean differences between treatment, intercrop and rotation experimental start years."]
      
      errors<-c(errors,list(error_dat))
      
      
      xy<-xy[N==1][,N:=NULL]
      
      # Add Exp.Duration to Data.Out
      Data.Out<-merge(Data.Out,xy,by=c("B.Code","Time"),all.x=T,sort=F)
      stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
      
      
    ### 8.4.13) Add dates ######
      # Create function to merge on time and site id taking into account all sites and all times values
      merge_time_site<-function(data,
                                Data.Out,
                                rename_vars=NULL){
        
        # All sites and all times
        mergedat_a<-data[Site.ID=="All Sites" & Time=="All Times"]
        N<-match(Data.Out[,B.Code],mergedat_a[,B.Code])
        mergedat<-mergedat_a[N]
        
        # All sites and specific times
        mergedat_a<-data[Site.ID=="All Sites" & Time!="All Times"]
        N<-match(Data.Out[,paste(B.Code,Time)],mergedat_a[,paste(B.Code,Time)])
        mergedat[!is.na(N)]<-mergedat_a[N[!is.na(N)]]
        
        # Specific sites and all times
        mergedat_a<-data[Site.ID!="All Sites" & Time=="All Times"]
        N<-match(Data.Out[,paste(B.Code,Site.ID)],mergedat_a[,paste(B.Code,Site.ID)])
        mergedat[!is.na(N)]<-mergedat_a[N[!is.na(N)]]
        
        # Specific sites and specific times
        mergedat_a<-data[Site.ID!="All Sites" & Time!="All Times"]
        N<-match(Data.Out[,paste(B.Code,Time,Site.ID)],mergedat_a[,paste(B.Code,Time,Site.ID)])
        mergedat[!is.na(N)]<-mergedat_a[N[!is.na(N)]]
        
        if(!is.null(rename_vars)){
          setnames(mergedat,names(rename_vars),rename_vars)
        }
        
        return(mergedat)
      }
      
      #### 8.4.13.1) Add planting dates #######
      variable<-c("Planting","Transplanting")
      data<-PD.Out[PD.Variable %in% variable,.(B.Code,Site.ID,PD.Variable,Time,PD.Date.Start,PD.Date.End)]
      
      mergedat<-merge_time_site(data=data,
                                Data.Out = Data.Out,
                                rename_vars = c(PD.Variable="PD.Plant.Variable",PD.Date.Start="PD.Plant.Start", PD.Date.End="PD.Plant.End"))
      
      nrow(mergedat)==n_rows
      
      if(F){
        # Check function outputs
        mergedat[Time=="All Times",unique(Site.ID)]
        mergedat[Time!="All Times",unique(Site.ID)]
        mergedat[Site.ID=="All Sites",unique(Time)]
        mergedat[Site.ID!="All Sites",unique(Time)]
        mergedat[Site.ID=="All Sites" & Time=="All Times",.N]
      }
      
      Data.Out<-cbind(Data.Out,mergedat[,!c("B.Code","Time","Site.ID")])
      
      ##### ***TO DO*** Bring in any planting dates from aggregated years/sites ####
      #### 8.4.13.1) Add harvest dates #######
      variable<-c("Harvesting")
      data<-PD.Out[PD.Variable %in% variable,.(B.Code,Site.ID,Time,PD.Date.Start,PD.Date.End,PD.Date.DAS,PD.Date.DAP)]
      
      mergedat<-merge_time_site(data=data,
                                Data.Out = Data.Out,
                                rename_vars = c(PD.Date.Start="PD.Harvest.Start", PD.Date.End="PD.Harvest.End",
                                                PD.Date.DAS="PD.Harvest.DAS",PD.Date.DAP="PD.Date.DAP"))
      
      nrow(mergedat)==n_rows
      
      Data.Out<-cbind(Data.Out,mergedat[,!c("B.Code","Time","Site.ID")])
      
    ### 8.5.14) Update start year & season #####
    #### ***ISSUE*** NEEDS LOGIC FOR ROT/INT as "Base"? / CONSIDER LOGIC FOR ONLY USING INT/ROT YEAR IF THEY ARE NOT BASE PRACTICES ####
      
      Data.Out[,Final.Start.Year:=
                 if(!is.na(R.Start.Year[1])){as.numeric(R.Start.Year[1])}else{
                   if(!is.na(IN.Start.Year[1])){as.numeric(IN.Start.Year[1])}else{
                     if(!is.na(T.Start.Year[1])){as.numeric(T.Start.Year[1])}}},by=.(R.Start.Year,IN.Start.Year,T.Start.Year)]
      
      # Could do with adding some logic to check large discrepancies in time.
      cols<-grep("Start.Year",colnames(Data.Out),value=T)
      unique(Data.Out[,..cols])
      
      Data.Out[,Final.Start.Season:=if(!is.na(R.Start.Season[1])){R.Start.Season[1]}else{
        if(!is.na(IN.Start.Season[1])){IN.Start.Season[1]}else{
          if(!is.na(T.Start.Season[1])){T.Start.Season[1]}
        }},by=.(R.Start.Year,IN.Start.Year,T.Start.Year)]
      
      
    ### 8.5.15) Add base practices #####
    Data.Out<-merge(Data.Out,Base.Out,by="B.Code",all.x=T,sort=F)
    stopifnot("Merge has increased length of Data.Out table"=nrow(Data.Out)==n_rows)
    ### 8.5.16) Update Structure Fields to reflect Level name rather than "Yes" or "No" ####
    Data.Out[P.Structure!="No",P.Structure:=NA][P.Structure=="No",P.Structure:=P.Level.Name]
    Data.Out[PD.Structure!="No",P.Structure:=NA][PD.Structure=="No",PD.Structure:=PD.Level.Name]
    Data.Out[O.Structure!="No",O.Structure:=NA][O.Structure=="No",O.Structure:=O.Level.Name]
    Data.Out[W.Structure!="No",W.Structure:=NA][W.Structure=="No",W.Structure:=W.Level.Name]
    Data.Out[C.Structure!="No",C.Structure:=NA][C.Structure=="No",C.Structure:=C.Level.Name]
    ### 8.5.17) Update monoculture and irrigation control codes ######
    Data.Out[is.na(IN.Level.Name) & is.na(R.Level.Name),
             T.Codes:=paste(sort(unique(c("h2",unlist(strsplit(T.Codes[1],"-"))))),collapse="-"),by=T.Codes]
    # Also update aggregated treatment codes
    Data.Out[is.na(IN.Level.Name) & is.na(R.Level.Name) & !is.na(T.Agg.Levels), 
             T.Codes.No.Agg:=paste(sort(unique(c("h2",unlist(strsplit(T.Codes.No.Agg[1],"-"))))),collapse="-"),by=T.Codes.No.Agg]
    
    # Get codes for irrigatoin
    irrig_codes<-master_codes$prac[grepl("Irrigat",Subpractice) & !grepl("No Irrigation",Subpractice),.(Code,Subpractice)]
    
    # Add logical field to indicate presence of irrigation
    Data.Out[,Irrig:=F][,Irrig:=any(unlist(strsplit(c(T.Codes[1],Base.Codes[1]),"-")) %in% irrig_codes$Code),by=.(T.Codes,Base.Codes)]
    
    # Add h23 rainfed code to non-irrigated treatments
    Data.Out[Irrig==F,T.Codes:=paste(sort(unique(c("h23",unlist(strsplit(T.Codes[1],"-"))))),collapse="-"),by=T.Codes]
    # Also update aggregated treatment codes
    Data.Out[Irrig==F,T.Codes.No.Agg:=paste(sort(unique(c("h23",unlist(strsplit(T.Codes.No.Agg[1],"-"))))),collapse="-"),by=T.Codes.No.Agg]
    
  # 8.6) Save errors #####
  errors<-rbindlist(errors,fill=T)
  error_list<-error_tracker(errors,filename = "enterdata_other_errors",error_dir=error_dir,error_list = error_list)
  
# 9) Save tables as a list  ####

  Tables<-list(
Pub.Out=Pub.Out, 
Site.Out=Site.Out, 
Soil.Out=Soil.Out,
ExpD.Out=ExpD.Out,
Times.Out=Times.Out,
Times.Clim=Times.Clim,
Prod.Out=Prod.Out,
Var.Out=Var.Out,
Till.Out=Till.Out,
Plant.Out=Plant.Out,
Plant.Method=Plant.Method,
PD.Codes=PD.Codes,
PD.Out=PD.Out,
Fert.Out=Fert.Out,
Fert.Method=Fert.Method,
Chems.Code=Chems.Code,
Chems.AI=Chems.AI,
Chems.Out=Chems.Out,
Weed.Out=Weed.Out,
Res.Out=Res.Out,
Res.Method=Res.Method,
Res.Comp=Res.Comp,
Har.Out=Har.Out,
pH.Out=pH.Out,
pH.Method=pH.Method,
Irrig.Codes=Irrig.Codes,
Irrig.Method=Irrig.Method,
WH.Out=WH.Out,
AF.Out=AF.Out,
AF.Trees=AF.Trees,
Other.Out=Other.Out,
Base.Out=Base.Out,
MT.Out=MT.Out,
Int.Out=Int.Out,
Rot.Out=Rot.Out,
Rot.Seq=Rot.Seq,
Rot.Seq.Summ=Rot.Seq.Summ,
Rot.Levels=Rot.Levels,
Out.Out=Out.Out,
Out.Econ=Out.Econ,
Data.Out=Data.Out
)

save(Tables,file=file.path(data_dir,paste0(project,"-",Sys.Date(),".RData")))

# 10) Summarize error tracking ####
tracking_files<-list.files(error_dir,".csv$",full.names = T)
tracking_files<-tracking_files[!grepl("1. QC tasks|harmonization|error_summary",tracking_files)]

tracking_summary<-rbindlist(lapply(i:length(tracking_files),FUN=function(i){
data<-fread(tracking_files[i])
data.table(tracking_file=basename(tracking_files[i]),
           n_B.Codes=length(unique(data$B.Code)),
           n_B.Codes_pending=length(unique(data$B.Code[!data$issue_addressed])),
           n_rows=nrow(data),
           nrow_pending=sum(!data$issue_addressed))
}))[order(n_B.Codes_pending,decreasing = T)]

fwrite(tracking_summary,file.path(error_dir,"error_summary.csv"))


tracking_files2<-tracking_files[grepl("_other_errors",tracking_files)]
tracking_tab<-rbindlist(lapply(tracking_files2,fread),use.names = T,fill = T)


