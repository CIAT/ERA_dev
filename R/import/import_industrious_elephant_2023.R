  # Make sure you have set the era working directory using the 0_set_env.R script ####
  # 0.0) Install and load packages ####
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
    
    # 0.2) Set directories and parallel cores ####
    
    # Set cores for parallel processing
    workers<-parallel::detectCores()-2
    
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
    
    # List column names for the sheets to be extracted
    XL.M<-sapply(SheetNames,FUN=function(SName){
      cat('\r                                                                                                                                          ')
      cat('\r',paste0("Importing Sheet = ",SName))
      flush.console()
      colnames(data.table(suppressWarnings(suppressMessages(readxl::read_excel(Master,sheet = SName)))))
    },USE.NAMES = T)
    
    # Subset Cols
    XL.M[["AF.Out"]]<-XL.M[["AF.Out"]][1:13] # Subset Agroforesty out tab to needed columns only
    
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
    if (.Platform$OS.type == "windows") {
      plan(multisession, workers = workers)
    } else {
      plan(multicore, workers = workers)
    }
    
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
    
    
  # 3) Process imported data ####
# 3.1) Publication (Pub.Out) #####
data<-lapply(XL,"[[","Pub.Out")

Pub.Out<-rbindlist(pblapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  X$filename<-names(XL)[i]
  X
}))

# Replace zeros with NAs
Pub.Out<-validator(data=Pub.Out,
                   zero_cols=c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4"),
                   tabname="Pub.Out")$data
                   

# Pub.Out: Validation: Duplicate or mismatched B.Codes
Pub.Out<-merge(Pub.Out,excel_files[,list(filename,era_code2)],all.x=T)
Pub.Out[,N:=.N,by=B.Code][,code_issue:=B.Code!=era_code2][,B.Code:=trimws(B.Code)][,era_code2:=trimws(era_code2)]

# Save any errors
errors<-Pub.Out[N>1|code_issue,list(B.Code,era_code2,filename,N,code_issue)][order(B.Code)]
error_list<-error_tracker(errors=errors,filename = "pub_code_errors",error_dir=error_dir,error_list = error_list)

# Reset B.Codes to filename
Pub.Out[,B.Code:=era_code2][,c("era_code2","filename","N","code_issue","...7"):=NULL]

  # 3.1.1) Harmonization ######
  results<-val_checker(data=Pub.Out,
                        tabname="Pub.Out",
                        master_codes=master_codes,
                        master_tab="journals",
                        h_field="B.Journal",
                        h_field_alt=NA,
                        exact=F)
  
  Pub.Out<-results$data
  
  harmonization_list<-error_tracker(errors=results$h_task[order(value)],filename = "pub_harmonization",error_dir=harmonization_dir,error_list = NULL)

# 3.2) Site.Out #####
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

error_list<-error_tracker(errors=rbindlist(list(errors1,errors2),fill=T),
                          filename = "site_other_errors",
                          error_dir=error_dir,
                          error_list = error_list)


dat<-Site.Out[!(is.na(Site.LatD)|is.na(Site.LonD)|is.na(ISO.3166.1.alpha.3))]
errors3<-check_coordinates(data=dat[,list(Site.LatD,Site.LonD,ISO.3166.1.alpha.3)])
errors3<-dat[!errors3][,list(value=paste(unique(Site.ID),collapse="/")),by=list(B.Code,Country,ISO.3166.1.alpha.3,Site.LatD,Site.LonD)
              ][,table:="Site.Out"
                ][,field:="Site.LonD/Site.LatD"
                  ][,issue:="Co-ordinates are not in the country specified."]

error_list<-error_tracker(errors=errors3,filename = "site_coordinate_errors",error_dir=error_dir,error_list = error_list)

  # 3.2.1) Harmonization ######
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
  
  
    # !!! TO DO !!! Site.Out: Update Fields From Harmonization Sheet - BEST LEFT TILL OTHER SHEETS READ IN ####
  if(F){
  # Make sure blanks read NA
  Site.Out[Site.Buffer.Manual==""|Site.Buffer.Manual=="NA"|is.na(Site.Buffer.Manual),Site.Buffer.Manual:=NA]
  Site.Out[Site.Lon.Unc==""|Site.Lon.Unc=="NA"|is.na(Site.Lon.Unc),Site.Lon.Unc:=NA]
  Site.Out[Site.Lat.Unc==""|Site.Lat.Unc=="NA"|is.na(Site.Lat.Unc),Site.Lat.Unc:=NA]
  
  Site.Out[,Site.ID:=trimws(Site.ID)]
  N<-match(Site.Out[, Site.ID],SiteHarmonization[,ED.Site.ID])
  
  # Sites/B.Codes combos with no match in harmonization table
  Site.No.Match<-unique(Site.Out[is.na(N) & !grepl("[.][.]",Site.ID),list(B.Code,Site.ID,Site.LatD,Site.LonD,Site.Lon.Unc,Site.Lat.Unc,Site.Buffer.Manual)])
  if(nrow(Site.No.Match)>0){
    View(Site.No.Match) 
    write.table(Site.No.Match,"clipboard",row.names = F,sep="\t")
  }
  rm(Site.No.Match)
  
  Site.Out[!is.na(N),Site.LatD:=SiteHarmonization[N[!is.na(N)],Site.LatD.Corrected]]
  Site.Out[!is.na(N),Site.LonD:=SiteHarmonization[N[!is.na(N)],Site.LonD.Corrected]]
  Site.Out[!is.na(N),Site.Buffer.Manual:=SiteHarmonization[N[!is.na(N)],Site.Buffer.Manual.Corrected]]
  Site.Out[!is.na(N),Site.ID:=SiteHarmonization[N[!is.na(N)],ED.Site.ID.Corrected]]
  
  # Make sure fields are numeric
  Site.Out[Site.Buffer.Manual=="",Site.Buffer.Manual:=NA]
  Site.Out[,Site.Buffer.Manual:=gsub(",","",Site.Buffer.Manual)][,Site.Buffer.Manual:=as.numeric(Site.Buffer.Manual)]
  Site.Out[,Site.Lon.Unc:=gsub(",","",Site.Lon.Unc)][Site.Lon.Unc=="",Site.Lon.Unc:=NA][,Site.Lon.Unc:=as.numeric(as.character(Site.Lon.Unc))]
  Site.Out[,Site.Lat.Unc:=gsub(",","",Site.Lat.Unc)][Site.Lat.Unc=="",Site.Lat.Unc:=NA][,Site.Lat.Unc:=as.numeric(as.character(Site.Lat.Unc))]
  
  # Update site buffer manual field
  Site.Out[is.na(Site.Buffer.Manual),Site.Buffer.Manual:=(Site.Lon.Unc+Site.Lat.Unc)/2]
  
  Site.Out[is.na(Site.Buffer.Manual) & !grepl("[.][.]",Site.ID),list(B.Code,Site.ID,Site.Lat.Unc,Site.Lon.Unc,Site.Buffer.Manual)]
  
  SiteHarmonization[,Code:= paste(B.Codes,ED.Site.ID)]
  
  # Update Aggregated Site Names
  
  Agg.Site.Name.Fun<-function(Site.ID,SiteHarmonization){
    if(grepl("[.][.]",Site.ID)){
      Site.ID<-unlist(strsplit(Site.ID,"[.][.]"))
      Y<-unique(SiteHarmonization[ED.Site.ID %in% Site.ID & !grepl("[.][.]",ED.Site.ID.Corrected),ED.Site.ID.Corrected])
      if(length(Y)>length(Site.ID)){
        "Issue: Multiple matches on Site.ID"
      }else{
        paste(sort(Y),collapse="..")
      }
    }else{
      Site.ID
    }
  }
  
  Site.Out[grepl("[.][.]",Site.ID),Site.ID:=Agg.Site.Name.Fun(Site.ID[1],SiteHarmonization),by=Site.ID]
  }
# 3.3) Times periods #####
data<-lapply(XL,"[[","Times.Out")
col_names<-colnames(data[[800]])
    
  # 3.3.1) Times.Out ###### 
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
  
  results<-validator(data=Times.Out,
                     unique_cols = c("Time"),
                     tabname="Times.Out",
                     do_time = F)
  
  errors5<-results$errors
  Times.Out<-results$data
  
  # Error checking odd values in Time.Year.Start or Time.Year.End
  errors<-Times.Out[is.na(as.numeric(Time.Year.End))|is.na(as.numeric(Time.Year.Start))|nchar(Time.Year.Start)>4|nchar(Time.Year.End)>4
                    ][!(is.na(Time.Year.Start)|is.na(Time.Year.End)|grepl("unspecified",Time.Year.Start,ignore.case = T))
                      ][,list(B.Code,Time.Year.End,Time.Year.Start)
                        ][,issue:="Non-YYYY values in time tab for year start or end."]
  
  error_list<-error_tracker(errors=errors,filename = "time_year_errors",error_dir=error_dir,error_list = error_list)
  
  Times.Out[,c("Time.Year.Start","Time.Year.End"):=list(as.integer(Time.Year.Start),as.integer(Time.Year.End))]
  
  # 3.3.1) Times.Clim ###### 
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
  
  results<-validator(data=Times.Clim,
                     numeric_cols=c("Time.Clim.SP","Time.Clim.TAP","Time.Clim.Temp.Mean","Time.Clim.Temp.Max","Time.Clim.Temp.Min"),
                     time_data = Times.Out,
                     site_data = Site.Out,
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
  
  
  errors<-rbind(errors1,errors2,errors3)[,Time.Clim.Notes:=NULL]
  
  error_list<-error_tracker(errors=errors,filename = "time_climate_errors",error_dir=error_dir,error_list = error_list)
  error_list<-error_tracker(errors=rbindlist(list(errors4,errors5),fill=T),filename = "time_other_errors",error_dir=error_dir,error_list = error_list)
  
  
# 3.4) Soil (Soil.Out) #####
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

results<-validator(data=Soil.Out,
                    numeric_cols=c("Soil.Upper","Soil.Lower","value"),
                    compulsory_cols=c(Site.ID="value"),
                    tabname="Soil.Out",
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

  # 3.4.1) Soil.Out: Calculate USDA Soil Texture from Sand, Silt & Clay ####
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

  # 3.4.1) ***!!!TO DO!!!***  harmonize methods, units and variables ####
  Soil.Out[,filename:=NULL]
# 3.5) Experimental Design (ExpD.Out) ####
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
                   tabname="ExpD.Out")

ExpD.Out<-results$data

error_list<-error_tracker(errors=results$errors,filename = "expd_other_errors",error_dir=error_dir,error_list = error_list)


# 3.6) Products (Prod.Out) ####
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

  # TO DO - updated mulched and incorporated codes, check product is in master codes ####
# 3.7) Var (Var.Out) ####
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
                   trim_ws=T)

errors1<-results$errors
Var.Out<-results$data

# Update V.Codes
Var.Out[,V.Codes:=master_codes$prac[match(Var.Out$V.Crop.Practice,master_codes$prac$Subpractice),Code]]

# Errors
errors<-unique(Var.Out[(V.Type %in% c("Local","Landrace")) & V.Crop.Practice != "Unimproved Variety",!c("V.Base","V.Codes")])
errors<-errors[,list(B.Code=paste(B.Code,collapse = "/")),by=list(V.Product,V.Var,V.Accession,V.Crop.Practice,V.Type,V.Trait1,V.Trait2,V.Trait3,V.Maturity)
               ][,issue:="Type is local, but not unimproved, possible error (but could be correct)."
                   ][,issue_addressed:=F
                     ][,addressed_by_whom:=""
                       ][,c("V.Accession","V.Trait1","V.Trait2","V.Trait3","V.Maturity"):=NULL
                         ][order(B.Code)]

errors2<-unique(Var.Out[V.Type %in% c("Hybrid") & V.Crop.Practice == "Unimproved Variety",!c("V.Base","V.Codes")])
errors2<-errors2[,list(B.Code=paste(B.Code,collapse = "/")),by=list(V.Product,V.Var,V.Accession,V.Crop.Practice,V.Type,V.Trait1,V.Trait2,V.Trait3,V.Maturity)
][,issue:="Type is Hybrid, but practice is improved, probable error."
][,issue_addressed:=F
][,addressed_by_whom:=""
][,c("V.Accession","V.Trait1","V.Trait2","V.Trait3","V.Maturity"):=NULL
][order(B.Code)]

error_list<-error_tracker(errors=errors,filename = "var_practice_check",error_dir=error_dir,error_list = error_list)


  # 3.7.1) Var.Out: Harmonize Variety Naming and Codes #####

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
harmonization_list<-error_tracker(errors=h_tasks2,filename = "var_master_dups2_harmoniazation",error_dir=harmonization_dir,error_list = harmonization_list)

# Non-matching varieties
h_tasks3<- Var.Out[is.na(mergedat$V.Var1) & !is.na(V.Var) & !grepl("local|unspecified|unimproved",V.Var,ignore.case=T),list(V.Product,V.Var,B.Code)
][,list(B.Code=paste(B.Code,collapse = "/")),by=list(V.Product,V.Var)
][,master_tab:="vars"
][,table:="Var.Out"
][,field:="V.Var"]
setnames(h_tasks3,"V.Var","value")

harmonization_list<-error_tracker(errors=errors3,filename = "var_varieties_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)


  # Q: Should we update traits and maturity too? ####
# 3.8) Agroforestry #####
data<-lapply(XL,"[[","AF.Out")
col_names<-colnames(data[[800]])

  # 3.8.1) AF.Out ######
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
  
  # 3.8.2) AF.Trees ######
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
                     tabname="AF.Trees")
  
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
  
  # combine and save errors
  errors<-rbindlist(list(errors2,errors3,errors1,errors4),fill=T)
  error_list<-error_tracker(errors=errors,filename = "af_errors",error_dir=error_dir,error_list = error_list)
  error_list<-error_tracker(errors=rbind(errors_a,errors_b),filename = "af_structure_errors",error_dir=error_dir,error_list = error_list)
  
  
  # 3.8.x) Harmonization - TO DO !!! ######
  # AF.Tree and AF.Tree.Unit

# 3.9) Tillage (Till.Out) #####
data<-lapply(XL,"[[","Till.Out")
col_names<-colnames(data[[1]])

  # 3.9.1) Till.Codes #######

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
  
  # 3.9.2) Till.Out #######
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
  
  results<-validator(data=Till.Out,
                     numeric_cols=c("T.Depth","T.Strip.P","T.Strip.WT","T.Strip.WU","T.Freq"),
                     date_cols=c("T.Date.End", "T.Date.Start"),
                     tabname="Till.Out",
                     valid_start=valid_start,
                     valid_end=valid_end,
                     site_data=Site.Out,
                     time_data=Times.Out,
                     ignore_values= c("All Times","Unspecified","Not specified","All Sites"))
  
  errors2<-results$errors
  Till.Out<-results$data
  
  # Error checking: look for non-matches in keyfield
  errors3<-check_key(parent = Till.Codes,child = Till.Out,tabname="Till.Out",keyfield="Till.Level.Name")[,issue:="A practice has some description in the tillage (typically base), but no practice has been associated with it."]
  
  # save errors
  errors<-rbindlist(list(errors1,errors2,errors3,errors4),fill=T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "till_errors",error_dir=error_dir,error_list = error_list)
  
    # 3.9.2.1) Harmonization ######
    h_params<-data.table(h_table="Till.Out",
                         h_field=c("T.Method","Till.Other","T.Mechanization"),
                         h_table_alt=c(NA,NA,"Fert.Out"),
                         h_field_alt=c(NA,NA,"F.Mechanization"))
    
    results<-harmonizer_wrap(data=Till.Out,
                             h_params=h_params,
                             master_codes = master_codes)
    
    harmonization_list<-error_tracker(errors=results$h_tasks,filename = "till_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
    
    Till.Out<-results$data
    
# 3.10) Planting  #####
data<-lapply(XL,"[[","Plant.Out")
col_names<-colnames(data[[800]])

  # 3.10.1) Plant.Out ####
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
  
  errors1<-validator(data=Plant.Out,
                     unique_cols = "P.Level.Name",
                     tabname="Plant.Out")$errors
  
  # 3.10.2) Plant.Method ####
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
                     tabname="Plant.Method")
  
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
  errors5<-unique(Plant.Method[is.na(Plant.Density.Unit) & !is.na(Plant.Density)
  ][,list(value=paste0(P.Level.Name,collapse="/")),by=B.Code
  ][,table:="Plant.Method"
  ][,field:="P.Level.Name"
  ][,issue:="Missing density unit."])
  
  error_list<-error_tracker(errors=rbind(errors1,errors2,errors3,errors4,errors5)[order(B.Code)],filename = "plant_method_value_errors",error_dir=error_dir,error_list = error_list)
  
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
  
    # 3.10.2.1) Harmonization ######
  
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
  
# 3.11) Fertilizer (Fert.Out) #####
data<-lapply(XL,"[[","Fert.Out")
col_names<-colnames(data[[1]])

  # 3.11.1) Fert.Out ######
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

    # 3.11.1.1) Harmonization #######
h_params<-data.table(h_table="Fert.Out",
                     h_field=c("F.I.Unit","F.O.Unit","F.Rate.Pracs","F.Timing.Pracs","F.Precision.Pracs","F.Info.Pracs"),
                     h_table_alt=c("Fert.Out","Fert.Out",NA,NA,NA,NA),
                     h_field_alt=c("F.Unit","F.Unit",NA,NA,NA,NA))

results<-harmonizer_wrap(data=Fert.Out,
                         h_params=h_params,
                         master_codes = master_codes)

errors4<-results$h_tasks[,issue:="Issue with units of inorganic or organic fertilizer, or practices selected."]

Fert.Out<-results$data

  # 3.11.2) Fert.Method ####
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

    # 3.11.2.1) Harmonization #######
h_params<-data.table(h_table="Fert.Method",
                     h_field=c("F.Unit","F.Method","F.Physical","F.Mechanization","F.Source","F.Fate"),
                     h_table_alt=c("Fert.Out",NA,NA,"Fert.Out","F.Method","Res.Method"),
                     h_field_alt=c("F.Unit",NA,NA,"F.Mechanization","M.Source","M.Fate"))

results<-harmonizer_wrap(data=Fert.Method,
                         h_params=h_params,
                         master_codes = master_codes)

h_tasks1<-results$h_tasks

Fert.Method<-results$data

# Update fertilizer names using fert tab of master_codes
fert_master<-unique(master_codes$fert[,list(F.Category,F.Type_New,Fert.Code1,Fert.Code2,Fert.Code3)][,match:=T])


h_params<-data.table(h_table="Fert.Method",
                     h_field=,
                     h_table_alt=c("Fert.Out",NA,NA,"Fert.Out","F.Method","Res.Method"),
                     h_field_alt=c("F.Unit",NA,NA,"F.Mechanization","M.Source","M.Fate"))


# Check if fertilizers already exist or not
mdat<-unique(master_codes$fert[,list(F.Category,F.Type)])[,check:=T]

h_tasks2<-unique(merge(Fert.Method[,list(B.Code,F.Category,F.Type)],mdat,all.x=T)[is.na(check)][!is.na(F.Category)][,check:=NULL])
h_tasks2<-h_tasks2[,list(B.Code=paste(B.Code,collapse = "/")),by=list(F.Category,F.Type)][,table:="Fert.Method"][,field:="F.Type"][,master_tab:="fert"]
setnames(h_tasks2,"F.Type","value")

  # 3.11.3) Fert.Composition ######
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
                   tabname="Fert.Comp")

errors8<-results$errors
Fert.Comp<-results$data

# Check key fields
errors9<-check_key(parent = Fert.Method,child = Fert.Comp,tabname="Fert.Comp",keyfield="F.Type")

    # 3.11.3.1) Harmonization #######
h_params<-data.table(h_table="Fert.Comp",
                     h_field=c("F.DM.Unit","F.OC.Unit","F.N.Unit","F.TN.Unit","F.AN.Unit","F.P.Unit","F.TP.Unit","F.AP.Unit","F.K.Unit","F.pH.Method"),
                     h_table_alt=c("Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out"),
                     h_field_alt=c("Soil.SOM.Unit","Soil.SOM.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.K.Unit","Soil.pH.Method"))

results<-harmonizer_wrap(data=Fert.Comp,
                         h_params=h_params,
                         master_codes = master_codes)

h_tasks3<-results$h_tasks

Fert.Comp<-results$data

  # 3.11.4) Save harmonization and error tasks #####
errors<-rbindlist(list(errors_a,errors_b,errors_c))
error_list<-error_tracker(errors=errors,filename = "fert_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors6,errors7,errors8,errors9),fill=T)[order(B.Code)]
error_list<-error_tracker(errors=errors,filename = "fert_other_errors",error_dir=error_dir,error_list = error_list)

h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3),fill=T)
harmonization_list<-error_tracker(errors=h_tasks,filename = "fert_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)

  # 3.11.x) ***!!!TO DO!!!*** Add Fertilizer codes   #######
    if(F){
      # Old Code
      
     # Fert_Out: Create F.Codes columns
     Fert.Out[,c("F.NI.Code","F.PI.Code","F.KI.Code","F.Urea","F.Compost","F.Manure","F.Biosolid","F.MicroN","F.Biochar","F.Codes"):=as.character(NA)]
    
     # Fert.Method: Update Codes in  Fert.Methods From MasterCodes FERT Tab ####
      
      # Validation - Check for non-matches
      fert_master<-unique(master_codes$fert[,list(F.Category,F.Type_New,Fert.Code1,Fert.Code2,Fert.Code3)][,match:=T])
      setnames(fert_master,"F.Type_New","F.Type")
      
      Fert.Method<-merge(Fert.Method,fert_master[,list(F.Category,F.Type, Fert.Code1,Fert.Code2,Fert.Code3,match)],all.x=T)[is.na(match),match:=F]
      
      Fert.Method[match==F]
      
      # Table should be blank if there are no issues
      FERT.Master.NonMatches<-unique(Fert.Method[is.na(match(F.Lab,FertCodes[,F.Lab])),c("B.Code","F.Category","F.Type")])
      
      if(nrow(FERT.Master.NonMatches)>0){
        View(FERT.Master.NonMatches)
      }
      rm(FERT.Master.NonMatches)
      
      # Add Codes
      Fert.Method$F.Lab<-apply(Fert.Method[,c("F.Category","F.Type")],1,paste,collapse="-")
      Fert.Method<-cbind(Fert.Method,FertCodes[match(Fert.Method$F.Lab,FertCodes$F.Lab),c("Fert.Code1","Fert.Code2","Fert.Code3")])
      Fert.Method[,F.Lab:=NULL]
      
      # Fert.Method: Update Fields From Harmonization Sheet ####
      N<-match(Fert.Method$F.Method,OtherHarmonization$Fert.Method)
      Fert.Method[,F.Method:=OtherHarmonization[N,Fert.Method.Correct]]
      rm(N)
      # Fert.Out: Add Fertilizer Codes from NPK rating ####
      
      # Fert.Out - Validation: Check codes for errors
      # View(unique(Fert.Method[!is.na(F.NPK),c("F.NPK","B.Code")][order(F.NPK)]))
      
      NPKrows<-Fert.Method[!is.na(F.NPK),c("B.Code","F.NPK")]
      
      if(nrow(NPKrows)>0){
        N<-which(!is.na(Fert.Method$F.NPK))
        X<-data.table(do.call("rbind",strsplit(Fert.Method[N,F.NPK],"-")))
        colnames(X)<-c("N","P","K")
        
        X[,Fert.Code1:=NA]
        X<-cbind(X,do.call("rbind",pblapply(1:nrow(X),FUN=function(i){
          Y<-as.numeric(t(X[i,1:3])[,1])
          Z<-c("b17","b21","b16")[!Y==0][order(Y[!Y==0],decreasing = T)]
          
          if((3-length(Z))>0){
            Z<-c(Z,rep(NA,3-length(Z)))
          }
          Z
        })))
        
        Fert.Method[N,Fert.Code1:=X$V1][N,Fert.Code2:=X$V2][N,Fert.Code3:=X$V3]
      }
      
      # Fert.Out: Add the codes above derived from Fert.Method tab to Fert.Out ####
      
      Fert.Out[,F.Codes2:=unlist(pblapply(1:nrow(Fert.Out),FUN=function(i){
        X<-Fert.Out[i,c("F.Level.Name","B.Code")]
        X<-unlist(Fert.Method[B.Code==X$B.Code & F.Name==X$F.Level.Name,c("Fert.Code1","Fert.Code2","Fert.Code3")])
        if(length(X)>0){
          X<-X[X!="NA"]
          X<-X[!is.na(X)]
          paste(unique(X[order(X)]),collapse = "-")
        }else{
          NA
        }
      }))]
      
      # Fert Out: Deal with Urea ####
      # If Urea is present in F.Codes2 without a b17 code, then b17 code should be removed from F.Codes
      
      X1<-grep("b23",Fert.Out$F.Codes)
      X2<-grep("b17",Fert.Out$F.Codes)
      X<-X1[X1 %in% X2]
      
      Y1<-grep("b23",Fert.Out$F.Codes2)
      Y2<-grep("b17",Fert.Out$F.Codes2)
      Y<-Y1[Y1 %in% Y2]
      
      X<-X[!X %in% Y]
      
      # Check for any instance where an NPK code has been associated with a Urea entry
      #X.Issue<-X[!X %in% Y1]
      #Fert.Out[X.Issue]
      
      # Remove "b17-" codes where required
      Fert.Out[X,F.Codes:=gsub("b17-","",F.Codes)]
      
      # List N-P-K 
      #write.table(unique(Fert.Method[!is.na(F.NPK),c("B.Code","F.NPK")][order(F.NPK)]),"clipboard",row.names = F,col.names=F,sep="\t")
      
      # List Aggregated Practices to see if we can disagg all of them - this was set as a task and should now be completed.
      #write.table(unique(Fert.Out[grep("[.][.]",F.Level.Name),"B.Code"]),"clipboard",row.names = F,col.names=F,sep="\t")
      
      # Rename F.Name col in Fert.Method
      setnames(Fert.Method, "F.Name", "F.Level.Name")
      
      # Fert.Out: Validation   ####
      
      # Validation: Check Codes from Fert.Methods vs. Codes from Fert.Out
      F.CodesvsF.Codes2<-unique(Fert.Out[F.Codes != F.Codes2,c("B.Code","F.Codes","F.Codes2","F.Level.Name")])
      
      if(nrow(F.CodesvsF.Codes2)>0){
        View(F.CodesvsF.Codes2)
      }
      rm(F.CodesvsF.Codes2)
      
      # Find any missing F.Codes
      # might indicate aggregation is still being used, missing NPK values or error in excel formulae in Fert.Out tab
      NoFCodes<-unique(Fert.Out[F.Level.Name!="Base" & is.na(F.Codes),c("B.Code","F.Level.Name","F.Codes")])
      
      if(nrow(NoFCodes)>0){
        View(NoFCodes)
      }
      rm(NoFCodes)
      # Fert.Out: Add in h10.1 Codes for no Fertilizer use ####
      Fert.Out[is.na(F.Codes) & !F.Level.Name=="Base",F.Codes:="h10.1"
      ][is.na(F.Codes2) & !F.Level.Name=="Base",F.Codes2:="h10.1"]
      
      # Fert.Out: To deal with new Ash & Organic (Other) Codes overwrite F.Codes with F.Codes2 ####
      Fert.Out[,F.Codes:=F.Codes2]
      
      # Fert.Out/Method: Update Naming Fields ####
      # Names could do with index or short name
      
      # Make sure amount is a numeric field
      Fert.Method[,F.Amount:=as.numeric(as.character(F.Amount))]
      
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
      Fert.Out[,N:=NULL]
      rm(ReName.Fun)
      
      Fert.Method[,F.Level.Name2:=Fert.Out[match(Fert.Method[,F.Level.Name],Fert.Out[,F.Level.Name]),F.Level.Name2]]
      
      # Fert.Out: Translate Fert Codes in Cols ####
      F.Master.Codes<-PracticeCodes[Theme=="Nutrient Management" | Subpractice =="Biochar",Code]
      F.Master.Codes<-F.Master.Codes[!grepl("h",F.Master.Codes)]
      Fert.Method[,Code:=paste(B.Code,F.Level.Name)]
      
      
      Fert.Levels<-rbindlist(pblapply(1:nrow(Fert.Out),FUN=function(i){
        
        F.Master<-data.frame(matrix(nrow=1,ncol = length(F.Master.Codes)))
        colnames(F.Master)<-F.Master.Codes
        F.Master2<-F.Master
        
        Z<-unlist(strsplit(Fert.Out[i,F.Codes2],"-"))
        Z<-Z[!grepl("h",Z)]
        
        if(!is.na(Z[1]) & length(Z)>0){
          Fert.Outx<-Fert.Out[i,]
          FO.Code<-Fert.Outx[,paste(B.Code,F.Level.Name)]
          Y<-Fert.Method[Code==FO.Code]
          
          for(Zn in Z){
            # print(paste(i,"-",Zn))
            W<-Y[grepl(Zn,unlist(Y[,paste(Fert.Code1,Fert.Code2,Fert.Code3)])),list(F.Type,F.Amount)]
            # Deal with rounding issues
            W<-rbind(W[is.na(F.Amount)],W[!is.na(F.Amount),list(F.Amount=sum(F.Amount)),by=F.Type])
            W[,F.Amount:=round(F.Amount*10*round(log(F.Amount,base=10)),0)/(10*round(log(F.Amount,base=10)))]
            W<-W[,paste(F.Type,F.Amount)]
            
            W<-paste(W[order(W)],collapse = "|")
            
            if(Zn %in% c("b17","b23")){
              V<-Fert.Outx[i,F.NI]
              if(!is.na(V)){
                W<-paste(V,W)
              }
            }
            if(Zn == "b21"){
              V<-unlist(Fert.Outx[i,c("F.PI","F.P2O5")])
              V<-V[!is.na(V)]
              if(length(V[!is.na(V)])>0){
                W<-paste(V,W)
              }
            }
            if(Zn == "b16"){
              V<-unlist(Fert.Outx[i,c("F.KI","F.K2O")])
              V<-V[!is.na(V)]
              if(length(V[!is.na(V)])>0){
                W<-paste(V,W)
              }
            }
            
            if(Zn %in% c("b29","b30")){
              V<-unlist(Fert.Outx[i,c("F.NO","F.PO","F.KO")])
              if(length(V[!is.na(V)])>0){
                V<-paste(V,collapse="-")
                W<-paste(V,W)
              }
            }
            
            
            F.Master[,Zn]<-W
          }
          
        }
        
        N<-which(!is.na(as.vector(F.Master)))
        if(length(N)>0){
          F.Master2[,N]<-colnames(F.Master)[N]
        }
        colnames(F.Master2)<-paste(colnames(F.Master2),"Code",sep=".")
        data.table(Fert.Out[i,c("B.Code","F.Level.Name")],F.Master,F.Master2)
        
        
      }))
      
      Fert.Method[,Code:=NULL]
      
    }

  # 3.11.x) ***!!!TO DO!!!*** Add in h10 code where there are fertilizer treatments, but fertilizer column is blank #######
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
# 3.12) Chemicals (Chems.Out) #####
data<-lapply(XL,"[[","Chems.Out")
col_names<-colnames(data[[1]])

 # 3.12.1) Chems.Code ####
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

errors8<-validator(data=Chems.Code,
                   unique_cols = "C.Level.Name",
                   tabname="Chems.Code")$errors

  # 3.12.2) Chems.Out ####
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

results<-validator(data=Chems.Out,
                   compulsory_cols = c(C.Level.Name="C.Type",C.Level.Name="C.Name"),
                   numeric_cols=c("C.Amount","C.Date.DAP","C.Date.DAE"),
                   date_cols=c("C.Date.Start", "C.Date.End", "C.Date"),
                   valid_start=valid_start,
                   valid_end=valid_end,
                   site_data=Site.Out,
                   time_data=Times.Out,
                   tabname="Chems.Out",
                   ignore_values = c("All Times","Unspecified","Not specified","All Sites"))

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
                ][,order(B.Code)]

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

    # 3.12.2.1) Harmonization #######
h_params<-data.table(h_table="Chems.Out",
                     h_field=c("C.App.Method","C.Mechanization","C.Unit"),
                     h_table_alt=c(NA,"Fert.Out",NA),
                     h_field_alt=c(NA,"F.Mechanisation",NA))

results<-harmonizer_wrap(data=Chems.Out,
                         h_params=h_params,
                         master_codes = master_codes)

h_tasks1<-results$h_tasks
Chems.Out<-results$data

mergedat<-setnames(master_codes$chem[,4:5],"C.Name.2020...5","C.Name")[!is.na(C.Type)][,check:=T]

h_tasks2<-setnames(merge(Chems.Out[,list(B.Code,C.Type,C.Name)],mergedat,all.x = T)[is.na(check)][,check:=NULL],"C.Name","value")[,field:="C.Name"][,field_alt:="C.Name.2020"][,table:="Chem.Out"][,master_tab:="chem"]

errors7<-unique(h_tasks2[!C.Type %in% mergedat$C.Type][,issue:="C.Type may be incorrect or simply missing from chem tab in master sheet."][,value:=C.Type][,field:="C.Type"][,C.Type:=NULL])
h_tasks2<-h_tasks2[C.Type %in% mergedat$C.Type]


# Add C.Code to Chems.Code tab
Chems.Code<-merge(Chems.Code,Chems.Out[,list(C.Code=unique(C.Code[!is.na(C.Code)])),by=list(C.Level.Name,B.Code)],all.x=T)

  # 3.12.3) Chems.AI ####
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

    # 3.12.3.1) Harmonization #######
h_params<-data.table(h_table="Chems.AI",
                     h_field="C.AI.Unit",
                     h_table_alt="Chems.Out",
                     h_field_alt="C.AI.Unit")

results<-harmonizer_wrap(data=Chems.AI,
                         h_params=h_params,
                         master_codes = master_codes)

h_tasks3<-results$h_tasks
Chems.AI<-results$data

mergedat<-setnames(master_codes$chem[,4:5],"C.Name.2020...5","C.Name.AI")[!is.na(C.Type)][,check:=T][,C.Name.AI:=tolower(C.Name.AI)]
h_tasks4<-setnames(merge(Chems.AI[!is.na(C.Name.AI),list(B.Code,C.Type,C.Name.AI)][,C.Name.AI:=tolower(C.Name.AI)],mergedat,all.x = T)[is.na(check)][,check:=NULL],"C.Name.AI","value")
h_tasks4<-h_tasks4[,list(B.Code=paste0(unique(B.Code),collapse="/")),by=list(C.Type,value)
                   ][,field:="C.Name.AI"
                     ][,field_alt:="C.Name.2020"
                       ][,table:="Chems.AI"
                         ][,master_tab:="chems"]
                         
errors6<-unique(h_tasks4[!C.Type %in% mergedat$C.Type][,issue:="C.Type may be incorrect, or simply missing from Master Sheet."][,value:=C.Type][,field:="C.Type"][,C.Type:=NULL])
h_tasks4<-h_tasks4[C.Type %in% mergedat$C.Type]

  # 3.12.5) Add/check herbicide codes ######
# In case there have been changes to C.Codes from harmonization process update these
mdat<-Chems.Out[C.Type=="Herbicide",list(B.Code,C.Level.Name,C.Code)]
colnames(mdat)[3]<-"C.Code2"

Chems.Code<-merge(Chems.Code,mdat,all.x=T)
Chems.Code<-Chems.Code[is.na(C.Code),C.Code:=C.Code2][,C.Code2:=NA]

  # 3.12.5) Join and save errors and harmonization tasks ######
errors<-rbindlist(list(errors_a,errors_b,errors_c))
error_list<-error_tracker(errors=errors,filename = "chem_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6,errors7,errors8,errors9,errors10),fill=T)[order(B.Code)]
error_list<-error_tracker(errors=errors,filename = "chem_other_errors",error_dir=error_dir,error_list = error_list)

h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4),fill=T)
harmonization_list<-error_tracker(errors=h_tasks,filename = "chem_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)


# 3.13) Weeding (Weed.Out) #####
data<-lapply(XL,"[[","Weed.Out")
col_names<-colnames(data[[1]])

Weed.Out<-lapply(1:length(data),FUN=function(i){
  X<-data[[i]]
  B.Code<-Pub.Out$B.Code[i]
  
  if(!all(col_names %in% colnames(X))){
    cat("Structural issue with file",i,B.Code,"\n")
    list(error=data.table(B.Code=B.Code,filename=basename(names(XL)[i]),issue="Problem with Chems.Out tab structure (first 3 cols A:C in excel)"))
  }else{
    X<-X[,..col_names]
    colnames(X)[1]<-"W.Level.Name"
    X<-X[!is.na(W.Method)]
    if(nrow(X)>0){
      X[,B.Code:=B.Code]
      list(data=X)
    }else{
      NULL
    }
  }})

errors_a<-rbindlist(lapply(Weed.Out,"[[","error"))
Weed.Out<-rbindlist(lapply(Weed.Out,"[[","data"))

# Code for Hand Weeding
Weed.Out[!W.Method %in% c("Mechanical","Ploughing"),W.Code:="h66.2"]

Weed.Code<-Weed.Out[,list(W.Code=unique(W.Code[!is.na(W.Code)]),W.Structure=W.Structure[1]),by=list(B.Code,W.Level.Name)]

Weed.Out<-Weed.Out[,!"W.Structure"]

# 3.14) Residues #######
data<-lapply(XL,"[[","Residues.Out")
col_names<-colnames(data[[1]])

  # 3.14.1) Res.Out #######
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
                     tabname="Res.Out")
  
  errors1<-results$errors
  Res.Out<-results$data
  
  Res.Out<-Res.Out[!(is.na(P1) & is.na(P2) & is.na(P3) & is.na(P4))]
  
  # Check units
  errors2<-Res.Out[is.na(M.Unit) & !(is.na(M.KO) & is.na(M.NO) & is.na(M.PO)),list(value=paste(M.Level.Name,collapse = "/")),by=B.Code
  ][,table:="Res.Out"
  ][,field:="M.Level.Name"
  ][,issue:="Amount is present, but unit is missing."
  ][order(B.Code)]
  
  # 3.14.1.1) Harmonization ########
  h_params<-data.table(h_table="Res.Out",
                       h_field=c("M.Unit"),
                       h_table_alt=c("Fert.Out"),
                       h_field_alt=c("F.Unit"))
  
  results<-harmonizer_wrap(data=Res.Out,
                           h_params=h_params,
                           master_codes = master_codes)
  
  errors2<-results$h_tasks[,issue:="Issue with units"]
  
  Res.Out<-results$data
  
  # 3.14.2) Res.Method #######
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
                     ignore_values = c("All Times","Unspecified","Not specified","All Sites"))
  
  errors3<-results$errors
  Res.Method<-results$data
  
  errors4<-check_key(parent = Res.Out,child = Res.Method,tabname="Res.Method",keyfield="M.Level.Name")
  
  # Check compulsory field
  errors5<-Res.Method[is.na(M.Tree) & is.na(M.Material),list(value=paste(M.Level.Name,collapse = "/")),by=B.Code
  ][,table:="Res.Method"
  ][,field:="M.Level.Name"
  ][,issue:="Compulsory field is NA M.Tree and M.Material"
  ][order(B.Code)]
  
    # 3.14.2.1) Harmonization ########
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
  
  # 3.14.3) Res.Composition ######
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
                     tabname="Res.Comp")
  
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
  
   # 3.14.3.1) Harmonization #######
  h_params<-data.table(h_table="Res.Comp",
                       h_field=c("M.DM.Unit","M.OC.Unit","M.N.Unit","M.TN.Unit","M.AN.Unit","M.P.Unit","M.TP.Unit","M.AP.Unit","M.K.Unit","M.pH.Method"),
                       h_table_alt=c("Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out","Soil.Out"),
                       h_field_alt=c("Soil.SOM.Unit","Soil.SOM.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.N.Unit","Soil.K.Unit","Soil.pH.Method"))
  
  results<-harmonizer_wrap(data=Res.Comp,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks5<-results$h_tasks
  
  Res.Comp<-results$data
  
  # 3.14.4) Join and save errors and harmonization tasks ######
  errors<-rbindlist(list(errors_a,errors_b,errors_c))
  error_list<-error_tracker(errors=errors,filename = "residue_structure_errors",error_dir=error_dir,error_list = error_list)
  
  errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6,errors7,errors8,errors9),fill=T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,filename = "residues_other_errors",error_dir=error_dir,error_list = error_list)
  
  h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4,h_tasks5),fill=T)
  harmonization_list<-error_tracker(errors=h_tasks,filename = "residue_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)
  
# 3.15) pH #####
data<-lapply(XL,"[[","pH.Out")
col_names<-colnames(data[[1]])
  
  # 3.15.1) pH.Out ######
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

errors1<-validator(data=pH.Out,
                   unique_cols = "pH.Level.Name",
                   tabname="pH.Out")$errors


  # 3.15.2) pH.Method ######
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
                   tabname="pH.Method")

errors2<-results$errors
pH.Method<-results$data

errors3<-check_key(parent = pH.Out,child = pH.Method,tabname="pH.Method",keyfield="pH.Level.Name")

# Check junk data issue (where Liming or Calcium Addition was accidently left in the Master Excel Template)
# These files have a pH practice present with no notes (notes would indicate it is not junk data)
check_codes<-pH.Out[pH.Level.Name=="Base" & pH.Prac=="Liming or Calcium Addition" & is.na(pH.Notes),B.Code]
# Subset to codes that have no corresponding entry in the pH.Method table (i.e. they are not described so probably junk data)
check_codes<-check_codes[!check_codes %in% pH.Method$B.Code]
errors4<-data.table(B.Code=check_codes,value="Liming or Calcium Addition",table="pH.Method",field="pH.Prac",issue="Junk data value accidently left in master template?")

errors<-rbindlist(list(errors_a,errors_b))
error_list<-error_tracker(errors=errors,filename = "pH_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4))
error_list<-error_tracker(errors=errors,filename = "pH_other_errors",error_dir=error_dir,error_list = error_list)

    # 3.15.2.1) Harmonization ########
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

# 3.16) Irrigation #####
data<-lapply(XL,"[[","Irrig.Out")
col_names<-colnames(data[[1]])

  # 3.16.1) Irrig.Out ######
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

errors2<-validator(data=Irrig.Codes,
                   unique_cols = "I.Level.Name",
                   compulsory_cols = c(I.Level.Name="I.Method",I.Level.Name="I.Strategy"),
                   tabname="Irrig.Codes")$errors
errors2<-errors2[!(grepl("Missing value",issue) & value=="Base")]


  # 3.16.2) Irrig.Method ######
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

# Fix format of date end
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
                   ignore_values = c("All Times","Unspecified","Not specified","All Sites"))

errors3<-unique(results$errors)
Irrig.Method<-results$data

errors4<-check_key(parent = Irrig.Codes,child = Irrig.Method,tabname="Irrig.Method",keyfield="I.Level.Name")


errors<-rbindlist(list(errors_a,errors_b))
error_list<-error_tracker(errors=errors,filename = "irrig_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4),use.names = T)
error_list<-error_tracker(errors=errors,filename = "irrig_other_errors",error_dir=error_dir,error_list = error_list)

  # 3.16.2.1) Harmonization #######
h_params<-data.table(h_table="Irrig.Method",
                     h_field=c("I.Unit","I.Water.Type"),
                     h_field_alt=c("I.Unit","I.Water.Type"),
                     h_table_alt=c("Irrig.Out","Irrig.Out"))

results<-harmonizer_wrap(data=Irrig.Method,
                         h_params=h_params,
                         master_codes = master_codes)

Irrig.Method<-results$data

harmonization_list<-error_tracker(errors=results$h_tasks,filename = "irrigation_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)


# 3.17) Water Harvesting #####
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

errors2<-validator(data=WH.Out,
                   unique_cols = "WH.Level.Name",
                   tabname="WH.Out")$errors

error_list<-error_tracker(errors=rbindlist(list(errors1,errors2),fill=T),filename = "wh_other_errors",error_dir=error_dir,error_list = error_list)
WH.Out[,c("N","N1"):=NULL]

# 3.18) Dates #####
data<-lapply(XL,"[[","PD.Out")
col_names<-colnames(data[[1]])

  # 3.18.1) PD.Codes ######
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

errors1<-validator(data=PD.Codes,
                   unique_cols = "PD.Level.Name",
                   tabname="PD.Codes")$errors


    # 3.18.1.1) Harmonization #######
h_params<-data.table(h_table="PD.Codes",
                     h_field=c("PD.Prac","PD.Prac.Info"),
                     h_field_alt=c("PD.Timing.Pracs","PD.Info.Pracs"),
                     h_table_alt=c("PD.Codes","PD.Codes"))

results<-harmonizer_wrap(data=PD.Codes,
                         h_params=h_params,
                         master_codes = master_codes)

PD.Codes<-results$data

harmonization_list<-error_tracker(errors=results$h_tasks,filename = "dates_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)


  # 3.18.2) PD.Out ######
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

results<-validator(data=PD.Out,
                   numeric_cols=c("PD.Date.DAS","PD.Date.DAP"),
                   date_cols=c("PD.Date.Start", "PD.Date.End"),
                   hilo_pairs = data.table(low_col="PD.Date.Start",high_col="PD.Date.End",name_field="PD.Level.Name"),
                   valid_start=valid_start,
                   valid_end=valid_end,
                   site_data=Site.Out,
                   time_data=Times.Out,
                   tabname="PD.Out",
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


errors<-rbindlist(list(errors_a,errors_b))
error_list<-error_tracker(errors=errors,filename = "dates_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4),fill=T)[order(B.Code)]
error_list<-error_tracker(errors=errors,filename = "dates_other_errors",error_dir=error_dir,error_list = error_list)

# 3.19) Harvest ######
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

errors1<-validator(data=Har.Out,
                   unique_cols = "H.Level.Name",
                   tabname="Har.Out")$errors

error_list<-error_tracker(errors=errors1,filename = "harvest_other_errors",error_dir=error_dir,error_list = error_list)


# 3.20) Other ######
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

errors1<-validator(data=Other.Out,
                   unique_cols = "O.Level.Name",
                   tabname="Other.Out")$errors


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

# 3.21) Weeding ######
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

errors1<-validator(data=Weed.Out,
                   unique_cols = "W.Level.Name",
                   tabname="Weed.Out")$errors

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
Weed.Out[]


  # 3.21.1) Harmonization #######
h_params<-data.table(h_table="Weed.Out",
                     h_field=c("W.Method","W.Freq.Time"),
                     h_field_alt=c("W.Type","W.Time.Units"),
                     h_table_alt=c("Weed.Out","Weed.Out"))

results<-harmonizer_wrap(data=Weed.Out,
                         h_params=h_params,
                         master_codes = master_codes)

Weed.Out<-results$data

harmonization_list<-error_tracker(errors=results$h_tasks,filename = "weed_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)

# 3.22) Base Practices (Base.Out) #####
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
  Weed.Code[!is.na(W.Code),c("B.Code","W.Code")],
  Chems.Code[!is.na(C.Code),c("B.Code","C.Code")]
)

Base.Out<-rbindlist(Base.Out[unlist(lapply(Base.Out,nrow))>0],use.names = F)
Base.Out<-Base.Out[,list(Base.Codes=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

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
                   compulsory_cols = c(T.Name="T.Name",T.Name="P.Product"),
                   tabname="MT.Out")

errors1<-results$errors
errors1<-errors1[value!="Unspecified"]

MT.Out<-results$data

# Error checking: look for non-matches in keyfield
keyfields<-colnames(MT.Out)[5:18]

# List of variable names as strings
prac_tabs <- c("AF.Out", "Chems.Code", "Har.Out", "Irrig.Codes", "Res.Out", "Fert.Out",
               "pH.Out", "Plant.Out", "PD.Codes", "Till.Codes", "Var.Out", "WH.Out",
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

errors<-rbindlist(list(errors1,errors2),fill = T)
error_list<-error_tracker(errors=errors,filename = "treatment_other_errors",error_dir=error_dir,error_list = error_list)

# Check for duplicate rows in make treatment table
dat<-MT.Out[B.Code=="CJ0162"]
dup_check<-dat[,..keyfields]
dat[duplicated(dup_check)]

keyfields<-unique(c(keyfields,"T.Residue.Prev","B.Code","P.Product","T.Reps","T.Start.Year","T.Start.Season"))
dup_check<-MT.Out[,..keyfields]
duplicates <- MT.Out[duplicated(dup_check)][!grepl("[.][.]",T.Name)]
duplicates<-duplicates[,list(value=paste0(T.Name,collapse="/"),n_dups=.N),by=B.Code][order(n_dups,decreasing = T)]

error_list<-error_tracker(errors=duplicates,filename = "treatment_duplicates",error_dir=error_dir,error_list = error_list)


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

  # 4.1) ***!!!TO DO!!!*** - update residue codes ####
  # I assume the issue is that there is no match for the product and therefore no associated codes
  # Will need  to cross reference to Prod and trees tabs of master_codes
  unique(MT.Out[is.na(T.Residue.Code) & 
           (!T.Residue.Prev %in% c("Burned","Grazed","Burned or Grazed","Removed","Unspecified","Other") & 
              !is.na(T.Residue.Prev)),list(P.Product,T.Residue.Prev,T.Residue.Code)])
  
  if(F){
  
    # Add codes for residues that are from Tree List rather than Product List
    master_codes$trees
    
      X<-MT.Out2[(!(T.Residue.Prev %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","NA") | is.na(T.Residue.Prev))) & is.na(T.Residue.Code),
    ][T.Comp %in% TreeCodes$Species,c("T.Comp","B.Code","N2","T.Residue.Prev")]
    X<-cbind(X,TreeCodes[match(X$T.Comp, TreeCodes$Species)])
    X[T.Residue.Prev=="Mulched (left on surface)",T.Residue.Code:=Mulched]
    X[T.Residue.Prev=="Incorporated",T.Residue.Code:=Incorp]
    X[T.Residue.Prev=="Retained (unknown if mulched/incorp.)",T.Residue.Code:=Unknown.Fate]
    
    MT.Out2[X$N2,T.Residue.Code:=X$T.Residue.Code]
    
    rm(X)
  }
  
  # 4.x) ***!!!MOVE TO COMPARISON LOGIC!!!***MT.Out: Correct Ridge & Furrow 
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
  
  # 4.4) ***!!!MOVE TO COMPARISON LOGIC!!!***: Update T.Codes ####
  if(F){
  T.Code.Fun<-function(AF.Codes,A.Codes,E.Codes,H.Codes,I.Codes,M.Codes,F.Codes,pH.Codes,PO.Codes,Till.Codes,V.Codes,WH.Codes,Weed.Code){
    X<- c(AF.Codes,A.Codes,E.Codes,H.Codes,I.Codes,M.Codes,F.Codes,pH.Codes,PO.Codes,Till.Codes,V.Codes,WH.Codes,Weed.Code)
    X<-X[!is.na(X)]
    X<-paste(unique(X[order(X)]),collapse = "-")
    return(X)
  }
  
  MT.Out[,N:=1:.N]
  MT.Out[,T.Codes:=T.Code.Fun(AF.Codes,A.Codes,E.Codes,H.Codes,I.Codes,M.Codes,F.Codes,pH.Codes,PO.Codes,Till.Codes,V.Codes,WH.Codes,Weed.Code),by="N"]
  MT.Out[,N:=NULL]
  
  rm(T.Code.Fun)
  }
  
  # 4.5) ***!!!MOVE TO COMPARISON LOGIC!!!***: Combine Aggregated Treatments - SLOW CONSIDER PARALLEL ####
  if(F){
  # MAKE SURE FERT AND VAR delims are changed from ".." to something else in Till.Level.Name (MT.Out and all Fert/Variety tabs, Data.Out,Int.Out, Rot.Out, Rot.Seq)
  # GIVEN THE ABOVE, A BETTER APPROCH IS COMPILE TABLES IN R RATHER THAN COMPILING IN EXCEL THEN AMENDING THESE TABLES AFTER ERRPR CORRECTIONS
  N<-grep("[.][.]",MT.Out$T.Name)
  Fields<-data.table(Levels=c("T.Residue.Prev",colnames(MT.Out)[grep("Level.Name",colnames(MT.Out))]),
                     Codes =c("T.Residue.Code","AF.Codes","A.Codes",NA,"E.Codes","H.Codes","I.Codes","M.Codes","F.Codes","pH.Codes",NA,"PO.Codes","Till.Codes","V.Codes","WH.Codes",NA,NA,"F.Codes"))
  Fields<-Fields[!grepl("F.Level.Name",Levels)]
  # T.Name2 - uses "..." delim, T.Name retains original ".." delim 
  
  Fields<-rbind(Fields[Levels!="F.Level.Name"],data.table(Levels=F.Master.Codes,Codes=paste0(F.Master.Codes,".Code")))
  
  MT.Out2<-rbindlist(pblapply(1:nrow(MT.Out),FUN=function(i){
    
    if(i %in% N){
      
      # Deal with ".." delim used in Fert tab and Varieties tab that matches ".." delim used to aggregate treatments in MT.Out tab
      # Above should not be required anyone as Var delim changed to "$$" and combined fertilizers disaggregated.
      
      Trts<-MT.Out[i,c("T.Name","F.Level.Name","V.Level.Name")]  
      F.N<-grep("[.][.]",Trts$F.Level.Name)
      
      if(length(F.N)>0){
        for(j in F.N){
          Trts$T.Name[F.N]<-gsub(Trts$F.Level.Name[F.N],gsub("[.][.]","---",Trts$F.Level.Name[F.N]),Trts$T.Name[F.N])
        }
      }
      
      V.N<-grep("[.][.]",Trts$V.Level.Name)
      if(length(V.N)>0){
        for(j in V.N){
          Trts$T.Name[V.N]<-gsub(Trts$V.Level.Name[V.N],gsub("[.][.]","---",Trts$V.Level.Name[V.N]),Trts$T.Name[V.N])
        }
      }
      
      Trts<-unlist(strsplit(Trts$T.Name,"[.][.]")) 
      
      Trts2<-Trts
      Trts<-gsub("---","..",Trts)
      Study<-MT.Out[i,B.Code]
      
      Y<-MT.Out[T.Name %in% Trts & B.Code == Study]
      
      # Aggregated Treatments: Split T.Codes & Level.Names into those that are the same and those that differ between treatments
      # This might need some more nuance for fertilizer treatments?
      Fields1<-Fields
      
      # Exclude Other, Chemical, Weeding or Planting Practice Levels if they do no structure outcomes.
      Exclude<-c("O.Level.Name","P.Level.Name","C.Level.Name","W.Level.Name")[apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure")],2,unique)!="Yes" | is.na(apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure")],2,unique))]
      Fields1<-Fields1[!Levels %in% Exclude]
      
      
      # Exception for residues from experimental crop (but not M.Level.Name as long as multiple products present
      # All residues set the the same code (removing N.fix/Non-N.Fix issue)
      # Fate labels should not require changing
      if(length(unique(Y$T.Comp))>1){
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
        
        COLS2<-gsub("F.Level.Name","F.Level.Name2",COLS)
        
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
      row.names(Y)<-1
      
      
      # Do not combine the Treatment names, keep this consistent with Enter.Data tab
      Y$T.Name2<-Y$T.Name
      Y$T.Name<-MT.Out[i,T.Name]
      Y$T.Agg.Levels<-Agg.Levels
      Y$T.Agg.Levels2<-Agg.Levels2
      Y$T.Agg.Levels3<-Agg.Levels3
      Y$T.Codes.No.Agg<-CODES.OUT
      Y$T.Codes.Agg<-CODES.IN
      Y
    }else{
      Y<-MT.Out[i]
      Y$T.Name2<-Y$T.Name
      Y$T.Agg.Levels<-NA
      Y$T.Agg.Levels2<-NA
      Y$T.Agg.Levels3<-NA
      Y$T.Codes.No.Agg<-NA
      Y$T.Codes.Agg<-NA
      Y
    }
    
  }))
  
  # Reset N
  MT.Out2[,N2:=1:nrow(MT.Out2)]
  
  # Check for missing values in B.Codes - table should have 0 rows
  unique(MT.Out2[B.Code==""]) 
  unique(MT.Out[B.Code==""])
  
  # Check for any remaining "aggregated" codes - table should have 0 rows
  unique(MT.Out2[grep("Aggregated",MT.Out2$T.Codes),"B.Code"])
  
  # Validate aggregated practices
  #write.table(MT.Out2[grep("[.][.]",T.Name),c("B.Code","T.Name2","T.Agg.Levels","T.Agg.Levels2","T.Codes.No.Agg","T.Codes.Agg","N2")],"clipboard-256000",row.names=F,sep="\t")
  #View(MT.Out2[grep("[.][.]",T.Name),c("B.Code","T.Name2","T.Agg.Levels","T.Agg.Levels2","T.Codes.No.Agg","T.Codes.Agg","N2")])
  
  # MT.Out2: Update final treatment codes (T.Codes) for aggregrated treatments
  MT.Out2[grep("[.][.][.]",T.Name2),T.Codes:=T.Codes.No.Agg]
  }
  
  # 4.6) ***!!!MOVE TO COMPARISON LOGIC!!!***: Deal with aggregated products  #####
  if(F){
  X<-unlist(pblapply(1:nrow(MT.Out2),FUN=function(i){
    Fate<-unlist(strsplit(MT.Out2$T.Residue.Prev[i],"[.][.][.]"))
    Y<-unlist(strsplit(MT.Out2$T.Comp[i],"[.][.][.]"))
    Y<-lapply(Y,strsplit,"[.][.]")
    if(length(Y)==1 & length(Fate)>1){
      Y<-rep(Y,length(Fate))
    }
    
    Y<-lapply(1:length(Y),FUN=function(j){
      Z<-unlist(Y[[j]])
      if(!is.na(Fate[j]) & Fate[j] == "Retained (unknown if mulched/incorp.)"){
        A<-EUCodes$Unknown.Fate[match(Z,EUCodes$Product.Simple)]
        B<-TreeCodes$Unknown.Fate[match(Z,TreeCodes$Species)]
      }else{
        
        if(!is.na(Fate[j]) & Fate[j] == "Mulched (left on surface)"){
          A<-EUCodes$Mulched[match(Z,EUCodes$Product.Simple)]
          B<-TreeCodes$Mulched[match(Z,TreeCodes$Species)]
        }else{
          
          if(!is.na(Fate[j]) & Fate[j] == "Incorporated"){
            A<-EUCodes$Incorp[match(Z,EUCodes$Product.Simple)]
            B<-TreeCodes$Incorp[match(Z,TreeCodes$Species)]
          }else{
            if(!is.na(Fate[j]) & any(Fate[j] %in% c("Removed","Grazed","Burned"))){
              if(Fate[j]=="Removed"){A<-"h35"}
              if(Fate[j]=="Grazed"){A<-"h39"}
              if(Fate[j]=="Burned"){A<-"h36"}
              B<-NA
              
            }else{
              A<-NA
              B<-NA
            }}}}
      
      A<-apply(rbind(A,B),2,FUN=function(X){X[!is.na(X)]})
      
      if(length(unique(A))>1){
        A[grep("b41",A)]<-"b41"
        A[grep("b40",A)]<-"b40"
        A[grep("b27",A)]<-"b27"
        A[A %in% c("a16","a17")]<-"a15"
        A[A %in% c("a16.1","a17.1")]<-"a15.1"
        A[A %in% c("a16.2","a17.2")]<-"a15.2"
      }
      
      
      A<-paste0(unique(A[order(A)]),collapse = "-")
      if(A %in% c("NA","")){NA}else{A}
      
    })
    
    if(length(unique(Y))==1){
      unique(Y)
    }else{
      paste(Y,collapse="...")
    }
  }))
  
  # T.Out2[,T.Residue.Code2:= X]
  # MT.Out2[T.Residue.Code2=="NA",T.Residue.Code2:= NA]
  # View(unique(MT.Out2[!(is.na(T.Residue.Code2) & is.na(T.Residue.Code)),c("T.Residue.Code","T.Residue.Code2","T.Comp")]))
  
  MT.Out2[,T.Residue.Code:= X]
  MT.Out2[T.Residue.Code=="NA",T.Residue.Code:= NA]
  
  # Find missing values in T.Residue.Prev field (Need to have Rot.Out table in environment first)
  # X<-unlist(unique(MT.Out2[!grep("[.][.][.]",T.Name2)][is.na(T.Residue.Prev),"B.Code"]))
  # write.table(data.table(X[!(X %in% Rot.Out$B.Code | X %in% Int.Out$B.Code | X %in% Animals.Out$B.Code | X %in% PO.Out$B.Code | X %in% E.Out$B.Code)]),"clipboard",row.names = F,sep="\t")
  
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
setnames(Int.Out,c("Practice","IN.Prods.Alll"),c("I.Practice","IN.Prods.All"),skip_absent=T)

results<-validator(data=Int.Out,
                   numeric_cols = c("IN.Start.Year","IN.Reps"),
                   unique_cols = "IN.Level.Name",
                   compulsory_cols = c(IN.Level.Name="I.Practice"),
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

error_list<-error_tracker(errors=rbind(errors1,errors2,fill=T),filename = "intercrop_other_errors",error_dir=error_dir,error_list = error_list)

  # Question: are aggregated practices allowed in intercropping tab? ######
  
  # 5.1)  ***!!!TO DO!!!*** Update Aggregated Treatment Delimiters #####
  if(F){
  # Agg Treat = "..." 
  
  Int.Out[,IN.Comp1:=gsub("[.][.]","...",IN.Comp1)
          ][,IN.Comp2:=gsub("[.][.]","...",IN.Comp2)
            ][,IN.Comp3:=gsub("[.][.]","...",IN.Comp3)
              ][,IN.Comp4:=gsub("[.][.]","...",IN.Comp4)]
  }
  
  
  # 5.x) ***!!!TO DO!!!*** - update products and residue codes #####
  if(F){
    
    # 1) Update fate from MT.Out table if NA
    # 1.1) Split IN.Residue.Fate
    
    # 2) Update residue codes from MT.Out sheet
    
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
        NA
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
    
    # 2) Simplify residue fate values
    
    Int.Out[IN.Residue.Fate=="Grazed/Mulched",IN.Residue.Fate:="Mulched (left on surface)"]
    Int.Out[IN.Residue.Fate=="Burned/Removed",IN.Residue.Fate:="Removed"]
    
    # 3) Add codes for non-ERA residue practices
    if(F){
      # Check there are not contradictory codes btw MT.Out2 and Int.Out
      View(Int.Out[!is.na(IN.Residue.Code) & IN.Residue.Fate %in% c("Removed","Grazed","Burned","Mulched (left on surface)"),
                   list(IN.Residue.Code,IN.Residue.Fate,B.Code)])
      
    }
    
    Int.Out[is.na(IN.Residue.Code) & IN.Residue.Fate == "Removed",IN.Residue.Code:="h35"]
    Int.Out[is.na(IN.Residue.Code) & IN.Residue.Fate == "Grazd",IN.Residue.Code:="h39"]
    Int.Out[is.na(IN.Residue.Code) & IN.Residue.Fate == "Burned",IN.Residue.Code:="h36"]
    
    # If residues are NA, but a residue fate for the component is specified then we classify the residues based on
    # matching the products to the residue fate specified.
    
    # Int.Out: Update In.Res column codes for residues that are from Tree List ####
    Int.Out[,N:=1:.N]
    Z<-Int.Out[is.na(IN.Residue.Code) & !IN.Residue.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified") & !is.na(IN.Residue.Fate),]
    
    X<-Z[IN.Prod1 %in% TreeCodes$Species | IN.Prod2 %in% TreeCodes$Species | IN.Prod3 %in% TreeCodes$Species |IN.Prod4 %in% TreeCodes$Species,
         c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4","B.Code","N","IN.Residue.Fate")]
    
    if(nrow(X)>1){
      X<-rbindlist(lapply(c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4"),FUN=function(COL){
        N2<-match(unlist(X[,..COL]), TreeCodes$Species)
        if(length(N2)!=0){
          X<-cbind(X,TreeCodes[match(unlist(X[,..COL]), TreeCodes$Species)])
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
    
    # Int.Out: Update In.Res column codes for residues that are from Product List #####
    X<-Z[IN.Prod1 %in% EUCodes$Product.Simple | IN.Prod2 %in% EUCodes$Product.Simple | IN.Prod3 %in% EUCodes$Product.Simple |IN.Prod4 %in% EUCodes$Product.Simple,
         c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4","B.Code","N","IN.Residue.Fate")]
    
    X<-rbindlist(lapply(c("IN.Prod1","IN.Prod2","IN.Prod3","IN.Prod4"),FUN=function(COL){
      N2<-match(unlist(X[,..COL]),EUCodes$Product.Simple)
      if(length(N2)!=0){
        X<-cbind(X,EUCodes[match(unlist(X[,..COL]), EUCodes$Product.Simple)])
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
    Int.Out[,IN.R.Codes.Shared:=as.character(Code.Shared.Fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4,Shared=T)),by="N"]
    Int.Out[,IN.R.Codes.Diff:=as.character(Code.Shared.Fun(IN.Res1,IN.Res2,IN.Res3,IN.Res4,Shared=F)),by="N"]
    
    
  }
  
  # 5.x) ***!!!TO DO!!!*** -  Update T.Codes
  if(F){
    # (for updated fertilizer codes & agg practices)  ####
    # Slow consider using parallel
    # Need rules for IN.T.Codes column? (keep all practices as per current method? keep common practices? keep if 50% or greater?)
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
    
    Int.Out<-rbindlist(pblapply(1:nrow(Int.Out),FUN=function(i){
      # print(i)
      X<-Int.Out[i,]
      Y<-unlist(X[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")])
      Y<-Y[!(Y==0 | is.na(Y))]
      Y<-unique(unlist(strsplit(MT.Out2[paste0(MT.Out2$T.Name2,MT.Out2$B.Code) %in% paste0(Y,X$B.Code),T.Codes],"-"))) 
      if(length(Y)!=0){
        Y<-Y[!is.na(Y)]
        Y<-c(Y,X$IN.Code)
        Y<-paste0(Y[order(Y)],collapse = "-")
        X$IN.T.Codes<-Y
      }
      
      Y<-unlist(X[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")])
      Z<-data.table(IN.Comp1.T.Codes=as.character(NA),IN.Comp2.T.Codes=as.character(NA),IN.Comp3.T.Codes=as.character(NA),IN.Comp4.T.Codes=as.character(NA))
      
      N<-MT.Out2[match(paste0(Y,X$B.Code),paste0(MT.Out2$T.Name2,MT.Out2$B.Code)),T.Codes]
      
      if(length(N)>0){
        Z[1,1:length(N)]<-as.list(N)
      }
      
      Z<-cbind(X,Z)
      
      # Update Combined T.Codes Column
      Z[,IN.T.Codes:=as.character(Code.Comb.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes))
      ][,IN.T.Codes.Shared:=as.character(Code.Shared.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes,Shared=T))
      ][,IN.T.Codes.Diff:=as.character(Code.Shared.Fun(IN.Comp1.T.Codes,IN.Comp2.T.Codes,IN.Comp3.T.Codes,IN.Comp4.T.Codes,Shared=F))]
      
    }))
  }
  
  # 5.x) ***!!!TO DO!!!*** -Special rule for when reduced & zero-tillage are both present ####
  # Where both these practices are present then count the combined practice as reduced only, by removing zero till
  
  if(F){
    N<-Int.Out[,grepl("b39",IN.T.Codes) & grepl("b38",IN.T.Codes)]
    
    # Remove b38 & b39 codes from difference col
    Int.Out[N,IN.T.Codes.Diff:=paste(unlist(strsplit(gsub("-b38|b38-|b38|-b39|b39-|b39","",IN.T.Codes.Diff[1]),"***",fixed = T)),collapse="***"),by=IN.T.Codes.Diff]
    
    # Remove b39 codes from IN.T.Codes col
    Int.Out[N,IN.T.Codes:=gsub("-b39|b39-|b39","",IN.T.Codes[1]),by=IN.T.Codes]
    
    # Add b38 code to shared col
    Int.Out[N,IN.T.Codes.Shared:=paste(sort(c(unlist(strsplit(IN.T.Codes.Shared[1],"-")),"b38")),collapse="-"),by=IN.T.Codes.Shared]
  }
  
  # 5.x) ***!!!TO DO!!!*** - Combine Products ####
  # A duplicate of In.Prods.All column, but using a "-" delimiter that distinguishes productsthat contribute to a system outcome from products aggregated 
  # in the Products tab (results aggregated across products) which use a ".." delim.
  if(F){
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
  }
  # 5.x) ***!!!TO DO!!!*** - Update Intercropping Delimiters ####
  if(F){
    Int.Out$IN.Level.Name2<-apply(Int.Out[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")],1,FUN=function(X){
      X<-X[!is.na(X)]
      X<-paste0(X[order(X)],collapse = "***")
    })
  }
  
  # 5.x) ***!!!TO DO!!!*** - Structure ####
  if(F){
    X<-MT.Out2[,c("C.Structure","P.Structure","W.Structure","O.Structure")]
    X[grepl("Yes",C.Structure),C.Structure:=MT.Out2[grepl("Yes",C.Structure),C.Level.Name]]
    X[grepl("Yes",P.Structure),P.Structure:=MT.Out2[grepl("Yes",P.Structure),P.Level.Name]]
    X[grepl("Yes",O.Structure),O.Structure:=MT.Out2[grepl("Yes",O.Structure),O.Level.Name]]
    X[grepl("Yes",W.Structure),W.Structure:=MT.Out2[grepl("Yes",W.Structure),W.Level.Name]]
    
    X<-pbapply(X,1,FUN=function(X){
      X<-paste(unique(X[!(is.na(X)|X=="")]),collapse=":::")
      if(is.null(X)|X==""){
        NA
      }else{
        X
      }
    })
    
    MT.Out2[,Structure.Comb:=X][!is.na(Structure.Comb),Structure.Comb:=paste(T.Comp,Structure.Comb)]
    
    I.Cols<-c("B.Code","T.Name2","T.Comp",grep("Structure",colnames(MT.Out2),value=T))
    X<-data.table(MT.Out2[,..I.Cols],Structure=X)
    
    X[!is.na(Structure),Structure:=paste(T.Comp,Structure)]
    
    if(F){
      unique(X[grep("[.][.][.]",Structure),c("B.Code","Structure")])
    }
    
    Z<-MT.Out2[,paste(B.Code,T.Name2)]
    Z<-unlist(pblapply(1:nrow(Int.Out),FUN = function(i){
      
      Y<-paste(Int.Out[i,B.Code],unlist(strsplit(Int.Out[i,IN.Level.Name2],"[*][*][*]")))
      
      N<-match(Y,Z)
      
      # Hack to fix strange order error coming from the aggregate script used MT.Out2, this only effected EO0053 Cowpea+10tManure+S2...Cowpea+10tManure+S1
      if(any(is.na(N))){
        if(any(grepl("[.][.][.]",Y[is.na(N)]))){
          for(j in 1:sum(is.na(N))){
            K<-paste(Int.Out[i,B.Code],paste(gsub(paste0(Int.Out[i,B.Code]," "),"",rev(unlist(strsplit(Y[is.na(N)][j],"[.][.][.]")))),collapse = "..."))
            N[is.na(N)][j]<-match(K,Z)
          }
        }}
      
      
      if(any(is.na(N))){
        cat(paste("\n No Match: i = ",i," - ",Y[is.na(N)]))
        "No Match"
      }else{
        Y<-X[N,unique(Structure)]
        Y<-paste(Y,collapse = "***")
        
        if(Y=="NA"){
          NA
        }else{
          Y
        }
      }
    }))
    
    Int.Out[,IN.Structure:=Z]
    
    rm(Z,X,I.Cols)
    
    if(F){
      write.table(Int.Out[!is.na(IN.Structure) & grepl("[:][:][:]",IN.Structure),c("B.Code","IN.Level.Name","IN.Structure")],"clipboard-256000",
                  row.names=F,sep="\t")
    }
  }
  
  # 5.x) ***!!!TO DO!!!*** - Residues in System Outcomes ####
  if(F){
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
    
    Int.Out[,IN.Res.System:=Recode.Res(IN.Residue.Code),by=N]
  rm(Recode.Res)
  }
  
  
  # X) ***!!!TO DO!!!*** - check is NA start.date or reps after treatments, intercropping and rotation are merged #####
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

  # 6.1) Rot.Seq #####
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
error_list<-error_tracker(errors=unique(rbind(errors_a,errors_b)),filename = "rot_structure_errors",error_dir=error_dir,error_list = error_list)

Rot.Seq<-rbindlist(lapply(Rot.Seq,"[[","data"))  
setnames(Rot.Seq,c("Time Period","Treatment","Fate Crop Residues The Previous Season"),c("Time","R.Treatment","R.Residue.Code"),skip_absent = T)

errors1<-validator(data=Rot.Seq,
                   tabname="Rot.Seq",
                   time_data=Times.Out)$errors


# Check treatments are in make.trt tab or intercropping tab
dat<-Rot.Seq[,list(B.Code,R.Treatment)]

colnames(dat)[2]<-"T.Name"
errors3.1<-check_key(parent=MT.Out,child=dat,tabname="Rot.Seq",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code = F)

colnames(dat)[2]<-"IN.Level.Name"
errors3.2<-check_key(parent=Int.Out,child=dat,tabname="Rot.Seq",tabname_parent="Int.Out",keyfield="IN.Level.Name",collapse_on_code = F)

# Value must be missing in both treatments and intercrops
errors2<-rbind(errors3.1,errors3.2)[value!="Natural or Bare Fallow"
][,N:=.N,by=list(value,B.Code)
][N==2
][,field:="R.Treatment"
][,list(value=paste(value,collapse = "/")),by=list(B.Code,table,field)
][,issue:="Treatname name used in time sequence does not match treatment or intercrop tabs."]


    # 6.1.x) ***!!!TO DO!!!***Add in products ######
    if(F){
      #  Add in Fallow
      Rot.Seq[R.Treatment=="Natural or Bare Fallow",R.Prod1:="Fallow"]
      
      # Combine Product codes
      Rot.Seq[,R.Prod:=apply(Rot.Seq[,c("R.Prod1","R.Prod2","R.Prod3","R.Prod4")],1,FUN=function(X){
        X<-unlist(X[!is.na(X)])
        paste(X[order(X)],collapse="***")
      })]
      
      Rot.Seq<-Rot.Seq[order(B.Code,`Rotation Treatment`,Time)]
      
      setnames(Rot.Seq, "R.Res14", "R.Res4")
    }
    
    # 6.1.x) ***!!!TO DO!!!*** Add in treatment codes ######
    if(F){
      
      Int.Out[,CodeX:=paste0(IN.Level.Name2,B.Code)]
      MT.Out2[,CodeX:=paste0(T.Name2,B.Code)]
      
      Rot.Seq<-rbindlist(pblapply(1:nrow(Rot.Seq),FUN=function(i){
        X<-Rot.Seq[i]
        Y<-X$`Rotation Component`
        INT<-NA
        if(length(grep("[*][*][*]",Y))>0){
          # Get T.Codes From Intercropping
          Z<-Int.Out[CodeX %in% paste0(Y,X$B.Code),IN.T.Codes]
          INT<-Int.Out[CodeX %in% paste0(Y,X$B.Code),IN.Code]
          # Get Any Intercropped Treatment Residue Codes  # Should be defined with reference to previous crop in rot tab
          # # Not required: Z1<-Int.Out[paste0(Int.Out$IN.Level.Name2,Int.Out$B.Code) %in% paste0(Y,X$B.Code),c("IN.Res1","IN.Res2","IN.Res3","IN.Res4")]
          # # Not required: Z2<-Int.Out$IN.Residue.Code[paste0(Int.Out$IN.Level.Name2,Int.Out$B.Code) %in% paste0(Y,X$B.Code)]
        }else{
          if(Y == "Natural or Bare Fallow"){
            Z<-NA
            Z1<-NA
          }else{
            Z<-MT.Out2[CodeX %in% paste0(Y,X$B.Code),T.Codes]
            # Not required: Get Any Treatment Residue Codes  # Should be defined with reference to previous crop in rot tab
            # Not required: Z1<-MT.Out2$T.Residue.Code[paste0(MT.Out2$T.Name2,MT.Out2$B.Code) %in% paste0(Y,X$B.Code)]
          }
          
        }
        
        if(is.null(Z) | length(Z)==0){
          print(paste0("Issue with i = ",i," - ",X$B.Code))
          Z<-NA
        }
        
        if(length(Z)>1){
          print(paste0("Length>1 indicating potential duplication of excel for i = ",i," - ",X$B.Code))
          Z<-unique(Z)
        }
        
        X$R.T.Codes<-Z
        X$R.IN.Codes<-INT
        X
      }))
      
      Int.Out[,CodeX:=NULL]
      MT.Out2[,CodeX:=NULL]
      
    }
    # 6.1.x) Add in fallow codes ######
    Rot.Seq[R.Treatment == "Natural or Bare Fallow",R.T.Codes:="h24"]
    
    # 6.1.x) ***!!!TO DO!!!*** Update delimiters ######
    # Intercropping
    if(F){
    # Old code
    # Change Rotation delim to "|||"
    Rot.Out[,R.T.Level.Names.All:=gsub("…","|||",R.T.Level.Names.All)]
    # Add row index
    Rot.Out[,N:=1:nrow(Rot.Out)]
    
    X<-Rot.Out[grep("[.][.]",R.T.Level.Names.All),c("R.T.Level.Names.All","B.Code","N")]
    A<-unlist(pblapply(1:nrow(X),FUN=function(i){
      A<-X[i]
      Y<-unlist(strsplit(A$R.T.Level.Names.All,"[|][|][|]"))
      # 1) Match against Int.Out$IN.Level.Name (new delim = "***")
      N<-Y %in% Int.Out$IN.Level.Name
      if(sum(N)>0){
        Y[N]<-gsub("[.][.]","***",Y[N])
      }
      # 2) Match against MT.Out$T.Level.Name (new delim = "<><>")
      N<-Y %in% MT.Out$T.Name
      if(sum(N)>0){
        Y[N]<-gsub("[.][.]","<><>",Y[N])
      }
      paste(Y,collapse = "|||")
    }))
    
    # Check for any remaining ".." delims, these may indicate that an aggregated treatment is used in an intercrop in a rotation seq
    if(F){A[grep("[.][.]",A)]}
    
    # Convert temporary <><> delimiters in Rot.Out$R.T.Level.Names.All back to ...
    Rot.Out[X$N,R.T.Level.Names.All:=gsub("<><>","...",A)]
    Rot.Out[,N:=NULL]
    rm(A)
    
    # Validation: Check aggregated treatments that remain - are people using the aggregation correctly?
    X<-grep("[.][.]",Rot.Out$R.T.Level.Names.All)
    Y<-grep("...",Rot.Out$R.T.Level.Names.All)
    # There should be no values of ".." that are not also in "...", table below should have 0 rows
    N<-X[!X %in% Y]
    
    if(length(N)>0){
      Rot.Out..Delim.Remains<-Rot.Out[N]
      View(Rot.Out..Delim.Remains)
      rm(Rot.Out..Delim.Remains)
    }
    rm(X,Y,N)
    }
    if(F){
      # Add row index
      Rot.Seq[,N:=1:nrow(Rot.Seq)]
      
      X<-Rot.Seq[grep("[.][.]",`Rotation Component`),c("Rotation Component","B.Code","N")]
      A<-unlist(lapply(1:nrow(X),FUN=function(i){
        A<-X[i]
        Y<-A$`Rotation Component`
        # 1) Match against Int.Out$IN.Level.Name (new delim = "***")
        N<-Y %in% Int.Out$IN.Level.Name
        if(sum(N)>0){
          Y[N]<-gsub("[.][.]","***",Y[N])
        }
        # 2) Match against MT.Out$T.Level.Name (new delim = "<><>")
        N<-Y %in% MT.Out2$T.Name
        if(sum(N)>0){
          Y[N]<-gsub("[.][.]","<><>",Y[N])
        }
        
        Y
        
      }))
      
      # Check for any remaining ".." delimwhich indicate that an aggregated treatment is used in an intercrop in a rotation seq
      Rot.Out..Delim.Final.Check<-A[grep("[.][.]",A)]
      if(length(Rot.Out..Delim.Final.Check)>0){
        View(Rot.Out..Delim.Final.Check)
      }
      rm(Rot.Out..Delim.Final.Check)
      
      # Replace delimiters in Rot.Out$R.T.Level.Names.All 
      Rot.Seq[X$N,`Rotation Component`:=gsub("<><>","...",A)]
      Rot.Seq[,N:=NULL]
      rm(A)
      
    }
    
    # 6.1.x) ***!!!TO DO!!!*** Update Residue codes based on previous season ######
    if(F){
    # Add Starting Year/Season to Rot.Seq
    Rot.Out[,N:=1:.N][,ID:=paste(B.Code,R.Level.Name),by="N"]
    Rot.Seq[,N:=1:.N][,ID:=paste(B.Code,`Rotation Treatment`),by="N"]
    
    Rot.Seq[,R.Start.Year:=Rot.Out$R.Start.Year[match(ID,Rot.Out$ID)]
    ][,R.Start.Season:=Rot.Out$R.Start.Season[match(ID,Rot.Out$ID)]
    ][,Year:=  unlist(lapply(strsplit(Rot.Seq$Time,"[.]"),"[",1))
    ][,Season:= as.character(unlist(lapply(strsplit(Rot.Seq$Time,"[.]"),"[",2)))]
    
    Rot.Seq[grep("-5",Year),Season:="Off"][,Year:=gsub("-5","",Year)]
    
    # Does min start year = min year? (is a complete temporal sequence presented or just part of the sequence)
    # I don't think enough attention was paid to the start season for it to be useful here.
    Rot.Seq[,Full.Seq:=F][,Min.Year:=min(Year),by="ID"][Min.Year<=R.Start.Year,Full.Seq:=T][grep("-",Year),Full.Seq:=NA]
    
    # Add column that shows product of the previous season
    
    Rot.Seq<-rbindlist(pblapply(unique(Rot.Seq$ID),FUN=function(X){
      #print(X)
      Y<-Rot.Seq[ID==X]
      Y[Season=="Off",Season:=NA]
      # Is sequence in order?
      N<-as.numeric(as.character(Y$Season))/10
      N[is.na(N)]<-0
      N<-N+as.numeric(Y$Year)
      
      Y[,Seq.Ordered:=all(N[2:length(N)]>=N[1:(length(N)-1)])]
      
      
      if(!Y$Full.Seq[1]){
        # Deal with sequences that have a starting date before the start of the sequence provided - we can guess what 
        Z<-unlist(Y[1:(nrow(Y)-1),R.Prod])
        # Find what previous crop for first value in sequence should have been
        Z<-c(Z[match(Z[1],Z[2:length(Z)])],Z)
        Y[,R.Prod.Prev:=Z]
      }else{
        # If full sequence provided then assume we do not know about the fate of residues
        Y[,R.Prod.Prev:=c(NA,unlist(Y[1:(nrow(Y)-1),R.Prod]))]
      }
      
      Y
    }))
    
    # Rot.Seq: Update residue codes cross-referencing to MASTERCODES sheets
    
    # Set Grazed/Mulched to be Mulched (as this value indicates at least some mulching took place)
    Rot.Seq[R.Resid.Fate=="Grazed/Mulched",R.Resid.Fate:="Mulched (left on surface)"]
    
    # ISSUE - ONLY DEALS WITH UP TO FOUR AGGREGATED PRODUCTS IN A ROTATION COMPONENT - MORE EXIST
    
    # Split into columns to deal with aggregated products or intercropped treatments
    X<- strsplit(gsub("[.][.]","***",Rot.Seq$R.Prod.Prev),"[*][*][*]")
    Rot.Seq[,R.Prod.Prev.1:=lapply(X,"[",1)
    ][,R.Prod.Prev.2:=lapply(X,"[",2)
    ][,R.Prod.Prev.3:=lapply(X,"[",3)
    ][,R.Prod.Prev.4:=lapply(X,"[",4)]
    
    # Issue - we need to have residue codes for unimproved fallows
    # Easy Fix - Add Fallow to EU codes
    EUCodes[nrow(EUCodes)+1,] <- NA
    EUCodes[nrow(EUCodes),Product.Simple:="Fallow"
    ][Product.Simple=="Fallow",Mulched:="b27"
    ][Product.Simple=="Fallow",Incorp:="b41"
    ][Product.Simple=="Fallow",Unknown.Fate:="b40"]
    
    # Add codes for residues that are from Tree List
    X1<-do.call("cbind",lapply(paste0("R.Prod.Prev.",1:4),FUN=function(Y){
      Z<-Rot.Seq
      Z[,R.Residues.Codes:=NA]
      Z$N<-unlist(Z[,..Y]) %in% TreeCodes$Species & (!(Z$R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","NA") | is.na(Z$R.Resid.Fate)))
      X<-TreeCodes[match(unlist(Z[N==T,..Y]), TreeCodes$Species)]
      
      Z[N==T & R.Resid.Fate=="Mulched (left on surface)", R.Residues.Codes:=X$Mulched[which(unlist(Z[N==T,R.Resid.Fate])=="Mulched (left on surface)")]]
      Z[N==T & R.Resid.Fate=="Incorporated", R.Residues.Codes:=X$Incorp[which(unlist(Z[N==T,R.Resid.Fate])=="Incorp")]]
      Z[N==T & R.Resid.Fate=="Retained (unknown if mulched/incorp.)", R.Residues.Codes:=X$Unknown.Fate[which(unlist(Z[N==T,R.Resid.Fate])=="Retained (unknown if mulched/incorp.)")]]
      Z$R.Residues.Codes
    }))
    colnames(X1)<-paste0("Trees",1:4)
    
    # Add codes for residues that are from EU List
    X2<-do.call("cbind",lapply(paste0("R.Prod.Prev.",1:4),FUN=function(Y){
      Z<-Rot.Seq
      Z[,R.Residues.Codes:=NA]
      Z$N<-unlist(Z[,..Y]) %in% EUCodes$Product.Simple & (!(Z$R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","NA") | is.na(Z$R.Resid.Fate)))
      X<-EUCodes[match(unlist(Z[N==T,..Y]), EUCodes$Product.Simple)]
      
      Z[N==T & R.Resid.Fate=="Mulched (left on surface)", R.Residues.Codes:=X$Mulched[which(unlist(Z[N==T,R.Resid.Fate])=="Mulched (left on surface)")]]
      Z[N==T & R.Resid.Fate=="Incorporated", R.Residues.Codes:=X$Incorp[which(unlist(Z[N==T,R.Resid.Fate])=="Incorp")]]
      Z[N==T & R.Resid.Fate=="Retained (unknown if mulched/incorp.)", R.Residues.Codes:=X$Unknown.Fate[which(unlist(Z[N==T,R.Resid.Fate])=="Retained (unknown if mulched/incorp.)")]]
      Z$R.Residues.Codes
    }))
    colnames(X2)<-paste0("EUs",1:4)
    
    # Remove Fallow from EUCodes
    EUCodes<-EUCodes[Product.Simple!="Fallow"]
    
    # 1) Combine cols in X1 and X2 
    X1<-data.table(X1,X2)[is.na(Trees1),Trees1:=EUs1
    ][is.na(Trees2),Trees2:=EUs2
    ][is.na(Trees3),Trees3:=EUs3
    ][is.na(Trees4),Trees4:=EUs4
    ][,EUs1:=NULL][,EUs2:=NULL][,EUs3:=NULL][,EUs4:=NULL]
    
    names(X1)<-paste0("R.Res.Prev",1:4)
    
    Rot.Seq<-cbind(Rot.Seq,X1)
    rm(X1,X2)
    
    # 2) Join using appropriate delimeter & update Rot.Seq - R.Residues.Codes
    Res.Fun<-function(A,B,C,D,delim){
      X<-paste0(c(A,B,C,D)[!is.na(c(A,B,C,D))],collapse = delim)
      if(length(X)==0 | X==""){
        NA
      }else{
        X
      }
    }
    
    Rot.Seq[,N:=1:.N]
    Rot.Seq[,R.Residues.Codes:=R.Res.Prev1]
    Rot.Seq[grep("[*][*][*]",`Rotation Component`),R.Residues.Codes:=Res.Fun(R.Res.Prev1,R.Res.Prev2,R.Res.Prev3,R.Res.Prev4,"***"),by="N"]
    Rot.Seq[grep("[.][.][.]",`Rotation Component`),R.Residues.Codes:=Res.Fun(R.Res.Prev1,R.Res.Prev2,R.Res.Prev3,R.Res.Prev4,"..."),by="N"]
    
    # 3) Update Non ERA residue codes (control codes)
    Rot.Seq[R.Resid.Fate == "Removed",R.Residues.Codes:="h35"]
    Rot.Seq[R.Resid.Fate == "Grazed",R.Residues.Codes:="h39"]
    Rot.Seq[R.Resid.Fate == "Burned",R.Residues.Codes:="h36"]
    
    # Rot.Out: List papers for QC
    # Papers with residues applied in first season
    if(F){
      Rot.Out.First.Season.Residues<-Rot.Seq[!R.Resid.Fate %in% c("Removed","Incorporated","Unspecified","Grazed","Burned") & is.na(R.Residues.Codes),c("B.Code","Rotation Treatment","Time","R.Start.Year","Rotation Component","R.Residues.Codes","R.Resid.Fate","R.Res.Prev1","R.Res.Prev2")] 
      View(Rot.Out.First.Season.Residues)
      rm(Rot.Out.First.Season.Residues)
    }
    
    # List Rotation Papers
    #write.table(unique(Rot.Out[,B.Code]),"clipboard",row.names = F,sep="\t")
    
    
    }
    # 6.1.x) ***!!!TO DO!!!*** Add Rotation Codes and remove codes for the first year of sequence ######
    if(F){
    Rot.Seq[,R.Code:=Rot.Out$R.Code[match(ID,Rot.Out$ID)]]
    
    # Find First Time that Product Switches 
    # ISSUE - DOES THIS NEED TO TAKE INTO ACCOUNT PRECEEDING CROP? 
    # Answer: If preceding crop is the same there is no rotation if it differsmthen there is,this should not be an isssue.
    P.Switch<-function(X){
      Y<-which(X!=X[1])
      if(length(Y)==0){
        rep(F,length(X))
      }else{
        Y<-Y[1]:length(X)  
        c(rep(F,length(1:(Y[1]-1))),rep(T,length(Y)))
      }
    }
    
    Rot.Seq[,Prod.Switch:=P.Switch(R.Prod),by="ID"][,Prod.Switch.All.F:=if(sum(Prod.Switch)==0){T}else{F},by="ID"]
    }

    # 6.1.x) ***!!!TO DO!!!*** Add structure ######
    if(F){
        X<-data.table(
        MT.Out2[match(Rot.Seq[,paste(B.Code,`Rotation Component`)],MT.Out2[,paste(B.Code,T.Name2)]),Structure.Comb],
        Int.Out[match(Rot.Seq[,paste(B.Code,`Rotation Component`)],Int.Out[,paste(B.Code,IN.Level.Name2)]),IN.Structure]
        )
        Join.Fun<-function(X,Y){c(X,Y)[!is.na(c(X,Y))]}
        X<-X[,N:=1:.N][,R.Structure:=Join.Fun(V1,V2),by=N][,R.Structure]
        Rot.Seq[,R.Structure:=X]
        rm(X,Join.Fun)
    }
    # 6.1.x) ***!!!TO DO!!!***  Other validation ######
    #  View Residues of Aggregated Products
    if(F){
      Rot.Out_Agg.Prod_Res<-Rot.Seq[grep("[.][.]",R.Prod.Prev)][!R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified")]
      View(Rot.Out_Agg.Prod_Res)
      rm(Rot.Out_Agg.Prod_Res)
    }
    
    #  View Residues of Trees **THIS TITLE DOES NOT MAKE SENSE**
    if(F){
      Rot.Out_Tree_Residues<-Rot.Seq[!is.na(R.Resid.Fate) & R.Resid.Fate !="Unspecified" & is.na(R.Residues.Codes)]
      View(Rot.Out_Tree_Residues)
      rm(Rot.Out_Tree_Residues)
    }

  # 6.2) Rot.Out #####
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
  
  errors3<-validator(data=Rot.Out,
                     tabname="Rot.Out",
                     unique_cols = "R.Level.Name",
                     compulsory_cols = c(R.Level.Name="R.Practice"),
                     time_data=Times.Out)$errors
  
  error_list<-error_tracker(errors=rbind(errors1,errors2,errors3),filename = "rot_other_errors",error_dir=error_dir,error_list = error_list)

    # 6.2.1) ***!!!TO DO!!!*** Update/add fields from Rot.Seq ######
      # ***!!!TO DO!!!*** R.T.Level.Names.All
      # ***!!!TO DO!!!*** R.T.Codes.All 
      if(F){
        # old code
        # Would it be better to have Agg Treats by component split with a "|||" as well as Cmn Trts? and retaining NA values
        # TO DO: NEEDS TO DEAL WITH (REMOVE) YEARS THAT STARTS BEFORE SEQUENCE BEGINNING #####
        # Perhaos R.T.Codes.All should be derived from Rot.Seq?
       
        
         Rot.Out$R.T.Codes.All<-unlist(lapply(1:nrow(Rot.Out),FUN=function(i){
          X<-Rot.Out[i,]
          Y<-unlist(strsplit(X$R.T.Level.Names.All,"|||",fixed=T))
          
          N<-Y %in% Int.Out$IN.Level.Name2
          if(sum(N)>0){
            Z<-Int.Out$IN.T.Codes[paste0(Int.Out$IN.Level.Name2,Int.Out$B.Code) %in% paste0(Y,X$B.Code)]
            Z<-unique(unlist(strsplit(Z,"-")))
            Z<-Z[!is.na(Z)]
          }else{
            Z<-NA
          }
          
          N<-Y %in% MT.Out2$T.Name2
          if(sum(N)>0){
            Z<-MT.Out2$T.Codes[paste0(MT.Out2$T.Name2,MT.Out2$B.Code) %in% paste0(Y,X$B.Code)]
            
            if(length(grep("[.][.][.]",Z))>0){
              A<-unique(MT.Out2$T.Codes.Agg[paste0(MT.Out2$T.Name2,MT.Out2$B.Code) %in% paste0(Y,X$B.Code)])
              A<-A[!is.na(A)]
              
              B<-unique(MT.Out2$T.Codes.No.Agg[paste0(MT.Out2$T.Name2,MT.Out2$B.Code) %in% paste0(Y,X$B.Code)])
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
        
        Rot.Out_Agg.T.Codes<-Rot.Out[grep("Agg",R.T.Codes.All)]
        if(nrow(Rot.Out_Agg.T.Codes)>0){
          View(Rot.Out_Agg.T.Codes)
        }
        rm(Rot.Out_Agg.T.Codes)
        
        # Rot.Out - Validation: Find Sequences with only one product listed
        # View(Rot.Out[-grep("[.][.]",R.All.Products)][R.User.Code!="Natural or Bare Fallow"])
        #write.table(Rot.Out[-grep("[.][.]",R.All.Products),c("R.All.Products","B.Code","R.Level.Name","R.T.Level.Names.All","R.User.Code")
        #                   ][R.User.Code!="Natural or Bare Fallow"],"clipboard",row.names = F,sep="\t")
      }
      # ***!!!TO DO!!!*** R.Residue.Codes.All
      if(F){
        # TO DO: NEEDS TO DEAL WITH (REMOVE) YEARS THAT STARTS BEFORE SEQUENCE BEGINNING #####
        # Consider adding a logical field that indicates if an observation is from a preceeding season.
        X<-Rot.Seq[,list(Residues=if(all(is.na(R.Residues.Codes))){as.character(NA)}else{paste(R.Residues.Codes,collapse = "|||")},
                         R.Residues.All = paste(unique(R.Residues.Codes[!is.na(R.Residues.Codes)])[order(unique(R.Residues.Codes[!is.na(R.Residues.Codes)]))],collapse="-")),by="ID"]
        
        X$R.Residue.Codes.All[match(Rot.Out$ID,X$ID)]
        
        Rot.Out[,R.Residue.Codes.All:=X$R.Residues.All[match(ID,X$ID)]][,R.Residues.All2:=X$Residues[match(ID,X$ID)]]
        Rot.Out[R.Residue.Codes.All=="",R.Residue.Codes.All:=NA]
        rm(X)
        
      }
      # ***!!!TO DO!!!*** R.All.Products
      # ***!!!TO DO!!!*** R.All.Structure
    # 6.2.x) ***!!!TO DO!!!*** Update delimiters in R.T.Level.Names.All ######
    # This should not be required 
  if(F){
     X<-Rot.Seq[,list(R.T.Level.Names.All2=paste0(`Rotation Component`,collapse="|||")),by=c("Rotation Treatment","B.Code")
    ][,R.ID:=paste(B.Code,`Rotation Treatment`)]
    
    Rot.Out[,R.ID:=paste(B.Code,R.Level.Name)]
    Rot.Out[,R.T.Level.Names.All2:=X$R.T.Level.Names.All2[match(Rot.Out$R.ID,X$R.ID)]]
  }
    # 6.2.x) ***!!!TO DO!!!*** Generate T.Codes & Residue for System Outcomes ####
    if(F){
    # Set Threshold for a practice to be considered present in the sequence (proportion of phases recorded)
    Threshold<-0.5
    
    X<-rbindlist(pblapply(Rot.Seq[,unique(ID)],FUN=function(TC){
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
      
      data.table(Code=TC,R.T.Codes.Sys=Y,R.Res.Codes.Sys=A,R.IN.Codes.Sys=INT)
      
    }))
    Rot.Out<-cbind(Rot.Out,X[match(Rot.Out[,paste(B.Code,R.Level.Name)],X[,Code]),c("R.T.Codes.Sys","R.Res.Codes.Sys","R.IN.Codes.Sys")])
    rm(X)
    
    Rot.Out[R.Res.Codes.Sys=="",R.Res.Codes.Sys:=NA]
    Rot.Out[R.IN.Codes.Sys=="",R.IN.Codes.Sys:=NA]
    }
  
    # 6.2.x) ***!!!TO DO!!!*** Add Practice Level Columns ######
    if(F){
    #Levels<-Fields[!Levels %in% c("P.Level.Name","O.Level.Name","W.Level.Name","C.Level.Name","T.Residue.Prev"),Levels]
    
    Levels<-Fields[!Levels %in% c("P.Level.Name","O.Level.Name","W.Level.Name","C.Level.Name","T.Residue.Prev")]
    Levels<-c(Levels[,Levels],Levels[,Codes])
    
    
    Rot.Out[,which(B.Code=="AN0088")][3]
    
    Rot.Levels<-rbindlist(pblapply(1:nrow(Rot.Out),FUN=function(i){
      
      X<-Rot.Out[i]
      
      # Note that intercropping uses "***" & aggregated trts "..."
      
      Trts.All<-unlist(strsplit(X[,R.T.Level.Names.All2],"[|][|][|]"))
      Trts.All<-unlist(strsplit(Trts.All,"[*][*][*]"))
      Trts.All<-unlist(strsplit(Trts.All,"[.][.][.]"))
      
      Trts<-MT.Out2[B.Code==X[,B.Code]][match(Trts.All,T.Name2)]
      
      Levels.Joined<-unlist(lapply(Levels,FUN=function(X){
        Y<-unlist(Trts[,..X])
        paste(Y,collapse = "---")}
      ))
      Levels.Joined<-data.table(matrix(nrow=1,ncol=length(Levels),Levels.Joined,dimnames=list(1,Levels)))
      data.table(X[,c("B.Code","R.Level.Name")],Levels.Joined)
      
    }))
    
    Rot.Levels[,R.ID:=paste(B.Code,R.Level.Name)]
    rm(Levels)
    
    
    X<-Rot.Seq[,list(Len=.N,Len.Till=sum(grepl("b38|b39",R.T.Codes))),by=c("B.Code","Rotation Treatment")
    ][Len.Till>0
    ][,Ratio:=Len.Till/Len
    ][Ratio<1 & Len>2]
    
    write.table(X,"clipboard",row.names = F,sep="\t")
      # 6.2.x) Other Validation ######
      if(F){
        # Check for sequences that have a rotation code, but no rotation in the sequence presented
        # Only improved fallow should be able to have a sequence with no change in product.
        Rot.Out_No.Rotation<-Rot.Seq[Prod.Switch.All.F==T]  
          if(nrow(Rot.Out_No.Rotation)>0){
          View(Rot.Out_No.Rotation)
        }
        rm(Rot.Out_No.Rotation)
        
        # Update Rotation Codes to NA where crop has not changed (with exception of improved fallows with no change in product)
        Rot.Seq[Prod.Switch==F & (Prod.Switch.All.F==F & !(R.Code %in% c("h24","b60.1","b60","b60.2"))),R.Code:=NA]
        
        # Rot.Out: Validation - Where do we have missing residue codes?
        Rot.Out_Missing.Res.Codes<-Rot.Seq[!R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified") & `Rotation Component` != "Natural or Bare Fallow" & 
                                             ((!is.na(R.Prod1) & is.na(R.Res.Prev1)) |(!is.na(R.Prod2) & is.na(R.Res.Prev2)) | (!is.na(R.Prod3) & is.na(R.Res.Prev4)) | (!is.na(R.Prod4) & is.na(R.Res.Prev4)))]
        
        if(F & nrow(Rot.Out_Missing.Res.Codes)>0){
          # EO0068 - Fate is reading 0 but sheet says "Unspecified - reload and see if issue resolves.
          View(Rot.Out_Missing.Res.Codes) 
        }
        rm(Rot.Out_Missing.Res.Codes)
      } 
    }

  # 6.3) ***!!!TO DO!!!*** Rot.Seq.Summ (summarise crop sequence) #####

  if(F){
  Seq.Fun<-function(A){
  paste(A[!is.na(A)],collapse = "|||")
}
  Rot.Seq.Summ<-Rot.Seq[,list(Seq=Seq.Fun(R.Prod)),by=c("B.Code","Rotation Treatment")]
  
  setnames(Rot.Seq.Summ, "Rotation Treatment", "R.Level.Name")
  
  Rot.Out$R.Prod.Seq<-Rot.Seq.Summ$Seq[match(paste(Rot.Out$R.Level.Name,Rot.Out$B.Code),paste(Rot.Seq.Summ$R.Level.Name,Rot.Seq.Summ$B.Code))]
  if(F){View(Rot.Out[,c("B.Code","R.Level.Name","R.T.Level.Names.All","R.Prod.Seq")])}
  }
  
  
# 7) Outcomes ####
  data<-lapply(XL,"[[","Out.Out")
  col_names<-colnames(data[[800]])
  
  # 7.1) Out.Out #####
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
  
  results<-validator(data=Out.Out,
                     numeric_cols=c("Out.Depth.Upper","Out.Depth.Lower","Out.NPV.Rate","Out.NPV.Time"),
                     compulsory_cols = c(Out.Code.Joined="Out.Unit"),
                     unique_cols = "Out.Code.Joined",
                     tabname="Out.Out")
  
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
  

    # 7.1.1) ***!!!TO DO!!!*** Update Fields From Harmonization Sheet ######
      if(F){
        N<-match(Out.Out[,Out.Unit],UnitHarmonization[,Out.Unit])
        Out.Out[!is.na(N),Out.Unit:=UnitHarmonization[N[!is.na(N)],Out.Unit.Correct]]
        rm(N)
        # Out.Out: Add columns from partial outcome data stored in Out.Group field
        # Out.Out[grepl("<P>",Out.Group),list(Studies=length(unique(B.Code)),Obs=length(B.Code))]
      }
      
    # 7.1.2) ***!!!TO DO!!!*** Partial Outcomes ######
    if(F){
    X<-t(Out.Out[grepl("<P>",Out.Group),strsplit(unlist(lapply(strsplit(Out.Group,"<P>"),"[[",2)),"[|]")])
    Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Name:=X[,1]]
    Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Code:=X[,2]]
    rm(X)
    }
  # 7.2) Out.Econ #####
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
  
  errors4<-validator(data=Out.Econ,
                     tabname="Out.Econ",
                     site_data = Site.Out,
                     time_data=Times.Out,
                     ignore_values = c("unspecified","All times","All sites","Not in template"))$errors[order(B.Code)]
  
  error_list<-error_tracker(errors=rbind(errors_a,errors_b),filename = "out_structure_errors",error_dir=error_dir,error_list = error_list)
  error_list<-error_tracker(errors=rbindlist(list(errors1,errors2,errors3,errors4),fill=T),filename = "out_other_errors",error_dir=error_dir,error_list = error_list)
  
# 8) Enter Data (Data.Out) ####
  data<-lapply(XL,"[[","Data.Out")
  col_names<-colnames(data[[800]])
  
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
  
  # Index products from MT.Out tab
  Data.Out[,P.Product:=NA]
  
  mdat<-Data.Out[!is.na(T.Name),list(B.Code,T.Name)][,Index:=1:.N]
  colnames(mdat)[2]<-"T.Name"
  mdat<-merge(mdat,MT.Out[,list(B.Code,T.Name,P.Product)],all.x=T)[order(Index)]
  Data.Out[!is.na(T.Name),P.Product:=mdat$P.Product]
  
  mdat<-Data.Out[is.na(T.Name) &!is.na(IN.Level.Name),list(B.Code,IN.Level.Name)][,Index:=1:.N]
  colnames(mdat)[2]<-"T.Name"
  colnames(mdat)[2]<-"IN.Level.Name"
  mdat<-merge(mdat,Int.Out[,list(B.Code,IN.Level.Name,IN.Prods.All)],all.x=T)[order(Index)]
  Data.Out[is.na(T.Name) &!is.na(IN.Level.Name),P.Product:=mdat$IN.Prods.All]
  
  # Convert "NA" to NA
  Data.Out[ED.Product.Comp=="NA",Data.Out:=NA]
  
  # Check for missing required fields that don't work with standard validator
  errors1<-unique(rbind(
    Data.Out[is.na(T.Name) & is.na(IN.Level.Name) & is.na(R.Level.Name)
             ][,list(value=paste0(unique(Out.Code.Joined),collapse="/")),by=B.Code
               ][,field:="Out.Code.Joined"
                 ][,issue:="Compulsory Treatment, Intercropping and Rotation fields are all blank (at least one value must be present)."],
    Data.Out[is.na(P.Product) & !(is.na(IN.Level.Name) & is.na(T.Name))
             ][,list(value=paste0(unique(IN.Level.Name),collapse="/")),by=B.Code
               ][,field:="IN.Level.Name/T.Name"
                 ][,issue:="Compulsory Product field is blank."]
  ))
  
  errors<-list(errors1[,list(value=paste(value,collapse="/")),by=list(B.Code,field,issue)
                   ][,table:="Data.Out"
                     ][order(B.Code)])
  
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
  
  # Check that sites match
  results<-validator(data=Data.Out,
                     numeric_cols = c("ED.Mean.T","ED.Error","ED.Reps"),
                     date_cols = c("ED.Sample.Start","ED.Sample.End"),
                     compulsory_cols = c(T.Name="Site.ID",T.Name="Time",T.Name="Out.Code.Joined",T.Name="ED.Mean.T"),
                     valid_start = valid_start,
                     valid_end = valid_end,
                     site_data = Site.Out,
                     tabname="Data.Out",
                     do_time=F,
                     convert_NA_strings=T)
  
  errors<-c(errors,list(results$errors))
  
  Data.Out<-results$data
  
  # Check that Outcomes match
  errors<-c(errors,list(check_key(parent=Out.Out,child=Data.Out[!is.na(Out.Code.Joined)],tabname="Data.Out",tabname_parent="Out.Out",keyfield="Out.Code.Joined",collapse_on_code=T)))
  # Check that Treatments match
  errors<-c(errors,list(check_key(parent=MT.Out,child=Data.Out[!is.na(T.Name)],tabname="Data.Out",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code=T)))
  errors<-c(errors,list(check_key(parent=MT.Out,child=Data.Out[!is.na(ED.Comparison1)][,T.Name:=ED.Comparison1],tabname="Data.Out",tabname_parent="MT.Out",keyfield="T.Name",collapse_on_code=T)[,field:="ED.Comparison1"]))
  errors<-c(errors,list(check_key(parent=MT.Out,child=Data.Out[!is.na(ED.Comparison2) & ED.Comparison2!="Not in template"][,T.Name:=ED.Comparison2],tabname_parent="MT.Out",tabname="Data.Out",keyfield="T.Name",collapse_on_code=T)[,field:="ED.Comparison2"]))
  
  # Check that Intercrops match
  errors<-c(errors,list(check_key(parent=Int.Out,child=Data.Out[!is.na(IN.Level.Name)],tabname="Data.Out",tabname_parent="Int.Out",keyfield="IN.Level.Name",collapse_on_code=T)))

  # Check that Rotations match
  errors<-c(errors,list(check_key(parent=Rot.Out,child=Data.Out[!is.na(R.Level.Name)],tabname="Data.Out",tabname_parent="Rot.Out",keyfield="R.Level.Name",collapse_on_code=T)))
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
  
  errors<-rbindlist(errors,fill=T)
  
  error_list<-error_tracker(errors,filename = "enterdata_other_errors",error_dir=error_dir,error_list = error_list)
  

  # Check error values used
  h_tasks1<-harmonizer(data=Data.Out, 
                       master_codes,
                       master_tab="lookup_levels",
                       h_table="Data.Out", 
                       h_field="ED.Error.Type",
                       h_table_alt="Data.Out", 
                       h_field_alt="Error.Type")$h_tasks[,issue:="Non-standard error value used."]
  
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
  #Base.Out=Base.Out,
  MT.Out=MT.Out,
  Int.Out=Int.Out,
  Rot.Out=Rot.Out,
  Rot.Seq=Rot.Seq,
  #Rot.Seq.Summ=Rot.Seq.Summ,
  #Rot.Levels=Rot.Levels,
  Out.Out=Out.Out,
  Out.Econ=Out.Econ,
  Data.Out=Data.Out
)

save(Tables,file=file.path(data_dir,paste0(project,"-",Sys.Date(),".RData")))
  
# 10) Summarize error tracking
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
rbindlist(lapply(tracking_files2,fread),use.names = T,fill = T)

# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# ***Values (Data.Out) - Reconstruct Data Table From Components (Rather than relying on Excel) ####
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 

Data.Out<-lapply(XL,"[[","Data.Out")
Data.Out<-rbindlist(pblapply(1:length(Data.Out),FUN=function(i){
  X<-Data.Out[[i]][,1:27]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("ED.Mean.T"))
  X
}))

# Data.Out: Add controls for outcomes that are ratios ####
Data.Out[,N:=1:.N]
Data.R<-Data.Out[!is.na(ED.Comparison) & !is.na(ED.Treatment),c("ED.Treatment","ED.Comparison","B.Code","N","ED.Outcome")]

# Missing practices not the ED.Table as Treatments - these need adding to the Data table
Data.R[,Missing:=match(Data.R[,paste(B.Code,ED.Comparison)],Data.Out[,paste(B.Code,ED.Treatment)])]
M.Treat<-Data.R[is.na(Missing),ED.Comparison]

Data.R<-Data.Out[Data.R[is.na(Missing),N]]
Data.R[,ED.Treatment:=M.Treat]
Data.R[,ED.Mean.T:=NA]
Data.R[,ED.Error:=NA]
Data.R[,ED.Error.Type:=NA]
Data.R[,ED.Comparison:=NA]
Data.R[,ED.Ratio.Control:=T]

Data.Out.Ratios.NoMatch.in.MT.Out2<-Data.R[!ED.Treatment %in% MT.Out2[,T.Name],unique(paste(B.Code,ED.Treatment))]
if(length(Data.Out.Ratios.NoMatch.in.MT.Out2)>0){
  View(Data.Out.Ratios.NoMatch.in.MT.Out2)
}

Data.Out[,ED.Ratio.Control:=F]

Data.Out<-rbind(Data.Out,Data.R)

rm(Data.R,M.Treat,Data.Out.Ratios.NoMatch.in.MT.Out2)

Data.Out[,N:=1:nrow(Data.Out)]

# Data.Out: Update Fields From Harmonization Sheet ####

Data.Out[,ED.Site.ID:=trimws(ED.Site.ID)]
Site.Match<-match(Data.Out[,ED.Site.ID],SiteHarmonization[,ED.Site.ID])

# If there are entries in the validation below, then this may mean a non-match in the excel between the EnterData tab
# and the Site.Out tab
Data.Out.Site.Out.No.Match<-unique(Data.Out[is.na(Site.Match) & !grepl("[.][.]",ED.Site.ID),list(B.Code,ED.Site.ID)])
if(nrow(Data.Out.Site.Out.No.Match)>0){
  View(Data.Out.Site.Out.No.Match)
}
rm(Data.Out.Site.Out.No.Match)

Data.Out[!is.na(Site.Match),ED.Site.ID:=SiteHarmonization[Site.Match[!is.na(Site.Match)],ED.Site.ID.Corrected]]

# Update Aggregated Site Name
Data.Out[grepl("[.][.]",ED.Site.ID),ED.Site.ID:=Agg.Site.Name.Fun(ED.Site.ID[1],SiteHarmonization),by=ED.Site.ID]

# Data.Out: Add MT.Out2 ####
MT.Out2[,T.ID:=paste(B.Code,T.Name),by="N2"]
Data.Out[,T.ID:=paste(B.Code,ED.Treatment),by="N"]
Data.Out[is.na(ED.Treatment),T.ID:=NA]

Data.Out<-cbind(Data.Out,MT.Out2[match(Data.Out$T.ID,MT.Out2$T.ID),-c("N2","T.ID")])

# Data.Out: Add Int.Out ####
Int.Out[,N:=1:.N][,IN.ID:=paste(B.Code,IN.Level.Name),by="N"]
Data.Out[,IN.ID:=paste(B.Code,ED.Int),by="N"]
Data.Out[is.na(ED.Int),IN.ID:=NA]

Data.Out<-cbind(Data.Out,Int.Out[match(Data.Out$IN.ID,Int.Out$IN.ID),-c("N","IN.ID")])
# Data.Out: Add Rot.Out ####
N<-match(Data.Out[,paste(B.Code,ED.Rot)],Rot.Out[,paste(B.Code,R.Level.Name)])
Data.Out<-cbind(Data.Out,Rot.Out[N,-c("B.Code")])

# Data.Out: Add Rot.Out: Validation ####
# Check for studies where Rot.Out is always NA for a ED.Rot treatment - there should be Rotation code but it is missing
Data.Out.Rot.Level.No.Match<-unique(Data.Out[!is.na(ED.Rot),list(Len=sum(!is.na(R.Code))),by=c("ED.Rot","B.Code","R.Code")][Len==0])
if(nrow(Data.Out.Rot.Level.No.Match)>0){
  View(Data.Out.Rot.Level.No.Match)
}
rm(N,Data.Out.Rot.Level.No.Match)

# Rotation Name Indexing:
# 1) Make sure rotation level names in Rot.Out are used in Data.Out tab
Rot.Out_Data.Out.Names.No.Match<-unique(Rot.Out[is.na(match(Rot.Out$R.ID,Data.Out$R.ID)),c("B.Code","R.Level.Name")])
# Need to filter out Agronomic Efficiency outcomes as per https://p4c-icraf.slack.com/archives/C012CVDRLUE/p1604474771016300?thread_ts=1604473249.009000&cid=C012CVDRLUE
# In the meantime remove DK0056 from this list
Rot.Out_Data.Out.Names.No.Match<-Rot.Out_Data.Out.Names.No.Match[!B.Code %in% c("DK0056")]

if(nrow(Rot.Out_Data.Out.Names.No.Match)>0){
  View(Rot.Out_Data.Out.Names.No.Match) 
  write.table(Rot.Out_Data.Out.Names.No.Match,"clipboard",row.names = F,sep="\t")
}
rm(Rot.Out_Data.Out.Names.No.Match)

# 2) Make sure rotation level names in Data.Out are used in Rot.Out tab
Data.Out_Rot.Names.NoMatch<-unique(Data.Out[is.na(match(Data.Out$R.ID,Rot.Out$R.ID)) & !is.na(ED.Rot),c("B.Code","ED.Rot")])
if(nrow(Data.Out_Rot.Names.NoMatch)>0){
  View(Data.Out_Rot.Names.NoMatch) 
  write.table(Data.Out_Rot.Names.NoMatch,"clipboard",row.names = F,sep="\t")
}
rm(Data.Out_Rot.Names.NoMatch)

# Data.Out: Add Publication  ####
Data.Out<-cbind(Data.Out,Pub.Out[match(Data.Out$B.Code,Pub.Out$B.Code),-c("B.Code")])

# Data.Out: Add Outcomes  ####
Data.Out<-cbind(Data.Out,Out.Out[match(Data.Out$ED.Outcome,Out.Out$Out.Code.Joined),-c("Out.Code.Joined","B.Code")])

# Data.Out: Add Outcomes: Update Crop Residue->Biomass outcomes based on crops or components reported ####
BiomassCrops<-EUCodes[Product.Subtype %in% c("Fodders","CoverCrop") & Component=="Biomass",unique(Product.Simple)]

# Find crop residue outcomes that are for biomass crops (fodders and cover crops)
Data.Out[Out.Subind == "Crop Residue Yield",Is.Biomass.Prod:=all(unlist(strsplit(ED.Product.Simple,"[.][.]")) %in% BiomassCrops),by=ED.Product.Simple]
# Replace crop residue yield with biomass yield
Data.Out[Is.Biomass.Prod==T & Out.Subind == "Crop Residue Yield",ED.Outcome:=gsub("Crop Residue Yield","Biomass Yield",ED.Outcome)
][Is.Biomass.Prod==T & Out.Subind == "Crop Residue Yield",Out.Subind:="Biomass Yield"]

# Update Outcomes 
# Which outcomes correspond to those changed?
X<-Out.Out[B.Code %in% Data.Out[Is.Biomass.Prod==T,unique(B.Code)] & Out.Subind=="Crop Residue Yield"]
# Duplicate and recode those outcomes that have changed
X[, Out.Code.Joined:=gsub("Crop Residue Yield","Biomass Yield", Out.Code.Joined)][,Out.Subind:="Biomass Yield"]
# Add modified outcomes back to Out.Out
Out.Out<-cbind(X,Out.Out)

# Subset outcomes to those in Data.Out, note we have only change the biomass join field, not the other outcomes that simple changed name. 
# Biomass is different to other changed outcomes as we are reclassifying outcomes based on the component field in the Data.Out table.
Out.Out<-Out.Out[Out.Code.Joined %in% Data.Out[,ED.Outcome]]

Data.Out[,Is.Biomass.Prod:=NULL]
rm(X)

if(F){
  # List remaining potential biomass outcomes for validation
  X<-unique(Data.Out[Out.Subind=="Crop Residue Yield" & grepl("Biomass",ED.Product.Comp) & !grepl("Stover|Leaves|Stalks|Leaf",ED.Product.Comp),list(B.Code,ED.Data.Loc,Out.Subind,ED.Product.Simple,ED.Product.Comp)])
  X[,unique(ED.Product.Comp)]
  write.table(X,"clipboard-256000",row.names = F,sep=",")
}

# Data.Out: Outcomes: Validation - Look For Non-matches & Add Codes ####

# Validation: Any values in table below indicate Outcomes used in an excel that have no match in the MASTERCODES
# Or indicate values in the Outcome table do not match outcome naming in the EnterData tab Data.Out$ED.Outcome
Data.Out_Outcomes_MCodes_NoMatch<-unique(Data.Out[!Out.Subind %in% OutcomeCodes$Subindicator,c("ED.Outcome","Out.Subind","B.Code")])
if(nrow(Data.Out_Outcomes_MCodes_NoMatch)>0){
  View(Data.Out_Outcomes_MCodes_NoMatch)
}
rm(Data.Out_Outcomes_MCodes_NoMatch)

# Add Outcomes Codes to Data.Out
Data.Out[,Out.Code:=OutcomeCodes$Code[match(Out.Subind, OutcomeCodes$Subindicator)]]

if(F){
  # Create table of outcome vs product to check for incompatible matches
  X<-rbindlist(lapply(Data.Out[,unique(Out.Subind)],FUN=function(X){
    unique(Data.Out[Out.Subind==X,c("B.Code","Out.Subind","ED.Product.Simple")])
  }))
  write.table(unique(X[,-"B.Code"]),"clipboard-256000",row.names = F,sep="\t")
  
  # Check for instances of aggregated products being associated with a crop yield
  write.table(X[grepl("[.][.]",ED.Product.Simple) & Out.Subind=="Crop Yield"],"clipboard-256000",row.names = F,sep="\t")
  
}

# Data.Out: Add Products ####
# Data.Out: Add Products: Validation Checks ####

# Add component level 1 to Data.Out (used to match component + product to EU list)
X<-Data.Out[,c("B.Code","ED.Product.Simple","ED.Product.Comp","Out.Subind")
][,EU.Comp.L1:=Comp.Codes$Comp.Level1[match(ED.Product.Comp,Comp.Codes$Component)]]

# Add code field based on product x component combined to ERA data and product (EU) MASTERCODES
X[,Code:=paste(ED.Product.Simple,EU.Comp.L1)]
EUCodes[,Code:=paste(Product.Simple,Component)]

# Validation: Check Yield Outcomes with no product component specified:
Data.Out_YieldOut_NoProductComp<-unique(X[is.na(ED.Product.Comp) & (grepl("Yield",Out.Subind)|grepl("Efficiency",Out.Subind)) & !grepl("[.][.]",ED.Product.Simple)])

if(nrow(Data.Out_YieldOut_NoProductComp)>1){
  View(Data.Out_YieldOut_NoProductComp)
  write.table(Data.Out_YieldOut_NoProductComp,"clipboard-256000",row.names = F,sep="\t")
}

rm(Data.Out_YieldOut_NoProductComp)

# Validation: Check product components that do not match component column in EU.Comp tab of MASTERCODES
Data.Out_Component_MCodes_NoMatch<-unique(X[is.na(EU.Comp.L1)& (grepl("Yield",Out.Subind)|grepl("Efficiency",Out.Subind)) & !is.na(ED.Product.Comp) & !grepl("[.][.]",ED.Product.Simple)])

if(nrow(Data.Out_Component_MCodes_NoMatch)>0){
  View(Data.Out_Component_MCodes_NoMatch)
}
rm(Data.Out_Component_MCodes_NoMatch)

# Validation: Check Product + Component Combinations Not in EU2 tab of MASTERCODES
Data.Out_Prod.Comp.Combo_MCodes_Nomatch<-unique(X[!Code %in% EUCodes$Code & !is.na(EU.Comp.L1) & !is.na(ED.Product.Comp) & 
                                                    !Out.Subind %in% c("Biomass Yield","Aboveground Biomass","Belowground Biomass") & !grepl("[.][.]",ED.Product.Simple)])

if(nrow(Data.Out_Prod.Comp.Combo_MCodes_Nomatch)>0){
  View(Data.Out_Prod.Comp.Combo_MCodes_Nomatch)
}
rm(Data.Out_Prod.Comp.Combo_MCodes_Nomatch)

# Data.Out: Add Products: Join Products to Data.Out ====
# 1) Match products in Data.Out to MASTERCODES using product x component code  ####
Data.Out[,ED.Product.Comp.L1:=Comp.Codes$Comp.Level1[match(ED.Product.Comp,Comp.Codes$Component)]]
Data.Out[,ED.Product.Code:=paste(ED.Product.Simple,ED.Product.Comp.L1)]

X<-EUCodes[match(Data.Out$ED.Product.Code,EUCodes$Code),c("EU","Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name" )]
X[,N:=1:.N]

# 2) For non-matches in 1, match products in Data.Out to MASTERCODES using product only  ####
N<-which(is.na(X$EU))
Y<-EUCodes[match(Data.Out$ED.Product.Simple[N],EUCodes$Product.Simple),c("EU","Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name" )
][,N:=which(is.na(X$EU))]

Y[,EU2:=lapply(strsplit(EU,"[.]"),"[",1)
][,EU2.Match:=EU2 %in% EUCodes$EU
][!is.na(EU2.Match),EU:=EU2
][,Component:=NA
][,EU2:=NULL
][,EU2.Match:=NULL]

# Join 1) & 2), dropping non-matches from 1)
X<-rbind(X[!is.na(EU)],Y)[order(N)]
rm(Y)

# 3) Add in Agroforestry Trees ####
NX<-is.na(X[,EU])  & Data.Out[,ED.Product.Simple] %in% TreeCodes$Product.Simple
X[NX,Product.Type :="Plant Product"]
X[NX,Product.Subtype :="Agroforestry Tree"]
X[NX,Product := Data.Out[NX,ED.Product.Simple]]
X[NX,Product.Simple := Product]
X[NX,Component  := Data.Out[NX,ED.Product.Comp.L1]]
X[NX,Latin.Name := Product]

X[NX,EU:=TreeCodes[match(Data.Out[NX,ED.Product.Simple],TreeCodes[,Product.Simple]),EU]]
rm(NX)

# 4) Repeat 1-3 but for aggregated products (SLOW consider parallel) ####
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

# 5) Join, order and cbind to Data.Out ####
# Remove aggregated products from 1) and 2)
X<-X[!grepl("[.][.]",Data.Out$ED.Product.Simple)]

X<-rbind(X,Z)[order(N)]
rm(Z,N)
X[,N:=NULL]
setnames(X,c("Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name"),paste0("EU.",c("Product.Type","Product.Subtype","Product","Product.Simple","Component","Latin.Name")))

Data.Out<-cbind(Data.Out,X)
Data.Out[,ED.Product.Code:=NULL]


# Data.Out: Product Validation ####
# Check ".." vs product yield (not allowed)

Data.Out_Yield.with.Agg.Prod<-unique(Data.Out[grep("[.][.]",ED.Product.Simple)
][Out.Subind %in% "Crop Yield",c("ED.Product.Simple","B.Code","Out.Subind","ED.Outcome")
][-grep("LER",ED.Outcome)
][-grep("Equivalent Ratio",ED.Outcome)
][-grep("equivalent ratio",ED.Outcome)])

if(nrow(Data.Out_Yield.with.Agg.Prod)>0){
  View(Data.Out_Yield.with.Agg.Prod)
  write.table(Data.Out_Yield.with.Agg.Prod, "clipboard",row.names=F,sep="\t")
}
rm(Data.Out_Yield.with.Agg.Prod)

# Check NA & "No Product Specified" against outcome (should not be a productivity outcome)

Data.Out_NoComp.with.Yield.Out<-unique(Data.Out[is.na(ED.Product.Simple) | ED.Product.Simple == "No Product Specified",c("ED.Product.Simple","B.Code","ED.Treatment","ED.Int","ED.Rot","Out.Subind","ED.Outcome")]
                                       [grep("Yield",Out.Subind)])

if(nrow(Data.Out_NoComp.with.Yield.Out)>0){
  View(Data.Out_NoComp.with.Yield.Out)
  write.table(Data.Out_NoComp.with.Yield.Out, "clipboard",row.names=F,sep="\t")     
}
rm(Data.Out_NoComp.with.Yield.Out)

# Validation: Find Non-match in aggregated treatment names
A.Non.Match<-unique(Data.Out[ED.Product.Simple %in% X[is.na(X$P.Simple)],c("ED.Product.Simple","B.Code")])
# Find Non-matachs in non-aggregated names
Non.Match<-unique(Data.Out[-grep("[.][.]",ED.Product.Simple),][is.na(EU.Product.Type) & !is.na(ED.Product.Simple),c("ED.Product.Simple","B.Code")])

Data.Out_Product_MCodes_NoMatch<-rbind(A.Non.Match,Non.Match)
if(nrow(Data.Out_Product_MCodes_NoMatch)>0){
  View(Data.Out_Product_MCodes_NoMatch)
}
rm(Data.Out_Product_MCodes_NoMatch)

# Redundant? Add Aggregated Values for type and subtype back to Data.Out
#X<-data.table(X,P.Simple,P.Type,P.Subtype)
#for(i in 1:nrow(X)){
# if(!is.na(X$X[i])){
#   N<-Data.Out$ED.Product.Simple %in% X$X[i]
#   Data.Out[N,EU.Product.Type:=X$P.Type[i]]
#   Data.Out[N,EU.Product.Subtype:=X$P.Subtype[i]]
# }
#}

# Data.Out: Recode NA Products ####
# ISSUE: Add products for aggregated treatments ####
Missing.Product<-unique(Data.Out[is.na(ED.Product.Simple) & !grepl("[.][.]",ED.Treatment), c("B.Code","ED.Outcome","ED.Product.Simple")])
if(nrow(Missing.Product)>0){
  View(Missing.Product)
  write.table(Missing.Product,"clipboard-256000",row.names = F,sep="\t")
}

rm(Missing.Product)

# Temporary Fix for soil outcomes with NA product - set product.simple to "Soil"
Data.Out[is.na(ED.Product.Simple) & grepl("Soil",ED.Outcome),ED.Product.Simple:="Soil"]
Data.Out[is.na(ED.Product.Simple) & grepl("Infiltration",ED.Outcome),ED.Product.Simple:="Soil"]


# Data.Out: Add Experimental Design  ####
Data.Out<-cbind(Data.Out,ExpD.Out[match(Data.Out$B.Code,ExpD.Out$B.Code),-c("B.Code")])

# Data.Out: Update Number of replicates =====
Data.Out[T.Reps==0,T.Reps:=NA] # Redundant? Should now be updated when reading in MT.Out

# Reps specified in EnterData tab > Rotation tab > Intercropping tab > Treatment tab
Data.Out[,Final.Reps:=if(!is.na(ED.Reps)){ED.Reps}else{
  if(!is.na(R.Reps)){R.Reps}else{
    if(!is.na(IN.Reps)){IN.Reps}else
      if(!is.na(T.Reps)){T.Reps}}},by="N"]
if(F){
  View(unique(Data.Out[is.na(Final.Reps),c("B.Code","ED.Treatment","ED.Int","ED.Rot","Final.Reps","ED.Reps","R.Reps","IN.Reps","T.Reps")]))
  write.table(unique(Data.Out[is.na(Final.Reps),c("B.Code","ED.Treatment","ED.Int","ED.Rot","Final.Reps")]),
              "clipboard=25600",row.names = F,sep="\t")
}

# Data.Out: Update Crop residues #####
# 1) IN.Residue.Code > T.Residue.Code > R.Residue.Codes.All ####
# Previously was T.Residue.Code > IN.Residue.Code but this caused issues where the intercrop received both crop and agroforesty residues, for example
# In that case the intercrop would only get the residue code of the component for which the outcome was being reported.

#Data.Out[,Final.Residue.Code:=if(!is.na(T.Residue.Code)){T.Residue.Code}else{
#    if(!is.na(IN.Residue.Code)){IN.Residue.Code}else{if(!is.na(R.Residue.Codes.All)){R.Residue.Codes.All}
#   }},by="N"]

Data.Out[,Final.Residue.Code:=if(!is.na(IN.Residue.Code)){IN.Residue.Code}else{
  if(!is.na(T.Residue.Code)){T.Residue.Code}else{if(!is.na(R.Residue.Codes.All)){R.Residue.Codes.All}
  }},by="N"]

# 2) If observation is a component of a rotation treatment update residues from the Rot.Seq table ####
# ***ISSUE*** WHAT ABOUT ROTATIONS WITH BOTH PHASES? IS THIS COVERED IN ROT.OUT SECTION (NEED TO DUPLICATE PHASES?) #####

Rot.Seq[,Temp.Code1:=paste(B.Code,`Rotation Component`,`Rotation Treatment`,Time)]
Data.Out[!is.na(ED.Rot) & !is.na(IN.Level.Name2) & ! grepl("[.][.]",ED.M.Year),Temp.Code.Int:= paste(B.Code,IN.Level.Name2,ED.Rot,ED.M.Year)]
Data.Out[!is.na(ED.Rot) & !is.na(T.Name2) & ! grepl("[.][.]",ED.M.Year),Temp.Code.Treat:= paste(B.Code,T.Name2,ED.Rot,ED.M.Year)]

# Intercrop present
X<-match(Data.Out$Temp.Code.Int,Rot.Seq$Temp.Code1)
Data.Out[!is.na(X),Final.Residue.Code:=Rot.Seq$R.Residues.Codes[X[!is.na(X)]]]
Data.Out[!is.na(X),R.Code:=Rot.Seq$R.Code[X[!is.na(X)]]]

# No Intercrop present
X<-match(Data.Out$Temp.Code.Treat,Rot.Seq$Temp.Code1)
Data.Out[!is.na(X),Final.Residue.Code:=Rot.Seq$R.Residues.Codes[X[!is.na(X)]]]
Data.Out[!is.na(X),R.Code:=Rot.Seq$R.Code[X[!is.na(X)]]]

# Tidy Up
rm(X)
Data.Out[,Temp.Code.Int:=NULL]
Data.Out[,Temp.Code.Rot:=NULL]
Rot.Seq[,Temp.Code1:=NULL]

# 3) Replace intercropping "***" or aggregation "..." delim in Final.Residue.Code ####



# Intercrops
PC1<-PracticeCodes[Practice %in% c("Agroforestry Pruning"),Code]
PC2<-PracticeCodes[Practice %in% c("Mulch","Crop Residue","Crop Residue Incorporation"),Code] 
Recode<-function(PC1,PC2,Final.Codes){
  
  X<-Final.Codes %in% PC1
  
  if(sum(X)>0){
    Final.Codes[X]<-gsub("a17","a15",Final.Codes[X])
    Final.Codes[X]<-gsub("a16","a15",Final.Codes[X])
  }
  
  X<-Final.Codes %in% PC2
  
  if(sum(X)>0){
    Y<-unlist(strsplit(Final.Codes[X],"[.]"))
    Final.Codes[X]<-Y[nchar(Y)>1]
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

# N<-grep("[*][*][*]",Data.Out[,Final.Residue.Code])
# Before<-Data.Out[N,Final.Residue.Code]

Data.Out[grep("[*][*][*]",Final.Residue.Code),Final.Residue.Code:=FunX(Final.Residue.Code),by=N]

#data.table(Before,After=Data.Out[N,Final.Residue.Code])

# Aggregations
# N<-grep("[.][.][.]",Data.Out[,Final.Residue.Code])
# Before<-Data.Out[N,Final.Residue.Code]

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

# data.table(Before,After=Data.Out[N,Final.Residue.Code])

rm(FunX,PC1,PC2,Recode)

# Check results seem sensible
if(F){
  Data.Out[,c("B.Code","T.Residue.Prev","T.Residue.Code","IN.Residue.Fate","IN.Residue.Code","R.Residue.Codes.All","Final.Residue.Code")
  ][(!is.na(T.Residue.Code) | !is.na(IN.Residue.Code) | !is.na(R.Residue.Codes.All)) & is.na(Final.Residue.Code)]
  
  Data.Out[,c("B.Code","T.Residue.Prev","T.Residue.Code","IN.Residue.Fate","IN.Residue.Code","R.Residue.Codes.All","Final.Residue.Code")
  ][!is.na(T.Residue.Code) | !is.na(IN.Residue.Code) | !is.na(R.Residue.Codes.All)]
}

# Data.Out: Update Start Year & Season #####
# ***ISSUE*** NEEDS LOGIC FOR ROT/INT as "Base"? / CONSIDER LOGIC FOR ONLY USING INT/ROT YEAR IF THEY ARE NOT BASE PRACTICES ####
Data.Out[,Final.Start.Year.Code:=if(!is.na(ED.Start.Year)){ED.Start.Year}else{
  if(!is.na(R.Start.Year)){R.Start.Year}else{
    if(!is.na(IN.Start.Year)){IN.Start.Year}else{
      if(!is.na(T.Start.Year)){T.Start.Year}
    }}},by="N"]

Data.Out[,Final.Start.Season.Code:=if(!is.na(ED.Start.Season)){ED.Start.Season}else{
  if(!is.na(R.Start.Season)){R.Start.Season}else{
    if(!is.na(IN.Start.Season)){IN.Start.Season}else{
      if(!is.na(T.Start.Season)){T.Start.Season}
    }}},by="N"]

# Data.Out: Add Site  ####
Site.Match<-match(Data.Out[,paste(B.Code,ED.Site.ID)],Site.Out[,paste(B.Code, Site.ID)])

# Make Sure of match between Data.Out and Site.Out
Data.Out.Site.No.Match2<-unique(Data.Out[is.na(Site.Match),list(B.Code,ED.Site.ID)])
Data.Out.Site.No.Match2<-Data.Out.Site.No.Match2[!grepl("[.][.]",ED.Site.ID)]

if(nrow(Data.Out.Site.No.Match2)>1){
  View(Data.Out.Site.No.Match2)
}

Data.Out<-cbind(Data.Out,Site.Out[Site.Match])

# Data.Out: Add Site: Validation ####
#Check to make sure all the site names used in the the EnterData tab were also present in the Site tab
Data.Out_Site_NoMatch<-Data.Out[ED.Site.ID!=Site.ID,c("ED.Site.ID","Site.ID")]

if(nrow(Data.Out_Site_NoMatch)>0){
  View(Data.Out_Site_NoMatch)
}

rm(Data.Out_Site_NoMatch)

# Data.Out: Aggregated Sites ####
# ***NOTE*** It would be more efficient & logical to modify the Site.Out tab then combine with Data.Out

# Calculate Lat/Lon and Buffer
X<-c("B.Code",colnames(Data.Out)[grep("Site",colnames(Data.Out))])
X<-unique(Data.Out[grep("[.][.]",ED.Site.ID),..X])[,c("B.Code","ED.Site.ID")]
X$SiteSplit<-strsplit(X$ED.Site.ID,"[.][.]")
X$SiteSplit<-lapply(1:nrow(X),FUN=function(i){
  Y<-X[i]
  paste0(Y$B.Code,"-",unlist(Y$SiteSplit))
})

Z<-paste0(Site.Out$B.Code,"-",Site.Out$Site.ID)
X$Match<-lapply(X$SiteSplit,FUN=function(Y){match(Y,Z)})

# Check for non-matches
X$NoMatch<-lapply(1:nrow(X),FUN=function(i){
  Y<-X[i]
  if(sum(is.na(unlist(Y$Match)))>0){{
    unlist(Y$SiteSplit)[which(is.na(unlist(Y$Match)))]
  }}else{
    NA
  }
})

X<-X[is.na(NoMatch)]

Site.Out.Agg<-rbindlist(lapply(1:nrow(X),FUN=function(i){
  # print(i)
  Y<-unlist(X[i,Match])
  Y<-Site.Out[Y[!is.na(Y)]]
  
  Y<-apply(Y,2,FUN=function(Z){
    # NEED TO SPLIT ON COLUMNS WHERE IT IS OK
    if(length(unique(unlist(Z)))==1){
      Z<-paste0(unique(unlist(Z)),collapse = "..")
    }else{
      Z<-paste0(unlist(Z),collapse = "..")
    }
    Z[Z=="NA"]<-NA
    Z
  })
  Y<-t(data.table(Y))
  colnames(Y)<-colnames(Site.Out)
  data.table(Y)
}))

#colnames(Site.Out.Agg)[colnames(Site.Out.Agg) %in% colnames(Data.Out)]
#colnames(Site.Out.Agg)[!colnames(Site.Out.Agg) %in% colnames(Data.Out)]

CNames<-colnames(Site.Out.Agg)[colnames(Site.Out.Agg) %in% colnames(Data.Out)]
CNames<-CNames[!CNames=="B.Code"]

Data.Out<-data.frame(Data.Out)

for(i in 1:nrow(Site.Out.Agg)){
  X<-which(!is.na(match(paste0(Data.Out$B.Code,Data.Out$ED.Site.ID),paste0(Site.Out.Agg$B.Code[i],Site.Out.Agg$Site.ID[i]))))
  Data.Out[X,CNames]<-data.frame(Site.Out.Agg[rep(i,length(X)),..CNames])
}

Data.Out<-data.table(Data.Out)

# Data.Out: Times: Validation #####

X<-Data.Out[,c("B.Code","ED.M.Year")]
X$Year.List<-strsplit(Data.Out$ED.M.Year,"[.][.]")

X$Year.List<- pblapply(X$Year.List,FUN=function(Y){
  patterns<-c(paste0("[.]",1:10),"-5")
  mgsub::mgsub(Y, patterns, rep("",length(patterns)))
})

X$High<-pblapply(X$Year.List,FUN = function(Y){
  if(is.na(Y[1])){
    NA
  }else{
    if(sum(Y>2018)>0){
      T
    }else{
      F
    }
  }
})

X$Low<-pblapply(X$Year.List,FUN = function(Y){
  if(is.na(Y[1])){
    NA
  }else{
    if(sum(Y<1950)>0){
      T
    }else{
      F
    }
  }
})

X$Diff<-pblapply(X$Year.List,FUN = function(Y){
  if(is.na(Y[1])|length(Y)<=1){
    NA
  }else{
    if((as.numeric(Y[length(Y)])-as.numeric(Y[length(Y)-1]))>1){
      T
    }else{
      F
    }
  }
})

unique(X[High==T,1:2])
unique(X[Low==T,1:2])
unique(X[Diff==T,1:2])

# ***TO DO*** Bring in any planting dates from aggregated years/sites ####

# Data.Out: Add Base Practices #####
Data.Out<-cbind(Data.Out,Base.Out[match(Data.Out$B.Code,Base.Out$B.Code),-c("B.Code")])
Data.Out<-cbind(Data.Out,Base.Out.No.Animal[match(Data.Out$B.Code,Base.Out.No.Animal$B.Code),-c("B.Code")])

# Data.Out: Validation: Error Base Codes - Check that only one base treatment is present  #### 
# 1)Varieties 
X<-unique(Var.Out[V.Base=="Yes",c("V.Var","V.Base","B.Code")][,N:=length(V.Base),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F)    
}

# 2) Agroforesty
X<-unique(AF.Out[AF.Level.Name=="Base",c("AF.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F)     
}

# 3) Tillage
X<-unique(Till.Codes[T.Level.Name=="Base",c("T.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F) 
}

# 4) Planting
X<-unique(Plant.Out[P.Level.Name=="Base",c("P.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F) 
}  
# 5) Fertilizer
X<-unique(Fert.Out[F.Level.Name=="Base",c("F.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F) 
}

# 5) Chemicals
X<-unique(Chems.Out[C.Level.Name=="Base",c("C.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
if(nrow(X)>1){
  print(X)
  write.table(X,"clipboard",sep="\t",row.names = F,col.names = F) 
}  
# 6) Residues
# CAREFUL WITH ENTRIES THAT DESCRIBE CROP RESIDUES (THESE SHOULD HAVE NO MATCH IN MAKE.TRT TAB - would not be base practice anyway?)
X<-unique(Res.Out[M.Level.Name=="Base",c("M.Level.Name","B.Code")][,N:=length(B.Code),by="B.Code"][N>1][,c("B.Code","N")])
# 7) Weeding
# 8) Harvest
# 9) pH
# 10) Water Harvesting
# 11) Irrigation
# 12) Other
# 13) Animals
# 14) Post Harvest
# 15) Energy

# Data.Out: Update Structure Fields to reflect Level name rather than "Yes" or "No" ####
Data.Out[!is.na(P.Structure),P.Structure:=P.Level.Name]
Data.Out[!is.na(O.Structure),O.Structure:=O.Level.Name]
Data.Out[!is.na(W.Structure),W.Structure:=W.Level.Name]
Data.Out[!is.na(C.Structure),C.Structure:=C.Level.Name]
Data.Out[is.na(P.Structure),P.Structure:=NA]
Data.Out[is.na(O.Structure),O.Structure:=NA]
Data.Out[is.na(W.Structure),W.Structure:=NA]
Data.Out[is.na(C.Structure),C.Structure:=NA]
Data.Out[,P.Level.Name:=P.Structure]
Data.Out[,O.Level.Name:=O.Structure]
Data.Out[,W.Level.Name:=W.Structure]
Data.Out[,C.Level.Name:=C.Structure]

# Data.Out: Reset N ####
Data.Out[,N:=NULL][,N:=1:.N]
# Data.Out: Validation: Check for Character Values in Outcomes and Errors ####
# These should be NA values for controls of ratio outcomes

Ratio.Controls<-unique(Data.Out[is.na(ED.Mean.T),c("B.Code","ED.Outcome")])
if(nrow(Ratio.Controls[!grepl("Efficiency|ARE",ED.Outcome)])>0){
  View(Ratio.Controls)
}
rm(Ratio.Controls)

# Check for any outcome values recorded as character rather than numeric
NErr<-Data.Out[!is.na(ED.Mean.T)][,ED.Mean.T:=as.numeric(ED.Mean.T)][is.na(ED.Mean.T),N]
if(length(NErr)>0){
  Out.Val.Is.Char<-Data.Out[N %in% NErr,c("B.Code","ED.Treatment","ED.Mean.T")]
  View(Out.Val.Is.Char)
  rm(Out.Val.Is.Char)
}
rm(NErr)

# Check for any error values recorded as character rather than numeric

NErr<-Data.Out[!is.na(ED.Error)][,ED.Error:=as.numeric(ED.Error)][is.na(ED.Error),N]

if(length(NErr)>0){
  Error.Val.Is.Char<-Data.Out[N %in% NErr,c("B.Code","ED.Treatment","ED.Error")]
  View(Error.Val.Is.Char)
  rm(Error.Val.Is.Char)
}
rm(NErr)

# Data.Out: Validation: Check for Errors in Planting and Harvest Dates ####
if(F){
  unique(Data[grep("/",ED.Plant.Start),list(B.Code,ED.Plant.Start)])
  unique(Data[grep("/",ED.Plant.End),list(B.Code,ED.Plant.End)])
  unique(Data[grep("/",ED.Harvest.Start),list(B.Code,ED.Harvest.Start)])
  unique(Data[grep("/",ED.Harvest.End),list(B.Code,ED.Harvest.End)])
  unique(Data[grep("/",ED.Sample.Start),list(B.Code,ED.Sample.Start)])
  unique(Data[grep("/",ED.Sample.End),list(B.Code,ED.Sample.End)])
}
# Data.Out: Add h-codes for monoculture & rainfed ####
Data.Out[T.Codes=="",T.Codes:=NA]
# Filter out papers that are animals, postharvest or energy
X.Codes<-PracticeCodesX[Theme %in% c("Energy","Animals","Postharvest") & !grepl("h",Code),Code]
T.Match<-function(T.Codes,X.Codes){
  T.Codes<-unique(unlist(strsplit(T.Codes,"-")))
  !any(T.Codes %in% X.Codes)
}

Data.Out[,Agron:=T.Match(T.Codes,X.Codes),by=B.Code]

#  Add h2-codes for monoculture

# If no diversification practice present and h2 is missing, then add h2
# We are adding to T.Codes for ease, but perhaps most logical to add to intercropping or rotation?

Add.Code<-function(T.Codes,IN.Code,R.Code){
  X<-unique(unlist(strsplit(c(T.Codes,IN.Code,R.Code),"-")))
  
  if(all(is.na(X))){
    return("h2")
  }else{
    X<-X[!is.na(X)]
    if(any(X=="h2")){
      return(T.Codes)
    }else{
      X<-c(unlist(strsplit(T.Codes,"-")),"h2")
      X<-paste(X[order(X)],collapse = "-")
      return(X)
    }
  }
}

Data.Out[Agron==T & is.na(IN.Level.Name) & is.na(R.Level.Name),T.Codes:=Add.Code(T.Codes,IN.Code,R.Code),by=N]

XPracs<-PracticeCodes[Practice %in% c("Irrigation"),Code]

Data.Out[,Irrig:=T.Match(T.Codes,XPracs),by=B.Code]

Add.Code<-function(T.Codes){
  X<-unique(unlist(strsplit(T.Codes,"-")))
  
  if(all(is.na(X))){
    return("h23")
  }else{
    X<-X[!is.na(X)]
    if(any(X=="h23")){
      return(T.Codes)
    }else{
      X<-c(unlist(strsplit(T.Codes,"-")),"h23")
      X<-paste(X[order(X)],collapse = "-")
      return(X)
    }
  }
}

Data.Out[Agron==T & Irrig==F,T.Codes:=Add.Code(T.Codes),by=N]

# Tidy up
Data.Out[,Agron:=NULL][,Irrig:=NULL]

rm(XPracs,Add.Code,T.Match)
# Data.Out: Update V.Animal.Practice [.][.][.] codes ####
Data.Out[,V.Animal.Practice:=paste(unique(unlist(strsplit(V.Animal.Practice,"[.][.][.]"))),collapse = "..."),by=N]
Data.Out[V.Animal.Practice=="NA",V.Animal.Practice:=NA]

