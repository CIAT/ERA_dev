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

# 0.2) Set directories and parallel cores ####

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
  Master<-list.files(paste0(project_dir,"/data/data_entry/",project,"/excel_data_extraction_template"),"xlsm$",full.names = T)
  
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
  overwrite<-T
  
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
                   tabname=table_name)$data


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
error_list<-error_tracker(errors=errors,filename = paste0(table_name,"_code_errors"),error_dir=error_dir,error_list = error_list)

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

errors_a<-rbindlist(lapply(Site.Out,"[[","error"))

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
                   tabname=table_name)

Site.Out<-results$data
errors1<-results$errors
# remove aggregated sites from error list
errors1<-errors1[!grepl("[.][.]",value)]

errors2<-Site.Out[(is.na(Site.Lat.Unc)|is.na(Site.Lon.Unc)) & is.na(Buffer.Manual) & !grepl("[.][.]",Site.ID)
][,list(value=paste0(unique(Site.ID),collapse = "/")),by=list(B.Code)
][,table:=table_name
][,field:="Site.ID"
][,issue:="Missing value in compulsory field location uncertainty."]

error_list<-error_tracker(errors=rbindlist(list(errors_a,errors1,errors2),fill=T),
                          filename = paste0(table_name,"_errors"),
                          error_dir=error_dir,
                          error_list = error_list)

# Add ISO.3166.1.alpha.3
c_dat<-master_codes$countries[,list(Country,`ISO.3166-1.alpha-3`)]
colnames(c_dat)[2]<-"ISO.3166.1.alpha.3"
Site.Out<-merge(Site.Out,c_dat,all.x=T)

dat<-Site.Out[!(is.na(Site.LatD)|is.na(Site.LonD)|is.na(ISO.3166.1.alpha.3))]
errors3<-check_coordinates(data=dat[,list(Site.LatD,Site.LonD,ISO.3166.1.alpha.3)])
errors3<-dat[!errors3][,list(value=paste(unique(Site.ID),collapse="/")),by=list(B.Code,Country,ISO.3166.1.alpha.3,Site.LatD,Site.LonD)
][,table:="Site.Out"
][,field:="Site.LonD/Site.LatD"
][,issue:="Co-ordinates may not be in the country specified."]

error_list<-error_tracker(errors=errors3,filename = paste0(table_name,"_coordinate_errors"),error_dir=error_dir,error_list = error_list)

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
  
  
  # !!! TO DO !!! Site.Out: Update Fields From Harmonization Sheet - BEST LEFT TILL OTHER SHEETS READ IN ####
  if(F){
    # Update from industrious elephant when complete
  }
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

errors_a<-rbindlist(lapply(Soil.Out,"[[","error"))
#error_list<-error_tracker(errors=errors_a,filename = "soil_structure_errors",error_dir=error_dir,error_list = error_list)

Soil.Out<-rbindlist(lapply(Soil.Out,"[[","data"))

# Correct duplicate column names
colnames(Soil.Out)[grep("Soil.N.Unit",colnames(Soil.Out))]<-c("Soil.TN.Unit","Soil.AN.Unit")

# Structure errors: Check for case where only one of upper or lower is present
errors1<-rbindlist(lapply(1:length(data),FUN=function(i){
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

zero_cols<-c("SND","SLT","CLY","Soil.Lower","Soil.BD","Soil.TC","Soil.SOC","Soil.SOM","Soil.pH","Soil.CEC","Soil.EC","Soil.FC","Soil.TN","Soil.NH4","Soil.NO3","Soil.AN",
             "SND.Unit","SLT.Unit","CLY.Unit","Soil.BD.Unit","Soil.TC.Unit","Soil.FC.Unit","Soil.SOC.Unit","Soil.SOM.Unit","Soil.CEC.Unit","Soil.EC.Unit","Soil.TN.Unit","Soil.NH4.Unit","Soil.NO3.Unit",
             "Soil.AN.Unit","Soil.SOC.Method","Soil.SOM.Method","Soil.pH.Method")

results<-validator(data=Soil.Out,
                   zero_cols =zero_cols,
                   numeric_cols=c("Soil.Upper","Soil.Lower","Soil.BD","Soil.TC","Soil.SOC","Soil.SOM","Soil.pH","Soil.CEC","Soil.EC","Soil.FC","Soil.TN","Soil.NH4","Soil.NO3","Soil.AN"),
                   tabname=table_name,
                   site_data=Site.Out)

errors2<-results$errors

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
  errors3<-unique(Soil.Out.Texture[N!=1 & (val>102|val<98),list(B.Code,Site.ID)])
  
  if(nrow(errors3)>0){
    setnames(errors3,"Site.ID","value")
    errors3[,table:=table_name][,field:="Site.ID"][,issue:="Sand, silt, clay sum to beyond 2% different to 100%"]
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

  # Combine & save errors
  errors<-rbindlist(list(errors_a,errors1,errors2,errors3),fill = T)[order(B.Code)]
  error_list<-error_tracker(errors=errors,
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
  # ***!!!TO DO!!!***  harmonize methods, units and variables ####
# 3.3) Times periods #####
data<-lapply(XL,"[[","Times")

Times<-rbindlist(pblapply(1:length(data),FUN=function(i){
  X<-data[[i]][7:30,10:18]
  names(X)<-c("Time","Site.ID","TSP","TAP","Plant.Start","Plant.End","Harvest.Start","Harvest.End","Harvest.DAP")
  X<-X[!is.na(Time)]
  X$B.Code<-Pub.Out$B.Code[i]
  X
}))
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

errors<-rbindlist(lapply(ExpD.Out,"[[","error"))
error_list<-error_tracker(errors=errors,filename = paste0(table_name,"_structure_errors"),error_dir=error_dir,error_list = error_list)

ExpD.Out<-rbindlist(lapply(ExpD.Out,"[[","data"))

results<-validator(data=ExpD.Out,
                   numeric_cols=c("EX.Plot.Size","EX.HPlot.Size"),
                   zero_cols = c("EX.Design","EX.Plot.Size","EX.HPlot.Size","EX.Notes"),
                   tabname="ExpD.Out")

ExpD.Out<-results$data

error_list<-error_tracker(errors=results$errors,filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

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

errors_a<-rbindlist(lapply(Prod.Out,"[[","error"))
Prod.Out<-rbindlist(lapply(Prod.Out,"[[","data"))

# Check products exists (non-tree)
mprod<-unique(master_codes$prod[,list(Product.Simple)])
colnames(mprod)<-c("P.Product")
mprod[,check:=T]
mergedat<-merge(Prod.Out,mprod,all.x=T)

mergedat<-mergedat[is.na(check),list(B.Code,P.Product)]
errors1<-mergedat[,value:=P.Product
][,table:=table_name
][,field:="P.Product"
][,issue:="No match for product in master codes Product tab"
][order(B.Code)][,P.Product:=NULL]

error_list<-error_tracker(errors=rbind(errors_a,errors1),filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

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

errors_a<-rbindlist(lapply(Var.Out,"[[","error"))

Var.Out<-rbindlist(lapply(Var.Out,"[[","data"))
setnames(Var.Out,"V.Subpecies","V.Subspecies")
Var.Out[,c("V.New.Var","V.New.Crop","V.New.Species","V.New.SubSpecies","...20","V.Trait1","V.Trait2","V.Trait3"):=NULL]

results<-validator(data=Var.Out,
                   numeric_cols=c("V.Maturity"),
                   zero_cols = c("V.Var","V.Species","V.Subspecies","V.Crop.Practice","V.Animal.Practice","V.Base",
                                 "V.Type","V.Maturity","V.Code.Crop","V.Code.Animal",
                                 "V.Codes"),
                   tabname=table_name,
                   trim_ws=T)

errors1<-results$errors
Var.Out<-results$data

# Check & fix for where Animal Practices has been entered in Crop Practice, remove crop related cols
Var.Out[!is.na(V.Crop.Practice) & V.Crop.Practice!=V.Animal.Practice]
Var.Out[!is.na(V.Crop.Practice) & V.Crop.Practice!=V.Animal.Practice,V.Animal.Practice:=V.Crop.Practice
        ][,c("V.Crop.Practice","V.Code.Crop","V.Code.Animal","V.Maturity"):=NULL]

# Replace V.Var with Join and remove join
Var.Out[,V.Var:=Join][,Join:=NULL]

# Update Variety Naming and Codes
mvars<-master_codes$vars_animals[,list(V.Product,V.Var,V.Var1,V.Animal.Practice)]
setnames(mvars,"V.Animal.Practice","V.Animal.Practice1")

Var.Out<-merge(Var.Out,mvars,all.x=T,by=c("V.Product","V.Var"))
Var.Out[!is.na(V.Var1),V.Var:=V.Var1
][!is.na(V.Animal.Practice1),V.Animal.Practice:=V.Animal.Practice1]

# Update V.Codes
Var.Out[,unique(V.Animal.Practice)]
Var.Out[,V.Codes:=master_codes$prac[match(Var.Out$V.Animal.Practice,master_codes$prac$Subpractice),Code]]

# Errors
errors1<-unique(Var.Out[(V.Type %in% c("Local","Landrace")) & V.Animal.Practice != "Unimproved Breed",!c("V.Base","V.Codes")])
errors1<-errors1[,.(value=paste(unique(V.Var),collapse = "/")),by=B.Code
                     ][,table:=table_name
                       ][,field:="V.Var"
                         ][,issue:="V.Type is local, but V.Animal.Practice is not Unimproved Breed"]

errors2<-unique(Var.Out[V.Type %in% c("Hybrid","Crossbreed") & V.Animal.Practice != "Hybridization or Cross Breeding",!c("V.Base","V.Codes")])
errors2<-errors2[,.(value=paste(unique(V.Var),collapse = "/")),by=B.Code
][,table:=table_name
][,field:="V.Var"
][,issue:="V.Type is hybrid/crossbreed, but V.Animal.Practice is not Hybridization or Cross Breeding"]

errors<-rbindlist(list(errors_a,errors1,errors2))

error_list<-error_tracker(errors=errors,filename = paste0(table_name,"_errors"),error_dir=error_dir,error_list = error_list)

 # 3.6.1) Harmonization #####
# Non-matching varieties
h_tasks3<- Var.Out[is.na(V.Var1) & !is.na(V.Var) & !grepl("local|unspecified|unimproved|[*][*][*]",V.Var,ignore.case=T),list(V.Product,V.Var,B.Code)
][,list(B.Code=paste(B.Code,collapse = "/")),by=list(V.Product,V.Var)
][,master_tab:="vars"
][,table:="Var.Out"
][,field:="V.Var"]
setnames(h_tasks3,"V.Var","value")

harmonization_list<-error_tracker(errors=errors3,filename = paste(table_name,"_harmonization"),error_dir=harmonization_dir,error_list = harmonization_list)

# Remove harmonization columns
Var.Out[,c("V.Var1","V.Animal.Practice1"):=NULL]


  
# 3.7) Diet ####
  # 3.7.1) Animals.Out ####
  table_name<-"Animals.Out"
  data<-lapply(XL,"[[",table_name)
  col_names<-colnames(data[[1]][,1:19])
  
  Animals.Out<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    setnames(X,"A.Level.Name...1","A.Level.Name",skip_absent = T)
    
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
  
  errors_a<-rbindlist(lapply(Animals.Out,"[[","error"))
  
  Animals.Out<-rbindlist(lapply(Animals.Out,"[[","data"))
  p_names<- c("A.Feed.Add.1","A.Feed.Add.2","A.Feed.Add.3", "A.Feed.Add.C", "A.Feed.Sub.1","A.Feed.Sub.2","A.Feed.Sub.3",
              "A.Feed.Sub.C","A.Feed.Pro.1","A.Feed.Pro.2","A.Feed.Pro.3","A.Manure.Man","A.Pasture.Man","A.Aquasilvaculture")
  setnames(Animals.Out,paste0("P",1:14),p_names)
  
  zero_cols<-c(p_names,"A.Notes","A.Grazing","A.Hay")
  
  results<-validator(data=Animals.Out,
                     zero_cols =zero_cols,
                     unique_cols = "A.Level.Name",
                     tabname=table_name)
  
  errors<-results$errors
  Animals.Out<-results$data
  
  # Errors -  Feed Add Code but no control or >1 control
  errors1<-Animals.Out[A.Level.Name!="Base",feed_add_prac:=sum(!is.na(A.Feed.Add.1),!is.na(A.Feed.Add.2),!is.na(A.Feed.Add.3))>1,by=.(B.Code,A.Level.Name)
                 ][A.Level.Name!="Base",feed_add_cont:=sum(A.Feed.Add.C=="Yes",na.rm=T),by=.(B.Code)]
  
  errors1<-errors1[feed_add_cont>1 & feed_add_prac==T,list(value=paste0(A.Level.Name,collapse = "/")),by=B.Code
                                            ][,table:=table_name
                                            ][,field:="A.Level.Name"
                                            ][,issue:="Possible error, there is more than 1 control present feed addition practice(s)."]
  
  # Errors - Feed Sub Code but no control
  errors2<-Animals.Out[A.Level.Name!="Base",feed_sub_prac:=sum(!is.na(A.Feed.Sub.1),!is.na(A.Feed.Sub.2),!is.na(A.Feed.Sub.3))>1,by=.(B.Code,A.Level.Name)
  ][A.Level.Name!="Base",feed_sub_cont:=sum(A.Feed.Sub.C=="Yes",na.rm=T),by=.(B.Code)]
  
  errors2<-errors2[feed_sub_cont==0 & feed_sub_prac==T,list(value=paste0(A.Level.Name,collapse = "/")),by=B.Code
  ][,table:=table_name
  ][,field:="A.Level.Name"
  ][,issue:="Possible error, there is no control present for feed substitution practice(s)."]
  
  # Errors - Animal diet has been specified but there are no practices listed
  cols<-zero_cols[!zero_cols %in% "A.Notes"]
  errors3 <- Animals.Out[, lapply(.SD, function(col) sum(!is.na(col))), by = list(A.Level.Name, B.Code), .SDcols = cols]
  errors3<-data.table(B.Code=errors3$B.Code,A.Level.Name=errors3$A.Level.Name,value=rowSums(errors3[,..cols]))[value==0 & A.Level.Name!="Base"]
  errors3<-errors3[,.(value=paste(A.Level.Name,collapse = "/")),by=B.Code
          ][,table:=table_name
            ][,field:="A.Level.Name"
              ][,issue:="Possible error, an animal diet has no associated practices."]
  
  error_list<-error_tracker(errors=rbind(errors_a,errors,errors1,errors2,errors3),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
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
  
  errors_a<-rbindlist(lapply(Animals.Diet,"[[","error"))
  
  Animals.Diet<-rbindlist(lapply(Animals.Diet,"[[","data"))
  
  zero_cols<-c("D.Item","D.Item.Group","D.Source","D.Process","D.Amount","D.Ad.lib","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","D.Day.Start","D.Day.End","DC.Is.Dry")
  
  results<-validator(data=Animals.Diet,
                     zero_cols =zero_cols,
                     numeric_cols=c("D.Amount"),
                     unit_pairs = data.table(unit="D.Unit.Amount",var="D.Amount",name_field="A.Level.Name"),
                     compulsory_cols = c(A.Level.Name="D.Type"),
                     trim_ws = T,
                     tabname=table_name)
  
  errors1<-results$errors
  Animals.Diet<-results$data
  
  # Tidy diet process field
  Animals.Diet[,D.Process:=trimws(D.Process)
               ][,D.Process:=gsub(" /","/",D.Process)
                 ][,D.Process:=gsub("/ ","/",D.Process)
                   ][D.Process=="",D.Process:=NA]
  
  # Create a merged process and diet item name field for harmonization.
  Animals.Diet[!is.na(D.Process),D.ItemxProcess:=paste0(D.Item,"||",D.Process)]
  write.table(Animals.Diet[!is.na(D.Process),.(B.Code=paste0(unique(B.Code),collapse ="/")),by=D.ItemxProcess],"clipboard",row.names = F,sep="\t")
  
  # Convert diet process to list
  Animals.Diet[,D.Process:=strsplit(D.Process,"/")]
  
  # Error where not entire diet and is.na(Diet.Item)
  errors2<-Animals.Diet[D.Type!="Entire Diet" & is.na(D.Item),
                        ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
                             ][,table:=table_name
                               ][,field:="A.Level.Name"
                                 ][,issue:="Rows in have no diet item selected and diet type is no Entire Diet."]
  
  errors3<-check_key(parent = Animals.Out,child = Animals.Diet,tabname=table_name,keyfield="A.Level.Name")
  
  error_list<-error_tracker(errors=rbind(errors_a,errors1,errors2,errors3),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
    # 3.7.2.1) Harmonization #######
  # Units
  h_params<-data.table(h_table=table_name,
                       h_field=c("D.Unit.Amount","D.Unit.Time","D.Unit.Animals"),
                       ignore_vals=c("unspecified","unspecified","unspecified"))[,c("h_field_alt","h_table_alt"):=NA]
          
  
  results<-harmonizer_wrap(data=Animals.Diet,
                           h_params=h_params,
                           master_codes = master_codes)
  
  Animals.Diet<-results$data
  h_tasks1<-results$h_tasks
  
  # Insert updated diet naming system 
  diets_new<-unique(master_codes$ani_diet[,.(D.Item,D.Item.Root,D.Item.Expanded,D.Item.Full,D.Item.Other,D.Item.Process,D.Item.Component)])
  
  diets_new[,N:=.N,by=D.Item][N>1]
  
  # NOTE THAT THIS HARMONIZATION NEEDS TO CASCADE TO SUBSEQUENT SHEETS
  # NOTE THERE IS SOME USE OF DIET GROUP IN THE DIET ITEM COLUMN, ADD LOGIC COLS TO INDICATE THIS.
  # CONSIDER IF ENTIRE DIET SHOULD USE A.LEVEL.NAME AS THE DIET.ITEM
  Animals.Diet<-merge(Animals.Diet,diets_new,by.x="D.ItemxProcess",by.y="D.Item",all.x=T)
  Animals.Diet[is.na(D.Item.Full),D.Item:=D.Item.Full]
  
  h_tasks2<-results$h_tasks
  
  harmonization_list<-error_tracker(errors=rbind(h_tasks1,h_tasks2),
                                    filename = table_name,
                                    error_dir=harmonization_dir,
                                    error_list = harmonization_list)
  
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
  
  errors_a<-rbindlist(lapply(Animals.Diet.Comp,"[[","error"))
  
  Animals.Diet.Comp<-rbindlist(lapply(Animals.Diet.Comp,"[[","data"))
  
  # Add columns to indicate if a "compound" diet item described by A.Level.Name or Diet.Group 
  diet_groups<-unique(Animals.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
  setnames(diet_groups,"D.Item.Group","D.Item")
  
  diet_entire<-unique(Animals.Diet[D.Type=="Entire Diet",.(B.Code,A.Level.Name)][,is_entire_diet:=T])
  setnames(diet_entire,"A.Level.Name","D.Item")
  
  Animals.Diet.Comp<-merge(Animals.Diet.Comp,diet_groups,all.x=T)[is.na(is_group),is_group:=F]
  Animals.Diet.Comp<-merge(Animals.Diet.Comp,diet_entire,all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
  
  # Run standard validation
  unit_pairs<-data.table(unit=paste0(numeric_cols,".Unit"),var=numeric_cols,name_field="D.Item")
  unit_pairs<-unit_pairs[var!="DC.CN"]
  
  results<-validator(data=Animals.Diet.Comp,
                     zero_cols =colnames(Animals.Diet.Comp),
                     numeric_cols=numeric_cols,
                     unit_pairs = unit_pairs,
                     compulsory_cols = c(D.Item="D.Item"),
                     trim_ws = T,
                     tabname=table_name)
  
  errors1<-results$errors
  Animals.Diet.Comp<-results$data
  
  error_list<-error_tracker(errors=rbind(errors_a,errors1),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
  # Set non-numerics cols to character
  non_numeric_cols<-colnames(Animals.Diet.Comp)[!colnames(Animals.Diet.Comp) %in% numeric_cols]
  non_numeric_cols<-non_numeric_cols[!non_numeric_cols %in% c("is_group","is_entire_diet")]
  Animals.Diet.Comp <- Animals.Diet.Comp[, (non_numeric_cols) := lapply(.SD, as.character), .SDcols = non_numeric_cols]
  
    # 3.7.3.1) Harmonization #######
  # Units
  target_cols<-grep(".Unit",col_names,value=T)
  h_params<-data.table(h_table=table_name,
                       h_field=target_cols,
                       h_field_alt=rep("DC.Unit",length(target_cols)),
                       h_table_alt=rep("Animals.Diet.Comp",length(target_cols)),
                       ignore_vals=rep("unspecified",length(target_cols)))
  
  results<-harmonizer_wrap(data=Animals.Diet.Comp,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks1<-results$h_tasks
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
  
  h_tasks2<-results$h_tasks
  Animals.Diet.Comp<-results$data
  
  harmonization_list<-error_tracker(errors=rbind(h_tasks1,h_tasks2),filename = table_name,error_dir=harmonization_dir,error_list = harmonization_list)
  
  
  # 3.7.4) Animals.Diet.Digest ######
  table_name<-"Animals.Diet.Digest"
  col_names<-colnames(data[[1]][,127:208])
  
  numeric_cols<-col_names[5:23]
  unit_cols<-paste0(numeric_cols,".Unit")
  method_cols<-paste0(numeric_cols,".Method")
  dv_cols<-paste0(numeric_cols,".DorV")
  copy_down_cols<-c(unit_cols,method_cols,dv_cols,"DD.Unit.Is.Dry")
  
  fun1<-function(x){x[1]}
  
  Animals.Diet.Digest<-lapply(1:length(data),FUN=function(i){
    X<-data[[i]]
    B.Code<-Pub.Out$B.Code[i]
    
    if(!all(col_names %in% colnames(X))){
      cat("Structural issue with file",i,B.Code,"\n")
      list(error=data.table(B.Code=B.Code,value=NA,table=table_name,field=NA,issue="Problem with table structure."))
    }else{
      X<-X[,..col_names]
      setnames(X,c("D.Item...129"),c("D.Item"),skip_absent = T)
      X[,c("...128","...149","...169","...189"):=NULL]
      X<-X[!is.na(D.Item)]
      if(nrow(X)>0){
        X <- X[, (copy_down_cols) := lapply(.SD,fun1), .SDcols = copy_down_cols]
        X[,B.Code:=B.Code]
        list(data=X)
      }
    }})
  
  errors_a<-rbindlist(lapply(Animals.Diet.Digest,"[[","error"))
  
  Animals.Diet.Digest<-rbindlist(lapply(Animals.Diet.Digest,"[[","data"))
  
  # Add columns to indicate if a "compound" diet item described by A.Level.Name or Diet.Group 
  diet_groups<-unique(Animals.Diet[!is.na(D.Item.Group),.(B.Code,D.Item.Group)][,is_group:=T])
  setnames(diet_groups,"D.Item.Group","D.Item")
  
  diet_entire<-unique(Animals.Diet[D.Type=="Entire Diet",.(B.Code,A.Level.Name)][,is_entire_diet:=T])
  setnames(diet_entire,"A.Level.Name","D.Item")
  
  Animals.Diet.Digest<-merge(Animals.Diet.Digest,diet_groups,all.x=T)[is.na(is_group),is_group:=F]
  Animals.Diet.Digest<-merge(Animals.Diet.Digest,diet_entire,all.x=T)[is.na(is_entire_diet),is_entire_diet:=F]
  
  # Run standard validation
  (numeric_cols<-col_names[5:23])
  
  unit_pairs<-data.table(unit=unit_cols,var=numeric_cols,name_field="D.Item")
  
  results<-validator(data=Animals.Diet.Digest,
                     zero_cols =colnames(Animals.Diet.Digest),
                     numeric_cols=numeric_cols,
                     unit_pairs = unit_pairs,
                     compulsory_cols = c(D.Item="D.Item"),
                     trim_ws = T,
                     tabname=table_name)
  
  
  errors1<-results$errors
  Animals.Diet.Digest<-results$data
  
  error_list<-error_tracker(errors=rbind(errors_a,errors1),
                            filename = paste0(table_name,"_errors"),
                            error_dir=error_dir,
                            error_list = error_list)
  
  # Set non-numerics cols to character
  non_numeric_cols<-colnames(Animals.Diet.Digest)[!colnames(Animals.Diet.Digest) %in% numeric_cols]
  non_numeric_cols<-non_numeric_cols[!non_numeric_cols %in% c("is_group","is_entire_diet")]
  Animals.Diet.Digest <- Animals.Diet.Digest[, (non_numeric_cols) := lapply(.SD, as.character), .SDcols = non_numeric_cols]
  
  
    # 3.7.4.1) Harmonization #######
  h_params<-data.table(h_table=table_name,
                       h_field=unit_cols,
                       h_field_alt=rep("DC.Unit",length(target_cols)),
                       h_table_alt=rep("Animals.Diet.Comp",length(target_cols)),
                       ignore_vals=rep("unspecified",length(target_cols)))
  
  results<-harmonizer_wrap(data=Animals.Diet.Digest,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks1<-results$h_tasks
  Animals.Diet.Digest<-results$data
  
  target_cols<-grep(".Method",col_names,value=T)
  h_params<-data.table(h_table=table_name,
                       h_field=method_cols,
                       h_field_alt=rep("DD.Method",length(target_cols)),
                       h_table_alt=rep("Animals.Diet.Digest",length(target_cols)))
  
  
  results<-harmonizer_wrap(data=Animals.Diet.Digest,
                           h_params=h_params,
                           master_codes = master_codes)
  
  h_tasks2<-results$h_tasks
  Animals.Diet.Digest<-results$data
  
  harmonization_list<-error_tracker(errors=rbind(h_tasks1,h_tasks2),
                                    filename = table_name,
                                    error_dir=harmonization_dir,
                                    error_list = harmonization_list)
  
  # 3.7.5) Harmonization of key fields (Diet Item) ####
  
  
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
  
  # 3.8.1) AF.Trees ######
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
  
  
  # 3.8.1) Harmonization - TO DO !!! ######
  # AF.Tree and AF.Tree.Unit
  
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

  # 3.12.6) Join and save errors and harmonization tasks ######
errors<-rbindlist(list(errors_a,errors_b,errors_c))
error_list<-error_tracker(errors=errors,filename = "chem_structure_errors",error_dir=error_dir,error_list = error_list)

errors<-rbindlist(list(errors1,errors2,errors3,errors4,errors5,errors6,errors7,errors8,errors9,errors10),fill=T)[order(B.Code)]
error_list<-error_tracker(errors=errors,filename = "chem_other_errors",error_dir=error_dir,error_list = error_list)

h_tasks<-rbindlist(list(h_tasks1,h_tasks2,h_tasks3,h_tasks4),fill=T)
harmonization_list<-error_tracker(errors=h_tasks,filename = "chem_harmonization",error_dir=harmonization_dir,error_list = harmonization_list)


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

# Save tables as a list  ####

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
