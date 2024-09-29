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
valid_end <- as.Date("2024-12-01")

# 0.2) Set project, directories and parallel cores ####

# Set cores for parallel processing
workers<-parallel::detectCores()-2

# Set the project name, this should usually refer to the ERA extraction template used
project<-era_projects$courageous_camel_2024

# Working folder where extraction excel files are stored during in active data entry
#excel_dir<-"G:/My Drive/Data Entry 2024"
#excel_dir<-"/Users/pstewarda/Library/CloudStorage/GoogleDrive-peetmate@gmail.com/My Drive/Data Entry 2024"

# Where extraction excel files are stored longer term
if(T){
  excel_dir<-file.path(era_dirs$era_dataentry_dir,project,"excel_files")
  if(!dir.exists(excel_dir)){
    dir.create(excel_dir,recursive=T)
  }
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
  
  aom<-copy(master_codes$AOM)[,Path:=NULL][,index:=1:.N]
  # Collapse heirarchy
  aom[,Path:=paste(na.omit(c(L1,L2,L3,L4,L5,L6,L7,L8,L9,L10)),collapse="/"),by=index]
  
  # 2.2) Load excel data entry template #####
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
  
  # 2.3) List extraction excel files #####
  Files<-list.files(excel_dir,".xlsm$",full.names=T,recursive = "T")
  #Files<-grep("Extracted/|QCed/",Files,value=T)
  
  # 2.4) Check for duplicate files #####
  FNames<-unlist(tail(tstrsplit(Files,"/"),1))
  FNames<-gsub(" ","",FNames)
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
  
  # 2.5) Read in data from excel files #####
   i<-1
  
    File <- excel_files$filename[i]
    era_code <- excel_files$era_code2[i]
    save_name <- file.path(extracted_dir, paste0(era_code, ".RData"))

    excel_dat <- tryCatch({
      lapply(SheetNames, FUN=function(SName){
        cat('\r', "Importing File ", i, "/", nrow(excel_files), " - ", era_code, " | Sheet = ", SName,"               ")
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
      warning(paste("Error file",excel_files$filename[i],"failed to load, moved to loading_issue folder."))
    }
    
# 3) Process imported data ####
    # initiate error list
    errors<-list()
  # 3.1) Publication (Pub.Out) #####
table_name<-"Pub.Out"
template_cols<-master_template_cols[[table_name]][-7]
Pub.Out<-excel_dat[[table_name]][,-7]
Pub.Out$filename<-basename(File)

# Replace zeros with NAs
results<-validator(data=Pub.Out,
                   compulsory_cols = c(filename="B.Code",filename="B.Author.Last",filename="B.Date"),
                   zero_cols=c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4"),
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

    # 3.1.1) Harmonization ######
results<-val_checker(data=Pub.Out,
                     tabname=table_name,
                     master_codes=master_codes,
                     master_tab="journals",
                     h_field="B.Journal",
                     h_field_alt=NA,
                     exact=F)

Pub.Out<-results$data
h_tasks<-list(results$h_task)


  # 3.2) Site.Out #####
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
                     template_cols = template_cols,
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
  ][,table:=table_name
  ][,field:="Site.LonD/Site.LatD"
  ][,issue:="Co-ordinates may not be in the country specified."]
  errors<-c(errors,list(error_dat))
  
    # 3.2.1) Harmonization ######
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
  error_dat<-unique(Soil.Out.Texture[N!=1 & (val>102|val<98),,.(value=Site.ID),by=.(B.Code,Site.ID)])
  error_dat[,table:=table_name][,field:="Site.ID"][,issue:="Sand, silt, clay sum to beyond 2% different to 100%"]
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
  
  # 3.5.6) !!!TO DO!!! Harmonization ######
  # Need to make a lookup table to supply from master_codes$AOM
# 3.6) Time periods #####
  # 3.6.1) Times.Out ######
  table_name<-"Times.Out"
  Times.Out<-excel_dat[[table_name]][1:9]
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
   
  

# 3.5) Herd (Herd.Out) ####
  table_name<-"Herd.Out"
   Herd.Out<-excel_dat[[table_name]][,1:20]
   template_cols<-c(master_template_cols[[table_name]][1:20],"B.Code")
   Herd.Out$B.Code<-Pub.Out$B.Code
   Herd.Out<-Herd.Out[!is.na(Herd.Level.Name)]
   
   setnames(Herd.Out,"Herd.Time","Time",skip_absent=T)
   template_cols[template_cols=="Herd.Time"]<-"Time"
   
   if(nrow(Herd.Out)==0){
     error_dat<-data.table(B.Code=Pub.Out$B.Code,value=NA,table=table_name,field="Herd.Level.Name",issue="No Herd.Level.Name exists")
     errors<-c(errors,list(error_dat))
   }
   
   unit_pairs<-data.table(unit=c("Herd.Start.Age.Unit","Herd.Start.Weight.Unit","Herd.N.Unit"),
                          var=c("Herd.Start.Age","Herd.Start.Weight","Herd.N"),
                          name_field="Herd.Level.Name")
   
   results<-validator(data=Herd.Out,
                      tabname=table_name,
                      time_data = Times.Out,
                      compulsory_cols = c(Herd.Level.Name="V.Product.Sci.Name",Herd.Level.Name="Herd.Rep"),
                      hilo_pairs = data.table(low_col="Herd.Start.Age",high_col="Herd.End.Age",name_field="Herd.Row.ID"),
                      unique_cols = c("Herd.Level.Name"),
                      numeric_cols = c("Herd.Start.Age","Herd.End.Age","Herd.Start.Weight","Herd.N","Herd.Rep"),
                      unit_pairs = unit_pairs,
                      template_cols = template_cols,
                      trim_ws = T)
   
   
   # NEED TO CREATE VALIDATION FUNCTION FOR CHECKING KEY FIELD WITH DELIM AGAINST TARGET COL #####
   # CAN ALSO CREATE A SIMPLER ALLOWED VALUES FUNCTION THAT TAKES A table with field and values cols #####
   error_dat<-results$errors
   errors<-c(errors,list(error_dat))
   
   Herd.Out<-results$data
   
   setnames(Herd.Out,"V.Var...7","V.Var",skip_absent=T)
   
   # 3.5.1) !!!TO DO!!! Harmonization ######
   # Product
   # Scientific Name
   # Variety
   # Practice
   # Sex
   # Stage
   # Parity
   # Age.Unit
   # Weight.Unit
   # Herd.Unit
   
# !!!!NOTE A MAJOR CHANGE IS THE PROCESS IS THAT THE USER IS NOT DEFINING ADDITION OR SUBSTITION PRACTICES WE WILL NEED TO DEFINE RULES THAT CREATE THESE PRACTICES AND ADD THEM TO THE TABLES TO MATCH ####
# !!!!WITH THE 2022 EXTRACTION IN SKINNY COW. ####

# 3.7) Animals.Out ######
   table_name<-"Ingredients.Out"
   Animals.Out<-excel_dat[[table_name]][,1:21]
   template_cols<-c(master_template_cols[[table_name]][1:21],"B.Code")
   Animals.Out$B.Code<-Pub.Out$B.Code

   setnames(Animals.Out,c("M.Year...3","A.Level.Name...1"),c("Time","A.Level.Name"),skip_absent=T)
   template_cols <- dplyr::recode(
     template_cols,
     "M.Year...3"       = "Time",
     "A.Level.Name...1" = "A.Level.Name",
     .default = template_cols  # Keeps original names if no match
   )
   
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
                                               c("Yes","No","Unspecified")
                                               ),
                              parent_tab_name=c("master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM",NA,NA,NA,"master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",NA,NA),
                              field=c("D.Process.Mech","D.Process.Chem","D.Process.Bio","D.Process.Therm","D.Process.Dehy","A.Grazing","D.Type","A.Hay","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","DC.Is.Dry","D.Ad.lib"))

    results<-validator(data=Animals.Out,
                       numeric_cols = c("D.Amount","D.Day.Start","D.Day.End"),
                       unique_cols = "A.Level.Name",
                       allowed_values=allowed_values,      
                       hilo_pairs = data.table(low_col="D.Day.Start",high_col="D.Day.End",name_field="A.Level.Name"),
                       time_data = Times.Out,
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

# 3.8) Animal.Diet ####
  table_name<-"Ingredients.Out"
  Animal.Diet<-excel_dat[[table_name]][,-(1:22)]
  template_cols<-c(master_template_cols[[table_name]][-(1:22)],"B.Code")
  Animal.Diet$B.Code<-Pub.Out$B.Code[1]

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
  
  unit_pairs<-data.table(unit=c("D.Amount.Unit"),
                         var=c("D.Amount"),
                         name_field="A.Level.Name")
  
  ingredient_types<-aom[grepl("Feed Ingredient",Path),unique(unlist(tstrsplit(unlist(tstrsplit(Path,"Feed Ingredient/",keep=2)),"/",keep=1)))]
  ingredient_types<-ingredient_types[!grepl("Ingredient ",ingredient_types)]
  
  # NOTE NEED TO INTEGRATE HARMONIZATION WITH ALLOWED VALUES TO ALLOW ADDITION OF AOM CODES AND/OR REPLACEMENT OF VALUES WITH NEW (replacement_vals,add_vals,add_vals_name) #####
  allowed_values<-data.table(allowed_values=list(aom[grepl("Mechanical Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Cheimcal Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Biological Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Thermal Process",Path),unique(Edge_Value)],
                                                 aom[grepl("Dehydration Process",Path),unique(Edge_Value)],
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Amount",Values_New],
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Time",Values_New],
                                                 master_codes$lookup_levels[Table=="Animals.Diet" & Field=="D.Unit.Animals",Values_New],
                                                 c("Yes","No","Unspecified"),
                                                 c("Yes","No","Unspecified"),
                                                 ingredient_types),
  parent_tab_name=c("master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$AOM","master_codes$lookup_levels","master_codes$lookup_levels","master_codes$lookup_levels",NA,NA,"master_codes$AOM"),
  field=c("D.Process.Mech","D.Process.Chem","D.Process.Bio","D.Process.Therm","D.Process.Dehy","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","DC.Is.Dry","D.Ad.lib","D.Type"))
  
  results<-validator(data=Animal.Diet,
                     zero_cols=zero_cols,
                     numeric_cols=c("D.Amount"),
                     numeric_ignore_vals="Unspecified",
                     unit_pairs = data.table(unit="D.Unit.Amount",var="D.Amount",name_field="A.Level.Name"),
                     hilo_pairs = data.table(low_col="D.Day.Start",high_col="D.Day.End",name_field="A.Level.Name"),
                     compulsory_cols = c(A.Level.Name="A.Level.Name"),
                     duplicate_field="D.Item",
                     check_keyfields=data.table(parent_tab=list(Animals.Out),
                                                parent_tab_name="Animals.Out",
                                                keyfield="A.Level.Name"),
                     time_data = Times.Out,
                     trim_ws = T,
                     tabname=table_name)
  
  error_dat<-results$errors
  errors<-c(errors,list(error_dat))
  
  Animals.Diet<-results$data

  # Add logic to indicate if a compound diet item
  Animals.Diet[,D.Is.Group:=D.Type %in% na.omit(D.Item.Group),by=B.Code]
  
  # Error where the entire diet is not being described and is.na(Diet.Item)
  error_dat<-Animals.Diet[(is.na(D.Type)|D.Type!="Entire Diet") & is.na(D.Item) & !D.Is.Group,
                        ][,list(value=paste0(unique(A.Level.Name),collapse="/")),by=B.Code
                             ][,table:=table_name
                               ][,field:="A.Level.Name"
                                 ][,issue:="Rows in have no diet item selected and diet type is not Entire Diet or a diet group value."]
  
  errors<-c(errors,list(error_dat))
  
  # 3.8.1) Harmonization #######
  # NOTE NEED TO INTEGRATE HARMONIZATION WITH ALLOWED VALUES TO ALLOW ADDITION OF AOM CODES AND/OR REPLACEMENT OF VALUES WITH NEW (replacement_vals,add_vals,add_vals_name) #####
  if(F){
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
            ][,B.Code:=Animals.Diet.Comp[D.Item==D.Item2[1],paste(unique(B.Code),collapse = "/")],by=D.Item2
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
  
  # 3.8.2) Merge AOM Diet Summary with Animals.Out (.inc Trees)  #######
  
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
  }
  

    
# 3.9) Animals.Diet.Comp ######
  table_name<-"Nutrition.Out"
  Animal.Diet.Comp<-excel_dat[[table_name]]
  col_names<-colnames(Animal.Diet.Comp)
  col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
  unit_cols<-grep("Unit",col_names,value=T)
  num_cols<-gsub("[.]Unit","",unit_cols)

  Animal.Diet.Comp<-Animal.Diet.Comp[,..col_names]
  
  # Copy down units and methods
  copy_down_cols<-c(unit_cols)
  Animal.Diet.Comp <- Animal.Diet.Comp[, (copy_down_cols) := lapply(.SD,function(x){x[1]}), .SDcols = copy_down_cols]
  
  # Add study code
  Animal.Diet.Comp$B.Code<-Pub.Out$B.Code[1]
  
  Animal.Diet.Comp<-Animal.Diet.Comp[!is.na(D.Item)]
  
  unit_pairs<-data.table(unit=unit_cols,
                         var=num_cols,
                         name_field=num_cols)
  
  item_options<-as.vector(na.omit(unique(c(Animals.Out$A.Level.Name,Animal.Diet$D.Item,Animal.Diet$D.Item.Group))))
  
  allowed_values<-data.table(allowed_values=list(item_options),
                             parent_tab_name=c("Diet.Ingredients"),
                             field=c("D.Item"))
  
results<-validator(data=Animal.Diet.Comp,
                   numeric_cols=num_cols,
                   unit_pairs = unit_pairs,
                   compulsory_cols = c(D.Item="D.Item"),
                   allowed_values = allowed_values,
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

  # 3.9.1) Harmonization #######

# Merge in updated name from Animals.Diet table
merge_dat<-unique(Animals.Diet[,.(D.Item.Raw,B.Code,D.Item.Root.Other.Comp.Proc_All)])
# Remove any duplicate rows (see error "Multiple matches between D.Item in Composition table and Diet Description table.")
merge_dat<-merge_dat[!duplicated(merge_dat[,.(B.Code,D.Item.Raw)])][,check:=T]

Animals.Diet.Comp<-merge(Animals.Diet.Comp,merge_dat,by.x=c("D.Item","B.Code"),by.y=c("D.Item.Raw","B.Code"),all.x=T,sort=F)

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

# 3.10) Animals.Diet.Digest ######
table_name<-"Digest.Out"
Animal.Diet.Digest<-excel_dat[[table_name]]

col_names<-colnames(Animal.Diet.Digest)
col_names<-col_names[!grepl("[.][.][.]|0[.]",col_names)]
unit_cols<-grep("Unit",col_names,value=T)
num_cols<-gsub("[.]Unit","",unit_cols)
method_cols<-grep("Method",col_names,value=T)

Animal.Diet.Digest<-Animal.Diet.Digest[,..col_names]

# Copy down units and methods
copy_down_cols<-c(unit_cols)
Animal.Diet.Digest <- Animal.Diet.Digest[, (copy_down_cols) := lapply(.SD,function(x){x[1]}), .SDcols = copy_down_cols]

# Add study code
Animal.Diet.Digest$B.Code<-Pub.Out$B.Code[1]

Animal.Diet.Digest<-Animal.Diet.Digest[!is.na(D.Item)]

unit_pairs<-data.table(unit=unit_cols,
                       var=num_cols,
                       name_field=num_cols)

item_options<-as.vector(na.omit(unique(c(Animals.Out$A.Level.Name,Animals.Diet$D.Item,Animals.Diet$D.Item.Group))))

allowed_values<-data.table(allowed_values=list(item_options),
                           parent_tab_name=c("Diet.Ingredients"),
                           field=c("D.Item"))

results<-validator(data=Animal.Diet.Digest,
                   numeric_cols=num_cols,
                   unit_pairs = unit_pairs,
                   compulsory_cols = c(D.Item="D.Item"),
                   allowed_values = allowed_values,
                   duplicate_field="D.Item",
                   trim_ws = T,
                   tabname=table_name)

error_dat<-results$errors
errors<-c(errors,list(error_dat))

Animal.Diet.Digest<-results$data

  # 3.10.1) !!TO DO !! Harmonization #######
if(F){
Animals.Diet.Digest<-merge(Animals.Diet.Digest,merge_dat,by.x=c("D.Item","B.Code"),by.y=c("D.Item.Raw","B.Code"),all.x=T,sort=F)

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
}

# 3.x) !! TO DO !! Update D.Item field with harmonized names ######
if(F){
  Animals.Diet[,D.Item_raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
  Animals.Diet.Comp[,D.Item_raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
  Animals.Diet.Digest[,D.Item_raw:=D.Item][!is.na(D.Item.Root.Other.Comp.Proc_All),D.Item:=D.Item.Root.Other.Comp.Proc_All]
}
# 3.8) Agroforestry #####
# 3.8.1) AF.Out######
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
                   time_data = Times.Out,
                   compulsory_cols = c(AF.Level.Name="B.Code"),
                   unique_cols = c("AF.Level.Name"),
                   template_cols = template_cols,
                   trim_ws = T)

error_dat<-results$errors
errors<-c(errors,list(error_dat))

AF.Out<-results$data

# 3.8.2) AF.Trees ######
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

# 3.9) Chemicals #####
  # 3.9.1) Chems.Code ####
  table_name<-"Chems.Out"
  Chems.Code<-excel_dat[[table_name]][,c(1:3)]
  template_cols<-c(master_template_cols[[table_name]][c(1:3)],"B.Code")
  Chems.Code$B.Code<-Pub.Out$B.Code
  
  setnames(Chems.Code,"C.Name","C.Level.Name")
  Chems.Code<-Chems.Code[!is.na(C.Level.Name)]
  
  
  error_dat<-validator(data=Chems.Code,
                     unique_cols = "C.Level.Name",
                     tabname="Chems.Code")$errors
  
  errors<-c(errors,list(error_dat))

  # 3.9.2) Chems.Out ####
  table_name<-"Chems.Out"
  Chems.Out<-excel_dat[[table_name]][,c(5:19)]
  template_cols<-c(master_template_cols[[table_name]][c(5:19)],"B.Code")
  Chems.Out$B.Code<-Pub.Out$B.Code
  
  Chems.Out<-Chems.Out[!is.na(C.Level.Name)]
  
  colnames(Chems.Out)<-unlist(tstrsplit(colnames(Chems.Out),"[.][.][.]",keep=1))
  template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))

  setnames(Chems.Out,"C.Brand","C.Name")
  Chems.Out[,C.Type:=gsub("Animal - ","",C.Type)]
  
  allowed_values<-data.table(allowed_values=list(unique(c(master_codes$chem[,C.Name],master_codes$chem[,C.Name.AI...16])),
                                                 master_codes$lookup_levels[Field=="C.App.Method",Values_New],
                                                 master_codes$lookup_levels[Field=="C.Unit",Values_New]),
                             parent_tab_name=c("master_codes$chem","master_codes$lookup_levels","master_codes$lookup_levels"),
                             field=c("C.Name","C.App.Method","C.Unit"))
  
  results<-validator(data=Chems.Out,
                   compulsory_cols = c(C.Level.Name="C.Type",C.Level.Name="C.Name"),
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
                   time_data = Times.Out,
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

    # 3.9.2.1) !!TO DO: Harmonization #######

if(F){
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

# 3.9.3) Chems.AI ####
table_name<-"Chems.Out"
Chems.AI<-excel_dat[[table_name]][,c(21:27)]
template_cols<-c(master_template_cols[[table_name]][c(21:27)],"B.Code")
Chems.AI$B.Code<-Pub.Out$B.Code
table_name<-"Chems.AI"

colnames(Chems.AI)<-unlist(tstrsplit(colnames(Chems.AI),"[.][.][.]",keep=1))
template_cols<-unlist(tstrsplit(template_cols,"[.][.][.]",keep=1))

colnames(Chems.AI)[1]<-"C.Name"
table_name[1]<-"C.Name"

Chems.AI<-Chems.AI[!is.na(C.Name)]

allowed_values<-data.table(allowed_values=list(unique(master_codes$chem[,C.Name.AI...16]),
                                               master_codes$lookup_levels[Field=="C.Unit",Values_New]),
                           parent_tab_name=c("master_codes$chem","master_codes$lookup_levels"),
                           field=c("C.Name.AI","C.AI.Unit"))

results<-validator(data=Chems.AI,
                   compulsory_cols = c(C.Name="C.Name.AI"),
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

# 3.x) Grazing Management ####
names(excel_dat)
# 3.x) Pasture ####
# 3.x) Planting ####
# 3.x) Tillage ####
# 3.x) Fertilizer ####

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

results<-validator(data=MT.Out,
                   numeric_cols = c("T.Reps","T.Animals","T.Start.Year"),
                   unique_cols = "T.Name",
                   zero_cols=c("T.Start.Year","T.Start.Season","A.Level.Name","AF.Level.Name","C.Level.Name","O.Level.Name","V.Level.Name"),
                   trim_ws = T,
                   check_keyfields=data.table(parent_tab=list(Animals.Out,AF.Out,Chems.Code,Other.Out,Var.Out),
                                              parent_tab_name=c("Animals.Out","AF.Out","Chems.Code","Other.Out","Var.Out"),
                                              keyfield=c("A.Level.Name","AF.Level.Name","C.Level.Name","O.Level.Name","V.Level.Name")),
                   compulsory_cols = c(T.Name="T.Name",T.Name="P.Product"),
                   duplicate_field = "T.Name",
                   duplicate_ignore_fields = c("T.Name"),
                   rm_duplicates=F,
                   tabname="MT.Out")

error_dat<-results$errors[!(grepl("[.][.]",value) & grepl("Duplicate rows exist",issue))]
errors<-c(errors,list(error_dat))

MT.Out<-results$data

# Convert T.Start.Year and T.Reps fields to integers
MT.Out[,T.Start.Year:=as.integer(T.Start.Year)][,T.Reps:=as.integer(T.Reps)][,T.Animals:=as.integer(T.Animals)]
  
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
                   zero_cols = colnames(Out.Out)[3:13],
                   compulsory_cols = c(Out.Code.Joined="Out.Unit"),
                   unit_pairs = data.table(unit="Out.WG.Unit",var="Out.WG.Start",name_field="Out.Code.Joined"),
                   unique_cols = "Out.Code.Joined",
                   duplicate_field = "Out.Code.Joined",
                   rm_duplicates = T,
                   trim_ws = T,
                   tabname="Out.Out")

error_dat<-results$errors[!grepl("Unspecified",value)]
errors<-c(errors,list(error_dat))

Out.Out<-results$data

# Incorrect depths
error_dat<-Out.Out[Out.Depth.Upper>Out.Depth.Lower
][,list(value=paste0(Out.Code.Joined,collapse="/")),by=B.Code
][,table:="Out.Out"
][,field:="Out.Code.Joined"
][,issue:="Upper depth is greater than lower depth."
]
errors<-c(errors,list(error_dat))

# Identical Out.Code.Joined due to Out.WG.Days missing from the text concatenation
error_dat<-Out.Out[,x:=paste(B.Code,Out.Code.Joined)][,N:=.N,by=x][N>1][,.(value=paste(unique(Out.Code.Joined),collapse="/")),by=B.Code][,table:=table_name][,field:="Out.Code.Joined"][,issue:="Non-unique Out.Code.Joined values due to Out.WG.Days values being missed in Out.Code.Joined name generation."]
errors<-c(errors,list(error_dat))

# Use setNames to map old values to new values
replacement_map <- setNames(out_name_changes$new_values, out_name_changes$old_values)
# Apply str_replace_all to the entire Out.Subind column using the named vector
Out.Out[, Out.Subind := stringr::str_replace_all(Out.Subind, replacement_map)]
Out.Out[, Out.Code.Joined := stringr::str_replace_all(Out.Code.Joined, replacement_map)]

Out.Out[,c("x","N"):=NULL]
  
  # 5.2) Harmonization #####
  # Harmonize Out.WG.Days 
  Out.Out[Out.WG.Days==488]  

  # 5.1) Save errors  ######
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
  