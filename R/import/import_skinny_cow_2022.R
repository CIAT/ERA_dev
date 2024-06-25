require(readxl)
require(openxlsx) 
require(xlsx)
require(doSNOW)
require(pbapply)
require(parallel)
require(soiltexture)
require(data.table)

waitifnot <- function(cond) {
  if (!cond) {
    message(deparse(substitute(cond)), " is not TRUE")  
    while (TRUE) {}
  }
}

# Replace 0 with NA
ZeroFun<-function(Data,Zero.Cols){
  Data[Data==0]<-NA
  return(Data)
}

# Set cores for parallel processing -----
Cores<-10
# Set Directories ------


DataDir<-gsub("ERA/ERA",'ERA/Data Entry/Data Entry 2022/Data',getwd())
DataDirs<-list.dirs(DataDir)[-1]

files<-lapply(DataDirs,FUN=function(Dir){
  X<-list.files(Dir)
  X<-gsub(".xlsm","",unlist(tstrsplit(X," - ",keep=2)),fixed = T)
  X<-unlist(tstrsplit(X," (",keep=1,fixed=T))
  X
})
names(files)<-unlist(tail(tstrsplit(DataDirs,"/"),1))

# Check for duplicates within folders
duplicates<-lapply(files,FUN=function(X){
  X<-table(X)
  X[X>1]
})

names(duplicates)<-names(files)

# Check for duplicates between folders
dups<-lapply(1:length(files),FUN=function(i){
  a<-unique(files[[i]])
  dups<-lapply(files[-i],FUN=function(X) {
    b<-a[a %in% X]
    if(length(b)==0){
      NULL
    }else{
      b
    }
  })
  
  dups[!sapply(dups,is.null)]
  
  
})

names(dups)<-names(files)

DataDirs<-DataDirs[grep("Quality Controlled|Extracted",DataDirs)]


# Create Functions ####
replace_zero_with_NA<-function(data){
  data[data==0]<-NA
  return(data)
}
# Read Concepts & Harmonization Sheets -----

# Read in Fertilizers
FertCodes<-data.table::fread("Concept Scheme/Fertilizer.csv",header=T,strip.white=F,encoding="Latin-1")
FertCodes[,F.Lab:=paste0(F.Category,"-",F.Type)]

# Read in Trees
TreeCodes<-data.table::fread("Concept Scheme/Trees.csv",header=T,strip.white=F,encoding="Latin-1")
TreeCodes<-na.omit(TreeCodes, cols=c("EU"))

# Read in Products
EUCodes<-data.table::fread("Concept Scheme/EU.csv",header=T,strip.white=F,encoding="Latin-1")
if(sum(table(EUCodes[,EU])>1)){print("Duplicate codes present in products")}

# Read in component concepts EU_Components
Comp.Codes<-data.table::fread("Concept Scheme/EU_Components.csv",header=T,strip.white=F,encoding="Latin-1")

# Read in Outcomes
OutcomeCodes<-data.table::fread("Concept Scheme/Outcomes.csv",header=T,strip.white=F,encoding="Latin-1")
if(sum(table(OutcomeCodes[,Code])>1)){print("Duplicate codes present in outcomes")}

# Read in Practice Codes
PracticeCodes<-data.table::fread("Concept Scheme/Practices.csv",header=T,strip.white=F,encoding="Latin-1")
if(sum(table(PracticeCodes[,Code])>1)){print("Duplicate codes present in practices")}
PracticeCodesX<-data.table::copy(PracticeCodes)

# Read in Levels (in particular for journal names and countries)
MasterLevels<-data.table::fread("Concept Scheme/Levels.csv",header=T,strip.white=F,encoding="Latin-1")

# Read in Variety Harmonization Data
VarHarmonization.Traits<-data.table::fread("Concept Scheme/Harmonization/Variety_Traits.csv",header=T,strip.white=F,encoding="Latin-1")
VarHarmonization<-data.table::fread("Concept Scheme/Harmonization/Varieties.csv",header=T,strip.white=F,encoding="Latin-1")

# Read in Animal Harmonization Data
AnimalHarmonization<-data.table::fread("Concept Scheme/Harmonization/Animal_Lists.csv",header=T,strip.white=F,encoding="Latin-1")
AnimalHarmonization.Diets<-AnimalHarmonization[,22:30][D.Type.Correct!=""]
AnimalHarmonization.Process<-AnimalHarmonization[,32:36][D.Process!=""]

# Read in Corrections
OtherHarmonization<-data.table::fread("Concept Scheme/Harmonization/Other_Fields.csv",header=T,strip.white=F,encoding="Latin-1")
N<-grep("[.][.][.]",colnames(OtherHarmonization))
OtherHarmonization<-OtherHarmonization[,-..N]

UnitHarmonization<-data.table::fread("Concept Scheme/Harmonization/Unit_Lists.csv",header=T,strip.white=F,encoding="Latin-1")

SiteHarmonization<-data.table::fread("Concept Scheme/Harmonization/Site_Names.csv",header=T,strip.white=T,encoding="Latin-1")
SiteHarmonization[is.na(ED.Site.ID.Corrected)|ED.Site.ID.Corrected=="",ED.Site.ID.Corrected:=ED.Site.ID]
SiteHarmonization[is.na(Site.LatD.Corrected)|Site.LatD.Corrected=="",Site.LatD.Corrected:=Site.LatD]
SiteHarmonization[is.na(Site.LonD.Corrected)|Site.LonD.Corrected=="",Site.LonD.Corrected:=Site.LonD]
SiteHarmonization[is.na(Site.Buffer.Manual.Corrected)|Site.Buffer.Manual.Corrected=="",Site.Buffer.Manual.Corrected:=Site.Buffer.Manual]
SiteHarmonization[Site.Buffer.Manual==""|Site.Buffer.Manual=="NA"|is.na(Site.Buffer.Manual),Site.Buffer.Manual:=NA]
SiteHarmonization[Site.Lon.Unc==""|Site.Lon.Unc=="NA"|is.na(Site.Lon.Unc),Site.Lon.Unc:=NA]
SiteHarmonization[Site.Lat.Unc==""|Site.Lat.Unc=="NA"|is.na(Site.Lat.Unc),Site.Lat.Unc:=NA]

# Duplicate rows and split on B.Code & ".." in Site Name
SiteHarmonization<-rbindlist(lapply(1:nrow(SiteHarmonization),FUN=function(i){
  X<-SiteHarmonization[i]
  Y<-X[,B.Codes]
  if(grepl("[/]",Y)){
    Y<-unlist(strsplit(Y,"[/]"))
    X<-X[rep(1,length(Y))]
    X[,B.Codes:=Y]
  }
  return(X)
}))
SiteHarmonization<-unique(rbindlist(lapply(1:nrow(SiteHarmonization),FUN=function(i){
  X<-SiteHarmonization[i]
  Y<-X[,ED.Site.ID]
  Z<-X[,ED.Site.ID.Corrected]
  if(grepl("[.][.]",Y)){
    Y<-unlist(strsplit(Y,"[.][.]"))
    X<-X[rep(1,length(Y))]
    X[,ED.Site.ID:=Y]
    X[,ED.Site.ID.Corrected:=unlist(strsplit(Z,"[.][.]"))]
  }
  return(X)
})))

# Set numeric columns to correct class
num_cols<-c("Site.LatD.Corrected","Site.LonD.Corrected","Site.Buffer.Manual.Corrected")
SiteHarmonization[,(num_cols):=lapply(.SD, as.character),.SDcols=num_cols]
SiteHarmonization[,(num_cols):=lapply(.SD, as.numeric),.SDcols=num_cols]

# Read In the Master Sheet ------

Master<-paste0(gsub("ERA/ERA",'ERA/Data Entry/Data Entry 2022/Excel Form',getwd()),"/V1.06.3 - Skinny Cow.xlsm")

# List sheet names that we need to extract
SheetNames<-openxlsx::getSheetNames(Master)[grep(".Out",openxlsx::getSheetNames(Master))]

SheetNames<-SheetNames[!grepl("Till|Plant|Fert|Residues|Weed|Harvest|pH|PH|WH|Irrig|Int|Rot",SheetNames)]

SheetNames<-c(SheetNames,"Times")

# List column names for the sheets to be extracted
XL.M<-lapply(SheetNames,FUN=function(SName){
  cat('\r                                                                                                                                          ')
  cat('\r',paste0("Importing Sheet = ",SName))
  flush.console()
  colnames(data.table(suppressWarnings(suppressMessages(readxl::read_excel(Master,sheet = SName)))))
})

names(XL.M)<-SheetNames
# Squish Bugs / Subset Cols
XL.M[["AF.Out"]]<-XL.M[["AF.Out"]][1:9]

# Check for Duplicate Files -----

Files<-unlist(lapply(DataDirs, list.files,".xlsm",full.names=T))
Files<-Files[!grepl("[/][~][$]",Files)]

FNames<-trimws(unlist(tstrsplit(Files,"-",keep=length(unlist(strsplit(Files[1],"-"))))))
FNames<-gsub("[(]1[])]","",FNames)
FNames<-gsub(" ","",FNames)

FNames[nchar(FNames)<11]<-gsub("00","000",FNames[nchar(FNames)<11])

DuplicateFiles<-lapply(names(table(FNames)[table(FNames)>1]),FUN=function(X){
  Files[grep(X,Files)]
})

if(length(DuplicateFiles)>0){
  print("Duplicated Filenames Detected")
  print(DuplicateFiles)
  View(unlist(DuplicateFiles))
}
rm(DuplicateFiles)

# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# <><><><>MAKE SURE THERE ARE NO DUPLICATES <><><><> ####
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# Read In Data from Excel Files ####


cl<-makeCluster(Cores)
clusterEvalQ(cl, list(library(data.table),library(openxlsx)))
clusterExport(cl,list("Files","SheetNames","FNames","XL.M"),envir=environment())
registerDoSNOW(cl)

XL<-parLapply(cl,1:length(Files),fun=function(i){
#XL<-lapply(1:length(Files),FUN=function(i){
  
  cat('\r                                                                                                                                          ')
  cat('\r',paste0("Importing File ",i,"/",length(Files)," - ",FNames[i]))
  flush.console()
  
  # Load the Excel file into a workbook object
  wb <- loadWorkbook(Files[i])
  
  X<-lapply(SheetNames,FUN=function(SName){
    cat('\r                                                                                                                                          ')
    cat('\r',paste0("Importing File ",i,"/",length(Files)," - ",FNames[i]," | Sheet = ",SName))
    flush.console()
    
    Y <- data.table(readWorkbook(wb, SName))
    
    if(SName=="AF.Out"){
      Y<-Y[,1:9]
    }
    
    Y
  })
  
  names(X)<-SheetNames
  X
  
})

stopCluster(cl)

names(XL)<-FNames

rm(Files,SheetNames,XL.M,Master)

# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# ***PREPARE DATA*** ----
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# ***Publication (Pub.Out) =====
Pub.Out<-rbindlist(lapply(XL,"[[",1))

# Replace zeros with NAs
Zero.Cols<-c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4")
Pub.Out<-Pub.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

# Add file name minus the ".xlsm"
Pub.Out[,FileName:=gsub(".xlsm","",names(XL))]

# Find mismatches between B.Codes reported in the excel and the excel file name
Pub.Out[,mismatch:=F][unlist(tstrsplit(B.Code,"[.]",keep=1))!=unlist(tstrsplit(FileName,"[.]",keep=1)),mismatch:=T]
if(nrow(Pub.Out[mismatch==T])>0){
  View(Pub.Out[mismatch==T,list(B.Code,FileName)])
}

# Set B.Codes to match filenames where there is no mismatch and remove filename fields from table
Pub.Out[B.Code!=FileName & mismatch!=T,B.Code:=FileName][,FileName:=NULL]

# Pub.Out: Validation: Duplicate B.Codes ####

# Create a data table called "Dups" with two columns:
Dups<-data.table(BibCode=Pub.Out[,B.Code],FileName=gsub(".xlsm","",names(XL)))

# Add two additional columns to the Dups data table & Identify duplicates based on BibCode and FileName columns
Dups[,BibCode_N:=.N,by=BibCode][,BibCode_Dup:=F][BibCode_N>1 & !grepl("[.]",FileName),BibCode_Dup:=T]

if(nrow(Dups[BibCode_Dup==T])>0){
  View(Dups[BibCode_Dup==T])
  print(grep(Dups[BibCode_Dup==T,paste(BibCode,collapse = "|")],Files,value = T))
  rm(N,Duplicate.B.Codes)
}
rm(Dups)

Pub.Out[,mismatch:=NULL]
# ***Site (Site.Out) =====
  Site.Out<-lapply(XL,"[[",2)
  Site.Out<-rbindlist(lapply(1:length(Site.Out),FUN=function(i){
    SS<-Site.Out[[i]]
    SS<-SS[-1,]
    SS$B.Code<-Pub.Out$B.Code[i]
    SS
  }))
  
  # Remove any parenthesis in names
  #Site.Out[,Site.ID:=gsub("[(]","",Site.ID)][,Site.ID:=gsub("[)]","",Site.ID)]
  
  # Tidy up issues with column names
  colnames(Site.Out)[colnames(Site.Out)=="LatD"]<-c("LatD","LonD")
  colnames(Site.Out)[colnames(Site.Out)=="MSP"]<-c("MSP.S1","MSP.S2")
  
  # Remove commas and set blanks or character "NA" to NA
  Site.Out[,LatD:=gsub(",","",LatD)][LatD %in% c("","NA"),LatD:=NA]
  Site.Out[,LonD:=gsub(",","",LonD)][LonD %in% c("","NA"),LonD:=NA]
  Site.Out[,Buffer.Manual:=gsub(",","",Buffer.Manual)][Buffer.Manual %in% c("","NA")|is.na(Buffer.Manual),Buffer.Manual:=NA]
  Site.Out[,Lon.Unc:=gsub(",","",Lon.Unc)][Lon.Unc %in% c("","NA")|is.na(Lon.Unc),Lon.Unc:=NA]
  Site.Out[,Lat.Unc:=gsub(",","",Lat.Unc)][Lat.Unc %in% c("","NA")|is.na(Lat.Unc),Lat.Unc:=NA]
  
  # Remove any whitespace from Site.ID field
  Site.Out[,Site.ID:=trimws(Site.ID)]
  
  # Replace zeros with NAs
  Zero.Cols<-c("Loc.Fixed","Lat.Unc","Lon.Unc","Rain.Seasons","Start.S1","End.S1","Length.S1","Start.S2","End.S2","Length.S2","MSP.S1","MSP.S2" ,"MAP","MAT","Soil.Texture")
  Site.Out<-Site.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]
  
  # Set numeric columns to correct class
  num_cols<-c("LatD","LonD","MAP","MAT","Elevation","Slope.Perc","Slope.Degree","Buffer.Manual","Lat.Unc","Lon.Unc")
  Site.Out[,(num_cols):=lapply(.SD, as.character),.SDcols=num_cols]
  Site.Out[,(num_cols):=lapply(.SD, as.numeric),.SDcols=num_cols]
  
  # Round values
  round_cols<-c("LatD","LonD","MAT","MAP")
  Site.Out<-Site.Out[,(round_cols):=lapply(.SD, round,6),.SDcols=round_cols]

  # Add "Site." prefix to colnames without none
  colnames(Site.Out)[-c(1:6,28)]<-paste0("Site.",colnames(Site.Out)[-c(1:6,28)])
  
  # Update site buffer manual field
  Site.Out[is.na(Site.Buffer.Manual),Site.Buffer.Manual:=(Site.Lon.Unc+Site.Lat.Unc)/2]
  

  # Site.Out: Update Fields From Harmonization Sheet ####
  Site.Out[,h_match_1:=match(Site.Out[, Site.ID],SiteHarmonization[,ED.Site.ID])]
  Site.Out[,h_match_2:=match(Site.Out[, Site.ID],SiteHarmonization[,ED.Site.ID.Corrected])]
  Site.Out[,h_match_12:=h_match_1][is.na(h_match_1),h_match_12:=h_match_2]
    
  # Sites/B.Codes combos with no match in harmonization table
  Site.No.Match<-unique(Site.Out[is.na(h_match_2) & !grepl("[.][.]",Site.ID),list(B.Code,Site.ID,Site.LatD,Site.LonD,Site.Lon.Unc,Site.Lat.Unc,Site.Buffer.Manual)])
  if(nrow(Site.No.Match)>0){
   View(Site.No.Match) 
   write.table(Site.No.Match,"clipboard",row.names = F,sep="\t")
  }
  rm(Site.No.Match)
  
  # Update sites with harmonization information
  if(Site.Out[!grepl("[.][.]",Site.ID),sum(is.na(h_match_2))>0]){
    N<- Site.Out[,which(!is.na(h_match_1) & is.na(h_match_2))]
    N2<-Site.Out[N,h_match_1]
    Site.Out[N,Site.LatD:=SiteHarmonization[N2,Site.LatD.Corrected]]
    Site.Out[N,Site.LonD:=SiteHarmonization[N2,Site.LonD.Corrected]]
    Site.Out[N,Site.Buffer.Manual:=SiteHarmonization[N2,Site.Buffer.Manual.Corrected]]
    Site.Out[N,Site.ID:=SiteHarmonization[N2,ED.Site.ID.Corrected]]
  }
  # Remove indexing columns
  Site.Out[,c("h_match_1","h_match_2","h_match_12"):=NULL]
  
  # Find any non-aggregated sites that are missing spatial uncertainty
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

# ***Soil (Soil.Out) =====
  Soil.Out<-lapply(XL,"[[","Soils.Out")
  
  Error.Cols<-c("Soil.CEC.Unit", "Soil.EC.Unit", "Soil.FC.Unit", "Soil.N.Unit", "Soil.NH4.Unit", "Soil.NO3.Unit", "Soil.SOC.Method", "Soil.SOM.Method", "Soil.pH.Method")
  
  Soil.Out<-rbindlist(lapply(1:length(Soil.Out),FUN=function(i){
    X<-Soil.Out[[i]]
    X<-na.omit(X, cols=c("Site.ID"))
    
    # Fix Excel Bug Where NA values showing in nrows>1
    if(nrow(X)>1){
     
      X<-as.data.frame(X)
      X[-1,Error.Cols]<-X[rep(1,nrow(X)-1),Error.Cols]
      X<-as.data.table(X)
      
    }
    
    X$B.Code<-Pub.Out$B.Code[i]
  
    X
  }))
  
  # Remove any parenthesis in names
  #Soil.Out[,Site.ID:=gsub("[(]","",Site.ID)][,Site.ID:=gsub("[)]","",Site.ID)]
  
  # Correct duplicate column names
  colnames(Soil.Out)[colnames(Soil.Out)=="Soil.N.Unit"]<-c("Soil.TN.Unit","Soil.AN.Unit")

  # Deal with 0 that should be NA
  # Where depth are both 0 set to NA
  Soil.Out[Soil.Upper==0 & Soil.Lower==0,Soil.Upper:=NA]
  Soil.Out[is.na(Soil.Upper) & Soil.Lower==0,Soil.Lower:=NA]
  Soil.Out[,Texture.Total:=SND+SLT+CLY]
  Soil.Out[Texture.Total<98 & SND==0,SND:=NA]
  Soil.Out[Texture.Total<98 & SLT==0,SLT:=NA]
  Soil.Out[Texture.Total<98 & CLY==0,CLY:=NA]
  
  # Replace zeros with NAs
  Zero.Cols<-c("Soil.BD","Soil.TC","Soil.SOC","Soil.SOM","Soil.pH","Soil.CEC","Soil.EC","Soil.FC","Soil.TN","Soil.NH4","Soil.NO3","Soil.AN","SND.Unit","SLT.Unit","CLY.Unit",
               "Soil.BD.Unit","Soil.TC.Unit","Soil.FC.Unit","Soil.SOC.Unit","Soil.SOM.Unit","Soil.CEC.Unit","Soil.EC.Unit","Soil.TN.Unit","Soil.NH4.Unit","Soil.NO3.Unit",
               "Soil.AN.Unit","Soil.SOC.Method","Soil.SOM.Method","Soil.pH.Method")
  Soil.Out<-Soil.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

  # Soil.Out: Calculate USDA Soil Texture from Sand, Silt & Clay ####
  
  Soil.Out[,N:=1:.N]
  Soil.Out[,Soil.Text.N:=sum(!is.na(CLY),!is.na(SND),!is.na(SLT)),by=N]
  
  N<-which(Soil.Out[,Soil.Text.N]==2)
  Soil.Out<-data.frame(Soil.Out)
  for(i in N){
    NX<-which(is.na(unlist(Soil.Out[i,c("CLY","SND","SLT")])))
    NX2<-c("CLY","SND","SLT")[!c("CLY","SND","SLT") %in% names(NX)]
    Soil.Out[i,names(NX)]<-100-sum(Soil.Out[i,NX2])
  }
  Soil.Out<-data.table(Soil.Out)
  
  X<-Soil.Out[!(is.na(CLY)|is.na(SND)|is.na(SLT)),c("CLY","SND","SLT")]
  setnames(X,c("CLY","SND","SLT"),c("CLAY","SAND","SILT"))
  Texture<-TT.points.in.classes(tri.data =X,class.sys = "USDA.TT",text.tol = 1) 
  
  Texture.FullName<-c("Clay","Silty Clay","Sandy Clay","Clay Loam","Silty Clay Loam","Sandy Clay Loam","Loam","Silty Loam",
                      "Sandy Loam","Silt","Loamy Sand","Sand")
  Texture<-unlist(apply(Texture,1,FUN=function(X){paste(Texture.FullName[which(X>=1)],collapse = "/")}))
  
  Soil.Out[!(is.na(CLY)|is.na(SND)|is.na(SLT)),USDA.Texture:=Texture]
  rm(Texture,Texture.FullName,X,N)
  Soil.Out[,Soil.Text.N:=NULL]
  
  # Site.Out: Update Site.Out Soil Texture from USDA Classes ####
  USDA.Text<-unlist(pblapply(1:nrow(Site.Out),FUN=function(i){
    Texture<-Soil.Out[Soil.Out[,Site.ID] %in% Site.Out[i,Site.ID],USDA.Texture]
    Texture<-unique(unlist(strsplit(Texture,"[/]")))
    if(all(is.na(Texture))){
      NA
    }else{
      paste(Texture[!is.na(Texture)],collapse = "/")
    }
  }))
  
  Site.Out[!is.na(USDA.Text),Site.Soil.Texture:=USDA.Text[!is.na(USDA.Text)]]
  rm(USDA.Text)
  
  # Soil.Out: Update Fields From Harmonization Sheet ####
  N<-match(trimws(Soil.Out[,Site.ID]),SiteHarmonization[,ED.Site.ID])
  
  Soil.Out[!is.na(N),ED.Site.ID:=SiteHarmonization[N[!is.na(N)],ED.Site.ID.Corrected]]
  
  # Soil.Out: Update Aggregated Site Name
  Soil.Out[grepl("[.][.]",Site.ID),Site.ID:=Agg.Site.Name.Fun(Site.ID[1],SiteHarmonization),by=Site.ID]

  # Soil.Out: Validation ####
  # Check Lower>Upper
  unique(Soil.Out[Soil.Upper>Soil.Lower,c("B.Code","Site.ID","Soil.Upper","Soil.Lower")])
  
# ***Experimental Design (ExpD.Out) ====
ExpD.Out<-lapply(XL,"[[","ExpD.Out")
ExpD.Out<-rbindlist(lapply(1:length(ExpD.Out),FUN=function(i){
  X<-ExpD.Out[[i]]
  X[,B.Code:=Pub.Out[,B.Code[i]]]
  X
}))

# Replace zeros with NAs
Zero.Cols<-c("EX.Design","EX.Plot.Size","EX.HPlot.Size","EX.Notes")
ExpD.Out<-ExpD.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

# ***Products (Prod.Out) =====
Prod.Out<-lapply(XL,"[[",5)
Prod.Out<-rbindlist(lapply(1:length(Prod.Out),FUN=function(i){
  X<-Prod.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("P.Product"))
  X
}))

# Validation: Problem of "." in product names using "sp." - change to sp. to sp
Prod.Out[grepl("Sp[.]",P.Product) | grepl("sp[.]",P.Product) & !grepl("sp[.][.]|Sp[.][.]",P.Product)]

# Remove unnecessary columns
Prod.Out[,c("P.New","P.New.Nfix","P.New.Tree","P.New.Name"):=NULL]

# ***Var (Var.Out) =====
Var.Out<-lapply(XL,"[[","Var.Out")
Var.Out<-rbindlist(lapply(1:length(Var.Out),FUN=function(i){
  X<-Var.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("V.Var"))
  X
}))

Var.Out[,c("V.New.Crop","V.New.Var","V.New.Species","V.New.SubSpecies","V.Crop.Practice","V.Code.Animal","V.Maturity"):=NULL]

# Replace zeros with NAs
Zero.Cols<-c("Join","V.Var","V.Species","V.Subpecies","V.Animal.Practice","V.Base","V.Type","V.Trait1","V.Trait2","V.Trait3","V.Codes")
Var.Out<-Var.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]


# Remove any parenthesis in names
#Var.Out[,V.Var:=gsub("[(]","",V.Var)
#        ][,V.Var:=gsub("","",V.Var)
#          ][,V.Species:=gsub("[(]","",V.Species)
#            ][,V.Species:=gsub("[)]","",V.Species)
#              ][,Join:=gsub("[(]","",Join)
#                ][,Join:=gsub("[)]","",Join)]

setnames(Var.Out,"V.Subpecies","V.Subspecies")

# Save Join Field before updating
Var.Join<-Var.Out[,list(B.Code,V.Var)]
setnames(Var.Join,"V.Var","Join")
Var.Join[is.na(Join),Join:=Var.Out[is.na(V.Var),V.Species]]

if(F){
  # Var.Out: Validation - Check V.Codes == 0
  X<-Var.Out[V.Codes==0,c("V.Product","V.Var","V.Species","V.Animal.Practice","B.Code")]
  write.table(X,"clipboard",row.names = F,sep="\t")
  
  # Extract values for external harmonization
  X<-unique(Var.Out[,c("V.Product","V.Var","V.Species","V.Subspecies","V.Animal.Practice","V.Type","V.Trait1","V.Trait2","V.Trait3")
                    ][order(V.Product,V.Var,V.Species)])
  write.table(X,"clipboard-256000",row.names = F,sep="\t")
  rm(X)
}

  # Var.Out: Harmonize Variety Naming and Codes ####

  Var.Fun<-function(V.Product,V.Var){
    Vars<-unlist(strsplit(V.Var,"[$][$]"))
    return(list(match(paste(V.Product,Vars),VarHarmonization[,paste(V.Product,V.Var1)])))
  }
  
  Var.Out[,N:=1:.N]
  Var.Out[,V.Match:=list(Var.Fun(V.Product,V.Var)),by=N]
  
  # Check for any non-matches
  Var.Out[,V.Match.NA:=sum(is.na(unlist(V.Match))),by=N]
  Variety.Non.Matches<-Var.Out[V.Match.NA>0,list(B.Code,V.Product,V.Var,V.Match)][!is.na(V.Var)]
  
  if(nrow(Variety.Non.Matches)>0){
    View(Variety.Non.Matches)
    X<-Variety.Non.Matches[,!"B.Code"][,!"V.Match"]
    X<-unique(X[,V.Var:=unlist(V.Var)])[order(V.Product,V.Var)]
    write.table(X,"clipboard",row.names = F,sep="\t")
    
    }
  
  rm(Variety.Non.Matches,Var.Fun)
  
  # Recreate Var.Out table from harmonization table
  if(F){
  Extract.Fun<-function(B.Code,V.Base,V.Match,N){
    if(!is.na(V.Match)){
    Y<-VarHarmonization[unlist(V.Match),list(V.Product,V.Var1,V.Species1,V.Subspecies1,V.Crop.Practice,V.Animal.Practice,V.Type,V.Trait1,V.Trait2,V.Trait3,V.Maturity,V.Code)]
    Y<-data.table(B.Code,V.Base,t(apply(Y,2,FUN=function(Z){paste(unique(Z),collapse = "$$")})))
    Y[V.Var1=="NA",V.Var1:=NA]
    Y[,Join:=if(!is.na(V.Var1)){V.Var1}else{V.Species1}]
    }else{
      Y<-Var.Out[N]
    }
    return(Y)
  }
  
  Var.Out2<-Var.Out[,Extract.Fun(B.Code,V.Base,V.Match,N=N),by=N]
  
  setnames(Var.Out2,"V.Var1","V.Var")
  setnames(Var.Out2,"V.Species1","V.Species")
  setnames(Var.Out2,"V.Subspecies1","V.Subspecies")
  setnames(Var.Out2,"V.Code","V.Codes")
  
  # Check to see if there are any entries with no species or variety
  Var.NoVar.NoSpecies<-Var.Out2[is.na(V.Var) & is.na(V.Species),]
  if(nrow(Var.NoVar.NoSpecies)>0){
    View(Var.NoVar.NoSpecies)
  }
  
  Var.Out<-Var.Out2
  
  rm(Var.NoVar.NoSpecies,Var.Out2,Extract.Fun)
  }
  
  # Var.Out: Update trait labels ####
  if(F){
  Trait.Match.Fun<-function(Trait){
    TraitM<-unlist(strsplit(Trait,"[$][$]"))
    paste(unique(VarHarmonization.Traits[match(TraitM,V.Trait),V.Trait1]),collapse="$$")
  }
  
  Var.Out[,Trait1.New:=Trait.Match.Fun(V.Trait1),by=N]
  Var.Out[,Trait2.New:=Trait.Match.Fun(V.Trait2),by=N]
  Var.Out[,Trait3.New:=Trait.Match.Fun(V.Trait3),by=N]
  
  Var.Out[Trait1.New=="NA",Trait1.New:=NA]
  Var.Out[Trait2.New=="NA",Trait2.New:=NA]
  Var.Out[Trait3.New=="NA",Trait3.New:=NA]
  Var.Out[V.Trait1=="NA",V.Trait1:=NA]
  Var.Out[V.Trait2=="NA",V.Trait2:=NA]
  Var.Out[V.Trait3=="NA",V.Trait3:=NA]
  
  Var.Out[,N:=NULL]
  
  No.Trait.Match<-unique(rbind(
  Var.Out[!is.na(V.Trait1) & is.na(Trait1.New),list(B.Code,V.Product,V.Trait1)],
  Var.Out[!is.na(V.Trait2) & is.na(Trait2.New),list(B.Code,V.Product,V.Trait2)],
  Var.Out[!is.na(V.Trait3) & is.na(Trait3.New),list(B.Code,V.Product,V.Trait3)],use.names=F))
  
  if(nrow(No.Trait.Match)>0){
    View(No.Trait.Match)
  }
  
  rm(No.Trait.Match,Trait.Match.Fun)
  
  Var.Out[,V.Trait1:=Trait1.New][,Trait1.New:=NULL]
  Var.Out[,V.Trait2:=Trait2.New][,Trait2.New:=NULL]
  Var.Out[,V.Trait3:=Trait3.New][,Trait3.New:=NULL]
  }
  
  Var.Out[,c("V.Code.Crop","N","V.Match","V.Match.NA"):=NULL]
  
# ***Agroforestry (AF.Out)=====
AF.Out<-lapply(XL,"[[","AF.Out")
AF.Out<-rbindlist(lapply(1:length(AF.Out),FUN=function(i){
  X<-AF.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("AF.Level.Name"))
  X
  
}))

# Remove any parenthesis in names
#AF.Out[,AF.Level.Name:=gsub("[(]","",AF.Level.Name)][,AF.Level.Name:=gsub("[)]","",AF.Level.Name)]

# ***Chemicals (Chems.Out) =====
Chems.Out<-lapply(XL,"[[","Chems.Out")
Chems.Out<-rbindlist(lapply(1:length(Chems.Out),FUN=function(i){
  X<-Chems.Out[[i]][,!"C.Name"]
  X$B.Code<-Pub.Out$B.Code[i]
  X
}))
setnames(Chems.Out, "C.Activity", "C.Level.Name")

# Replace zeros with NAs
Zero.Cols<-c("C.AI.Name","C.AI.Amount","C.AI.Unit","C.Amount","C.Unit","C.Applications","C.Date","C.Date.Stage","C.Date.DAP","C.Date.Text","C.Brand","C.Target")
Chems.Out<-Chems.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

# C.Type field must contain data
Chems.Out<-Chems.Out[!is.na(C.Type)]

# Remove any parenthesis in names
#Chems.Out[,C.Level.Name:=gsub("[(]","",C.Level.Name)][,C.Level.Name:=gsub("[)]","",C.Level.Name)]

#Code for Herbicide
Chems.Out[C.Type=="Herbicide",C.Code:="h66.1"]

Chems.Code<-Chems.Out[,list(C.Code=unique(C.Code[!is.na(C.Code)])),by=list(B.Code,C.Level.Name,C.Structure)]

# ***Animals (Animals.Out) =====

#Animals.Diet ####
  Animals.Out<-lapply(XL,"[[","Animals.Out")
  
  Animals.Diet<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
    X<-Animals.Out[[i]][,20:33]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("A.Level.Name"))
    X
  }))
  
  # Replace zeros with NAs
  Zero.Cols<-c("D.Item","D.Item.Group","D.Source","D.Process","D.Amount","D.Ad.lib","D.Unit.Amount","D.Unit.Time","D.Unit.Animals","D.Day.Start","D.Day.End","DC.Is.Dry")
  Animals.Diet<-Animals.Diet[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

  # Animals.Diet: Remove any parenthesis in names ####
  #Animals.Diet[, A.Level.Name:=gsub("[(]","", A.Level.Name)
  #             ][, A.Level.Name:=gsub("[)]","", A.Level.Name)]
  
  #Animals.Diet[, D.Item:=gsub("[(]","", D.Item)
  #                 ][, D.Item:=gsub("[)]","", D.Item)]
  
  # Trim whitespace
  Animals.Diet[,D.Process:=trimws(D.Process)][,D.Process:=gsub(" /","/",D.Process)][,D.Process:=gsub("/ ","/",D.Process)][D.Process=="",D.Process:=NA]
  
  # Animals.Diet: Validation - Check for any missing D.Items

  # Create an in_group option that indicates if a D.Item.Group name is being used
  Animals.Diet[,in_group:=F][,in_group:=D.Type %in% D.Item.Group,by=B.Code]
  Missing.Diet.Item<-Animals.Diet[!is.na(D.Type) & is.na(D.Item) & D.Type !="Entire Diet" & in_group==F]

  if(nrow(Missing.Diet.Item)){
    View(Missing.Diet.Item)
    write.table(Missing.Diet.Item,"clipboard",row.names = F,sep="\t")
  }
  rm(Missing.Diet.Item)
  
  # Find New Diet Items
  New.Diet.Items<-unique(Animals.Diet[!is.na(D.Type) & !is.na(D.Item) & D.Type !="Entire Diet" & in_group==F,list(D.Type,D.Item,B.Code)])
  New.Diet.Items<-New.Diet.Items[!D.Item %in% AnimalHarmonization.Diets$D.Item.Correct][order(D.Type,D.Item,B.Code)]
  write.table(New.Diet.Items,"clipboard-25600",row.names = F,sep="\t")
  
  # Find New Processes
  New.Diet.Processes<-unique(Animals.Diet[!is.na(D.Process),list(D.Process,B.Code)])
  New.Diet.Processes<-New.Diet.Processes[!D.Process %in% AnimalHarmonization.Process$D.Process.Correct][order(D.Process,B.Code)]
  New.Diet.Processes<-unique(unlist(strsplit(New.Diet.Processes$D.Process,"/")))
  New.Diet.Processes<-New.Diet.Processes[!New.Diet.Processes %in% AnimalHarmonization.Process$D.Process.Correct]
  write.table(New.Diet.Processes,"clipboard-25600",row.names = F,sep="\t")
  
  # Extract Diet Items & Units
  if(F){
    X<-unique(Animals.Diet[,c("D.Type","D.Item","B.Code")][order(D.Type,D.Item)][!is.na(D.Item)])
    X<-X[,list(B.Code=paste(B.Code,collapse="/")),by=c("D.Type","D.Item")]
    write.table(X,"clipboard-25600",row.names = F,sep="\t")
    
    write.table(unique(Animals.Diet[,"D.Unit.Amount"][order(D.Unit.Amount)][!is.na(D.Unit.Amount)]),"clipboard-25600",row.names = F,sep="\t")
    write.table(unique(Animals.Diet[,"D.Unit.Animals"][order(D.Unit.Animals)][!is.na(D.Unit.Animals)]),"clipboard-25600",row.names = F,sep="\t")
    write.table(unique(Animals.Diet[,"D.Unit.Time"][order(D.Unit.Time)][!is.na(D.Unit.Time)]),"clipboard-25600",row.names = F,sep="\t")
  }
  # Animals.Diet.Comp ####
  Animals.Diet.Comp<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
    X<-Animals.Out[[i]][,34:121]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("D.Item"))
    # temporary hack, we can remove next time excels are imported (sheet is corrected)
    setnames(X,c("Unspecified","Unspecified.Unit","Unspecified.Method"),
             c("DC.Calcium","DC.Calcium.Unit","DC.Calcium.Method"),
             skip_absent = T)
    X
  }))

  # Animals.Diet.Comp: Remove any parenthesis in names
  # Animals.Diet.Comp[, D.Item:=gsub("[(]","", D.Item)][, D.Item:=gsub("[)]","", D.Item)]
  
  # Replace zeros with NAs (assuming all zeros should be NAs)
  Animals.Diet.Comp<-Animals.Diet.Comp[,lapply(.SD, ZeroFun),.SDcols=colnames(Animals.Diet.Comp)]
  
  # Animals.Diet.Digest ####
  Animals.Diet.Digest<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
 
    X<-Animals.Out[[i]]
    
    if(ncol(X)<199){
      print(Pub.Out$B.Code[i])
      NULL
    }else{
      X<-X[,122:199]
      X$B.Code<-Pub.Out$B.Code[i]
      X<-na.omit(X, cols=c("D.Item"))
      X
    }
  }))
  
  Animals.Diet.Digest<-Animals.Diet.Digest[,lapply(.SD, ZeroFun),.SDcols=colnames(Animals.Diet.Digest)]
  
  # Animals.Out ####
  
  Animals.Out<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
    X<-Animals.Out[[i]][,1:19]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("A.Level.Name"))
    X
  }))
  
  # Animals.Out: Remove any parenthesis in names
  # Animals.Out[, A.Level.Name:=gsub("[(]","", A.Level.Name)][, A.Level.Name:=gsub("[)]","", A.Level.Name)]
  
   # Animals.Out: Validation Checks ####
  Animals.Out[,Psum:=sum(c(P1,P2,P3,P4,P5,P6,P7,P8,P9,P10,P11,P12,P13,P14,A.Grazing,A.Hay)!="0"),by=list(A.Level.Name,B.Code)]
  
  # Error? Animal diet has been specified but there are no practices listed
  Animals.Out[Psum==0 & A.Level.Name!="Base"]
  
  # Error? Base practices with notes specified but no practice listed (this might be just someone record additional notes)
  write.table(
    Animals.Out[Psum==0 & A.Level.Name=="Base" & A.Notes!="0",list(B.Code,A.Level.Name,A.Notes)],
    "clipboard-256000",sep="\t",row.names = F)
  
  # Replace zeros with NAs
  Zero.Cols<-c(paste0("P",1:14),"A.Notes","A.Grazing","A.Hay")
  
  Animals.Out<-Animals.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]
  
  # Rename P.Cols to something more meaningful
  setnames(Animals.Out,paste0("P",1:14),
           c("A.Feed.Add.1","A.Feed.Add.2","A.Feed.Add.3", "A.Feed.Add.C", "A.Feed.Sub.1","A.Feed.Sub.2","A.Feed.Sub.3",
             "A.Feed.Sub.C","A.Feed.Pro.1","A.Feed.Pro.2","A.Feed.Pro.3","A.Manure.Man","A.Pasture.Man","A.Aquasilvaculture"))

  # Animals.Out: Validation - Feed Add Code but no control or >1 control
  
  X<-Animals.Out[(!is.na(A.Feed.Add.1)|!is.na(A.Feed.Add.2)|!is.na(A.Feed.Add.3)) & A.Level.Name!="Base",B.Code]
  Ani.Out_Feed.Add_Controls<-Animals.Out[B.Code %in% X,list(N.Controls=sum(!is.na(A.Feed.Add.C))),by="B.Code"][N.Controls!=1]
  
  if(nrow(Ani.Out_Feed.Add_Controls)>0){
    View(Ani.Out_Feed.Add_Controls)
  }
  rm(Ani.Out_Feed.Add_Controls,X)
  
  # Animals.Out: Validation - Feed Sub Code but no control
  
  X<-Animals.Out[(!is.na(A.Feed.Sub.1)|!is.na(A.Feed.Sub.2)|!is.na(A.Feed.Sub.3)) & A.Level.Name!="Base",B.Code]
  Ani.Out_Feed.Sub_Controls<-Animals.Out[B.Code %in% X,list(N.Controls=sum(A.Feed.Sub.C=="Yes",na.rm = T)),by="B.Code"][N.Controls!=1]
  
  if(nrow(Ani.Out_Feed.Sub_Controls[!B.Code %in% c("AG0051","AG0121","DK0007","EO0080","NJ0013")])>0){
    View(Ani.Out_Feed.Sub_Controls)
  }
  rm(Ani.Out_Feed.Sub_Controls,X)
  
  # Check Processing Values
  #unique(Animals.Diet[!is.na(D.Process),c("D.Process","B.Code")])
  # Check Diet,Item Values
  #table(Animals.Diet[!is.na(D.Item) & !A.Level.Name=="Entire Diet",D.Item])
  # Missing Item Names
  
  write.table(
    Animals.Diet[is.na(D.Item) & !D.Type=="Entire Diet",]
    ,"clipboard-256000",sep="\t",row.names = F)
  
  # Check A.Level.Name has a match
  # Animals.Diet[is.na(match(paste0(Animals.Diet$A.Level.Name,Animals.Diet$B.Code),
  #                         paste0(Animals.Out$A.Level.Name,Animals.Out$B.Code))),]
  
# Other.Out
  Other.Out<-lapply(XL,"[[","Other.Out")
  Other.Out<-rbindlist(lapply(1:length(Other.Out),FUN=function(i){
    X<-Other.Out[[i]][,1:3]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("O.Level.Name"))
    X<-X[!(O.Level.Name=="Base" & O.Structure=="No" & O.Notes==0)]
    X
  }))
  
  Other.Out[O.Notes==0,O.Notes:=NA]
  
  Animals.Out[,Psum:=NULL]
# ***Base Practices (Base.Out) =====
Base.Out<-list(
  Var.Out=Var.Out[V.Base=="Yes" & !is.na(V.Codes),c("B.Code","V.Codes")],
  AF.Out=AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
 # Till.Codes[T.Level.Name=="Base"& !is.na(T.Code),c("B.Code","T.Code")],
 # Fert.Out[F.Level.Name=="Base" & !is.na(F.Codes),c("B.Code","F.Codes")],
 # Res.Out[M.Level.Name=="Base" & !is.na(M.Codes),c("B.Code","M.Codes")],
 # Har.Out[H.Level.Name=="Base" & !is.na(H.Code),c("B.Code","H.Code")],
 # pH.Out[pH.Level.Name=="Base" & !is.na(pH.Codes),c("B.Code","pH.Codes")],
 # WH.Out[WH.Level.Name=="Base" & !is.na(WH.Codes),c("B.Code","WH.Codes")],
 # Irrig.Codes[I.Level.Name=="Base" & !is.na(I.Codes),c("B.Code","I.Codes")],
 Animals.Out=Animals.Out[A.Level.Name=="Base" & !is.na(A.Codes),c("B.Code","A.Codes")],
 # PO.Out[PO.Level.Name=="Base" & !is.na(PO.Codes),c("B.Code","PO.Codes")],
 # E.Out[E.Level.Name=="Base" & !is.na(E.Codes),c("B.Code","E.Codes")],
 # Weed.Code[!is.na(W.Code),c("B.Code","W.Code")],
 Chems.Out=Chems.Out[C.Level.Name=="Base",c("B.Code","C.Code")]
)

Base.Out<-rbindlist(Base.Out[unlist(lapply(Base.Out,nrow))>0],use.names = F)
Base.Out<-Base.Out[,list(Base.Codes=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

# ***Treatments (MT.Out)  =====
MT.Out<-lapply(XL,"[[","MT.Out")
MTCols<-c("T.Name","T.Comp","T.Residue.Prev","T.Residue.Code","T.Control","T.Reps","T.Animals","T.Start.Year","T.Start.Season",
          grep("Level.Name",colnames(MT.Out[[1]]),value = T))

MT.Out<-rbindlist(pblapply(1:length(MT.Out),FUN=function(i){
  X<-MT.Out[[i]][,..MTCols]
  X<-data.frame(X)
  X<-subset(X, select = -AF.Level.Name.1)
  X<-data.table(X)
  
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("T.Name"))
  
  # Replicates for Aggregated treatments
  if(length(grep("[.][.]",X$T.Name))>0){
    N<-X$T.Name[grep("[.][.]",X$T.Name)]
    Y<-strsplit(N,"[.][.]")            
    Y<-data.table(Comb.T=rep(N,unlist(lapply(Y,length))),Reps=X$T.Reps[match(unlist(Y),X$T.Name)])
    Y<-Y[,list(Reps=mean(Reps)),by=Comb.T]
    X$T.Reps<-Y$Reps[match(X$T.Name,Y$Comb.T)]
  }
  X
}))

# Add row index to Mt.Out
MT.Out[,N:=1:nrow(MT.Out)]


setnames(MT.Out,c("T.Level.Name"),c("Till.Level.Name"))

# Change 0 to NA
Zero.Cols<-c("AF.Level.Name","A.Level.Name","C.Level.Name","E.Level.Name","H.Level.Name","I.Level.Name","M.Level.Name","F.Level.Name",
             "P.Level.Name","pH.Level.Name","PO.Level.Name","V.Level.Name","WH.Level.Name","O.Level.Name","Till.Level.Name","W.Level.Name",
             "T.Residue.Prev","T.Residue.Code","T.Control","T.Animals","T.Start.Year","T.Start.Season","T.Reps")

MT.Out<-MT.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

# Remove Parenthesis from Treatment Names
#ParRmFun<-function(X){
#  X<-gsub("[(]|[)]","",X)
#  return(X)
#}

#Par.Cols<-grep("Level.Name",colnames(MT.Out),value = T)
#Par.Cols<-Par.Cols[Par.Cols!="AF.Level.Name"]

#MT.Out<-MT.Out[,(Par.Cols):=lapply(.SD, ParRmFun),.SDcols=Par.Cols]

# ***Times ####
Times<-lapply(XL,"[[","Times")

Times<-rbindlist(pblapply(1:length(Times),FUN=function(i){
  X<-Times[[i]][7:30,10:18]
  names(X)<-c("Time","Site.ID","TSP","TAP","Plant.Start","Plant.End","Harvest.Start","Harvest.End","Harvest.DAP")
  X<-X[!is.na(Time)]
  X$B.Code<-Pub.Out$B.Code[i]
  X
}))

# ***Outcomes (Out.Out) ----
Out.Out<-lapply(XL,"[[","Out.Out")
Out.Out<-rbindlist(pblapply(1:length(Out.Out),FUN=function(i){
  X<-Out.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("Out.Subind"))
  X
}))

# Change "0" to NA
# Replace zeros with NAs
Zero.Cols<-c("Out.Depth.Lower","Out.Group","Out.NPV.Rate","Out.NPV.Time","Out.WG.Start","Out.WG.Unit","Out.WG.Days")
Out.Out<-Out.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

Out.Out[is.na(Out.Depth.Lower),Out.Depth.Upper:=NA] # This line intentionally uses Out.Depth.Lower

  # Out.Out: Correct any old Outcome indicator names ####
  Out.Out[Out.Subind=="Nitrogen ARE (Product)",Out.Subind:="Nitrogen Use Efficiency (ARE Product)"]
  Out.Out[Out.Subind=="Phosphorus ARE (Product)",Out.Subind:="Phosphorus Use Efficiency (ARE Product)"]
  Out.Out[Out.Subind=="Potassium ARE (Product)",Out.Subind:="Potassium Use Efficiency (ARE Product)"]
  Out.Out[Out.Subind=="Nitrogen ARE (AGB)",Out.Subind:="Nitrogen Use Efficiency (ARE AGB)"]
  Out.Out[Out.Subind=="Nitrogen Recovery Efficiency (AGB)",Out.Subind:="Nitrogen Use Efficiency (ARE AGB)"]
  Out.Out[Out.Subind=="Nitrogen ARE (Unspecificed)",Out.Subind:="Nitrogen Use Efficiency (Unspecificed)"]
  Out.Out[Out.Subind=="Aboveground Biomass",Out.Subind:="Aboveground Carbon Biomass"]
  Out.Out[Out.Subind=="Belowground Biomass",Out.Subind:="Belowground Carbon Biomass"]
  Out.Out[Out.Subind=="Biomass Yield",Out.Subind:="Crop Residue Yield"]

  # Out.Out: Update Fields From Harmonization Sheet ####
  N<-match(Out.Out[,Out.Unit],UnitHarmonization[,Out.Unit])
  Out.Out[!is.na(N),Out.Unit:=UnitHarmonization[N[!is.na(N)],Out.Unit.Correct]]
  rm(N)
  # Out.Out: Add columns from partial outcome data stored in Out.Group field
  # Out.Out[grepl("<P>",Out.Group),list(Studies=length(unique(B.Code)),Obs=length(B.Code))]
  
  if(nrow(Out.Out[grepl("<P>",Out.Group)])>0){
    X<-t(Out.Out[grepl("<P>",Out.Group),strsplit(unlist(lapply(strsplit(Out.Group,"<P>"),"[[",2)),"[|]")])
    Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Name:=X[,1]]
    Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Code:=X[,2]]
    rm(X)
  }
  

# ***Enter Data (Data.Out) ####
Data.Out<-lapply(XL,"[[","Data.Out")
Data.Out<-rbindlist(pblapply(1:length(Data.Out),FUN=function(i){
  X<-Data.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("ED.Mean.T"))
  X
}))

# Set zero values to NA

# Change 0 to NA
Zero.Cols<-c("ED.Rot","ED.Int","ED.Product.Simple","ED.Product.Comp","ED.M.Year","ED.Treatment","ED.Reps","ED.Start.Year",
             "ED.Start.Season","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
             "ED.Harvest.End","ED.Comparison","ED.I.Unit","ED.Animals","ED.Variety","ED.Data.Loc")

Data.Out<-Data.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

Data.Out[ED.Product.Comp=="NA",ED.Product.Comp:=NA]
Data.Out[is.na(ED.I.Unit) & ED.I.Amount ==0,ED.I.Amount:=NA]
Data.Out[ED.Error.Type ==0,ED.Error.Type:=NA]

# Data.Out: Remove any parenthesis in names ####
#Par.Cols<-c("ED.Treatment","ED.Site.ID","ED.Int","ED.Rot","ED.Comparison")
#Data.Out<-Data.Out[,(Par.Cols):=lapply(.SD, ParRmFun),.SDcols=Par.Cols]

  # Data.Out: Add controls for outcomes that are ratios ####
  Data.Out[,N:=1:.N]
  Data.R<-Data.Out[!is.na(ED.Comparison) & !is.na(ED.Treatment),c("ED.Treatment","ED.Comparison","B.Code","N","ED.Outcome")]
  
  if(nrow(Data.R)>0){
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
  
  Data.Out.Ratios.NoMatch.in.MT.Out<-Data.R[!ED.Treatment %in% MT.Out[,T.Name],unique(paste(B.Code,ED.Treatment))]
  if(length(Data.Out.Ratios.NoMatch.in.MT.Out)>0){
    View(Data.Out.Ratios.NoMatch.in.MT.Out)
  }

  Data.Out[,ED.Ratio.Control:=F]
  
  Data.Out<-rbind(Data.Out,Data.R)
  
  }
  
  rm(Data.R,M.Treat,Data.Out.Ratios.NoMatch.in.MT.Out)
  
  Data.Out[,N:=NULL]
  
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
    
  # Save tables as a list  ####
  Livestock_Tables_2022<-list(
    Pub.Out=Pub.Out, 
    Site.Out=Site.Out, 
    Soil.Out=Soil.Out,
    ExpD.Out=ExpD.Out,
    Prod.Out=Prod.Out,
    Var.Out=Var.Out,
    Chems.Out=Chems.Out,
    Animals.Out=Animals.Out,
    Animals.Diet=Animals.Diet,
    Animals.Diet.Comp=Animals.Diet.Comp,
    Animals.Diet.Digest=Animals.Diet.Digest,
    AF.Out=AF.Out,
    Other.Out=Other.Out,
    Base.Out=Base.Out,
    Times=Times,
    Out.Out=Out.Out,
    MT.Out=MT.Out,
    Data.Out=Data.Out
  )
  
  save(Livestock_Tables_2022,file=paste0("Data/Compendium Master Database/era_skinny_cow_tables-",as.character(Sys.Date()),".RData"))
  
  # Save as a series of .csv tables ####
  
  for(i in 1:length(Livestock_Tables_2022)){
    data<-Livestock_Tables_2022[[i]]
    filename<-paste0("Data/Compendium Master Database/era_skinny_cow_tables-",names(Livestock_Tables_2022)[i],".csv")
    fwrite(data,file = filename)
  }
  
  files<-list.files("Data/Compendium Master Database","era_skinny_cow_tables",full.names = T)
  files<-grep(".csv",files,value = T)
  
  filename<-paste0("Data/Compendium Master Database/era_skinny_cow_tables-",as.character(Sys.Date()),".zip")
  
  zip(zipfile=filename,files=files)
  
  unlink(files)
  
  # Save as an excel file ####
  filename<-paste0("Data/Compendium Master Database/era_skinny_cow_tables-",as.character(Sys.Date()),".xlsx")
  openxlsx::write.xlsx(Livestock_Tables_2022, file = filename)
  
  # Generate meta data####
  require(googlesheets4)
  
  livestock_cs <- data.table(read_sheet("https://docs.google.com/spreadsheets/d/15CSw2G5YVJ5R5Wi9O0pGWkBhL8NenW7HH-1o8cKHGjI/edit?usp=sharing",sheet="CS - Livestock"))
  crops_fields<-data.table(read_excel("Concept Scheme/era_codes/era_master_sheet.xlsx",sheet = "era_fields"))
  
  # Fields shared with ERA crops ####
  # Pub.Out
  # Check all fields are present
  colnames(Pub.Out)[!colnames(Pub.Out) %in% crops_fields$Field]
  N.Pub<-match(colnames(Pub.Out),crops_fields$Field)
  
  # Site.Out
  # Check all fields are present
  colnames(Site.Out)[!colnames(Site.Out) %in% crops_fields$Field]
  N.Site<-match(colnames(Site.Out),crops_fields$Field)
  
  # Soil.Out
  # Check all fields are present
  colnames(Soil.Out)[!colnames(Soil.Out) %in% crops_fields$Field]
  N.Soil<-match(colnames(Soil.Out),crops_fields$Field)
  
  # ExpD.Out
  # Check all fields are present
  colnames(ExpD.Out)[!colnames(ExpD.Out) %in% crops_fields$Field]
  N.ExpD<-match(colnames(ExpD.Out),crops_fields$Field)
  
  #Prod.Out
  # Check all fields are present
  colnames(Prod.Out)[!colnames(Prod.Out) %in% crops_fields$Field]
  N.Prod<-match(colnames(Prod.Out),crops_fields$Field)
  
  #Var Out
  # Check all fields are present
  colnames(Var.Out)[!colnames(Var.Out) %in% crops_fields$Field]
  N.Var<-match(colnames(Var.Out),crops_fields$Field)
  
  #Animals.Out
  # Check all fields are present
  colnames(Animals.Out)[!colnames(Animals.Out) %in% crops_fields$Field]
  N.Animals<-match(colnames(Animals.Out),crops_fields$Field)
  
  #Animals.Diet
  colnames(Animals.Diet)[!colnames(Animals.Diet) %in% crops_fields$Field]
  N.Animals.Diet<-match(colnames(Animals.Diet),crops_fields$Field)
  
  #Animals.Diet.Comp
  missing<-colnames(Animals.Diet.Comp)[!colnames(Animals.Diet.Comp) %in% crops_fields$Field]
  missing[!grepl(".Unit|.Method",missing)]
  write.table(data.frame(missing[!grepl(".Unit|.Method",missing)]),"clipboard",sep="\t",row.names=F,col.names = F)
  
  missing[grepl(".Unit",missing)]
  write.table(data.frame(missing[grepl(".Unit",missing)]),"clipboard",sep="\t",row.names=F,col.names = F)
  
  missing[grepl(".Method",missing)]
  write.table(data.frame(missing[grepl(".Method",missing)]),"clipboard",sep="\t",row.names=F,col.names = F)
  
  #Animals.Diet.Digest
  missing<-colnames(Animals.Diet.Digest)[!colnames(Animals.Diet.Digest) %in% crops_fields$Field]
  missing[!grepl(".Unit|.Method",missing)]
  write.table(data.frame(missing),"clipboard",sep="\t",row.names=F,col.names = F)
  
  #AF.Out=AF.Out,
  #Other.Out=Other.Out,
  #Base.Out=Base.Out,
  #Times=Times,
  #Out.Out=Out.Out,
  #MT.Out=MT.Out,
  #Data.Out=Data.Out
  
  
