require(data.table)
require(readxl) # is one of these redundant?
require(openxlsx) # is one of these redundant?
require(doSNOW)
require(pbapply)
require(parallel)
require(soiltexture)
require(sp)
require(rgeos)

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
Cores<-detectCores()-1

# Set Directories ------

DataDir<-gsub("ERA/ERA",'ERA/Data Entry/Data Entry 2020',getwd())
DataDirs<-list.dirs(DataDir)
DataDirs<-DataDirs[grep("Quality Controlled|Extracted",DataDirs)]
DataDirs<-DataDirs[!grepl("rejected|Rejected",DataDirs)]

# Read Concepts & Harmonization Sheets -----

# Read in Fertilizers
FertCodes<-fread("Concept Scheme/Fertilizer.csv",header=T,strip.white=F)
FertCodes[,F.Lab:=paste0(F.Category,"-",F.Type)]

# Read in Trees
TreeCodes<-fread("Concept Scheme/Trees.csv",header=T,strip.white=F)
TreeCodes<-na.omit(TreeCodes, cols=c("EU"))

# Read in Products
EUCodes<-fread("Concept Scheme/EU.csv",header=T,strip.white=F)
if(sum(table(EUCodes[,EU])>1)){print("Duplicate codes present in products")}

# Read in component concepts EU_Components
Comp.Codes<-fread("Concept Scheme/EU_Components.csv",header=T,strip.white=F)

# Read in Outcomes
OutcomeCodes<-fread("Concept Scheme/Outcomes.csv",header=T,strip.white=F)
if(sum(table(OutcomeCodes[,Code])>1)){print("Duplicate codes present in outcomes")}

# Read in Practice Codes
PracticeCodes<-fread("Concept Scheme/Practices.csv",header=T,strip.white=F)
if(sum(table(PracticeCodes[,Code])>1)){print("Duplicate codes present in practices")}
PracticeCodesX<-data.table::copy(PracticeCodes)

# Read in Levels (in particular for journal names and countries)
MasterLevels<-fread("Concept Scheme/Levels.csv",header=T,strip.white=F)

# Read in Variety Harmonization Data
VarHarmonization.Traits<-fread("Concept Scheme/Harmonization/Variety_Traits.csv",header=T,strip.white=F)
VarHarmonization<-fread("Concept Scheme/Harmonization/Varieties.csv",header=T,strip.white=F)

# Read in Corrections
OtherHarmonization<-fread("Concept Scheme/Harmonization/Other_Fields.csv",header=T,strip.white=F)
N<-grep("[.][.][.]",colnames(OtherHarmonization))
OtherHarmonization<-OtherHarmonization[,-..N]


UnitHarmonization<-fread("Concept Scheme/Harmonization/Unit_Lists.csv",header=T,strip.white=F)

SiteHarmonization<-fread("Concept Scheme/Harmonization/Site_Names.csv",header=T,strip.white=T)
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

# Read In the Master Sheet ------
Master<-paste0(DataDir,"/Excel Form/V1.14.3 - Majestic Hippo.xlsm")

# List sheet names that we need to extract
SheetNames<-getSheetNames(Master)[grep(".Out",getSheetNames(Master))]

# Add in sheets where Excel errors need to be fixed by extraction from entry forms ####
SheetNames<-c(SheetNames,"Site.Soils","Times")

# List column names for the sheets to be extracted
XL.M<-lapply(SheetNames,FUN=function(SName){
  cat('\r                                                                                                                                          ')
  cat('\r',paste0("Importing Sheet = ",SName))
  flush.console()
  colnames(data.table(suppressWarnings(suppressMessages(read_excel(Master,sheet = SName)))))
})

names(XL.M)<-SheetNames
# Squish Bugs / Subset Cols
Rot1<-XL.M[["Rot.Out"]][15] # Bug in one of the rotation column names
XL.M[["Rot.Out"]][15]<- "Rotation Treatment"
XL.M[["AF.Out"]]<-XL.M[["AF.Out"]][1:9] # Subset Agroforesty out tab to needed columns only

# Check for Duplicate Files -----

Files<-unlist(lapply(DataDirs, list.files,".xlsm",full.names=T))
Files<-Files[!grepl("[/][~][$]",Files)]

FNames<-lapply(strsplit(Files," - "),"[",3)
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
clusterEvalQ(cl, list(library(data.table),library(readxl)))
clusterExport(cl,list("Files","SheetNames","FNames","XL.M","Rot1"),envir=environment())
registerDoSNOW(cl)

XL<-parLapply(cl,1:length(Files),fun=function(i){
  
  #XL<-lapply(1:length(Files),FUN=function(i){
  File<-Files[i]
  X<-lapply(SheetNames,FUN=function(SName){
    cat('\r                                                                                                                                          ')
    cat('\r',paste0("Importing File ",i,"/",length(Files)," - ",FNames[i]," | Sheet = ",SName))
    flush.console()
    Y<-data.table(suppressMessages(suppressWarnings(read_excel(File,sheet = SName,trim_ws = F))))
    
    if(SName=="Rot.Out"){
      if(length(grep(Rot1,colnames(Y)))!=0){
        colnames(Y)<-gsub(Rot1,"Rotation Treatment",colnames(Y))
      }
    }
    
    if(SName=="AF.Out"){
      Y<-Y[,1:9]
    }
    
    if(sum(!colnames(Y) %in% XL.M[[SName]])>1){
      print("")
      print("Colnames Error")
      print("")
      
      Y
    }else{
      Y
    }
  })
  
  names(X)<-SheetNames
  X
  
})

stopCluster(cl)

names(XL)<-FNames

rm(Files,SheetNames,XL.M,Rot1,Master)
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# ***PREPARE DATA*** ----
# <><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #### 
# ***Publication (Pub.Out) =====
Pub.Out<-rbindlist(lapply(XL,"[[",1))

# Replace zeros with NAs
Zero.Cols<-c("B.Url","B.DOI","B.Link1","B.Link2","B.Link3","B.Link4")
Pub.Out<-Pub.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

  # Pub.Out: Validation: Duplicate B.Codes ####
Dups<-names(table(Pub.Out[,B.Code]))[table(Pub.Out[,B.Code])>1]
FNames<-gsub(".xlsm","",FNames)
if(length(Dups)>0){
  N<-which(Pub.Out[,B.Code] %in% Dups)
  Duplicate.B.Codes<-FNames[N]
  View(Duplicate.B.Codes)
  rm(N,Duplicate.B.Codes)
}
rm(Dups)

# Validation needed - B.Code does not match excel name
N<-unlist(lapply(1:nrow(Pub.Out),FUN=function(i){
  grepl(Pub.Out[i,B.Code], FNames[i])
}))

if(sum(!N)>0){
  View(data.table(Pub.Tab=Pub.Out[!N,B.Code],Excel.Name=FNames[!N]))
}


# ***Site (Site.Out) =====
Site.Out<-lapply(XL,"[[",2)
Site.Out<-rbindlist(lapply(1:length(Site.Out),FUN=function(i){
  SS<-Site.Out[[i]]
  SS<-SS[-1,]
  SS$B.Code<-Pub.Out$B.Code[i]
  SS
}))

# Remove any parenthesis in names
Site.Out[,Site.ID:=gsub("[(]","",Site.ID)][,Site.ID:=gsub("[)]","",Site.ID)]

# Cleaning
colnames(Site.Out)[colnames(Site.Out) == "LatD...8"]<-"LatD"
colnames(Site.Out)[colnames(Site.Out) == "LatD...10"]<-"LonD"
colnames(Site.Out)[colnames(Site.Out) == "MSP...20"]<-"MSP.S1"
colnames(Site.Out)[colnames(Site.Out) == "MSP...21"]<-"MSP.S2"

Site.Out[,LatD:=gsub(",","",LatD)][LatD=="",LatD:=NA][,LatD:=as.numeric(as.character(LatD))]
Site.Out[,LonD:=gsub(",","",LonD)][LonD=="",LonD:=NA][,LonD:=as.numeric(as.character(LonD))]

Site.Out[,LatD:=round(as.numeric(LatD),6)]
Site.Out[,LonD:=round(as.numeric(LonD),6)]



colnames(Site.Out)[-c(1:6,28)]<-paste0("Site.",colnames(Site.Out)[-c(1:6,28)])

N<-colnames(Site.Out)[!colnames(Site.Out) %in% c("Site.LonD","Site.LatD","Site.Elevation","Site.Slope.Perc","Site.Slope.Degree")]

Site.Out<-data.frame(Site.Out)

for(NN in N){
  # print(NN)
  NNN<-which(Site.Out[,NN]==0)
  Site.Out[NNN,NN]<-NA
}

Site.Out<-data.table(Site.Out)

  # Site.Out: Update Fields From Harmonization Sheet ####
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

# ***Soil (Soil.Out) =====
Soil.Out<-lapply(XL,"[[","Soils.Out")

Error.Cols<-c("Soil.CEC.Unit", "Soil.EC.Unit", "Soil.FC.Unit", "Soil.N.Unit...29", "Soil.NH4.Unit", "Soil.NO3.Unit", "Soil.N.Unit...32", "Soil.SOC.Method", "Soil.SOM.Method", "Soil.pH.Method")

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
Soil.Out[,Site.ID:=gsub("[(]","",Site.ID)][,Site.ID:=gsub("[)]","",Site.ID)]

setnames(Soil.Out, "Soil.N.Unit...29", "Soil.TN.Unit")
setnames(Soil.Out, "Soil.N.Unit...32", "Soil.AN.Unit")

  # Soil.Out: Fix Soils.Out Excel Bug ####
  # Prior to 16/07/2020 Soil.SOC.Method	Soil.SOM.Method	Soil.pH.Method not populating with correct data
  
  # Extract soils entry form from excel
  Site.Soils<-lapply(XL,"[[","Site.Soils")
  
  # Extract cells for values that have an issue
  Site.Soils<-rbindlist(lapply(1:length(Site.Soils),FUN=function(i){
    X<-Site.Soils[[i]][33,c(9,10,11)]
    names(X)<-c("Soil.SOC.Method","Soil.SOM.Method","Soil.pH.Method")
    X$B.Code<-Pub.Out$B.Code[i]
    X
  }))
  
  # Papers with an issue
  Err.B.Codes<-Soil.Out[grepl("Soil.SOC.Method",Soil.SOC.Method),unique(B.Code)]
  
  N<-match(Soil.Out[B.Code %in% Err.B.Codes,B.Code],Site.Soils[,B.Code])
  Soil.Out[B.Code %in% Err.B.Codes,Soil.SOC.Method:=Site.Soils[N,Soil.SOC.Method]]
  Soil.Out[B.Code %in% Err.B.Codes,Soil.SOM.Method:=Site.Soils[N,Soil.SOM.Method]]
  Soil.Out[B.Code %in% Err.B.Codes,Soil.pH.Method:=Site.Soils[N,Soil.pH.Method]]
  
  rm(Site.Soils,Err.B.Codes,N)
  
  
  # Deal with 0 that should be NA
  # Where depth are both 0 set to NA
  Soil.Out[Soil.Upper==0 & Soil.Lower==0,Soil.Upper:=NA]
  Soil.Out[is.na(Soil.Upper) & Soil.Lower==0,Soil.Lower:=NA]
  Soil.Out[,Texture.Total:=SND+SLT+CLY]
  Soil.Out[Texture.Total<98 & SND==0,SND:=NA]
  Soil.Out[Texture.Total<98 & SLT==0,SLT:=NA]
  Soil.Out[Texture.Total<98 & CLY==0,CLY:=NA]
  Soil.Out[Soil.BD==0,Soil.BD:=NA]
  Soil.Out[Soil.TC==0,Soil.TC:=NA]
  Soil.Out[Soil.SOC==0,Soil.SOC:=NA]
  Soil.Out[Soil.SOM ==0,Soil.SOM:=NA]
  Soil.Out[Soil.pH==0,Soil.pH:=NA]
  Soil.Out[Soil.CEC==0,Soil.CEC:=NA]
  Soil.Out[Soil.EC==0,Soil.EC:=NA]
  Soil.Out[Soil.FC==0,Soil.FC:=NA]
  Soil.Out[Soil.BD==0,Soil.BD:=NA]
  Soil.Out[Soil.TN==0,Soil.TN:=NA]
  Soil.Out[Soil.NH4==0,Soil.NH4:=NA]
  Soil.Out[Soil.NO3==0,Soil.NO3:=NA]
  Soil.Out[SND.Unit==0,SND.Unit:=NA]
  Soil.Out[SLT.Unit==0,SLT.Unit:=NA]
  Soil.Out[CLY.Unit==0,CLY.Unit:=NA]
  Soil.Out[Soil.BD.Unit==0,Soil.BD.Unit:=NA]
  Soil.Out[Soil.TC.Unit==0,Soil.TC.Unit:=NA]
  Soil.Out[Soil.FC.Unit==0,Soil.FC.Unit:=NA]
  Soil.Out[Soil.SOC.Unit==0,Soil.SOC.Unit:=NA]
  Soil.Out[Soil.SOM.Unit==0,Soil.SOM.Unit:=NA]
  Soil.Out[Soil.CEC.Unit==0,Soil.CEC.Unit:=NA]
  Soil.Out[Soil.EC.Unit==0,Soil.EC.Unit:=NA]
  Soil.Out[Soil.TN.Unit==0,Soil.TN.Unit:=NA]
  Soil.Out[Soil.NH4.Unit==0,Soil.NH4.Unit:=NA]
  Soil.Out[Soil.NO3.Unit==0,Soil.NO3.Unit:=NA]
  Soil.Out[Soil.AN.Unit==0,Soil.AN.Unit:=NA]
  
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

# Covnert 0 to NA
ExpD.Out[EX.Design==0,EX.Design:=NA]
ExpD.Out[EX.Plot.Size==0,EX.Plot.Size:=NA]
ExpD.Out[EX.HPlot.Size==0,EX.HPlot.Size:=NA]
ExpD.Out[EX.Notes==0,EX.Notes:=NA]

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

# ***Var (Var.Out) =====
Var.Out<-lapply(XL,"[[",6)
Var.Out<-rbindlist(lapply(1:length(Var.Out),FUN=function(i){
  X<-Var.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("V.Var"))
  X
}))

Var.Out[,"...20":=NULL]
Var.Out[,V.New.Crop:=NULL]
Var.Out[,V.New.Var:=NULL]
Var.Out[,V.New.Species:=NULL]
Var.Out[,V.New.SubSpecies:=NULL]
Var.Out[Join==0,Join:=NA]
Var.Out[V.Var==0,V.Var:=NA]

# Remove any parenthesis in names
Var.Out[,V.Var:=gsub("[(]","",V.Var)][,V.Var:=gsub("[)]","",V.Var)
                                      ][,V.Species:=gsub("[(]","",V.Species)][,V.Species:=gsub("[)]","",V.Species)
                                                                              ][,Join:=gsub("[(]","",Join)][,Join:=gsub("[)]","",Join)]

setnames(Var.Out,"V.Subpecies","V.Subspecies")

# Save Join Field before updating
Var.Join<-Var.Out[,list(B.Code,V.Var)]
setnames(Var.Join,"V.Var","Join")
Var.Join[is.na(Join),Join:=Var.Out[is.na(V.Var),V.Species]]

if(F){
  # Var.Out: Validation - Check V.Codes == 0
  X<-Var.Out[V.Codes==0,c("V.Product","V.Var","V.Species","V.Crop.Practice","V.Animal.Practice","B.Code")]
  write.table(X,"clipboard",row.names = F,sep="\t")
  
  # Extract values for external harmonization
  X<-unique(Var.Out[,c("V.Product","V.Var","V.Species","V.Subspecies","V.Crop.Practice","V.Animal.Practice","V.Type","V.Trait1","V.Trait2","V.Trait3","V.Maturity")
                    ][order(V.Product,V.Var,V.Species)])
  write.table(X,"clipboard-256000",row.names = F,sep="\t")
  rm(X)
}

# Set 0 values to NA
Var.Out[V.Var==0,V.Var:=NA]
Var.Out[V.Animal.Practice==0,V.Animal.Practice:=NA]
Var.Out[V.Crop.Practice==0,V.Crop.Practice:=NA]
Var.Out[V.Species==0,V.Species:=NA]
Var.Out[V.Subspecies==0,V.Subspecies:=NA]
Var.Out[V.Type==0,V.Type:=NA]
Var.Out[V.Trait1==0,V.Trait1:=NA]
Var.Out[V.Trait2==0,V.Trait2:=NA]
Var.Out[V.Trait3==0,V.Trait3:=NA]
Var.Out[V.Maturity==0,V.Maturity:=NA]
Var.Out[V.Code.Animal==0,V.Code.Animal:=NA]
Var.Out[V.Codes==0,V.Codes:=NA]


# Odd Rounding of Variety names in EO0092 (Excel is correct, but there is an error created when reading in excels)
Var.Out[V.Var=="396038.10499999998",V.Var:="396038.105"]
Var.Out[V.Var=="396038.10100000002",V.Var:="396038.101"]
Var.Out[V.Var=="396031.10800000001",V.Var:="396031.108"]
Var.Out[V.Var=="396038.10700000002",V.Var:="396038.107"]
Var.Out[V.Var=="396034.26799999998",V.Var:="396034.268"]

  # Var.Out: Harmonize Variety Naming and Codes ####
  
  Var.Fun<-function(V.Product,V.Var){
    Vars<-unlist(strsplit(V.Var,"[$][$]"))
    return(list(match(paste(V.Product,Vars),VarHarmonization[,paste(V.Product,V.Var)])))
  }
  
  Var.Out[,N:=1:.N]
  Var.Out[,V.Match:=list(Var.Fun(V.Product,V.Var)),by=N]
  
  # Check for any non-matches
  Var.Out[,V.Match.NA:=sum(is.na(unlist(V.Match))),by=N]
  Variety.Non.Matches<-Var.Out[V.Match.NA>0,list(B.Code,V.Product,V.Var,V.Match)]
  
  if(nrow(Variety.Non.Matches)>0){
    View(Variety.Non.Matches)
  }
  
  rm(Variety.Non.Matches,Var.Fun)
  
  # Recreate Var.Out table from harmonization table
  Extract.Fun<-function(B.Code,V.Base,V.Match){
    Y<-VarHarmonization[unlist(V.Match),list(V.Product,V.Var1,V.Species1,V.Subspecies1,V.Crop.Practice,V.Animal.Practice,V.Type,V.Trait1,V.Trait2,V.Trait3,V.Maturity,V.Code)]
    Y<-data.table(B.Code,V.Base,t(apply(Y,2,FUN=function(Z){paste(unique(Z),collapse = "$$")})))
    Y[V.Var1=="NA",V.Var1:=NA]
    Y[,Join:=if(!is.na(V.Var1)){V.Var1}else{V.Species1}]
    return(Y)
  }
  
  Var.Out2<-Var.Out[,Extract.Fun(B.Code,V.Base,V.Match),by=N]
  
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
  
  # Var.Out: Update trait labels ####
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
  
# ***Agroforestry (AF.Out)=====
AF.Out<-lapply(XL,"[[",7)
AF.Out<-rbindlist(lapply(1:length(AF.Out),FUN=function(i){
  X<-AF.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("AF.Level.Name...1"))
  X
  
}))
setnames(AF.Out, "AF.Level.Name...1","AF.Level.Name")

# Remove any parenthesis in names
AF.Out[,AF.Level.Name:=gsub("[(]","",AF.Level.Name)][,AF.Level.Name:=gsub("[)]","",AF.Level.Name)]


# ***Tillage (Till.Out) =====
Till.Out<-lapply(XL,"[[",8)
Till.Out<-rbindlist(lapply(1:length(Till.Out),FUN=function(i){
  X<-Till.Out[[i]][,-(1:4)]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("T.Level.Name...5"))
  X
}))
setnames(Till.Out, "T.Level.Name...5", "T.Level.Name")

# Replace 0 with NA
Till.Out[T.Method==0,T.Method:=NA]
Till.Out[T.Method.Other==0,T.Method.Other:=NA]
Till.Out[T.Depth==0,T.Depth:=NA]
Till.Out[T.Freq==0,T.Freq:=NA]
Till.Out[T.Strip.P==0,T.Strip.P:=NA]
Till.Out[T.Strip.WT==0,T.Strip.WT:=NA]
Till.Out[T.Strip.WU==0,T.Strip.WU:=NA]

Till.Codes<-lapply(XL,"[[",8)
Till.Codes<-rbindlist(lapply(1:length(Till.Codes),FUN=function(i){
  X<-Till.Codes[[i]][,1:3]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("T.Level.Name...2"))
  X
}))
setnames(Till.Codes, "T.Level.Name...2", "T.Level.Name")

# Remove any parenthesis in names
Till.Codes[,T.Level.Name:=gsub("[(]","",T.Level.Name)][,T.Level.Name:=gsub("[)]","",T.Level.Name)]

#write.table(Till.Out[!is.na(T.Method)][order(T.Method),unique(T.Method)],"clipboard",row.names = F,sep="\t")
#write.table(Till.Out[!is.na(T.Method.Other)][order(T.Method.Other),unique(T.Method.Other)],"clipboard",row.names = F,sep="\t")

  # Till.Out: Update Fields From Harmonization Sheet ####
N<-match(Till.Out[,T.Method],OtherHarmonization[,Till.Method])
Till.Out[!is.na(N),Till.Method:=OtherHarmonization[N[!is.na(N)],Till.Method.Correct]]

N<-match(Till.Out[,T.Method.Other],OtherHarmonization[,T.Method.Other])
Till.Out[!is.na(N),T.Method.Other:=OtherHarmonization[N[!is.na(N)],T.Method.Other.Correct]]
rm(N)

# ***Planting (Plant.Out) =====
Plant.Out<-lapply(XL,"[[",9)
Plant.Out<-rbindlist(lapply(1:length(Plant.Out),FUN=function(i){
  X<-Plant.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("P.Product"))
  X
}))

# Remove any parenthesis in names
Plant.Out[,P.Level.Name:=gsub("[(]","",P.Level.Name)][,P.Level.Name:=gsub("[)]","",P.Level.Name)]

# Replace 0 with NA
Plant.Out[P.Method==0,P.Method:=NA]
Plant.Out[Plant.Density==0,Plant.Density:=NA]
Plant.Out[Plant.Density.Unit==0,Plant.Density.Unit:=NA]
Plant.Out[Plant.Row==0,Plant.Row:=NA]
Plant.Out[Plant.Station==0,Plant.Station:=NA]
Plant.Out[Plant.Seeds==0,Plant.Seeds:=NA]
Plant.Out[Plant.Thin==0,Plant.Thin:=NA]
Plant.Out[Plant.Tram.Row==0,Plant.Tram.Row:=NA]
Plant.Out[Plant.Tram.N==0,Plant.Tram.N:=NA]
Plant.Out[Plant.Intercrop==0,Plant.Intercrop:=NA]
Plant.Out[Plant.Block.Rows==0,Plant.Block.Rows:=NA]
Plant.Out[Plant.Block.Perc==0,Plant.Block.Perc:=NA]
Plant.Out[Plant.Block.Width==0,Plant.Block.Width:=NA]

#write.table(Plant.Out[!is.na(P.Method)][order(P.Method),unique(P.Method)],"clipboard",row.names = F,sep="\t")
#write.table(Plant.Out[!is.na(Plant.Density.Unit)][order(Plant.Density.Unit),unique(Plant.Density.Unit)],"clipboard",row.names = F,sep="\t")
#Plant.Out[grepl("m3",Plant.Density.Unit)]

  # Plant.Out: Update Fields From Harmonization Sheet ####
N<-match(Plant.Out[,P.Method],OtherHarmonization[,P.Method])

Plant.Out[!is.na(N),P.Method:=OtherHarmonization[N[!is.na(N)],P.Method.Correct]]
rm(N)
# ***Fertilizer (Fert.Out) =====
# Fert.Out
Fert.Out<-lapply(XL,"[[","Fert.Out")

  # Fert.Method ####
  colnames(Fert.Out[[1]])
  colnames(Fert.Out[[7]])


  Fert.Method<-rbindlist(lapply(1:length(Fert.Out),FUN=function(i){
    X<-Fert.Out[[i]][,-(c(1:21,36:59))]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("F.Name"))
    X
  }))
  setnames(Fert.Method, "F.Type...24", "F.Type")
  Fert.Method[F.NPK==0,F.NPK:=NA]
  Fert.Method[F.Unit==0,F.Unit:=NA]
  Fert.Method[F.Method==0, F.Method:=NA]
  Fert.Method[F.Fate==0,F.Fate:=NA]
  Fert.Method[F.Date==0,F.Date:=NA]
  Fert.Method[F.Date.Stage==0,F.Date.Stage:=NA]
  Fert.Method[F.Date.DAP==0,F.Date.DAP:=NA]
  Fert.Method[F.Date.DAE ==0,F.Date.DAE :=NA]
  Fert.Method[F.Source ==0,F.Source :=NA]
  Fert.Method[F.Date.Text==0,F.Date.Text:=NA]
  Fert.Method[F.Amount==0,F.Amount:=NA]
  
  # Fert.Method: Remove any parenthesis in names
  Fert.Method[,F.Name:=gsub("[(]","",F.Name)][,F.Name:=gsub("[)]","",F.Name)]
  
 # write.table(Fert.Method[!is.na(F.Method)][order(F.Method),unique(F.Method)],"clipboard",row.names = F,sep="\t")

  # Fert.Composition
  Fert.Composition<-rbindlist(lapply(1:length(Fert.Out),FUN=function(i){
    X<-Fert.Out[[i]][,-(c(1:36))]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("F.Type...37"))
    X
  }))
  setnames(Fert.Composition, "F.Type...37", "F.Type")
  
  Fert.Composition<-data.table(apply(Fert.Composition,2,FUN = function(X){
     X[X==0]<-NA
     X
    }))
  
  # Fert.Out - Validation:
  Fert.Out<-lapply(XL,"[[",10)
  Fert.Out<-rbindlist(lapply(1:length(Fert.Out),FUN=function(i){
    X<-Fert.Out[[i]][,1:20]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("F.Level.Name"))
    X
  }))
  
  # Fert.Out - Validation: Check for non-use of ".." delim
  Fert.Out<-lapply(XL,"[[",10)
  Fert.Out<-rbindlist(lapply(1:length(Fert.Out),FUN=function(i){
    X<-Fert.Out[[i]][33:70,1:20] # Get only rows that refer to combo pracs
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("F.Level.Name"))
    X
  }))
  
  # Fert.Out - Validation: Check for non 999999 code use
  Code999999Issue<-unique(Fert.Out[F.NI %in% c(9999,99999)| F.PI %in% c(9999,99999)| F.P2O5 %in% c(9999,99999)| F.KI %in% c(9999,99999)| F.K2O %in% c(9999,99999),B.Code])
  
  if(length(Code999999Issue)>0){
    View(Code999999Issue)
  }
  rm(Code999999Issue)
  
  # Fert.Out: Fix Potassium & Biochar Coding Errors  #####
  # This is probably redundant and could be implemented from the Fert.Methods table
  
  Fert.Out<-lapply(XL,"[[",10)
  Fert.Out<-pblapply(1:length(Fert.Out),FUN=function(i){
    X<-Fert.Out[[i]][,1:20]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols="F.Level.Name")
    X[F.KI.Code=="b21",F.KI.Code:="b22"]
    X$F.Biochar<-NA
    
    Y<-Fert.Method[B.Code==Pub.Out$B.Code[i]]
    if((nrow(Y)>0) & ("Biochar" %in% Y$F.Category)){
      X$F.Biochar[X$F.Level.Name %in% Y[F.Category=="Biochar",F.Name]]<-"b28"
    }
    
    X
  })
  
  Fert.Out<-rbindlist(pblapply(Fert.Out,FUN=function(X){
    N<-grep("[.][.]",X$F.Level.Name)
    if(length(N)>0){
      for(j in N){
        FL.Name<-unlist(strsplit(X$F.Level.Name[j],"[.][.]"))
        Y<-unlist(apply(X[F.Level.Name %in% FL.Name,c("F.NI.Code","F.PI.Code","F.KI.Code","F.Urea","F.Compost","F.Manure","F.Biosolid","F.MicroN","F.Biochar")],1,FUN=function(k){
          k[!is.na(k)]
        }))
        Y<-unique(Y[order(Y)])
        X$F.Codes[j]<-paste(Y,collapse="-")
      }
      
    }
    
    N<-(1:nrow(X))[!(1:nrow(X) %in% N)]
    
    X$F.Codes[N]<-apply(X[N,c("F.NI.Code","F.PI.Code","F.KI.Code","F.Urea","F.Compost","F.Manure","F.Biosolid","F.MicroN","F.Biochar")],1,FUN=function(Z){
      Z<-unlist(Z)
      Z<-Z[!is.na(Z)]
      if(length(Z)==0){
        NA
      }else{
        paste0(Z[order(Z)],collapse = "-")
      }})
    
    X[F.NI>100000,F.NI:=NA]
    X[F.PI>100000,F.PI:=NA]
    X[F.KI>100000,F.KI:=NA]
    X[F.PI>100000,F.PI:=NA]
    X[F.P2O5>100000,F.P2O5:=NA]
    X[F.K2O>100000,F.K2O:=NA]
    X
    
  }))
  
  # Set zeros to NAs as we can't be sure of the difference anyway
  Fert.Out[F.NO==0,F.NO:=NA]
  Fert.Out[F.PO==0,F.PO:=NA]
  Fert.Out[F.KO==0,F.KO:=NA]
  Fert.Out[F.NI==0,F.NI:=NA]
  Fert.Out[F.PI==0,F.PI:=NA]
  Fert.Out[F.P2O5==0,F.P2O5:=NA]
  Fert.Out[F.KI==0,F.KI:=NA]
  Fert.Out[F.K2O==0,F.K2O:=NA]
  Fert.Out[F.O.Unit==0,F.O.Unit:=NA]
  Fert.Out[F.I.Unit==0,F.I.Unit:=NA]
  
  # Fert.Out: Remove any parenthesis in names
  Fert.Out[,F.Level.Name:=gsub("[(]","",F.Level.Name)][,F.Level.Name:=gsub("[)]","",F.Level.Name)]
  
  # Fix inorganic NPK codes
  Fert.Out[!is.na(F.NI.Code),F.NI.Code:="b17"
           ][!is.na(F.PI.Code),F.PI.Code:="b21"
             ][!is.na(F.KI.Code),F.KI.Code:="b16"
               ][!is.na(F.Biosolid),F.Biosolid:="b67"]
  
  # Update F.Codes
  Fert.Out$F.Codes<-apply(data.frame(Fert.Out[,c("F.NI.Code","F.PI.Code","F.KI.Code","F.Urea","F.Compost","F.Manure","F.Biosolid","F.MicroN","F.Biochar")]),1,FUN=function(X){
    X<-as.character(X[!is.na(X)])
    paste(X[order(X)],collapse="-")
  })
  Fert.Out[F.Codes=="",F.Codes:=NA]
  
  # Fert.Method: Update Codes in  Fert.Methods From MasterCodes FERT Tab ####
  
  # Validation - Check for non-matches
  Fert.Method[,F.Lab:=paste0(F.Category,"-",F.Type)]
  
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
  
  Fert.Method[!is.na(F.NPK),c("B.Code","F.NPK")]
  
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
  
# ***Chemicals (Chems.Out) =====
Chems.Out<-lapply(XL,"[[","Chems.Out")
Chems.Out<-rbindlist(lapply(1:length(Chems.Out),FUN=function(i){
  X<-Chems.Out[[i]][,-c(1,3)]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("C.Type"))
  X
}))
setnames(Chems.Out, "C.Name...6", "C.Name")
setnames(Chems.Out, "C.Activity", "C.Level.Name")

Chems.Out[C.IUPAC==0,C.IUPAC:=NA]
Chems.Out[C.AI.Amount==0,C.AI.Amount:=NA]
Chems.Out[C.AI.Unit==0,C.AI.Unit:=NA]
Chems.Out[C.Amount==0,C.Amount:=NA]
Chems.Out[C.Unit==0,C.Unit:=NA]
Chems.Out[C.Applications==0,C.Applications:=NA]
Chems.Out[C.Date==0,C.Date:=NA]
Chems.Out[C.Date.Stage==0,C.Date.Stage:=NA]
Chems.Out[C.Date.DAP==0,C.Date.DAP:=NA]
Chems.Out[C.Date.Text==0, C.Date.Text:=NA]

# Remove any parenthesis in names
Chems.Out[,C.Level.Name:=gsub("[(]","",C.Level.Name)][,C.Level.Name:=gsub("[)]","",C.Level.Name)]
# Code for Herbicide
Chems.Out[C.Type=="Herbicide",C.Code:="h66.1"]

Chems.Code<-Chems.Out[,list(C.Code=unique(C.Code[!is.na(C.Code)])),by=list(B.Code,C.Level.Name,C.Structure)]

# ***Weeding (Weed.Out) =====
Weed.Out<-lapply(XL,"[[","Weed.Out")
Weed.Out<-rbindlist(lapply(1:length(Weed.Out),FUN=function(i){
  X<-Weed.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-X[W.Method!="0"]
  X
}))

Weed.Out[W.Freq==0, W.Freq:=NA]
Weed.Out[W.Freq.Time==0, W.Freq.Time:=NA]
Weed.Out[W.New.Method==0, W.New.Method:=NA]
Weed.Out[W.Notes==0, W.Notes:=NA]
Weed.Out[,W.Method:=trimws(W.Method)]

# Code for Hand Weeding
Weed.Out[!W.Method %in% c("Mechanical","Ploughing"),W.Code:="h66.2"]

Weed.Code<-Weed.Out[,list(W.Code=unique(W.Code[!is.na(W.Code)]),W.Structure=W.Structure[1]),by=list(B.Code,W.Level.Name)]

Weed.Out<-Weed.Out[,!"W.Structure"]

# ***Residues (Res.Out) =====
  # Res.Method ####
  Res.Out<-lapply(XL,"[[",12)
  Res.Method<-rbindlist(lapply(1:length(Res.Out),FUN=function(i){
    X<-Res.Out[[i]][,-(c(1:12,29:53))]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("M.Level.Name...13"))
    X
  }))
  
  setnames(Res.Method, "M.Level.Name...13", "M.Level.Name")
  setnames(Res.Method, "M.Tree...14", "M.Tree")
  setnames(Res.Method, "M.Material...15", "M.Material")
  setnames(Res.Method, "M.Unit...18", "M.Unit")
  
  # Res.Method: Remove any parenthesis in names
  Res.Method[,M.Level.Name:=gsub("[(]","",M.Level.Name)][,M.Level.Name:=gsub("[)]","",M.Level.Name)]
  
  Res.Method<-data.table(apply(Res.Method,2,FUN = function(X){
    X[X==0]<-NA
    X
  }))
  
  # Res.Method - Update Fields From Harmonization Sheet ####
  N<-match(Res.Method[,M.Material],OtherHarmonization[,M.Material])
  Res.Method[!is.na(N),M.Material:=OtherHarmonization[N[!is.na(N)],M.Material.Correct]]
  
  N<-match(Res.Method[,M.Unit],OtherHarmonization[,M.Unit])
  Res.Method[!is.na(N),M.Unit:=OtherHarmonization[N[!is.na(N)],M.Unit.Correct]]
  rm(N)
  # Res.Composition ####
  Res.Composition<-rbindlist(lapply(1:length(Res.Out),FUN=function(i){
    X<-Res.Out[[i]][,-(1:29)]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-X[!(is.na(M.Tree...30) & is.na(M.Material...31))]
    X
  }))
  
  setnames(Res.Composition, "M.Tree...30", "M.Tree")
  setnames(Res.Composition, "M.Material...31", "M.Material")
  
  Res.Composition<-data.table(apply(Res.Composition,2,FUN = function(X){
    X[X==0]<-NA
    X
  }))
  
  # Res.Out ####
  Res.Out<-lapply(XL,"[[",12)
  Res.Out<-rbindlist(lapply(1:length(Res.Out),FUN=function(i){
    X<-Res.Out[[i]][,1:11]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("M.Level.Name...1"))
    X<-X[!is.na(M.Codes)]
    X
  }))
  
  setnames(Res.Out, "M.Level.Name...1", "M.Level.Name")
  setnames(Res.Out, "M.Unit...11", "M.Unit")
  
  # Res.Out: Remove any parenthesis in names
  Res.Out[,M.Level.Name:=gsub("[(]","",M.Level.Name)][,M.Level.Name:=gsub("[)]","",M.Level.Name)]
  
  
  #write.table(Res.Method[!is.na(M.Material)][order(M.Material),unique(M.Material)],"clipboard",row.names = F,sep="\t")
  #write.table(Res.Method[!is.na(M.Unit)][order(M.Unit),unique(M.Unit)],"clipboard",row.names = F,sep="\t")
  
# ***Harvest (Har.Out) ====
Har.Out<-lapply(XL,"[[","Harvest.Out")
Har.Out<-rbindlist(lapply(1:length(Har.Out),FUN=function(i){
  X<-Har.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols="H.Level.Name")
  X
}))

Har.Out[H.Notes==0,H.Notes:=NA]

# ***pH (pH.Method) =====
pH.Out<-lapply(XL,"[[","pH.Out")
pH.Method<-rbindlist(lapply(1:length(pH.Out),FUN=function(i){
  X<-pH.Out[[i]][,-(1:5)]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("pH.Level.Name...6"))
  X
}))

setnames(pH.Method, "pH.Level.Name...6", "pH.Level.Name")

pH.Method[pH.Material==0,pH.Material:=NA]
pH.Method[pH.ECCE==0,pH.ECCE:=NA]
pH.Method[pH.CCE==0,pH.CCE:=NA]
pH.Method[pH.CaCO3==0,pH.CaCO3:=NA]
pH.Method[pH.MgCO3==0,pH.MgCO3:=NA]
pH.Method[pH.Amount==0,pH.Amount:=NA]
pH.Method[pH.Unit==0,pH.Unit:=NA]

# Remove any parenthesis in names
pH.Method[,pH.Level.Name:=gsub("[(]","",pH.Level.Name)][,pH.Level.Name:=gsub("[)]","",pH.Level.Name)]

# pH Codes
pH.Out<-lapply(XL,"[[","pH.Out")
pH.Out<-rbindlist(lapply(1:length(pH.Out),FUN=function(i){
  X<-pH.Out[[i]][,1:4]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("pH.Level.Name...1"))
  X
}))

setnames(pH.Out, "pH.Level.Name...1", "pH.Level.Name")
pH.Out[pH.Prac==0,pH.Prac:=NA]
pH.Out[pH.Notes==0,pH.Notes:=NA]

# ***Irrig (Irrig.Out) =====
Irrig.Out<-lapply(XL,"[[",18)
Irrig.Out<-rbindlist(lapply(1:length(Irrig.Out),FUN=function(i){
  X<-Irrig.Out[[i]][,-(1:6)]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("I.Name"))
  X
}))

# Change 0 to NA
Irrig.Out[I.Unit==0,I.Unit:=NA]
Irrig.Out[I.Water.Type==0,I.Water.Type:=NA]
Irrig.Out[I.New.Unit==0,I.New.Unit:=NA]
Irrig.Out[I.New.Type==0,I.New.Type:=NA]

# Irrig.Out: Validation - should be no rows where amount of water is zero
Irrig.Out[I.Amount==0]

Irrig.Out[I.Amount==0,I.Amount:=Na]

# Remove any parenthesis in names
Irrig.Out[,I.Name:=gsub("[(]","",I.Name)][,I.Name:=gsub("[)]","",I.Name)]

# Irrig.Codes
Irrig.Codes<-lapply(XL,"[[",18)
Irrig.Codes<-rbindlist(lapply(1:length(Irrig.Codes),FUN=function(i){
  X<-Irrig.Codes[[i]][,1:5]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("I.Level.Name"))
  X
}))

# Change 0 to NA
Irrig.Codes[I.Method==0,I.Method:=NA]
Irrig.Codes[I.Strategy==0,I.Strategy:=NA]
Irrig.Codes[I.Notes==0,I.Notes:=NA]
Irrig.Codes[I.Codes==0,I.Codes:=NA]

# ***Water Harvesting (WH.Out) =====
WH.Out<-lapply(XL,"[[","WH.Out")
WH.Out<-rbindlist(lapply(1:length(WH.Out),FUN=function(i){
  X<-WH.Out[[i]]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("WH.Level.Name"))
  
  X
}))

WH.Out[P1==0,P1:=NA
       ][P2==0,P2:=NA
         ][P3==0,P3:=NA
           ][P4==0,P4:=NA
             ]

WH.Out<-WH.Out[apply(WH.Out[,c("P1","P2","P3","P4")],1,FUN=function(X){
  sum(is.na(X))!=4
})]

# Remove any parenthesis in names
WH.Out[,WH.Level.Name:=gsub("[(]","",WH.Level.Name)][,WH.Level.Name:=gsub("[)]","",WH.Level.Name)]

  # WH.Out: Check for potential invalid comparisons that will need ordering practices specifying ####
# Count have many ERA practices present for each practice
WH.Out[,N:=1:.N][,N.P:=sum(!is.na(P1),!is.na(P2),!is.na(P3),!is.na(P4)),by=N]

# Count practices >2, practices=1 and controls for each study 
# List those studies with >1 practices with >2 PH practices, or >1 control
if(F){
  (X<-WH.Out[,list(N.2=sum(N.P>1),N.1=sum(N.P==1 & !grepl("h",WH.Codes)),C=sum(grepl("h",WH.Codes))),by=B.Code][N.2>0 & N.1>0])
  write.table(X,"clipboard",row.names = F,sep="\t")
}
WH.Out[,N:=NULL][,N.P:=NULL]

# ***Energy (E.Out)=====
E.Out<-lapply(XL,"[[","PH.E.Out")
E.Out<-rbindlist(lapply(1:length(E.Out),FUN=function(i){
  X<-E.Out[[i]][,9:14]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("E.Level.Name"))
  
  X
}))

setnames(E.Out, "P1...10", "P1")
setnames(E.Out, "P2...11", "P2")
setnames(E.Out, "P3...12", "P3")

E.Out[P1==0,P1:=NA
      ][P2==0,P2:=NA
        ][P3==0,P3:=NA
          ]

E.Out<-E.Out[apply(E.Out[,c("P1","P2","P3")],1,FUN=function(X){
  sum(is.na(X))!=3
})]

# Remove any parenthesis in names
E.Out[,E.Level.Name:=gsub("[(]","",E.Level.Name)][,E.Level.Name:=gsub("[)]","",E.Level.Name)]

  # E.Out: Check for potential invalid comparisons that will need ordering practices specifying ####
# Count have many ERA practices present for each practice
E.Out[,N:=1:.N][,N.P:=sum(!is.na(P1),!is.na(P2),!is.na(P3)),by=N]

# Count practices >2, practices=1 and controls for each study 
# List those studies with >1 practices with >2 PH practices, or >1 control
if(F){
  (X<-E.Out[,list(N.2=sum(N.P>1),N.1=sum(N.P==1 & !grepl("h",E.Codes)),C=sum(grepl("h",E.Codes))),by=B.Code][N.2>0 & N.1>0|C>1])
  write.table(X,"clipboard",row.names = F,sep="\t")
}
E.Out[,N:=NULL][,N.P:=NULL]


# ***PostHarvest (PO.Out)=====
PO.Out<-lapply(XL,"[[","PH.E.Out")
PO.Out<-rbindlist(lapply(1:length(PO.Out),FUN=function(i){
  X<-PO.Out[[i]][,1:7]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("PO.Level.Name"))
  
  X
}))

setnames(PO.Out, "P1...2", "P1")
setnames(PO.Out, "P2...3", "P2")
setnames(PO.Out, "P3...4", "P3")

PO.Out[P1==0,P1:=NA
       ][P2==0,P2:=NA
         ][P3==0,P3:=NA
           ]

PO.Out<-PO.Out[apply(PO.Out[,c("P1","P2","P3")],1,FUN=function(X){
  sum(is.na(X))!=3
})]

# Remove any parenthesis in names
PO.Out[,PO.Level.Name:=gsub("[(]","",PO.Level.Name)][,PO.Level.Name:=gsub("[)]","",PO.Level.Name)]

  # PO.Out: Check for potential invalid comparisons that will need ordering practices specifying ####
# Count have many ERA practices present for each practice
PO.Out[,N:=1:.N][,N.P:=sum(!is.na(P1),!is.na(P2),!is.na(P3)),by=N]

# Count practices >2, practices=1 and controls for each study 
# List those studies with >1 practices with >2 PH practices, or >1 control
if(F){
(X<-PO.Out[,list(N.2=sum(N.P>1),N.1=sum(N.P==1 & !grepl("h",PO.Codes)),C=sum(grepl("h",PO.Codes))),by=B.Code][N.2>0 & N.1>0|C>1])
  write.table(X,"clipboard",row.names = F,sep="\t")
}
PO.Out[,N:=NULL][,N.P:=NULL]

# ***Animals (Animals.Out) =====

#Animals.Diet
Animals.Out<-lapply(XL,"[[",19)
Animals.Diet<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
  X<-Animals.Out[[i]][,-(c(1:20,32:74))]
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("A.Level.Name...21"))
  X
}))

setnames(Animals.Diet, "A.Level.Name...21", "A.Level.Name")
setnames(Animals.Diet, "D.Item...23", "D.Item")
Animals.Diet[D.Process==0,D.Process:=NA]
Animals.Diet[D.Item==0,D.Item:=NA]

  # Animals.Diet: Remove any parenthesis in names ####
  Animals.Diet[, A.Level.Name:=gsub("[(]","", A.Level.Name)][, A.Level.Name:=gsub("[)]","", A.Level.Name)
                                                             ][, D.Item:=gsub("[(]","", D.Item)][, D.Item:=gsub("[)]","", D.Item)]
  
  # Set 0 values to NA
  Animals.Diet[D.Source==0,D.Source:=NA]
  Animals.Diet[D.Ad.lib==0,D.Ad.lib:=NA]
  Animals.Diet[D.Unit.Time==0,D.Unit.Time:=NA]
  Animals.Diet[D.Unit.Amount==0,D.Unit.Amount:=NA]
  Animals.Diet[D.Unit.Animals==0,D.Unit.Animals:=NA]
  Animals.Diet[D.New.Item==0,D.New.Item:=NA]
  
  # Animals.Diet: Validation - Check for any missing D.Items
  Missing.Diet.Item<-Animals.Diet[!is.na(D.Type) & is.na(D.Item) & D.Type !="Entire Diet"]
  if(nrow(Missing.Diet.Item)){
    View(Missing.Diet.Item)
  }
  rm(Missing.Diet.Item)
  
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
    X<-Animals.Out[[i]][,-(1:32)]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("D.Item...33"))
    X
  }))
  setnames(Animals.Diet.Comp, "D.Item...33", "D.Item")
  
  # Animals.Diet.Comp: Remove any parenthesis in names
  Animals.Diet.Comp[, D.Item:=gsub("[(]","", D.Item)][, D.Item:=gsub("[)]","", D.Item)]
  
  # Replace zeros with NAs (assuming all real zeros should be NAs)
  Animals.Diet.Comp<-Animals.Diet.Comp[,lapply(.SD, ZeroFun),.SDcols=colnames(Animals.Diet.Comp)]
  
  # Animals.Out ####
  Animals.Out<-lapply(XL,"[[",19)
  Animals.Out<-rbindlist(lapply(1:length(Animals.Out),FUN=function(i){
    X<-Animals.Out[[i]][,1:19]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("A.Level.Name...1"))
    X
  }))
  setnames(Animals.Out, "A.Level.Name...1", "A.Level.Name")
  
  Cols<-paste0("P",1:14)
  Animals.Out<-Animals.Out[apply(Animals.Out[,..Cols],1,FUN=function(X){
    X<-unlist(X)
    sum(X!=0)>0
  })]
  rm(Cols)
  
  # Animals.Out: Remove any parenthesis in names
  Animals.Out[, A.Level.Name:=gsub("[(]","", A.Level.Name)][, A.Level.Name:=gsub("[)]","", A.Level.Name)]
  
  # Animals.Out: Validation Checks ####
  Cols<-paste0("P",1:14)
  Animals.Out$P.Count<-apply(Animals.Out[,..Cols],1,FUN=function(Z){sum(Z!="0")})>0
  
  A<-Animals.Out[!(!P.Count & is.na(A.Codes) & A.Level.Name == "Base") & is.na(A.Codes)]
  
  Animals.Out[,P.Count:=NULL]
  
  # Check pratices that do not appear to be a Base and have no practices at all
  if(nrow(A)>0){
    A
    write.table(A,"clipboard",sep="\t",row.names = F)
  }
  
  # Replace 0 with NA
  Animals.Out[P1==0,P1:=NA]
  Animals.Out[P2==0,P2:=NA]
  Animals.Out[P3==0,P3:=NA]
  Animals.Out[P4==0,P4:=NA]
  Animals.Out[P4=="No",P4:=NA]
  Animals.Out[P5==0,P5:=NA]
  Animals.Out[P6==0,P6:=NA]
  Animals.Out[P7==0,P7:=NA]
  Animals.Out[P8==0,P8:=NA]
  Animals.Out[P8=="No",P8:=NA]
  Animals.Out[P9==0,P9:=NA]
  Animals.Out[P10==0,P10:=NA]
  Animals.Out[P11==0,P11:=NA]
  Animals.Out[P12==0,P12:=NA]
  Animals.Out[P13==0,P13:=NA]
  Animals.Out[P14==0,P14:=NA]
  Animals.Out[A.Notes==0,A.Notes:=NA]
  Animals.Out[A.Grazing==0,A.Grazing:=NA]
  Animals.Out[A.Hay==0,A.Hay:=NA]
  
  # Rename P.Cols to something more meaningful
  setnames(Animals.Out, "P1", "A.Feed.Add.1")
  setnames(Animals.Out, "P2", "A.Feed.Add.2")
  setnames(Animals.Out, "P3", "A.Feed.Add.3")
  setnames(Animals.Out, "P4", "A.Feed.Add.C")
  setnames(Animals.Out, "P5", "A.Feed.Sub.1")
  setnames(Animals.Out, "P6", "A.Feed.Sub.2")
  setnames(Animals.Out, "P7", "A.Feed.Sub.3")
  setnames(Animals.Out, "P8", "A.Feed.Sub.C")
  setnames(Animals.Out, "P9", "A.Feed.Pro.1")
  setnames(Animals.Out, "P10", "A.Feed.Pro.2")
  setnames(Animals.Out, "P11", "A.Feed.Pro.3")
  setnames(Animals.Out, "P12", "A.Manure.Man")
  setnames(Animals.Out, "P13", "A.Pasture.Man")
  setnames(Animals.Out, "P14", "A.Aquasilvaculture")
  
  
  # Animals.Out: Validation - Feed Add Code but no control or >1 control
  
  X<-Animals.Out[(!is.na(A.Feed.Add.1)|!is.na(A.Feed.Add.2)|!is.na(A.Feed.Add.3)) & A.Level.Name!="Base",B.Code]
  Ani.Out_Feed.Add_Controls<-Animals.Out[B.Code %in% X,list(N.Controls=sum(!is.na(A.Feed.Add.C))),by="B.Code"][N.Controls!=1]
  
  if(nrow(Ani.Out_Feed.Add_Controls[!B.Code %in% c("EO0080","AG0018","NJ0190")])>0){
    View(Ani.Out_Feed.Add_Controls)
  }
  rm(Ani.Out_Feed.Add_Controls,X)
  
  # Animals.Out: Validation - Feed Sub Code but no control
  
  X<-Animals.Out[(!is.na(A.Feed.Sub.1)|!is.na(A.Feed.Sub.2)|!is.na(A.Feed.Sub.3)) & A.Level.Name!="Base",B.Code]
  Ani.Out_Feed.Sub_Controls<-Animals.Out[B.Code %in% X,list(N.Controls=sum(!is.na(A.Feed.Sub.C))),by="B.Code"][N.Controls!=1]
  
  if(nrow(Ani.Out_Feed.Sub_Controls[!B.Code %in% c("AG0051","AG0121","DK0007","EO0080","NJ0013")])>0){
    View(Ani.Out_Feed.Sub_Controls)
  }
  rm(Ani.Out_Feed.Sub_Controls,X)
  
  
  Animals.Out.Raw<-lapply(XL,"[[",19)
  Animals.Out.Raw<-rbindlist(lapply(1:length(Animals.Out.Raw),FUN=function(i){
    X<-Animals.Out.Raw[[i]][,1:19]
    X$B.Code<-Pub.Out$B.Code[i]
    X
  }))
  setnames(Animals.Out.Raw, "A.Level.Name...1", "A.Level.Name")
  Animals.Out.Raw<-Animals.Out.Raw[!is.na(A.Level.Name)& A.Level.Name!="Base"]
  
  # Animals.Out: Validation - A.Level.Name, but no A.Codes
  Animals.Out.Raw[is.na(A.Codes),-"A.Notes"]
  
  # Check Processing Values
  #unique(Animals.Diet[!is.na(D.Process),c("D.Process","B.Code")])
  # Check Diet,Item Values
  #table(Animals.Diet[!is.na(D.Item) & !A.Level.Name=="Entire Diet",D.Item])
  # Missing Item Names
  #Animals.Diet[is.na(D.Item) & !D.Type=="Entire Diet",]
  # Check A.Level.Name has a match
  #Animals.Diet[is.na(match(paste0(Animals.Diet$A.Level.Name,Animals.Diet$B.Code),paste0(Animals.Out$A.Level.Name,Animals.Out$B.Code))),]
  
# Other.Out
  Other.Out<-lapply(XL,"[[","Other.Out")
  Other.Out<-rbindlist(lapply(1:length(Other.Out),FUN=function(i){
    X<-E.Out[[i]][,1:3]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("O.Level.Name"))
    X<-X[!(O.Level.Name=="Base" & O.Structure=="No" & O.Notes==0)]
    X
  }))
  
  Other.Out[O.Notes==0,O.Notes:=NA]
  
# ***Base Practices (Base.Out) =====

Base.Out<-list(
  Var.Out[V.Base=="Yes" & !is.na(V.Codes),c("B.Code","V.Codes")],
  AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
  Till.Codes[T.Level.Name=="Base"& !is.na(T.Code),c("B.Code","T.Code")],
  Fert.Out[F.Level.Name=="Base" & !is.na(F.Codes),c("B.Code","F.Codes")],
  Res.Out[M.Level.Name=="Base" & !is.na(M.Codes),c("B.Code","M.Codes")],
  Har.Out[H.Level.Name=="Base" & !is.na(H.Code),c("B.Code","H.Code")],
  pH.Out[!is.na(pH.Out) & pH.Level.Name=="Base",c("B.Code","pH.Out")],
  WH.Out[WH.Level.Name=="Base" & !is.na(WH.Codes),c("B.Code","WH.Codes")],
  Irrig.Codes[I.Level.Name=="Base" & !is.na(I.Codes),c("B.Code","I.Codes")],
  Animals.Out[A.Level.Name=="Base" & !is.na(A.Codes),c("B.Code","A.Codes")],
  PO.Out[PO.Level.Name=="Base" & !is.na(PO.Codes),c("B.Code","PO.Codes")],
  E.Out[E.Level.Name=="Base" & !is.na(E.Codes),c("B.Code","E.Codes")],
  Weed.Code[!is.na(W.Code),c("B.Code","W.Code")],
  Chems.Code[!is.na(C.Code),c("B.Code","C.Code")]
)

Base.Out<-rbindlist(Base.Out[unlist(lapply(Base.Out,nrow))>0],use.names = F)
Base.Out<-Base.Out[,list(Base.Codes=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

Base.Out.No.Animal<-list(
  Var.Out[V.Base=="Yes" & !is.na(V.Codes),c("B.Code","V.Codes")],
  AF.Out[AF.Level.Name=="Base" & !is.na(AF.Codes),c("B.Code","AF.Codes")],
  Till.Codes[T.Level.Name=="Base"& !is.na(T.Code),c("B.Code","T.Code")],
  Fert.Out[F.Level.Name=="Base" & !is.na(F.Codes),c("B.Code","F.Codes")],
  Res.Out[M.Level.Name=="Base" & !is.na(M.Codes),c("B.Code","M.Codes")],
  Har.Out[H.Level.Name=="Base" & !is.na(H.Code),c("B.Code","H.Code")],
  pH.Out[!is.na(pH.Codes) & pH.Level.Name=="Base",c("B.Code","pH.Codes")],
  WH.Out[WH.Level.Name=="Base" & !is.na(WH.Codes),c("B.Code","WH.Codes")],
  Irrig.Codes[I.Level.Name=="Base" & !is.na(I.Codes),c("B.Code","I.Codes")],
  PO.Out[PO.Level.Name=="Base" & !is.na(PO.Codes),c("B.Code","PO.Codes")],
  E.Out[E.Level.Name=="Base" & !is.na(E.Codes),c("B.Code","E.Codes")],
  Weed.Code[!is.na(W.Code),c("B.Code","W.Code")],
  Chems.Code[!is.na(C.Code),c("B.Code","C.Code")]
)

Base.Out.No.Animal<-rbindlist(Base.Out.No.Animal[unlist(lapply(Base.Out.No.Animal,nrow))>0],use.names = F)
Base.Out.No.Animal<-Base.Out.No.Animal[,list(Base.Codes.No.Animals=paste(unique(V.Codes[order(V.Codes,decreasing = F)]),collapse="-")),by=B.Code]

# ***Treatments (MT.Out)  =====
MT.Out<-lapply(XL,"[[","MT.Out")
MT.Out<-rbindlist(pblapply(1:length(MT.Out),FUN=function(i){
  X<-MT.Out[[i]][,-(31:35)]
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

colnames(MT.Out)[colnames(MT.Out) == "AF.Level.Name...12"]<-"AF.Level.Name"
colnames(MT.Out)[colnames(MT.Out) == "AF.Codes...13"]<-"AF.Codes"
colnames(MT.Out)[colnames(MT.Out) == "Notes...14"]<-"AF.Notes"
colnames(MT.Out)[colnames(MT.Out) == "AF.Tree...15"]<-"AF.Tree"
colnames(MT.Out)[colnames(MT.Out) == "AF.Subspecies...16"]<-"AF.Subspecies"
colnames(MT.Out)[colnames(MT.Out) == "T.Code"]<-"Till.Codes"
colnames(MT.Out)[colnames(MT.Out) == "T.Level.Name"]<-"Till.Level.Name"
colnames(MT.Out)[colnames(MT.Out) == "H.Code"]<-"H.Codes"

# Correct 0s that should really be NAs
MT.Out[AF.Level.Name==0,AF.Level.Name:=NA]
MT.Out[A.Level.Name==0,A.Level.Name:=NA]
MT.Out[C.Level.Name==0,C.Level.Name:=NA]
MT.Out[E.Level.Name==0,E.Level.Name:=NA]
MT.Out[H.Level.Name==0,H.Level.Name:=NA]
MT.Out[I.Level.Name==0,I.Level.Name:=NA]
MT.Out[M.Level.Name==0,M.Level.Name:=NA]
MT.Out[F.Level.Name==0,F.Level.Name:=NA]
MT.Out[P.Level.Name==0,P.Level.Name:=NA]
MT.Out[pH.Level.Name==0,pH.Level.Name:=NA]
MT.Out[PO.Level.Name==0,PO.Level.Name:=NA]
MT.Out[V.Level.Name==0,V.Level.Name:=NA]
MT.Out[WH.Level.Name==0,WH.Level.Name:=NA]
MT.Out[O.Level.Name==0,O.Level.Name:=NA]
MT.Out[Till.Level.Name==0,Till.Level.Name:=NA]
MT.Out[W.Level.Name==0,W.Level.Name:=NA]
MT.Out[T.Residue.Prev==0,T.Residue.Prev:=NA]
MT.Out[T.Residue.Code==0,T.Residue.Code:=NA]
MT.Out[T.Control==0,T.Control:=NA]
MT.Out[M.NO==0,M.NO:=NA]
MT.Out[M.PO==0,M.PO:=NA]
MT.Out[M.KO==0,M.KO:=NA]
MT.Out[M.Notes==0,M.Notes:=NA]
MT.Out[M.Unit==0,M.Unit:=NA]
MT.Out[T.Animals==0,T.Animals:=NA]
MT.Out[T.Start.Year==0,T.Start.Year:=NA]
MT.Out[T.Start.Season==0,T.Start.Season:=NA]
MT.Out[T.Reps==0,T.Reps:=NA]
MT.Out[F.NO==0,F.NO:=NA]
MT.Out[F.PO==0,F.PO:=NA]
MT.Out[F.KO==0,F.KO:=NA]
MT.Out[F.NI==0,F.NI:=NA]
MT.Out[F.KI==0,F.KI:=NA]
MT.Out[F.PI==0,F.PI:=NA]
MT.Out[F.P2O5==0,F.P2O5:=NA]
MT.Out[F.K2O==0,F.K2O:=NA]
MT.Out[F.O.Unit==0,F.O.Unit:=NA]
MT.Out[F.I.Unit==0,F.I.Unit:=NA]

MT.Out[P.Method==0,P.Method:=NA]
MT.Out[Plant.Density==0,Plant.Density:=NA]
MT.Out[Plant.Density.Unit==0,Plant.Density.Unit:=NA]
MT.Out[Plant.Row==0,Plant.Row:=NA]
MT.Out[Plant.Station==0,Plant.Station:=NA]
MT.Out[Plant.Seeds==0,Plant.Seeds:=NA]
MT.Out[Plant.Thin==0,Plant.Thin:=NA]
MT.Out[Plant.Tram.Row==0,Plant.Tram.Row:=NA]
MT.Out[Plant.Tram.N==0,Plant.Tram.N:=NA]
MT.Out[Plant.Intercrop==0,Plant.Intercrop:=NA]
MT.Out[Plant.Block.Rows==0,Plant.Block.Rows:=NA]
MT.Out[Plant.Block.Perc==0,Plant.Block.Perc:=NA]
MT.Out[Plant.Block.Width==0,Plant.Block.Width:=NA]

MT.Out[P.Structure==0,P.Structure:=NA]
MT.Out[O.Structure==0,O.Structure:=NA]
MT.Out[W.Structure==0,W.Structure:=NA]
MT.Out[C.Structure==0,C.Structure:=NA]
MT.Out[T.Structure==0,T.Structure:=NA]

# Set "No" to NA for later treatment comparison logic
MT.Out[C.Structure=="No",C.Structure:=NA]
MT.Out[P.Structure=="No",P.Structure:=NA]
MT.Out[W.Structure=="No",W.Structure:=NA]
MT.Out[O.Structure=="No",O.Structure:=NA]

# Remove Parenthesis from Treatment Names
MT.Out[,T.Name:=gsub("[(]","",T.Name)
       ][,T.Name:=gsub("[)]","",T.Name)
         ][,AF.Level.Name:=gsub("[(]","",AF.Level.Name)
           ][,AF.Level.Name:=gsub("[)]","",AF.Level.Name)
             ][,A.Level.Name:=gsub("[(]","",A.Level.Name)
               ][,A.Level.Name:=gsub("[)]","",A.Level.Name)
                 ][,C.Level.Name:=gsub("[(]","",C.Level.Name)
                   ][,C.Level.Name:=gsub("[)]","",C.Level.Name)
                     ][,E.Level.Name:=gsub("[(]","",E.Level.Name)
                       ][,E.Level.Name:=gsub("[)]","",E.Level.Name)
                         ][,H.Level.Name:=gsub("[(]","",H.Level.Name)
                           ][,H.Level.Name:=gsub("[)]","",H.Level.Name)
                             ][,I.Level.Name:=gsub("[(]","",I.Level.Name)
                               ][,I.Level.Name:=gsub("[)]","",I.Level.Name)
                                 ][,M.Level.Name:=gsub("[(]","",M.Level.Name)
                                   ][,M.Level.Name:=gsub("[)]","",M.Level.Name)
                                     ][,F.Level.Name:=gsub("[(]","",F.Level.Name)
                                       ][,F.Level.Name:=gsub("[)]","",F.Level.Name)
                                         ][,pH.Level.Name:=gsub("[(]","",pH.Level.Name)
                                           ][,pH.Level.Name:=gsub("[)]","",pH.Level.Name)
                                             ][,P.Level.Name:=gsub("[(]","",P.Level.Name)
                                               ][,P.Level.Name:=gsub("[)]","",P.Level.Name)
                                                 ][,PO.Level.Name:=gsub("[(]","",PO.Level.Name)
                                                   ][,PO.Level.Name:=gsub("[)]","",PO.Level.Name)
                                                     ][,Till.Level.Name:=gsub("[(]","",Till.Level.Name)
                                                       ][,Till.Level.Name:=gsub("[)]","",Till.Level.Name)
                                                         ][,V.Level.Name:=gsub("[(]","",V.Level.Name)
                                                           ][,V.Level.Name:=gsub("[)]","",V.Level.Name)
                                                             ][,WH.Level.Name:=gsub("[(]","",WH.Level.Name)
                                                               ][,WH.Level.Name:=gsub("[)]","",WH.Level.Name)
                                                                 ][,O.Level.Name:=gsub("[(]","",O.Level.Name)
                                                                   ][,O.Level.Name:=gsub("[)]","",O.Level.Name)
                                                                     ][,W.Level.Name:=gsub("[(]","",W.Level.Name)
                                                                       ][,W.Level.Name:=gsub("[)]","",W.Level.Name)]

  # MT.Out: Add in corrected Fertilizer Codes ####
  #C.ori<-MT.Out$F.Codes
  MT.Out$F.Codes<-Fert.Out$F.Codes[match(paste0(MT.Out$B.Code,MT.Out$F.Level.Name),paste0(Fert.Out$B.Code,Fert.Out$F.Level.Name))]
  
  #cbind(C.ori,MT.Out$F.Codes) - NAs still present due to missing ".." delim in several papers
  
  # Combine all practice codes together again in T.Code column (assimilating corrected fertilizer coding)
  N<-grep("Codes",colnames(MT.Out))[-1]
  
  #Before<-MT.Out$T.Codes
  MT.Out$T.Codes<-unlist(pblapply(1:nrow(MT.Out),FUN=function(i){
    #print(i)
    X<-unlist(MT.Out[i,..N])
    X<-unlist(strsplit(X,"-"))
    X<-unique(X[!is.na(X)])
    
    
    if(length(X)>1){
      if(MT.Out$T.Codes[i]=="Aggregated"){
        "Aggregated"
      }else{
        paste0(X[order(X)],collapse = "-")
      }
    }else{
      if(length(X)==0){NA}else{X}
    }
    
    
  }))
  #cbind(MT.Out$T.Name,Before,MT.Out$T.Codes) # Check NAs once ".." delim issue is fixed
  
  MT.Out[,F.Level.Name2:=Fert.Out[match(MT.Out[,paste(B.Code,F.Level.Name)],Fert.Out[,paste(B.Code,F.Level.Name)]),F.Level.Name2]]

  MT.Out<-cbind(MT.Out,Fert.Levels[match(MT.Out[,paste(B.Code,F.Level.Name)],Fert.Levels[,paste(B.Code,F.Level.Name)]),!c("B.Code","F.Level.Name")])
  
  MT.Out[,F.Level.Name2:=Fert.Out[match(MT.Out[,F.Level.Name],Fert.Out[,F.Level.Name]),F.Level.Name2]]
  
  # MT.Out: Add in h10 code where there are fertilizer treatments, but fertilizer column is blank ####
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
  
  # MT.Out: Correct Varieties Codes ####
  N<-match(MT.Out[,paste(B.Code,V.Level.Name)],Var.Join[,paste(B.Code,Join)])
  
  # Check for non-matches
  Non.Match.Vars.MT.Out<-unique(MT.Out[!is.na(V.Level.Name) & is.na(N),c("B.Code","V.Level.Name")])
  
  if(nrow(Non.Match.Vars.MT.Out)>0){
    View(Non.Match.Vars.MT.Out)
  }
  
  # Add in replacement cols
  V.Cols<-colnames(MT.Out)[grep("V[.]",colnames(MT.Out))]
  V.Cols<-V.Cols[!V.Cols %in% c("B.Code","V.Level.Name")]
  
  MT.Out<-MT.Out[,!..V.Cols]
  MT.Out<-cbind(MT.Out,Var.Out[N,! c("B.Code")])
  MT.Out[,V.Level.Name:=Join][,Join:=NULL]
  
  # Update T.Codes with new Var information 
  #This could/does happen in a single step later on once all updates have been made, but note the A[A!=0] line which corrects an error in the excel MT.Out sheet
  
  MT.Out$T.Codes<-unlist(pblapply(1:nrow(MT.Out),FUN=function(i){
    #print(i)
    A<-unlist(MT.Out[i,"T.Codes"])
    A[A==0]<-NA
    
    if(!is.na(A)){
      A<-unlist(strsplit(A,"-"))
    }
    B<-unlist(MT.Out[i,"V.Codes"])
    if(!is.na(B)){
      B<-unlist(strsplit(B,"-"))
    }else{
      B<-NA
    }
    X<-c(A,B)
    X<-unique(X)
    X<-X[!is.na(X)]
    if(length(X)>0){
      paste0(X[order(X)],collapse="-")
    }else{
      NA
    }
    
  }))
  
  rm(V.Cols,N)
  
  # MT.Out: Correct Ridge & Furrow - remove water harvesting code if ridge and furrow is a conventional tillage control ####
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
  
  
  # MT.Out: Add h-codes for Weeding & Herbicide use ####
     MT.Out[,Weed.Code:=Weed.Code[match(MT.Out[,paste(B.Code,W.Level.Name)],paste(B.Code,W.Level.Name)),W.Code]]
     MT.Out[,Weed.Code2:=Chems.Code[match(MT.Out[,paste(B.Code,C.Level.Name)],paste(B.Code,C.Level.Name)),C.Code]]
     Weed.Join<-function(A,B){
       C<-c(A,B)
       C<-C[!is.na(C)]
       if(length(C)==0){
         NA
       }else{
         paste(C,collapse="-")
       }
       
     }
     MT.Out[,N:=1:.N][,Weed.Code:=Weed.Join(Weed.Code,Weed.Code2),by=N][,N:=NULL][,Weed.Code2:=NULL]
     
     # unique(MT.Out[,list(Weed.Code,W.Level.Name,C.Level.Name,B.Code)])[!is.na(W.Level.Name) & !is.na(C.Level.Name)]
     rm(Weed.Join)
  
  # MT.Out: Update T.Codes ####
  
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
  
  # MT.Out2: Combine Aggregated Treatments - SLOW CONSIDER PARALLEL ####
  # MAKE SURE FERT AND VAR delims are changed from ".." to something else in T.Level.Name (MT.Out and all Fert/Variety tabs, Data.Out,Int.Out, Rot.Out, Rot.Seq)
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
  
  # MT.Out2: Residue Codes ####
  
  # Set Grazed/Mulched to be Mulched (as this value indicates at least some mulching took place)
  MT.Out2[T.Residue.Prev=="Grazed/Mulched",T.Residue.Prev:="Mulched (left on surface)"]
  
  # Add codes for residues that are from Tree List rather than Product List
  X<-MT.Out2[(!(T.Residue.Prev %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","NA") | is.na(T.Residue.Prev))) & is.na(T.Residue.Code),
             ][T.Comp %in% TreeCodes$Species,c("T.Comp","B.Code","N2","T.Residue.Prev")]
  X<-cbind(X,TreeCodes[match(X$T.Comp, TreeCodes$Species)])
  X[T.Residue.Prev=="Mulched (left on surface)",T.Residue.Code:=Mulched]
  X[T.Residue.Prev=="Incorporated",T.Residue.Code:=Incorp]
  X[T.Residue.Prev=="Retained (unknown if mulched/incorp.)",T.Residue.Code:=Unknown.Fate]
  
  MT.Out2[X$N2,T.Residue.Code:=X$T.Residue.Code]
  
  rm(X)
  
  # MT.Out2: Deal with aggregated products  #####
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
  
  # MT.Out2: Validation ####
# Check for any values where T.Residue.Prev has a value but there is no associated residue code, this probably means values need to be specified in MASTERCODES EU2 tab
MT.Out2.ResPrev.vs.ResCode<-MT.Out2[(!(T.Residue.Prev %in% c("Removed","Incorporated","Grazed","Burned","Unspecified","NA") | is.na(T.Residue.Prev))) & is.na(T.Residue.Code),c("B.Code","T.Name","T.Residue.Prev","T.Residue.Code")]

if(nrow(MT.Out2.ResPrev.vs.ResCode)>0){
  View(MT.Out2.ResPrev.vs.ResCode)
}

rm(MT.Out2.ResPrev.vs.ResCode)


  # MT.Out2: Update final treatment codes (T.Codes) for aggregrated treatments ####
    MT.Out2[grep("[.][.][.]",T.Name2),T.Codes:=T.Codes.No.Agg]

# ***Times ####
Times<-lapply(XL,"[[","Times")

Times<-rbindlist(pblapply(1:length(Times),FUN=function(i){
  X<-Times[[i]][7:30,10:18]
  names(X)<-c("Time","Site.ID","TSP","TAP","Plant.Start","Plant.End","Harvest.Start","Harvest.End","Harvest.DAP")
  X<-X[!is.na(Time)]
  X$B.Code<-Pub.Out$B.Code[i]
  X
}))

# ***Intercropping (Int.Out)  =====
  # TO DO - Link to planting information to attempt to better describe intercropping system and estimate % of each component by area ####
  Int.Out<-lapply(XL,"[[","Int.Out")
  Int.Out<-rbindlist(pblapply(1:length(Int.Out),FUN=function(i){
    X<-Int.Out[[i]][,1:27]
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("IN.Level.Name"))
    X
  }))
  
  Int.Out[,"...18"]<-NULL
  
  setnames(Int.Out, "IN.Prods.Alll", "IN.Prods.All")
  
  # Deal with 0 in names fields
  Int.Out[IN.Comp1==0,IN.Comp1:=NA]
  Int.Out[IN.Comp2==0,IN.Comp2:=NA]
  Int.Out[IN.Comp3==0,IN.Comp3:=NA]
  Int.Out[IN.Comp4==0,IN.Comp4:=NA]
  Int.Out[IN.Residue.Fate==0,IN.Residue.Fate:=NA]
  Int.Out[IN.Reps==0,IN.Reps:=NA]
  Int.Out[IN.Control==0,IN.Control:=NA]
  Int.Out[IN.Notes==0,IN.Notes:=NA]
  Int.Out[IN.Start.Year==0,IN.Start.Year:=NA]
  Int.Out[IN.Start.Season==0,IN.Start.Season:=NA]
  Int.Out[IN.Residue.Fate=="NA",IN.Residue.Fate:=NA]
  
  # Int.Out: Remove any parenthesis in names ####
  Int.Out[,IN.Level.Name:=gsub("[(]","",IN.Level.Name)][,IN.Level.Name:=gsub("[)]","",IN.Level.Name)
                                                        ][,IN.Comp1:=gsub("[(]","",IN.Comp1)][,IN.Comp1:=gsub("[)]","",IN.Comp1)
                                                                                              ][,IN.Comp2:=gsub("[(]","",IN.Comp2)][,IN.Comp2:=gsub("[)]","",IN.Comp2)
                                                                                                                                    ][,IN.Comp3:=gsub("[(]","",IN.Comp3)][,IN.Comp3:=gsub("[)]","",IN.Comp3)
                                                                                                                                                                          ][,IN.Comp4:=gsub("[(]","",IN.Comp4)][,IN.Comp4:=gsub("[)]","",IN.Comp4)]
  
  # Int.Out: Update Aggregated Treatment Delimiters ####
  # Agg Treat = "..." 
  
  Int.Out[,N:=1:.N
          ][,IN.Comp1:=gsub("[.][.]","...",IN.Comp1),by="N"
            ][,IN.Comp2:=gsub("[.][.]","...",IN.Comp2),by="N"
              ][,IN.Comp3:=gsub("[.][.]","...",IN.Comp3),by="N"
                ][,IN.Comp4:=gsub("[.][.]","...",IN.Comp4),by="N"]
  
  
  # Int.Out: Update Residue Codes ####
  
  # 1) Update Codes from MT.Out2
  # Check for any non-matches with MT.Out2
  N<-match(paste(Int.Out$B.Code,Int.Out$IN.Comp1),paste(MT.Out2$B.Code,MT.Out2$T.Name))
  Int.Out.MT.Out2.NoMatch<-Int.Out[is.na(N),c("B.Code","IN.Comp1")]
  if(nrow(Int.Out.MT.Out2.NoMatch)>0){
    View(Int.Out.MT.Out2.NoMatch)
  }
  rm(Int.Out.MT.Out2.NoMatch)
  
  # Update Codes
  Int.Out[,IN.Res1:=MT.Out2[match(Int.Out[,paste(B.Code,IN.Comp1)],MT.Out2[,paste(B.Code,T.Name)]),T.Residue.Code]]
  Int.Out[,IN.Res2:=MT.Out2[match(Int.Out[,paste(B.Code,IN.Comp2)],MT.Out2[,paste(B.Code,T.Name)]),T.Residue.Code]]
  Int.Out[,IN.Res3:=MT.Out2[match(Int.Out[,paste(B.Code,IN.Comp3)],MT.Out2[,paste(B.Code,T.Name)]),T.Residue.Code]]
  Int.Out[,IN.Res4:=MT.Out2[match(Int.Out[,paste(B.Code,IN.Comp4)],MT.Out2[,paste(B.Code,T.Name)]),T.Residue.Code]]
  
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

  # Int.Out: Update T.Codes (for updated fertilizer codes & agg practices)  ####
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
  
  # Int.Out: Special rule for when reduced & zero-tillage are both present ####
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
  # Int.Out: Combine Products ####
  # A duplicate of In.Prods.All column, but using a "-" delimiter that distinguishes productsthat contribute to a system outcome from products aggregated 
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
  
  
  # Int.Out: Update Intercropping Delimiters ####
  Int.Out$IN.Level.Name2<-apply(Int.Out[,c("IN.Comp1","IN.Comp2","IN.Comp3","IN.Comp4")],1,FUN=function(X){
    X<-X[!is.na(X)]
    X<-paste0(X[order(X)],collapse = "***")
  })
  
  
  
  # Int.Out: Structure ####
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

  # Int.Out: Residues in System Outcomes ####
  

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

# ***Rotation (Rot.Out) =====

Rot.Out<-lapply(XL,"[[","Rot.Out")
Rot.Out<-rbindlist(pblapply(1:length(Rot.Out),FUN=function(i){
  X<-Rot.Out[[i]][-1,c(1:13)]
  
  X$B.Code<-Pub.Out$B.Code[i]
  X<-na.omit(X, cols=c("R.Level.Name"))
  X
}))

# Change "0" to NA as appropriate
Rot.Out[R.Start.Year==0,R.Start.Year:=NA]
Rot.Out[R.Start.Season==0,R.Start.Season:=NA]
Rot.Out[R.Reps==0,R.Reps:=NA]
Rot.Out[R.Control==0,R.Control:=NA]
Rot.Out[R.Phases==0,R.Phases:=NA]

  # Rot.Out: Remove any parenthesis in names ####
  Rot.Out[, R.Level.Name:=gsub("[(]","", R.Level.Name)][, R.Level.Name:=gsub("[)]","", R.Level.Name)
                                                        ][,R.T.Level.Names.All:=gsub("[(]","",R.T.Level.Names.All)][,R.T.Level.Names.All:=gsub("[)]","",R.T.Level.Names.All)]
  
  # Rot.Out: Update Delimiters ####
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
  
  # Rot.Out: Update R.T.Codes.All ####
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
  
  # Rot.Seq (Rotation) #####
  
  Rot.Seq<-lapply(XL,"[[","Rot.Out")
  Rot.Seq<-rbindlist(pblapply(1:length(Rot.Seq),FUN=function(i){
    X<-Rot.Seq[[i]][,-c(1:14)]
    
    if("Temporal Component" %in% colnames(X)){
      setnames(X, "Temporal Component", "Rotation Component")
    }
    
    if("Temporal Component " %in% colnames(X)){
      setnames(X, "Temporal Component ", "Rotation Component")
    }
    
    if("Rotation Component " %in% colnames(X)){
      setnames(X, "Rotation Component ", "Rotation Component")
    }
    
    
    X$B.Code<-Pub.Out$B.Code[i]
    X<-na.omit(X, cols=c("Rotation Treatment"))
    X
  }))
  
  setnames(Rot.Seq, "Fate Crop Residues The Previous Season", "R.Resid.Fate")
  
  Rot.Seq[R.Resid.Fate==0,R.Resid.Fate:=NA]
  
  # Rot.Seq: Add in fallow codes #####
  Rot.Seq[`Rotation Component` == "Natural or Bare Fallow",R.T.Codes:="h24"]
  
  # Rot.Seq: Remove any parenthesis in names ####
  Rot.Seq[, `Rotation Treatment`:=gsub("[(]","", `Rotation Treatment`)][, `Rotation Treatment`:=gsub("[)]","", `Rotation Treatment`)
                                                                        ][,`Rotation Component`:=gsub("[(]","",`Rotation Component`)][,`Rotation Component`:=gsub("[)]","",`Rotation Component`)]
  # Rot.Seq: Update delimiters ####
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
  
  # Rot.Seq: Add in Fallow ####
  Rot.Seq[`Rotation Component`=="Natural or Bare Fallow",R.Prod1:="Fallow"]
  
  # Rot.Seq: Combine Product codes ####
  Rot.Seq[,R.Prod:=apply(Rot.Seq[,c("R.Prod1","R.Prod2","R.Prod3","R.Prod4")],1,FUN=function(X){
    X<-unlist(X[!is.na(X)])
    paste(X[order(X)],collapse="***")
  })]
  
  Rot.Seq<-Rot.Seq[order(B.Code,`Rotation Treatment`,Time)]
  
  setnames(Rot.Seq, "R.Res14", "R.Res4")
  
  # Rot.Seq: Summarise Crop Sequence ####
  
  Seq.Fun<-function(A){
    paste(A[!is.na(A)],collapse = "|||")
  }
  Rot.Seq.Summ<-Rot.Seq[,list(Seq=Seq.Fun(R.Prod)),by=c("B.Code","Rotation Treatment")]
  
  setnames(Rot.Seq.Summ, "Rotation Treatment", "R.Level.Name")
  
  Rot.Out$R.Prod.Seq<-Rot.Seq.Summ$Seq[match(paste(Rot.Out$R.Level.Name,Rot.Out$B.Code),paste(Rot.Seq.Summ$R.Level.Name,Rot.Seq.Summ$B.Code))]
  if(F){View(Rot.Out[,c("B.Code","R.Level.Name","R.T.Level.Names.All","R.Prod.Seq")])}
  
  # Rot.Seq: Update T.Codes - Slow Consider Parallel ####

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
  
  # Rot.Seq: Update Residue codes based on previous season ####
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
  
  # Rot.Seq: Update residue codes cross-referencing to MASTERCODES sheets ####
  
  # Set Grazed/Mulched to be Mulched (as this value indicates at least some mulching took place)
  Rot.Seq[R.Resid.Fate=="Grazed/Mulched",R.Resid.Fate:="Mulched (left on surface)"]
  
  # ISSUE - ONLY DEALS WITH UP TO FOUR AGGREGATED PRODUCTS IN A ROTATION COMPONENT - MORE EXIST ####
  
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
  
  # Rot.Out: Update Residues Codes from Rot.Seq ####
  # TO DO: NEEDS TO DEAL WITH (REMOVE) YEARS THAT STARTS BEFORE SEQUENCE BEGINNING #####
  # Consider adding a logical field that indicates if an observation is from a preceeding season.
  X<-Rot.Seq[,list(Residues=if(all(is.na(R.Residues.Codes))){as.character(NA)}else{paste(R.Residues.Codes,collapse = "|||")},
                   R.Residues.All = paste(unique(R.Residues.Codes[!is.na(R.Residues.Codes)])[order(unique(R.Residues.Codes[!is.na(R.Residues.Codes)]))],collapse="-")),by="ID"]
  
  X$R.Residue.Codes.All[match(Rot.Out$ID,X$ID)]
  
  Rot.Out[,R.Residue.Codes.All:=X$R.Residues.All[match(ID,X$ID)]][,R.Residues.All2:=X$Residues[match(ID,X$ID)]]
  Rot.Out[R.Residue.Codes.All=="",R.Residue.Codes.All:=NA]
  rm(X)
  
  # Rot.Seq: Add Rotation Codes and remove codes for the first year of sequence ####
  Rot.Seq[,R.Code:=Rot.Out$R.Code[match(ID,Rot.Out$ID)]]
  
  # Find First Time that Product Switches 
  # ISSUE - DOES THIS NEED TO TAKE INTO ACCOUNT PRECEEDING CROP? 
  # Answer: If preceding crop is the same there is no rotation if it differsmthen there is,this should not be an isssue.
  P.Switch=function(X){
    Y<-which(X!=X[1])
    if(length(Y)==0){
      rep(F,length(X))
    }else{
      Y<-Y[1]:length(X)  
      c(rep(F,length(1:(Y[1]-1))),rep(T,length(Y)))
    }
  }
  
  Rot.Seq[,Prod.Switch:=P.Switch(R.Prod),by="ID"][,Prod.Switch.All.F:=if(sum(Prod.Switch)==0){T}else{F},by="ID"]
  
  # Rot.Out: Validation ####
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
  
  #  Rot.Seq: Validation - View Residues of Aggregated Products
  if(F){
    Rot.Out_Agg.Prod_Res<-Rot.Seq[grep("[.][.]",R.Prod.Prev)][!R.Resid.Fate %in% c("Removed","Incorporated","Grazed","Burned","Unspecified")]
    View(Rot.Out_Agg.Prod_Res)
    rm(Rot.Out_Agg.Prod_Res)
  }
  
  #  Rot.Seq: Validation - View Residues of Trees **THIS TITLE DOES NOT MAKE SENSE**
  if(F){
    Rot.Out_Tree_Residues<-Rot.Seq[!is.na(R.Resid.Fate) & R.Resid.Fate !="Unspecified" & is.na(R.Residues.Codes)]
    View(Rot.Out_Tree_Residues)
    rm(Rot.Out_Tree_Residues)
  }
  
  # Rot.Out: Update delimiters in R.T.Level.Names.All
  
  X<-Rot.Seq[,list(R.T.Level.Names.All2=paste0(`Rotation Component`,collapse="|||")),by=c("Rotation Treatment","B.Code")
             ][,R.ID:=paste(B.Code,`Rotation Treatment`)]
  
  Rot.Out[,R.ID:=paste(B.Code,R.Level.Name)]
  Rot.Out[,R.T.Level.Names.All2:=X$R.T.Level.Names.All2[match(Rot.Out$R.ID,X$R.ID)]]
  
  
  # Rot.Out: Generate T.Codes & Residue for System Outcomes ####
  
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
  
  # Rot.Out/Seq: Add Structure ####
  X<-data.table(
    MT.Out2[match(Rot.Seq[,paste(B.Code,`Rotation Component`)],MT.Out2[,paste(B.Code,T.Name2)]),Structure.Comb],
    Int.Out[match(Rot.Seq[,paste(B.Code,`Rotation Component`)],Int.Out[,paste(B.Code,IN.Level.Name2)]),IN.Structure]
  )
  Join.Fun<-function(X,Y){c(X,Y)[!is.na(c(X,Y))]}
  X<-X[,N:=1:.N][,R.Structure:=Join.Fun(V1,V2),by=N][,R.Structure]
  Rot.Seq[,R.Structure:=X]
  rm(X,Join.Fun)
  
  # Rot.Out: Add Practice Level Columns
  
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

  # Out.Out: Correct old Outcome indicator names ####
Out.Out[Out.Subind=="Nitrogen ARE (Product)",Out.Subind:="Nitrogen Use Efficiency (ARE Product)"]
Out.Out[Out.Subind=="Phosphorus ARE (Product)",Out.Subind:="Phosphorus Use Efficiency (ARE Product)"]
Out.Out[Out.Subind=="Potassium ARE (Product)",Out.Subind:="Potassium Use Efficiency (ARE Product)"]
Out.Out[Out.Subind=="Nitrogen ARE (AGB)",Out.Subind:="Nitrogen Use Efficiency (ARE AGB)"]
Out.Out[Out.Subind=="Nitrogen Recovery Efficiency (AGB)",Out.Subind:="Nitrogen Use Efficiency (ARE AGB)"]
Out.Out[Out.Subind=="Nitrogen ARE (Unspecificed)",Out.Subind:="Nitrogen Use Efficiency (Unspecificed)"]
  # Out.Out: Update Fields From Harmonization Sheet ####
  N<-match(Out.Out[,Out.Unit],UnitHarmonization[,Out.Unit])
  Out.Out[!is.na(N),Out.Unit:=UnitHarmonization[N[!is.na(N)],Out.Unit.Correct]]
  rm(N)
  # Out.Out: Add columns from partial outcome data stored in Out.Group field
  # Out.Out[grepl("<P>",Out.Group),list(Studies=length(unique(B.Code)),Obs=length(B.Code))]
  
  X<-t(Out.Out[grepl("<P>",Out.Group),strsplit(unlist(lapply(strsplit(Out.Group,"<P>"),"[[",2)),"[|]")])
  Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Name:=X[,1]]
  Out.Out[grepl("<P>",Out.Group),Out.Partial.Outcome.Code:=X[,2]]
  rm(X)
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

# Set zero values to NA
Data.Out[ED.Rot==0,ED.Rot:=NA]
Data.Out[ED.Int==0,ED.Int:=NA]
Data.Out[ED.Product.Simple==0,ED.Product.Simple:=NA]
Data.Out[ED.Product.Comp==0,ED.Product.Comp:=NA]
Data.Out[ED.Product.Comp=="NA",ED.Product.Comp:=NA]
Data.Out[ED.M.Year==0,ED.M.Year:=NA]
Data.Out[ED.Treatment==0,ED.Treatment:=NA]
Data.Out[ED.Reps==0,ED.Reps:=NA]
Data.Out[ED.Start.Year==0,ED.Start.Year:=NA]
Data.Out[ED.Start.Season==0,ED.Start.Season:=NA]
Data.Out[ED.Sample.Start==0,ED.Sample.Start:=NA]
Data.Out[ED.Sample.End==0,ED.Sample.End:=NA]
Data.Out[ED.Sample.DAS==0,ED.Sample.DAS:=NA]
Data.Out[ED.Plant.Start==0,ED.Plant.Start:=NA]
Data.Out[ED.Plant.End==0,ED.Plant.End:=NA]
Data.Out[ED.Harvest.Start==0,ED.Harvest.Start:=NA]
Data.Out[ED.Harvest.End==0,ED.Harvest.End:=NA]
Data.Out[ED.Comparison==0,ED.Comparison:=NA]

  # Data.Out: Remove any parenthesis in names ####
  Data.Out[,ED.Treatment:=gsub("[(]","",ED.Treatment)
           ][,ED.Treatment:=gsub("[)]","",ED.Treatment)
             ][,ED.Site.ID:=gsub("[(]","",ED.Site.ID)
               ][,ED.Site.ID:=gsub("[)]","",ED.Site.ID)
                 ][,ED.Int:=gsub("[(]","",ED.Int)
                   ][,ED.Int:=gsub("[)]","",ED.Int)
                     ][,ED.Rot:=gsub("[(]","",ED.Rot)
                       ][,ED.Rot:=gsub("[)]","",ED.Rot)
                          ][,ED.Comparison:=gsub("[(]","",ED.Comparison)
                            ][,ED.Comparison:=gsub("[)]","",ED.Comparison)]

  # Replace zero with NA
  Zero.Cols<-c("ED.Error","ED.Error.Type","ED.I.Amount","ED.I.Unit","ED.Variety")
  Data.Out<-Data.Out[,(Zero.Cols):=lapply(.SD, ZeroFun),.SDcols=Zero.Cols]

  
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
  
  # Save tables as a list  ####
  Tables_2020<-list(
    Pub.Out=Pub.Out, 
    Site.Out=Site.Out, 
    Soil.Out=Soil.Out,
    ExpD.Out=ExpD.Out,
    Prod.Out=Prod.Out,
    Var.Out=Var.Out,
    Till.Out=Till.Out,
    Plant.Out=Plant.Out,
    Fert.Out=Fert.Out,
    Fert.Method=Fert.Method,
    Chems.Out=Chems.Out,
    Weed.Out=Weed.Out,
    Res.Out=Res.Out,
    Res.Method=Res.Method,
    Res.Composition=Res.Composition,
    Har.Out=Har.Out,
    pH.Out=pH.Out,
    pH.Method=pH.Method,
    Irrig.Out=Irrig.Out,
    WH.Out=WH.Out,
    E.Out=E.Out,
    Animals.Out=Animals.Out,
    Animals.Diet=Animals.Diet,
    Animals.Diet.Comp=Animals.Diet.Comp,
    Other.Out=Other.Out,
    Base.Out=Base.Out,
    MT.Out=MT.Out,
    MT.Out2=MT.Out2,
    Int.Out=Int.Out,
    Rot.Out=Rot.Out,
    Rot.Seq=Rot.Seq,
    Rot.Seq.Summ=Rot.Seq.Summ,
    Rot.Levels=Rot.Levels,
    Out.Out=Out.Out,
    Data.Out=Data.Out,
    AF.Out=AF.Out,
    Times=Times
  )
  
  save(Tables_2020,file=paste0("Data/Compendium Master Database/ERA V1.1 Tables ",as.character(Sys.Date()),".RData"))
