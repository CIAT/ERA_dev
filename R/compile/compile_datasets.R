# First run R/0_set_env.R
# Uses datasets created import and compare R scripts. Also may use the older version of ERA created in 2018.

# 0) Prepare environment ####
  # 0.1) Load Packages #####

if(!require("pacman", character.only = TRUE)){
  install.packages("pacman",dependencies = T)
}

pacman::p_load(data.table, 
               countrycode,
               future,
               future.apply,
               progressr,
               pbapply, 
               ggplot2,
               miceadds,
               openxlsx,
               reshape2,
               rworldmap,
               tidyr,
               arrow,
               viridis)


  # 0.2) Create Functions #####

CompileCompendiumWide<-function(Data,
                                PracticeCodes,
                                OutcomeCodes,
                                EUCodes,
                                SaveName=NA, # Specify a save name, set to NA to use default file naming (Folder/Folder-Clean-Full-Wide.filetype)
                                cores,       # Number of cores to use for parallel processing
                                SaveCsv,    # Save compiled dataset (T/F)?
                                SaveR,       # Save as .RData (T) or .csv (F)
                                Folder,      # Save Folder
                                VERBOSE=F,
                                AddText=T,   # Add descriptive outcome and practice names as fields? (T/F) 
                                delim="-",   # Specify delimiter using in outcome and practice names (default = "-")
                                Keep.hcode=F, # Consider non-CSA h-codes in practice naming? (T/F)
                                AddBuffer=T, # Harmonize latitude and longitude and add spatial uncertainty? (T/F), Note if Buffer.Manual field is not blank these values will be used.
                                Keep.No.Spatial=T, # Keep observations with no latitude or longitude (T/F)
                                N.Cols, # The number of practice columns
                                DoWide=F # Add logical columns for presence of each practices in plist?
){
  
  if(colnames(EUCodes)[1]!="EU"){colnames(EUCodes)[1]<-"EU"}
  Data<-as.data.table(Data)
  
  # Create Functions ####
  # Function to harmonize CSA compendium spatial locations into decimal degrees and spatial uncertainty into a buffer radius in meters . 
  # Arguments:
  # Data = "raw" uncompiled meta-dataset with the fields #LatD, LatM, LatS, LatH, LonD, LonM, LonS, LonH, LatDiff, Buffer.Manual. These fields are 
  # used to append Latitude (numeric decimal degrees), Longitude (numeric decimal degrees) and buffer (numeric, m) fields to the supplied "Data"
  # object. The supplied fields are removed from the returned object. (dataframe or data.table).
  # SaveError = Save a file detailing sites with missing spatial information (logical T/F)
  # Folder = Where to save Error file (NA or location of folder as character string)
  # Keep.No.Spatial (T/F) = keep observation with missing spatial in the returned object? (logical T/F)
  
  BufferCalc<-function(Data,SaveError=T,Folder,Keep.No.Spatial=T){
    options(scipen=999)
    Data<-as.data.frame(Data)
    # Note & exclude observations with no spatial information
    MissingXY<-Data[(is.na(Data$LatD)|is.na(Data$LonD)),]
    
    Data<-Data[!(is.na(Data$LatD)|is.na(Data$LonD)),]
    
    # Set Lat/Lon fields to numeric
    Data$LatD<-as.numeric(as.character(Data$LatD))
    Data$LonD<-as.numeric(as.character(Data$LonD))
    
    X<-which(
      grepl("[.]",as.character(Data$LatD)) & (!is.na(Data$LatM) | !is.na(Data$LatS))
      |
        grepl("[.]",as.character(Data$LonD)) & (!is.na(Data$LonM) | !is.na(Data$LonS))
    )
    if(length(X)>0){
      Data[X,c("LatM","LatS","LonM","LonS")]<-NA
    }
    
    # Create lat/lon columns for use in analysis
    Data$Latitude<-Data$LatD
    Data$Longitude<-Data$LonD
    # Create Buffer Column:
    Data$Buffer<-as.numeric("")
    Data$BufferLa<-as.numeric("")
    Data$BufferLo<-as.numeric("")
    
    # Calculate Accuracy from Decimal Degree spatial locations:
    DD<-grep("[.]",as.character(Data$LatD))
    DecimalPlaces<-nchar(unlist(strsplit(as.character(Data$LatD[DD]),"[.]"))[seq(2,length(unlist(strsplit(as.character(Data$LatD[DD]),"[.]"))),2)])
    DecimalPlaces[DecimalPlaces==1]<-11000/2
    DecimalPlaces[DecimalPlaces==2]<-1100/2
    DecimalPlaces[DecimalPlaces>=3 & DecimalPlaces<=10]<-500/2
    Data$BufferLa[DD]<-DecimalPlaces
    
    DD2<-grep("[.]",as.character(Data$LonD))
    DecimalPlaces<-nchar(unlist(strsplit(as.character(Data$LonD[DD2]),"[.]"))[seq(2,length(unlist(strsplit(as.character(Data$LonD[DD2]),"[.]"))),2)])
    DecimalPlaces[DecimalPlaces==1]<-11000/2
    DecimalPlaces[DecimalPlaces==2]<-1100/2
    DecimalPlaces[DecimalPlaces>=3 & DecimalPlaces<=10]<-500/2  # Minimum radius of unvalidated locations (i.e. those without a manual buffer estimation) = 500 m
    Data$BufferLo[DD2]<-DecimalPlaces
    
    # Convert DMS to DD and calculate accuracy from DMS spatial locations:
    # Degrees Only: No decimal point for DD, no minutes and no seconds recorded
    Data$BufferLa[!grepl("[.]",as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS)]<-110000/2
    Data$BufferLo[!grepl("[.]",as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS)]<-110000/2
    
    # Minutes Only:
    Minute<-round(0.5*110000/60) # 1/60th of a degree = diameter (half of this is the radius)
    
    X<-which(!is.na(Data$LatM) & is.na(Data$LatS))
    if(length(X)>0){
      Data$BufferLa[X]<-Minute
      Data$Latitude[X]<-Data$LatD[X]+Data$LatM[X]/60
    }
    
    X<-which(!is.na(Data$LonM) & is.na(Data$LonS))
    if(length(X)>0){
      Data$BufferLo[X]<-Minute
      Data$Longitude[X]<-Data$LonD[X]+Data$LonM[X]/60  
    }
    
    # Minutes and Seconds:
    Second<-500/2 # Minimum radius of unvalidated locations (i.e. those without a manual buffer estimation) = 500 m
    
    X<-which(!is.na(Data$LatM) & !is.na(Data$LatS))
    if(length(X)>0){
      Data$BufferLa[X]<-Second
      Data$Latitude[X]<-Data$LatD[X]+Data$LatM[X]/60+Data$LatS[X]/60^2
    }
    X<-which(!is.na(Data$LonM) & !is.na(Data$LonS))
    if(length(X)>0){
      Data$BufferLo[X]<-Second
      Data$Longitude[X]<-Data$LonD[X]+Data$LonM[X]/60+Data$LonS[X]/60^2
    }
    
    # Situations where lat and lon are recorded in inconsistent formats
    DD3<-which(!grepl("[.]",as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS))
    DD4<-which(!grepl("[.]",as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS))
    
    X<-unique(c(DD[is.na(match(DD,DD2))],DD2[is.na(match(DD2,DD))],DD3[is.na(match(DD3,DD4))],DD4[is.na(match(DD4,DD3))]))
    
    # Choose minimum buffer width (assume where accuracy is lower for one value it's a rounding issue)
    Data$Buffer<-apply(cbind(Data$BufferLa,Data$BufferLo),1,min,na.rm=TRUE)
    
    # Set decimal degrees to correct hemispheres:
    X<-which(Data$LatH=="S" & Data$Latitude>0)
    if(length(X)>0){
      Data$Latitude[X]<-Data$Latitude[X]*-1
    }
    
    X<-which(Data$LonH=="W" & Data$Longitude>0)
    if(length(X)>0){
      Data$Longitude[X]<-Data$Longitude[X]*-1
    }
    
    # Add in accuracy where locations are averaged, this should be in minutes
    Data$Lat.Diff<-as.numeric(Data$Lat.Diff)/60
    Data$Buffer[!is.na(Data$Lat.Diff)]<-1500*Data$Lat.Diff[!is.na(Data$Lat.Diff)]/2 # Divided by two as this is the diameter not the radius
    
    #Round locational accuracy to a sensible number of decimal places
    Data$Latitude<-round(Data$Latitude,5)
    Data$Longitude<-round(Data$Longitude,5)
    
    # Round buffer
    Data$Buffer<-as.integer(round(Data$Buffer))
    
    # Replace Calculated values with Manual values?
    if(!is.null(Data$Buffer.Manual)){
      Data$Buffer[!is.na(Data$Buffer.Manual)]<-Data$Buffer.Manual[!is.na(Data$Buffer.Manual)]
      Data$Buffer.Manual<-NULL
      if(nrow(MissingXY)>0){
        MissingXY$Buffer.Manual<-NULL
      }
    }
    
    # Remove unecessary fields
    Data[,c("LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff","BufferLa","BufferLo")]<-NULL
    
    # Save "Error" data & add back in sites with missing spatial data?
    if(nrow(MissingXY)>0){
      if(SaveError){
        if(!dir.exists(paste0(Folder,"/Errors"))){
          dir.create(paste0(Folder,"/Errors"),recursive = T)
        }
        fwrite(unique(MissingXY[,c("Code","Site.ID","Country","LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff")]),file=paste0(Folder,"/Errors/",Folder,"-MissingXY.csv"))
      }
      
      if(Keep.No.Spatial){
        MissingXY[,c("LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff")]<-NULL
        MissingXY[,c("Latitude","Longitude","Buffer")]<-NA
        Data<-rbind(Data,MissingXY)
      }
    }
    
    
    Data<-data.table(Data)
    return(Data)
  }
  
  # Function to add descripts of outcomes and practices
  # Possibly more efficient to use something like dplyr::left_join(DataZ,EUCodes[,c("EU","Product.Type","Product.Subtype","Product.Simple")],by = "EU"))
  
  Code.2.Text<-function(Data,OutcomeCodes,PracticeCodes,EUCodes,Keep.hcode,AddCodes=F){
    
    
    AddNames<-function(Data.Code,Master.Name,Master.Code,delim){
      paste(sort(unique(Master.Name[match(Data.Code,Master.Code)])),collapse=delim)
    }
    
    RmHCodes<-function(Codes,Delim){
      X<-unlist(strsplit(Codes,"-"))  
      X<-X[!grepl("h",X)]
      paste(sort(X),collapse = "-")
    }
    
    # Outcomes
    Data[,Outcode:=as.character(Outcode)]
    delim<-"-"
    Data[,Out.Pillar:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Pillar],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
    Data[,Out.SubPillar:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Subpillar],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
    Data[,Out.Ind:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Indicator],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
    Data[,Out.SubInd:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Subindicator],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
    
    if(AddCodes){
      Data[,Out.SubInd.S:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Subindicator.Short],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
      delim<-"/"
      Data[,Out.Pillar.Code:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Pillar.Code],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
      Data[,Out.SubPillar.Code:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Subpillar.Code],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
      Data[,Out.Ind.Code:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Indicator.Code],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
      Data[,Out.SubInd.Code:=AddNames(Data.Code=unlist(strsplit(Outcode[1],"-")),Master.Name = OutcomeCodes[,Subindicator.Code],Master.Code = OutcomeCodes[,Code],delim=delim),by=Outcode]
    }
    
    # Practices
    if(!Keep.hcode){
      Data[,plist:=RmHCodes(plist,"-"),by=plist]
      Data[,base.list:=RmHCodes(base.list,"-"),by=base.list]
    }
    
    delim<-"-"
    Data[,SubPrName:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Subpractice.S],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
    Data[,PrName:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Practice],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
    Data[,Theme:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Theme],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
   
    if(AddCodes){
      delim<-"/"
      Data[,SubPrName.Code:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Subpractice.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
      Data[,PrName.Code:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Practice.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
      Data[,Theme.Code:=AddNames(Data.Code=unlist(strsplit(plist[1],"-")),Master.Name = PracticeCodes[,Theme.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=plist]
    }
    
    if(!is.null(Data$base.list)){
      delim<-"-"
      Data[,SubPrName.Base:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Subpractice.S],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
      Data[,PrName.Base:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Practice],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
      Data[,Theme.Base:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Theme],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
     
      if(AddCodes){
        delim<-"/"
        Data[,SubPrName.Base.Code:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Subpractice.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
        Data[,PrName.Base.Code:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Practice.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
        Data[,Theme.Base.Code:=AddNames(Data.Code=unlist(strsplit(base.list[1],"-")),Master.Name = PracticeCodes[,Theme.Code],Master.Code = PracticeCodes[,Code],delim=delim),by=base.list]
      }
        }
    
    # Products
    delim<-"-"
    Data[,Product:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
    Data[,Product.Type:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Type],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
    Data[,Product.Subtype:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Subtype],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
    Data[,Product.Simple:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Simple],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
    
    if(AddCodes){
      delim<-"/"
      Data[,Product.Type.Code:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Type.Code],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
      Data[,Product.Subtype.Code:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Subtype.Code],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
      Data[,Product.Simple.Code:=AddNames(Data.Code=unlist(strsplit(EUlist[1],"-")),Master.Name = EUCodes[,Product.Simple.Code],Master.Code = EUCodes[,EU],delim=delim),by=EUlist]
    }
    
    return(Data)
  }
  
  SplitProd<-function(X){
    X<-unlist(strsplit(X,"[.]"))
    Y<-X[1]
    if(length(X)>1){
      for(i in 2:length(X)){
        if(nchar(X[i])==1){
          Y[i-1]<-paste0( Y[i-1],".",X[i])
        }else{
          Y<-c(Y,X[i])
        }
      }
    }
    Y
    
  }
  
  # Error Checking ####
  
  # Create Error Save Directory
  if(!dir.exists(paste0(Folder,"/Errors"))){
    dir.create(paste0(Folder,"/Errors"),recursive = T)
  }
  
  Codes<-PracticeCodes$Code
  
  # Check T/C codes match PracticeCodes$Codes
  P.Error<-lapply(c("T","C"),FUN=function(X){
    Y<-lapply(1:N.Cols,FUN=function(x){
      i<-paste0(X,x)
      T.Code<-unlist(Data[,..i])
      N<-which((!T.Code %in% Codes) & !is.na(T.Code))
      T.Code<-T.Code[N]
      names(T.Code)<-N
      T.Code
    })
    
    names(Y)<-paste0(X,1:N.Cols)
    Y<-Y[unlist(lapply(Y,length))>0]
    Y
  })
  
  if(length(P.Error[[1]])>0){
    if(VERBOSE){
      View(P.Error[[1]])
    }
    fwrite(data.frame(Code = unique(unlist(P.Error[[1]]))),file=paste0(Folder,"/Errors/ T.Codes do not match Master Practice Codes.csv"))
  }
  
  if(length(P.Error[[2]])>0){
    if(VERBOSE){
      View(P.Error[[2]])
    }
    fwrite(data.frame(Code = unique(unlist(P.Error[[2]]))),file=paste0(Folder,"/Errors/ C.Codes do not match Master Practice Codes.csv"))
  }
  
  # Check EU codes against EUCodes$Codes
  Codes<-EUCodes$EU
  EU<-SplitProd(as.character(Data$EU))
  
  EU.Error<-lapply(EU,FUN=function(x){
    x<-x[!x %in% Codes]
    x
  })
  names(EU.Error)<-1:length(EU.Error)
  EU.Error<-EU.Error[unlist(lapply(EU.Error,length))>0]
  EU.Error<-unique(unlist(EU.Error))
  
  if(length(EU.Error)>0){
    if(VERBOSE){
      View(EU.Error)
    }
    fwrite(data.frame(EU.Error),file=paste0(Folder,"/Errors/ EU Codes do not match Master EU Codes.csv"))
  }
  
  # Data$EU <-as.factor(Data$EU)
  
  # Check outcome codes against OutcomeCodes$Codes
  Codes<-OutcomeCodes[,Code]
  
  if(length(which(!Data$Outcode %in% Codes))>0){
    N<-which(!Data$Outcode %in% Codes)
    O.Error<-Data$Outcode[N]
    names(O.Error)<-N
    if(VERBOSE){
      View(unique(O.Error))
    }
    fwrite(data.frame(unique(O.Error)),file=paste0(Folder,"/Errors/Outcome Codes do not match Master Outcomes Codes.csv"))
  }
  
  # Check no codes are NA
  NACols<-c("Code","Author","Date","Journal","Country","EU","T1","C1","T.Descrip","C.Descrip","CID","TID","Site.ID")
  Z<-apply(Data[,..NACols],1,FUN=function(X){any(is.na(X)|X==" ")})
  if(any(Z)){
    Y<-Data[Z,..NACols]
    if(VERBOSE){
      print(paste("Note observations with missing critical data present"))
    }
    fwrite(Y,file=paste0(Folder,"/Errors/Observations missing critical information.csv"))
  }
  
  #Create a unique numeric id for each study
  Data[,ID:=as.numeric(factor(Code))]
  
  # Compile Data ####
  #Calculate number of observations from each study and outcome per study, creates new column obs_count and out_count
  STEP<-1
  print(paste0("Step ",STEP,": Calculating number of observations from each study and outcome per study"))
  STEP<-STEP+1
  
  Data<-Data[,`:=`(obs_count=.N,out_count=length(unique(Outcome))),by=ID]
  
  # Deal with characters in numeric columns ####
  
  Cols<-c("LonD","LatD","LatS","LatD","LatM","LatS","MeanT","MeanC","Rep")
  Errors<-lapply(Cols,FUN=function(i){
    
    A<-unlist(Data[,..i])
    N1<-is.na(A)
    N2<-is.na(as.numeric(A))
    N<-which(N1!=N2)
    
    if(length(N)>0){
      N
    }else{
      
    }
    
  })
  
  names(Errors)<-Cols
  Errors<-Errors[unlist(lapply(Errors,length))>0]
  
  if(length(Errors)>0 ){
    if(VERBOSE){
      View(Errors,row.names=F)
    }
    for(i in 1:length(Errors)){
      fwrite(Data[Errors[[i]],],paste0(Folder,"/Errors/Rows containing character observations in numeric columns - ",names(Errors)[i],".csv"))
    }
  }
  
  Data[,LonD:=as.numeric(as.character(LonD))]
  Data[,LonM:=as.numeric(as.character(LonM))]
  Data[,LonS:=as.numeric(as.character(LonS))]
  Data[,LatD:=as.numeric(as.character(LatD))]
  Data[,LatM:=as.numeric(as.character(LatM))]
  Data[,LatS:=as.numeric(as.character(LatS))]
  Data[,MeanT:=as.numeric(as.character(MeanT))]
  Data[,MeanC:=as.numeric(as.character(MeanC))]
  Data[,Rep:=as.numeric(as.character(Rep))]
  
  # Convert to DMS locations to decimal degrees ####
  Data[,Lat:=LatD + ((LatS/60)+LatM)/60]
  Data[,Lon:=LonD + ((LonS/60)+LonM)/60]
  
  Data[LatH=='S',Lat:=Lat*-1]
  Data[LonH=='W',Lon:=Lon*-1]
  
  # C alculate effect size (% change) ####
  #if no reps are given, assume no replication (e.g. a replication of 1)
  Data[is.na(Rep),Rep:=1]
  Data[,yi:=suppressWarnings(log(MeanT/MeanC))] #yi is the response ratio
  Data[,pc:=100*(MeanT - MeanC)/MeanC]  #pc is the percent change
  # Data[,w:=(Rep^2/(2*Rep))/obs_count] #w is the weighting
  
  # Deal with LER outcomes
  Data[Outcome==103 & is.na(MeanC),yi:=log(MeanT)]
  Data[Outcome==103 & is.na(MeanC),pc:=100*(MeanT-1)]
  
  # Extract treatments by comparing the treatment codes to the control codes ####
  
  # Fast function to that extract treatments by comparing the treatment codes to the control codes.
  # Function either outputs a list, using parallel processing and lapply, or a vector, using apply.
  # Cores are the number of cores to use in parallel processing, it's a good idea to leave at least one free (so total system cores -1)
  
  ExtractTreatment<-function(Data,cores,N.Cols,LIST=T){
    Data<-data.frame(Data)
    if(LIST==T){
      cl<-makeCluster(cores)
      clusterEvalQ(cl, library(data.table))
      clusterExport(cl,list("Data","N.Cols"))
      registerDoSNOW(cl)
      
      # Parallel function when list output is needed
      Data$plist<-parLapply(cl,1:nrow(Data),function(i){
        cset<-Data[i,paste0("C",1:N.Cols)]
        tset<-Data[i,paste0("T",1:N.Cols)]
        cset<-cset[!is.na(cset)]
        tset<-tset[!is.na(tset)]
        Y<-as.character(unlist(setdiff(tset, cset)))
        Y[!Y==""]
      })
      
      Data$base.list<-unlist(parLapply(cl,1:nrow(Data),function(X){
        cset<-Data[X,paste0("C",1:N.Cols)]
        tset<-Data[X,paste0("T",1:N.Cols)]
        cset<-cset[!is.na(cset)]
        tset<-tset[!is.na(tset)]
        Y<-as.character(unlist(intersect(tset, cset)))
        Y<-Y[!(Y=="" | is.na(Y))]
        paste(Y,collapse = "-")
      })
      )
      
      stopCluster(cl)
    }else{
      # Alternative function if a vector output is needed
      Data$plist<-apply(Data,1,FUN=function(X){
        cset<-X[paste0("C",1:N.Cols)]
        tset<-X[paste0("T",1:N.Cols)]
        cset<-cset[!is.na(cset)]
        tset<-tset[!is.na(tset)]
        Y<-as.character(setdiff(tset, cset))
        Y[!Y==""]
      })
      
      Data$base.list<-unlist(parLapply(cl,1:total,function(X){
        cset<-Data[X,paste0("C",1:N.Cols)]
        tset<-Data[X,paste0("T",1:N.Cols)]
        cset<-cset[!is.na(cset)]
        tset<-tset[!is.na(tset)]
        Y<-as.character(unlist(intersect(tset, cset)))
        Y<-Y[!(Y=="" | is.na(Y))]
        paste(Y,collapse = "-")
      })
      )
    }
    
    return(data.table(Data))
  }
  
  print(paste0("Step ",STEP,": Extracting treatments by comparing the treatment codes to the control codes"))
  STEP<-STEP+1
  
  Data<-ExtractTreatment(Data,cores,N.Cols,LIST=T)
  
  DataZ<-Data # Redundant?
  
  setnames(DataZ,"Outcome","Outcode")
  # Make treatments wide? ####
  if(DoWide){
    print(paste0("Step ",STEP,": Making Treatments Wide"))
    STEP<-STEP+1
    
    # Get unique treatment codes from plist
    U.A<-DataZ[,sort(unique(unlist(plist)))]
    
    # Convert plist to a matrix
    A<-as.data.frame(t(sapply(DataZ[,plist], `length<-`, max(lengths(DataZ[,plist])))))
    
    # Create logical T/F column for each treatment code with +/- parallel functionality
    if(cores>1){
      cl<-makeCluster(cores)
      clusterExport(cl,list("A","U.A"),envir=environment())
      registerDoSNOW(cl)
      Z<-parLapply(cl,1:length(U.A),fun=function(X){
        Reduce(`|`,lapply(A, function(x) x %in% U.A[X]))
      })
      stopCluster(cl)
    }else{
      Z<-lapply(1:length(U.A),FUN=function(X){
        Reduce(`|`,lapply(A, function(x) x %in% U.A[X]))
      })
    }
    
    Z<-as.data.table(Z)
    colnames(Z)<-U.A
    DataZ<-cbind(DataZ,Z)
  }
  
  # Unlist lists
  print(paste0("Step ",STEP,": Unlisting Lists"))
  STEP<-STEP+1
  
  DataZ[,N:=1:.N
  ][,plist:=paste(unlist(plist),collapse="-"),by=N
  ][,N:=NULL]
  
  DataZ$plist<-unlist(DataZ$plist)
  
  # Add products ####
  DataZ[,EUlist:=paste(sort(unlist(SplitProd(EU))),collapse="-"),by=EU]
  
  
  # Add Text ####
  
  print(paste0("Step ",STEP,": Adding descriptive names for practice and outcomes"))
  STEP<-STEP+1
  
  if(AddText){
    DataZ<-Code.2.Text(Data=DataZ,OutcomeCodes,PracticeCodes,EUCodes,Keep.hcode)
  }
  
  # Add Buffer ####
  print(paste0("Step ",STEP,": Harmonizing spatial data and estimating uncertainty buffers"))
  STEP<-STEP+1
  
  if(AddBuffer){
    DataZ<-BufferCalc(Data=DataZ,SaveError=F,Folder,Keep.No.Spatial=T)
  }
  
  # Use Lat, Long and Buffer to create a folder name and a unique spatial ID ####
  DataZ[,Site.Key:=paste0(sprintf("%07.4f",Latitude)," ",sprintf("%07.4f",Longitude)," B",Buffer),by=list(Latitude,Longitude,Buffer)]
  
  # Harmonize lat/long of any sites with duplicate Site.Keys (differ at 5dp but not 4)
  X<-table(unique(DataZ[,list(Site.Key,Latitude,Longitude,Buffer)])[,Site.Key])
  X<-names(X[X>1])
  
  unique(DataZ[Site.Key %in% X,list(Site.Key,Latitude,Longitude,Buffer)])
  
  DataZ[,Latitude:=round(mean(unique(Latitude)),5),by=Site.Key]
  DataZ[,Longitude:=round(mean(unique(Longitude)),5),by=Site.Key]
  
  # Save Data ####
  
  if(SaveCsv){
    NAME<-paste0(if(is.na(SaveName)){paste0(Folder,"/",Folder,"-Clean-Full-Wide.csv")}else{paste0(Folder,"/",SaveName,".csv")})
    print(paste0("Final Step: Saving Wide dataset as ",NAME))
    fwrite(DataZ, file=NAME)
  }
  
  if(SaveR){
    NAME<-paste0(if(is.na(SaveName)){paste0(Folder,"/",Folder,"-Clean-Full-Wide.R.Data")}else{paste0(Folder,"/",SaveName,".RData")})
    print(paste0("Final Step: Saving Wide dataset as ",NAME))
    save(DataZ,file=NAME)
  }
  
  return(DataZ)
}

# Site Buffer Function
BufferCalc<-function(Data,SaveError=T,Folder,Keep.No.Spatial=T){
  options(scipen=999)
  Data<-as.data.frame(Data)
  # Note & exclude observations with no spatial information
  MissingXY<-Data[(is.na(Data$LatD)|is.na(Data$LonD)),]
  
  Data<-Data[!(is.na(Data$LatD)|is.na(Data$LonD)),]
  
  X<-which(
    grepl("[.]",as.character(Data$LatD)) & (!is.na(Data$LatM) | !is.na(Data$LatS))
    |
      grepl("[.]",as.character(Data$LonD)) & (!is.na(Data$LonM) | !is.na(Data$LonS))
  )
  if(length(X)>0){
    Data[X,c("LatM","LatS","LonM","LonS")]<-NA
  }
  
  # Create lat/lon columns for use in analysis
  Data$Latitude<-Data$LatD
  Data$Longitude<-Data$LonD
  # Create Buffer Column:
  Data$Buffer<-as.numeric("")
  Data$BufferLa<-as.numeric("")
  Data$BufferLo<-as.numeric("")
  
  # Calculate Accuracy from Decimal Degree spatial locations:
  DD<-grep("[.]",as.character(Data$LatD))
  DecimalPlaces<-nchar(unlist(strsplit(as.character(Data$LatD[DD]),"[.]"))[seq(2,length(unlist(strsplit(as.character(Data$LatD[DD]),"[.]"))),2)])
  DecimalPlaces[DecimalPlaces==1]<-11000/2
  DecimalPlaces[DecimalPlaces==2]<-1100/2
  DecimalPlaces[DecimalPlaces>=3 & DecimalPlaces<=10]<-500/2
  Data$BufferLa[DD]<-DecimalPlaces
  
  DD2<-grep("[.]",as.character(Data$LonD))
  DecimalPlaces<-nchar(unlist(strsplit(as.character(Data$LonD[DD2]),"[.]"))[seq(2,length(unlist(strsplit(as.character(Data$LonD[DD2]),"[.]"))),2)])
  DecimalPlaces[DecimalPlaces==1]<-11000/2
  DecimalPlaces[DecimalPlaces==2]<-1100/2
  DecimalPlaces[DecimalPlaces>=3 & DecimalPlaces<=10]<-500/2  # Minimum radius of unvalidated locations (i.e. those without a manual buffer estimation) = 500 m
  Data$BufferLo[DD2]<-DecimalPlaces
  
  # Convert DMS to DD and calculate accuracy from DMS spatial locations:
  # Degrees Only: No decimal point for DD, no minutes and no seconds recorded
  Data$BufferLa[!grepl("[.]",as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS)]<-110000/2
  Data$BufferLo[!grepl("[.]",as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS)]<-110000/2
  
  # Minutes Only:
  Minute<-round(0.5*110000/60) # 1/60th of a degree = diameter (half of this is the radius)
  
  X<-which(!is.na(Data$LatM) & is.na(Data$LatS))
  if(length(X)>0){
    Data$BufferLa[X]<-Minute
    Data$Latitude[X]<-Data$LatD[X]+Data$LatM[X]/60
  }
  
  X<-which(!is.na(Data$LonM) & is.na(Data$LonS))
  if(length(X)>0){
    Data$BufferLo[X]<-Minute
    Data$Longitude[X]<-Data$LonD[X]+Data$LonM[X]/60  
  }
  
  # Minutes and Seconds:
  Second<-500/2 # Minimum radius of unvalidated locations (i.e. those without a manual buffer estimation) = 500 m
  
  X<-which(!is.na(Data$LatM) & !is.na(Data$LatS))
  if(length(X)>0){
    Data$BufferLa[X]<-Second
    Data$Latitude[X]<-Data$LatD[X]+Data$LatM[X]/60+Data$LatS[X]/60^2
  }
  X<-which(!is.na(Data$LonM) & !is.na(Data$LonS))
  if(length(X)>0){
    Data$BufferLo[X]<-Second
    Data$Longitude[X]<-Data$LonD[X]+Data$LonM[X]/60+Data$LonS[X]/60^2
  }
  
  # Situations where lat and lon are recorded in inconsistent formats
  DD3<-which(!grepl("[.]",as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS))
  DD4<-which(!grepl("[.]",as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS))
  
  X<-unique(c(DD[is.na(match(DD,DD2))],DD2[is.na(match(DD2,DD))],DD3[is.na(match(DD3,DD4))],DD4[is.na(match(DD4,DD3))]))
  
  # Choose minimum buffer width (assume where accuracy is lower for one value it's a rounding issue)
  Data$Buffer<-apply(cbind(Data$BufferLa,Data$BufferLo),1,min,na.rm=TRUE)
  
  # Set decimal degrees to correct hemispheres:
  X<-which(Data$LatH=="S" & Data$Latitude>0)
  if(length(X)>0){
    Data$Latitude[X]<-Data$Latitude[X]*-1
  }
  
  X<-which(Data$LonH=="W" & Data$Longitude>0)
  if(length(X)>0){
    Data$Longitude[X]<-Data$Longitude[X]*-1
  }
  
  # Add in accuracy where locations are averaged, this should be in minutes
  Data$Lat.Diff<-as.numeric(Data$Lat.Diff)/60
  Data$Buffer[!is.na(Data$Lat.Diff)]<-1500*Data$Lat.Diff[!is.na(Data$Lat.Diff)]/2 # Divided by two as this is the diameter not the radius
  
  #Round locational accuracy to a sensible number of decimal places
  Data$Latitude<-round(Data$Latitude,5)
  Data$Longitude<-round(Data$Longitude,5)
  
  # Round buffer
  Data$Buffer<-as.integer(round(Data$Buffer))
  
  # Replace Calcuated values with Manual values?
  if(!is.null(Data$Buffer.Manual)){
    Data$Buffer[!is.na(Data$Buffer.Manual)]<-Data$Buffer.Manual[!is.na(Data$Buffer.Manual)]
    Data$Buffer.Manual<-NULL
    if(nrow(MissingXY)>0){
      MissingXY$Buffer.Manual<-NULL
    }
  }
  
  # Remove unecessary fields
  Data[,c("LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff","BufferLa","BufferLo")]<-NULL
  
  # Use Lat, Long and Buffer to create a folder name and a unique spatial29* ID
  Data$Site.Key<-paste0(sprintf("%07.4f",Data$Latitude)," ",sprintf("%07.4f",Data$Longitude)," B",Data$Buffer)
  
  # Save "Error" data & add back in sites with missing spatial data?
  if(nrow(MissingXY)>0){
    if(SaveError){
      if(!dir.exists(paste0(Folder,"/Errors"))){
        dir.create(paste0(Folder,"/Errors"),recursive = T)
      }
      fwrite(unique(MissingXY[,c("Code","Site.ID","Country","LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff")]),file=paste0(Folder,"/Errors/",Folder,"-MissingXY.csv"))
    }
    
    if(Keep.No.Spatial){
      MissingXY[,c("LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff")]<-NULL
      MissingXY[,c("Latitude","Longitude","Buffer","Site.Key")]<-NA
      Data<-rbind(Data,MissingXY)
    }
  }
  
  
  Data<-data.table(Data)
  return(Data)
}
  # 0.3) Set Cores for Parallel #####
# Detect the number of cores in your system & set number for parallel processing 
cores<-max(1, parallel::detectCores() - 1)

# 1) Load data ####
  # 1.1) Load era vocab #####
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)

  # 1.2) Read in concept and harmonization sheets ######
# Outcomes
#OutcomeCodes<-data.table::fread("Concept Scheme/Outcomes.csv",header=T,strip.white=F,encoding="Latin-1")
OutcomeCodes<-master_codes$out
if(sum(table(OutcomeCodes[,Code])>1)){print("Duplicate codes present in outcomes")}

# Practice Codes
#PracticeCodes<-data.table::fread("Concept Scheme/Practices.csv",header=T,strip.white=F,encoding="Latin-1")
PracticeCodes<-master_codes$prac
if(sum(table(PracticeCodes[,Code])>1)){print("Duplicate codes present in practices")}

# Products
#EUCodes<-data.table::fread("Concept Scheme/EU.csv",header=T,strip.white=F,encoding="Latin-1")[,1:18]
EUCodes<-master_codes$prod
if(sum(table(EUCodes[,EU])>1)){print("Duplicate codes present in products")}

# Trees
#TreeCodes<-data.table::fread("Concept Scheme/Trees.csv",header=T,strip.white=F,encoding="Latin-1")
TreeCodes<-master_codes$trees
setnames(TreeCodes, "Tree.Latin.Name", "Latin.Name")
TreeCodes<-na.omit(TreeCodes, cols=c("EU"))

# Duplicate Papers (entered in both 2020 and 2018, or some other reason)
#Duplicates<-data.table::fread("Concept Scheme/Harmonization/Duplicates.csv",header=T,strip.white=F,encoding="Latin-1")
Duplicates<-master_codes$dups
master_codes$

# Unit Harmonization
#UnitHarmonization<-data.table::fread("Concept Scheme/Harmonization/Unit_Lists.csv",header=T,strip.white=F,encoding="Latin-1")
UnitHarmonization<-master_codes$unit_harmonization
UnitHarmonization[Out.Unit.Correct==""|is.na(Out.Unit.Correct),Out.Unit.Correct:=Out.Unit]

# Download AEZ code mappings
#AEZ.Mappings<-data.table::fread("Concept Scheme/AEZ_Mappings.csv",header=T,strip.white=F,encoding="Latin-1")
AEZ.Mappings<-master_codes$aez

  # 1.3) Set era dataset locations ####
# Find most recent 2018 dataset
filename18<-grep(".parquet",tail(list.files(era_dirs$era_masterdata_dir,"v1[.]0",recursive = F,full.names = T),1),value=T)
filename18_simple<-gsub("era_data_|[.]parquet","",basename(filename18))

# Find most recent majestic hippo 2020 dataset
filename20<-tail(grep(".parquet",list.files(era_dirs$era_masterdata_dir,"v1[.]1",recursive = F,full.names = T),value=T),1)
filename20_simple<-gsub("era_data_|[.]parquet","",basename(filename20))

# Find most recent skinny cow 2022 dataset
filename22<-tail(grep("comparisons",grep(".parquet",list.files(era_dirs$era_masterdata_dir,era_projects$skinny_cow_2022,recursive = F,full.names = T),value=T),value=T),1)
filename22_simple<-gsub("-","_",gsub("era_data_|_comparisons[.]parquet","",basename(filename22)))

# Create combined file name (this is the name used to save the compiled dataset)
filename_comb<-paste(c("era_compiled",filename18_simple,filename20_simple,filename22_simple),collapse="-")

# !!Remove: Set climate information locations ####
#era_geodata_dir<-era_dirs$era_geodata_dir

# Note that data in the below Climate, Soil and AEZ  directories are extracted for ERA sites using another script
# Set climate directory containing "AEZ_MAP_MAT.RData"
#ClimateDir<-paste0(getwd(),"/Climate/Climate Past/")

# Set soils directory containing "SoilGrids Data.csv"
#SoilsDir<-paste0(getwd(),"/Soils/")

# Set directory containing AEZ zone data "HChoice.csv"
#AEZDir<-paste0(getwd(),"/Other Linked Datasets/")

# 1.4) Read in and prepare ERA datasets ####
# Check if .R object, if not read in xlsx file and convert to R

  # 1.4.1) 2018 ######
  era_2018<-data.table(arrow::read_parquet(filename18))

  # Fix bug with joined M.Year missing last 0 in 2018 data
  era_2018[nchar(M.Year)==8,M.Year:=paste0(M.Year,0)]
  
  # Reject invalid comparisons and remove column from 2018
  era_2018<-era_2018[Invalid.Comparison!="Y"][,Invalid.Comparison:=NULL]
  
  # Add Analysis function column to 2018
  era_2018[,Analysis.Function:="Manual"]
  
  # Add Version
  era_2018[,Version:=2018]
  
  # 1.4.1.x) FIX THE RAW DATASET - Fix encoding issue with Site.ID ######
  install.packages("gsubfn")
  library(gsubfn)
  
  decode_hex_entities <- function(text) {
    gsubfn("<([0-9A-Fa-f]{2})>", function(hex) {
      rawToChar(as.raw(strtoi(hex, 16L)))
    }, text)
  }
  
  era_2018[, Site.ID := decode_hex_entities(Site.ID[1]),by=Site.ID]
  
  # Check encoding issue has disappeared
  print(era_2018$Site.ID[942])  
  
    # 1.4.1.1) Recode residue/mulch codes in diversified systems (residues/mulch from more than just the crop can be present) ####
  Dpracs<-c("Crop Rotation","Intercropping","Improved Fallow","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
  PC<-PracticeCodes[Practice %in% Dpracs,Code]
  PC1<-PracticeCodes[Practice %in% c("Agroforestry Pruning"),Code]
  PC2<-PracticeCodes[Practice %in% c("Mulch","Crop Residue","Crop Residue Incorporation"),Code] 

  FunX<-function(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10){
    return(list(c(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10)))
  }
  era_2018[,C.Codes:=list(FunX(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10)),by=Index]
  era_2018[,T.Codes:=list(FunX(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10)),by=Index]
  era_2018[,Div:=any(unlist(T.Codes) %in% PC),by=Index]
  
  Recode<-function(PC1,PC2,Codes){
    Codes<-unlist(Codes)
    X<-Codes %in% PC1
    
    if(sum(X)>0){
      Codes[X]<-gsub("a17","a15",Codes[X])
      Codes[X]<-gsub("a16","a15",Codes[X])
    }
    
    X<-Codes %in% PC2
    
    if(sum(X)>0){
      Y<-unlist(strsplit(Codes[X],"[.]"))
      Codes[X]<-Y[nchar(Y)>1]
    }
    
    return(unique(Codes))
    
  }

  # Enable progressr
  progressr::handlers(global = TRUE)
  progressr::handlers("txtprogressbar")
  
  # Prepare indices
  indices <- unique(era_2018[Div == TRUE, Index])
  
  # Set up parallel processing
  plan(multisession, workers = cores)
  
  # Recode in parallel with progress reporting
  with_progress({
    # Define the progress bar
    progress <- progressr::progressor(along = indices)
    
    DataX<-rbindlist(future_lapply(indices,function(i){
    # DataX<-rbindlist(pblapply(era_2018[Div==T,Index],FUN=function(i){

    progress()
    
    Z<-era_2018[Index==i]
    C.Codes<-unlist(Z[,C.Codes])
    T.Codes<-unlist(Z[,T.Codes])
    
    if(any(T.Codes %in% PC)){
      
      T.Codes<-Recode(PC1,PC2,T.Codes)
      T.Codes<-c(T.Codes,rep("",10-length(T.Codes)))
      C.Codes<-Recode(PC1,PC2,C.Codes)
      C.Codes<-c(C.Codes,rep("",10-length(C.Codes)))
      
      Z<-data.frame(Z)
      Z[,paste0("C",1:10)]<-C.Codes
      Z[,paste0("T",1:10)]<-T.Codes
      Z<-data.table(Z)
      
    }
    
    progress()
    Z
  }))
  
  })

  era_2018<-rbind(era_2018[Div!=T],DataX)[,T.Codes:=NULL][,C.Codes:=NULL][,Div:=NULL]
  
  rm(PC1,PC2,Recode,DataX)
  
    # 1.4.1.2) Update units in  dataset ####
  N<-match(era_2018[,Units],UnitHarmonization[,Out.Unit])
  era_2018[!is.na(N),Units:=UnitHarmonization[N[!is.na(N)],Out.Unit.Correct]]
  
  error_dat<-era_2018[!Units %in% UnitHarmonization[,Out.Unit.Correct] & Units!="" & !is.na(Units),.(value=paste0(unique(Units),collapse = "|")),by=Code
                      ][,dataset:=era_projects$v1.0_2018
                        ][,field:="Units"
                          ][,issue:="No match for unit in era_master_table/unit_harmonization table."]
  
  errors<-list(units_2018=error_dat)

    # 1.4.1.3) Recode 232.3, 232.4, 232.5 outcomes to 232.1 (Water Use) #######
    era_2018[grep("232.3|232.4|232.5",Outcome),Outcome:=232.1]
    # 1.4.1.3) Change Aggregated Site & Country Delimiter from . to .. #######
    era_2018[,Site.ID:=gsub("[.] ",".",Site.ID)]
    era_2018[,Site.ID:=gsub("site.1","Site 1",Site.ID)]
    era_2018[,Site.ID:=gsub("[.]","..",Site.ID)]
    era_2018[,Country:=gsub("[.]","..",Country)]
    # 1.4.1.4) set soil texture to lowercase #######
    era_2018[,Soil.Texture:=tolower(Soil.Texture)]
    # 2018: Check for duplicate row indices ####
    error_dat<-era_2018[,.(N=.N,Code=unique(Code)),by=Index
                        ][N>1,.(value=paste(unique(Index),collapse="/")),by=Code
                          ][,table:=era_projects$v1.0_2018
                            ][,field:="Index"
                              ][,issue:="Duplicate index value(s)."]
    
    if(nrow(error_dat)>0){
      errors$duplicate_indices_2018<-error_dat
      warning("Duplicate row indices in 2018 dataset.")
    }
    
    # 1.4.1.5) remove SRI b15 ####
    X<-grepl("b15",era_2018[,N:=1:.N][,list(TCols=paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10),collapse="-")),by=N][,TCols])
    
    SRI.Present<-unique(era_2018[X,list(Index,Code,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10)])
    if(nrow(SRI.Present)>0){
      warning(paste0("SRI present in ",nrow(SRI.Present)," rows of era_2018. These codes will be removed."))
    }
    
    error_dat<-era_2018[X,.(value=paste(unique(T.Descrip),collapse="/")),by=Code
    ][,table:=era_projects$v1.0_2018
    ][,field:="T.Descrip"
    ][,issue:="SRI codes are present, can these be converted into component practices?."]
    
    # Remove SRI rows
    era_2018<-era_2018[!X]
    era_2018[,N:=NULL]
    
  # 1.4.2) 2020 ######
  era_2020<-data.table(arrow::read_parquet(filename20))
  # How many C/T columns are in the 2020 dataset?
  N.Cols<-sum(paste0("C",1:30) %in% colnames(era_2020))
  
  # Add Index to 2020
  era_2020[,Index:=(era_2018[,max(Index)]+1):(era_2018[,max(Index)]+nrow(era_2020))]
  era_2020[,Version:=2020]
  
  # Check for NA values in T1 field, if values are present this indicates an issue with the excel extraction script. In the past issues have been caused by non-matches in concept or harmonization sheets due to character encoding issues.
  era_2020[,list(Len=sum(is.na(T1))),by=list(Code)][Len>0]
  
  # 1.4.3) 2022 ######
  era_2022<-data.table(arrow::read_parquet(filename22))
 
  # Update tree field name
  setnames(era_2022,c("Tree.AF","Tree.AF.Clean"),c("Tree","Tree.Clean"))

  # Add Index to 2022
  era_2022[,Index:=(era_2020[,max(Index)]+1):(era_2020[,max(Index)]+nrow(era_2022))]
  era_2022[,Version:=era_projects$skinny_cow_2022]

  # Check for NA values in T1 field, if values are present this indicates an issue with the excel extraction script. In the past issues have been caused by non-matches in concept or harmonization sheets due to character encoding issues.
  era_2022[,list(Len=sum(is.na(T1))),by=list(Code)][Len>0]

# 1.5) Match & add missing cols to 2018/2022 ####
# Missing cols in 2018 vs 2020
missing_cols<-colnames(era_2020)[!colnames(era_2020) %in% colnames(era_2018)]
era_2018[,(missing_cols):=as.character("")]

# Missing cols in 2022 vs 2020
missing_cols<-colnames(era_2020)[!colnames(era_2020) %in% colnames(era_2022)]
era_2022[,(missing_cols):=as.character("")]

# Missing cols in 2018 vs 2022
missing_cols<-colnames(era_2022)[!colnames(era_2022) %in% colnames(era_2018)]
era_2018[,(missing_cols):=as.character("")]

# Missing cols in 2020 vs 2022
missing_cols<-colnames(era_2022)[!colnames(era_2022) %in% colnames(era_2020)]
era_2020[,(missing_cols):=as.character("")]

# Missing cols in 2020 vs 2018
missing_cols<-colnames(2018)[!colnames(2018) %in% colnames(era_2020)]
if(length(missing_cols)>0){
era_2020[,(missing_cols):=as.character("")]
}
  

# 2018 & 2020: Fix format of dates ####
# https://stackoverflow.com/questions/43230470/how-to-convert-excel-date-format-to-proper-date-in-r/62334132
Data2[,Plant.End:=as.Date(Plant.End, origin = "1899-12-30")
      ][,Plant.Start:=as.Date(Plant.Start, origin = "1899-12-30")
        ][,Harvest.Start:=as.Date(Harvest.Start, origin = "1899-12-30")
          ][,Harvest.End:=as.Date(Harvest.End, origin = "1899-12-30")]

Data[,Harvest.Start:=as.Date(Harvest.Start,"%d.%m.%Y")
][,Harvest.End:=as.Date(Harvest.End,"%d.%m.%Y")
][,Plant.Start:=as.Date(Plant.Start,"%d.%m.%Y")
][,Plant.End:=as.Date(Plant.End,"%d.%m.%Y")]

# 2018: Update outcomes ####
# Check cost outcomes with costs per kg or similar
if(F){
unique(Data[Outcome %in% c(150,151,152,152.1) & grepl("/kg|/m3|/sack",Units),list(Code,Outcome,Units,DataLoc)])
}

# Check & update biomass yield outcomes 
Data[Outcome==102,EUX:=EU
     ][Outcome==102,Product.Subtype:=EUCodes[match(unlist(strsplit(EUX,"[.]")),EU),paste(unique(Product.Subtype),collapse = ".")],by=EUX
       ][Outcome==102,Product.Simple:=EUCodes[match(unlist(strsplit(EUX,"[.]")),EU),paste(unique(Product.Simple),collapse = ".")],by=EUX]

# Change outcomes that are only for fodders to biomass yield
unique(Data[Outcome==102 & Product.Subtype=="Fodders",list(Product.Subtype,Product.Simple)])
Data[Outcome==102 & Product.Subtype=="Fodders",Outcome:=101.1]

if(F){
  X<-unique(Data[Outcome==102,list(Code,Outcome,Units,EU,Product.Subtype,Product.Simple)])
  write.table(X,"clipboard-256000",row.names = F,sep=",")
  }

Data[,c("Product.Subtype","Product.Simple","EUX"):=NULL]

# Join Datasets ####
Data<-rbindlist(list(Data,Data2),use.names = T)

# Remove any all NA rows
Data<-Data[apply(Data,1,function(X){!all(is.na(X))}),]

# Use Clean Columns (where various fields in 2018 have been tidied using scripts)
Data[T.Descrip.Clean=="",T.Descrip.Clean:=T.Descrip]
Data[C.Descrip.Clean=="",C.Descrip.Clean:=C.Descrip]
Data[Variety.Clean=="",Variety.Clean:=Variety]
Data[Tree.Clean=="",Variety.Clean:=Tree]
Data[Diversity.Clean=="",Variety.Clean:=Diversity]

Clean<-colnames(Data)[grep("Clean",colnames(Data))]
if(length(Clean)>0){
  Data<-data.frame(Data)
  Replace.Col<-gsub(".Clean","",Clean)
  Data[,Replace.Col]<-Data[,Clean]
  Data[,Clean]<-NULL
  Data<-data.table(Data)
}
rm(Clean,Data2)

Data[is.na(C13),C13:=""]

# Check for absence of t codes ####
View(Data[is.na(T1),list(Code,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13)])

Data<-Data[!is.na(T1)]

# Add Irrigation Present Columns ####
CCols<-paste0("C",1:13)
TCols<-paste0("T",1:13)

Data[,Irrigation.C:=as.logical(apply(Data[,..CCols],1,FUN=function(X){sum(grepl("b37|b54|b34|b72|b36|b53|h7",X))}))]
Data[,Irrigation.T:=as.logical(apply(Data[,..TCols],1,FUN=function(X){sum(grepl("b37|b54|b34|b72|b36|b53|h7",X))}))]

# Remove t and s codes from EUCodes and move to Diversity/Tree fields respectively ####

# If biomass outcome with s/t code exclude observation
Data<-Data[!(grepl("s|t",EU) & Outcome %in% c(101,102,103))]

# check tree and diversity cols
Trees<-Data[(is.na(Tree)|Tree=="") & grepl("t",EU),list(Code,EU,Outcome)][,EU]

Trees<-rbindlist(lapply(unique(Trees),FUN=function(TREES){
  X<-unlist(strsplit(TREES,"[.]"))
  X<-grep("t",X,value = T)
  data.table(Code=TREES,Tree.Col=paste(TreeCodes[match(X,EU),Product],collapse="-"))
}))

if(nrow(Trees)>0){
  for(i in 1:nrow(Trees)){
    Data[grepl(Trees[i,Code],EU) & !grepl(Trees[i,Tree.Col],Tree),Tree:=paste(Tree,Trees[i,Tree.Col])]
  }
}

CoverCrops<-Data[(is.na(Diversity)|Diversity=="") & grepl("s",EU),list(Code,EU,Outcome)][,EU]

CoverCrops<-rbindlist(lapply(unique(CoverCrops),FUN=function(CC){
  X<-unlist(strsplit(CC,"[.]"))
  X<-grep("s",X,value = T)
  X<-X[!X %in% c("s1","s2")]
  if(length(X)>0){
    data.table(Code=CC,Div.Col=paste(EUCodes[match(X,EU),Product.Simple],collapse="-"))
  }
}))


for(i in 1:nrow(CoverCrops)){
  Data[grepl(CoverCrops[i,Code],EU) & !grepl(CoverCrops[i,Div.Col],Diversity),Diversity:=paste(Diversity,CoverCrops[i,Div.Col])]
}

# remove t & s codes

RemoveEUCode<-function(X,Code){
  X<-gsub("[.]1","-1",X)
  X<-gsub("[.]2","-2",X)
  X<-gsub("[.]3","-3",X)
  X<-gsub("[.]4","-4",X)
  
  X<-unlist(strsplit(X,"[.]"))
  
  X<-paste(X[!grepl(Code,X)],collapse=".")
  X<-gsub("-",".",X)
  return(X)
}

Data[,EU:=RemoveEUCode(X=EU,Code="t|s"),by=EU]

OnlyTSCodes<-Data[is.na(EU)|EU==""|nchar(EU)==0,list(Code,EU,Tree,Diversity,DataLoc,Outcome)]
View(OnlyTSCodes)

Data<-Data[!(is.na(EU)|EU==""|nchar(EU)==0)]

# Restructure Irrigation Practices ####
pasteNA<-function(X){paste(X[!is.na(X)],collapse="-")}

Data[,C.Codes:=paste(c(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13),collapse = "-"),by=Index]

Data[grep("b34",C.Codes),Irrig.Meth.C1:="Drip Irrigation"
][grep("b36",C.Codes),Irrig.Meth.C2:="Alternate Partial Rootzone Irrigation"
][grep("b72",C.Codes),Irrig.Meth.C3:="Sprinkler Irrigation"
][grep("b53",C.Codes),Irrig.Meth.C4:="Irrigation (Other)"
][,Irrig.Meth.C:=pasteNA(c(Irrig.Meth.C1,Irrig.Meth.C2,Irrig.Meth.C3,Irrig.Meth.C4)),by=Index
][,Irrig.Meth.C1:=NULL
][,Irrig.Meth.C2:=NULL
][,Irrig.Meth.C3:=NULL
][,Irrig.Meth.C4:=NULL
][,C.Codes:=NULL]

Data[,T.Codes:=paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13),collapse = "-"),by=Index]

Data[grep("b34",T.Codes),Irrig.Meth.C1:="Drip Irrigation"
][grep("b36",T.Codes),Irrig.Meth.C2:="Alternate Partial Rootzone Irrigation"
][grep("b72",T.Codes),Irrig.Meth.C3:="Sprinkler Irrigation"
][grep("b53",T.Codes),Irrig.Meth.C4:="Irrigation (Other)"  
][,Irrig.Meth.T:=pasteNA(c(Irrig.Meth.C1,Irrig.Meth.C2,Irrig.Meth.C3,Irrig.Meth.C4)),by=Index
][,Irrig.Meth.C1:=NULL
][,Irrig.Meth.C2:=NULL
][,Irrig.Meth.C3:=NULL
][,Irrig.Meth.C4:=NULL
][,T.Codes:=NULL]

# Remove Codes 
TC.Cols<-paste0(c("T","C"),rep(1:13,2))

RemoveIrrigCode<-function(X){
  X[X %in% c("b72","b36","b34","b53")]<-""
  return(X)
}

Data[,(TC.Cols):=lapply(.SD, RemoveIrrigCode),.SDcols=TC.Cols]

# Recode Cost Benefit Ratios to Benefit Cost Ratios ####
X<-data.table(In=c(126,126.1,126.2,126.3),Out=c(125,125.1,125.2,125.3))

for(i in 1:nrow(X)){
  Data[Outcome==X[i,In],MeanC:=1/MeanC
  ][Outcome==X[i,In],MeanT:=1/MeanT
  ][Outcome==X[i,In],Outcome:=X[i,Out]]
}
# Inconsistent or Missing NI/NO numeric 0s ####
# This is where there is inconsistency between a N fertilizer code and columns indicating the amount of N added
# Remove Inconsistent Practices
NI.Pracs<-c("b17|b23")
NO.Pracs<-c("b29|b30|b75|b73|b67")

Data[,C.Codes:=trimws(paste(c(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13),collapse=" ")),by=list(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13)]
Data[,T.Codes:=trimws(paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13),collapse=" ")),by=list(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13)]

Data[,C.NI:=as.numeric(C.NI)
][,T.NI:=as.numeric(T.NI)
][,C.NO:=as.numeric(C.NO)
][,T.NO:=as.numeric(T.NO)]

# 2020: T.NI or C.NI has inorganic fertilizer added, but there are no fertilizer codes in the T/C cols
unique(Data[!grepl(NI.Pracs,C.Codes) & C.NI>0 & Version==2020,list(Code)])
unique(Data[!grepl(NI.Pracs,T.Codes) & T.NI>0 & Version==2020,list(Code)])

# 2018: T.NI or C.NI has inorganic fertilizer added, but there are no fertilizer codes in the T/C cols
write.table(unique(Data[!grepl(NI.Pracs,C.Codes) & C.NI>0 & Version==2018,list(Code,DataLoc,C.NI,C.Descrip,C.Codes)]),"clipboard-256000",sep="\t",row.names = F)
write.table(unique(Data[!grepl(NI.Pracs,T.Codes) & T.NI>0 & Version==2018,list(Code,DataLoc,T.NI,T.Descrip,T.Codes)]),"clipboard-256000",sep="\t",row.names = F)

# 2020: T.NO or C.NO has a value, but there are no organic input codes in the T/C cols
unique(Data[!grepl(NO.Pracs,C.Codes) & C.NO>0 & Version==2020,list(Code)])
unique(Data[!grepl(NO.Pracs,T.Codes) & T.NO>0 & Version==2020,list(Code)])

# 2018: T.NI or C.NI has inorganic fertilizer added, but there are no fertilizer codes in the T/C cols
write.table(unique(Data[!grepl(NO.Pracs,C.Codes) & C.NO>0 & Version==2018,list(Code,DataLoc,C.NO,C.Descrip,C.Codes)]),"clipboard-256000",sep="\t",row.names = F)
write.table(unique(Data[!grepl(NO.Pracs,T.Codes) & T.NO>0 & Version==2018,list(Code,DataLoc,T.NO,T.Descrip,T.Codes)]),"clipboard-256000",sep="\t",row.names = F)

# Remove rows with inconsistencies (Temporary solution until QC on potential errors is complete)
Remove.Indices<-c(Data[!grepl(NI.Pracs,C.Codes) & C.NI>0,Index],
                  Data[!grepl(NI.Pracs,T.Codes) & T.NI>0,Index],
                  Data[!grepl(NO.Pracs,C.Codes) & C.NO>0,Index],
                  Data[!grepl(NO.Pracs,T.Codes) & T.NO>0,Index])

length(Remove.Indices)
Data<-Data[!Index %in% Remove.Indices]

# Update missing NI or NO codes for crop yield outcomes
Data[!grepl(NI.Pracs,C.Codes) & is.na(C.NI) & Outcome %in% c(101,102),C.NI:=0]
Data[!grepl(NI.Pracs,T.Codes) & is.na(T.NI) & Outcome %in% c(101,102),T.NI:=0]

Data[!grepl(NO.Pracs,C.Codes) & is.na(C.NO) & Outcome %in% c(101,102),C.NO:=0]
Data[!grepl(NO.Pracs,T.Codes) & is.na(T.NO) & Outcome %in% c(101,102),T.NO:=0]

Data[,C.Codes:=NULL][,T.Codes:=NULL]

# Currency Harmonization  ====
# Read in PPP, XRAT and CPI datasets ####
year_target<-2010

ppp_file<-paste0("./Currency Conversion Tables/PA.NUS.PPP.csv")
if(!file.exists(ppp_file)){
  ppp_data <- data.table(wbstats::wb_data("PA.NUS.PPP", country="countries_only"))
  fwrite(ppp_data,file=ppp_file)
}else{
  ppp_data<-fread(ppp_file)
}

xrat_file<-paste0("./Currency Conversion Tables/PA.NUS.FCRF.csv")
if(!file.exists(xrat_file)){
  exchange_rates <- data.table(wbstats::wb_data("PA.NUS.FCRF",country="countries_only"))
  fwrite(exchange_rates,file=xrat_file)
}else{
  exchange_rates<-fread(xrat_file)
}

cpi_file<-paste0("./Currency Conversion Tables/FP.CPI.TOTL.csv")
if(!file.exists(cpi_file)){
  cpi_data <- data.table(wbstats::wb_data("FP.CPI.TOTL", country="countries_only"))
  fwrite(cpi_data,file=cpi_file)
}else{
  cpi_data<-fread(cpi_file)
}

# List Indices for Currency Harmonization
PPP.N<-Data[grepl(paste(exchange_rates[,unique(`iso3c`)],collapse = "|"),ISO.3166.1.alpha.3) &
              Outcome %in% OutcomeCodes[Subpillar=="Economics"|Code %in% c(242,243,133),Code] &
              !is.na(M.Year.Start) & 
              !grepl("[.]",ISO.3166.1.alpha.3) &
              !Units %in% c("%",""),Index]

Data[Index %in% PPP.N,unique(Units)]

# Subset data
PPP.Data<-Data[Index %in% PPP.N,list(ISO.3166.1.alpha.3,M.Year.Start,M.Year.End,MeanC,MeanT,Units)]
PPP.Data[,M.Year:=round((M.Year.Start+M.Year.End)/2,0)]

# Add iso 3 currency codes
PPP.Data[,currency_iso3:=countrycode::countrycode(ISO.3166.1.alpha.3, origin = 'iso3c', destination = 'iso4217c')
         ][,curr_match:=grepl(currency_iso3[1],Units[1]),by=list(currency_iso3,Units)
           ][curr_match==F,currency_iso3:=NA
             ][grep("USD",Units),currency_iso3:="USD"
               ][,curr_match:=NULL]

# Add historical exchange rate
PPP.Data<-merge(x=PPP.Data,y=exchange_rates[,list(iso3c,date,PA.NUS.FCRF)],
                 by.x=c("ISO.3166.1.alpha.3","M.Year"),by.y=c("iso3c","date"),all.x=T)
setnames(PPP.Data,"PA.NUS.FCRF","xrat_obs")

PPP.Data[is.na(currency_iso3),xrat_obs:=NA]

# Set non-USD currency to exchange rate of 1
PPP.Data[currency_iso3!="USD",xrat_obs:=1]

# Calculate USD equivalent
PPP.Data[,MeanT_USD:=MeanT][currency_iso3!="USD",MeanT_USD:=MeanT*xrat_obs]
PPP.Data[,MeanC_USD:=MeanC][currency_iso3!="USD",MeanC_USD:=MeanC*xrat_obs]

# Calculate USD in local currency
PPP.Data[,MeanT_local:=MeanT][currency_iso3=="USD",MeanT_local:=MeanT*xrat_obs]
PPP.Data[,MeanC_local:=MeanC][currency_iso3=="USD",MeanC_local:=MeanC*xrat_obs]
PPP.Data[is.na(currency_iso3),MeanC_local:=NA][is.na(currency_iso3),MeanC_local:=NA]

# Add future exchange rate
PPP.Data[,xrat_target:=exchange_rates[date==year_target,list(iso3c,PA.NUS.FCRF)][match(PPP.Data$ISO.3166.1.alpha.3,iso3c),PA.NUS.FCRF]]
PPP.Data[is.na(currency_iso3),xrat_target:=NA]

# Add historical CPI
PPP.Data<-merge(x=PPP.Data,y=cpi_data[,list(iso3c,date,FP.CPI.TOTL)],
                 by.x=c("ISO.3166.1.alpha.3","M.Year"),by.y=c("iso3c","date"),all.x = T)
setnames(PPP.Data,"FP.CPI.TOTL","cpi_obs")
PPP.Data[is.na(currency_iso3),cpi_obs:=NA]

# Add future CPI
PPP.Data[,cpi_target:=cpi_data[date==year_target,list(iso3c,FP.CPI.TOTL)][match(PPP.Data$ISO.3166.1.alpha.3,iso3c),FP.CPI.TOTL]]
PPP.Data[is.na(currency_iso3),cpi_target:=NA]

# Add future PPP
PPP.Data[,ppp_target:=ppp_data[date==year_target,list(iso3c,PA.NUS.PPP)][match(PPP.Data$ISO.3166.1.alpha.3,iso3c),PA.NUS.PPP]]
PPP.Data[is.na(currency_iso3),ppp_target:=NA]

# Calculate inflation adjusted values for future period
PPP.Data[,MeanT_local_target:=(MeanT_local/cpi_obs)*cpi_target]
PPP.Data[,MeanC_local_target:=(MeanC_local/cpi_obs)*cpi_target]

# Calculate int $ equivalent
PPP.Data[,MeanT_target_ppp_intusd:=MeanT_local_target/ppp_target]
PPP.Data[,MeanC_target_ppp_intusd:=MeanC_local_target/ppp_target]

# Calculate USD equivalent future
PPP.Data[,MeanT_target_usd:=MeanT_local_target/xrat_target]
PPP.Data[,MeanC_target_usd:=MeanC_local_target/xrat_target]

# Add transformed values back to dataset
Data[,USD2010.C:=NA]
Data[,USD2010.T:=NA]

Data[Index %in% PPP.N,USD2010.C:=PPP.Data[,MeanC_target_usd]]
Data[Index %in% PPP.N,USD2010.T:=PPP.Data[,MeanT_target_usd]]

# Harmonize data location names ####
if(T){
  Data[,DataLoc:=gsub("fig","Fig",DataLoc)]
  Data[,DataLoc:=gsub("tab","Tab",DataLoc)]
  Data[,DataLoc:=gsub("Table","Tab",DataLoc)]
  Data[,DataLoc:=gsub("Figure","Fig",DataLoc)]
  Data[,DataLoc:=gsub("txt","Text",DataLoc)]
  Data[,DataLoc:=gsub(" ","",DataLoc)]
  Data[,DataLoc:=gsub("Tab","Tab ",DataLoc)]
  Data[,DataLoc:=gsub("A","a",DataLoc)]
  Data[,DataLoc:=gsub("B","b",DataLoc)]
  Data[,DataLoc:=gsub("C","c",DataLoc)]
  Data[,DataLoc:=gsub("D","d",DataLoc)]
  Data[,DataLoc:=gsub("E","e",DataLoc)]
  Data[,DataLoc:=gsub("G","g",DataLoc)]
  Data[,DataLoc:=gsub("H","h",DataLoc)]
  Data[,DataLoc:=gsub("[[]","",DataLoc)]
  Data[,DataLoc:=gsub("[]]","",DataLoc)]
  Data[,DataLoc:=gsub("fib","Fig",DataLoc)]
  Data[,DataLoc:=gsub("fi1","Fig 1",DataLoc)]
  Data[,DataLoc:=gsub("V","v",DataLoc)]
  Data[,DataLoc:=gsub("I","i",DataLoc)]
  Data[,DataLoc:=gsub("X","x",DataLoc)]
  Data[,DataLoc:=gsub(",",".",DataLoc)]
  Data[,DataLoc:=gsub("&",".",DataLoc)]
  Data[,DataLoc:=gsub("Fig","Fig ",DataLoc)]
  Data[DataLoc==0,DataLoc:=NA]
  Data[,DataLoc:=gsub("page3"," Page 3",DataLoc)]
  
  # Data[,unique(DataLoc)]
  
}

# Remove duplicates ####

# List partial duplicates
Partial.Dup<-Duplicates[CompleteDuplicate=="No",paste(B.Code2,DataLoc2)]
Partial.Dup2<-Duplicates[CompleteDuplicate=="No",list(B.Code2,DataLoc2)]
# Any non-matches?
NonMatch<-Partial.Dup2[!Partial.Dup %in% Data[,paste(Code,DataLoc)]]

# List full paper duplicates
Full.Dup<-Duplicates[CompleteDuplicate=="Yes",B.Code2]
# Any non-matches?
Full.Dup[!Full.Dup %in% Data[,Code]]

# Remove complete partial duplicate data
unique(Data[Code %in% NonMatch[,B.Code2],list(Code,DataLoc)])
Data<-Data[!paste(Code,DataLoc) %in% Partial.Dup]

# Remove complete paper duplicates
Data<-Data[!Code %in% Full.Dup]

# Tidy up
rm(Full.Dup,Partial.Dup,Partial.Dup2,NonMatch)


# Simplify complex rotation and intercropping codes ####
Cols<-c(paste0(rep(c("C","T"),each=N.Cols),1:N.Cols))

# Complex Rotation - b44 - All are legume/non-legume sequences ####
FunX<-function(X){gsub("b44","b43",X)}
Z<-Data[,lapply(.SD,FunX),.SDcols=Cols]

# Partial Intercrop - Rotation code is already dealt with so we just need to change intercropping code
FunX<-function(X){gsub("b55.1","b25",X)}
Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
FunX<-function(X){gsub("b55.2","b50.1",X)}
Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
FunX<-function(X){gsub("b55.3","b50.2",X)}
Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
# Intercrop Rotation & Complex Intercrop Rotation -  Not used

# Update h-codes ####
# Simplify rotation and intercropping controls ####
FunX<-function(X){gsub("h38","h2",X)}
Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]

Dpracs<-c("Crop Rotation","Intercropping","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
PC<-PracticeCodes[Practice %in% Dpracs,Code]

# *** SLOW MAKE PARALLEL*** h2 & h23 codes ####
# Could be made faster by giving all unique rows an ID and using data.table by? 
FunX<-function(X){
  X<-unlist(X)
  # Add missing control "h2" code if rotation/intercropping/alleycropping present in treatment and absent in control
  if(any(X[(N.Cols+1):length(X)] %in% PC[!grepl("h",PC)]) & !"h2" %in% X[1:N.Cols] & !any(X[1:N.Cols] %in% PC[!grepl("h",PC)])){
    X[which(X[1:N.Cols]=="")[1]]<-"h2"
  }
  # Add h23 deficit irrigation code where missing from control (mostly 2020 data)
  if(any(X[(N.Cols+1):length(X)]=="b54") & all(X[1:N.Cols]!="b54") & !any(X[1:N.Cols]=="h23")){
    X[which(X[1:N.Cols]=="")[1]]<-"h23"
  }
  
  return(data.table(t(X)))
}

# Enable below to debug issues.
if(F){
  lapply(1:nrow(Z),FUN=function(i){
    X<-Z[i]
    print(i)
    FunX(X)
    
  })
}

Z<-rbindlist(pbapply(Z,1,FunX))

# Simplify intercrop rotation control - h18 ####
FunX<-function(X){any(grepl("h18|h50",X[1:N.Cols]))}

Z1<-apply(Z,1,FunX)
write.table(unique(Data[Z1,list(Code,DataLoc,T.Descrip,C.Descrip)]),"clipboard-256000",row.names = F,sep = "\t")
rm(Z1)

# Remove h1, h10 & h10.2 codes ####
# h1 - Non-CSA codes
# h10 - Conventional input (fertilizer) control (vs recommended rate)
# h10.2 - Addition of recommended amount of fertilizer following soil tests

FunX<-function(X,Cols){
  X[X %in% c("h1","h10","h10.2")]<-""
  X
}
Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]

# h7 deficit irrigation code ####
# Here we are checking the control is present where it should be
FunX<-function(X){
  any(X[(N.Cols+1):length(X)]=="b37") & all(X[1:N.Cols]!="b37") & !any(X[1:N.Cols]=="h7")
}

Z1<-apply(Z,1,FunX)
Issue.With.2020.Deficit.Irrig<-unique(Data[Z1 & Code!="EO0134",list(Code,DataLoc,T.Descrip,C.Descrip,Version)])
if(nrow(Issue.With.2020.Deficit.Irrig)>0){
  View(Issue.With.2020.Deficit.Irrig)
}
rm(Z1,Issue.With.2020.Deficit.Irrig)


# remove any duplicate h codes

CCols<-paste0("C",1:13)
TCols<-paste0("T",1:13)

CC<-pbapply(Z[,..CCols],1,FUN=function(X){
  X<-unique(X[X!=""])
  if(length(X)<length(CCols)){
    X<-c(X,rep("",length(CCols)-length(X)))
  }
  X
})

TT<-pbapply(Z[,..TCols],1,FUN=function(X){
  X<-unique(X[X!=""])
  if(length(X)<length(TCols)){
    X<-c(X,rep("",length(TCols)-length(X)))
  }
  X
})

Z<-data.table(t(CC),t(TT))
colnames(Z)<-Cols

# Combine datasets back together ####
Data<-cbind(Data[,!(..Cols)],Z)

rm(Z,FunX,Cols,Dpracs,PC,CC,TT)

# Carbon change outcomes - change % to proportion ####
Data[Outcome == 224.1 & Units=="%",MeanC:=(100+MeanC)/100]
Data[Outcome == 224.1 & Units=="%",MeanT:=(100+MeanT)/100]
Data[Outcome == 224.1 & Units=="%",Units:="Proportion"]

# Remove product = n1 (Soil)
Data[,EU:=gsub("-n1","",EU)]
Data[,EU:=gsub("n1-","",EU)]
Data[,EU:=gsub("n1","",EU)]

# Check for Practice in control not in treatment ####

CCols<-paste0("C",1:13)
TCols<-paste0("T",1:13)

#DataX<-Data[!Analysis.Function %in% c("DietSub","Sys.Rot.vs.Mono","Ratios") & is.na(Invalid.Comparison)] # unhash if Invalid.Comparison field is present
DataX<-Data[!Analysis.Function %in% c("DietSub","Sys.Rot.vs.Mono","Ratios")]

X<-cbind(DataX[,..CCols],DataX[,..TCols])

NotInT<-pbapply(X,1,FUN=function(Y){
  N<-!Y[1:13] %in% Y[14:26]
  Y<-Y[1:13][N]
  Y<-Y[Y!="" & !grepl("h",Y)]
  if(length(Y)>0){paste(Y,collapse="-")}else{NA}
})

ERA.in.C.not.in.T<-unique(DataX[!is.na(NotInT),list(Code,Version,Analysis.Function)])

Cols<-c("Index","Code","Version","Analysis.Function","C.Descrip","T.Descrip","Outcome","M.Year",CCols,TCols)
ERA.in.C.not.in.T2<-DataX[!is.na(NotInT),..Cols][,NotInT:=NotInT[!is.na(NotInT)]]

if(nrow(ERA.in.C.not.in.T)>1){
  View(ERA.in.C.not.in.T)
  write.table(ERA.in.C.not.in.T,"clipboard",row.names = F,sep = "\t")
  write.table(ERA.in.C.not.in.T2,"clipboard-256000",row.names = F,sep = "\t")
}

Data<-Data[!Index %in% ERA.in.C.not.in.T2[,Index]]


# ***Compile ERA*** ####
#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><>
system.time(
  DataZ<-CompileCompendiumWide(Data=Data,
                               OutcomeCodes=OutcomeCodes,
                               EUCodes=EUCodes,
                               PracticeCodes=PracticeCodes,
                               Folder=FilenameCombined,
                               SaveName="ERA Wide",
                               cores=cores,
                               SaveCsv=F,
                               SaveR=F,
                               VERBOSE=F,  
                               AddText=T,
                               delim="-",
                               AddBuffer=T,
                               Keep.No.Spatial=T,
                               Keep.hcode=F,
                               N.Cols=N.Cols,
                               DoWide=F)
)


DataZ[Product.Simple=="NA",Product.Simple:=NA]

# Compare 2020 vs 2018 ####
if(F){
  DataZ.1820<-DataZ[Code %in% unique(DataZ[Version==2018,Code][DataZ[Version==2018,Code] %in% DataZ[Version==2020,Code]])]
  DataZ.1820[,EUlist:=as.character(paste(EUlist,collapse = "-")),by=Index][,EUlist:=as.character(EUlist)]
  DataZ.1820[,plist:=as.character(paste(plist,collapse = "-")),by=Index]
  DataZ.1820[,base.list:=as.character(paste(base.list,collapse = "-")),by=Index]
  
  write.table(DataZ.1820,"clipboard-256000",row.names = F,sep="\t")
  
  X<-DataZ.1820[,list(Obs=.N),by=list(Code,Version,Out.SubInd)][,Version:=paste0("V",Version)]
  X<-dcast(X,Code+Out.SubInd~Version,value.var = "Obs")[is.na(V2020),V2020:=0]
  X[,Diff:=V2020-V2018]
  write.table(X,"clipboard",row.names = F,sep="\t")
}

#<><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><><> #

# Remove 2018 papers in 2020 version ####
ERA.Compiled<-DataZ[!(Version==2018 & Code %in% unique(DataZ[Version==2018,Code][DataZ[Version==2018,Code] %in% DataZ[Version==2020,Code]]))]

# Remove non-comparisons ###
ERA.Compiled[,N.Prac:=length(unlist(strsplit(plist,"-"))),by=plist]

# Validation: Check for where no ERA practices are present
No.Comparisons<-ERA.Compiled[N.Prac==0]
if(nrow(No.Comparisons)>0){
  View(unique(No.Comparisons[,list(Code,plist,Version,CID,TID,Out.SubInd,DataLoc)]))
  fwrite(No.Comparisons,paste0(FilenameCombined,"/Errors/Check Practice Codes - No diff btw C & T or only h code differs.csv"))
}

ERA.Compiled<-ERA.Compiled[!N.Prac==0][,N.Prac:=NULL]
rm(No.Comparisons)

# Add Environmental Data ####
# Note that environmental data scripts must have been run on the compiled ERA dataset prior to the below
# Load environmental data
Env.Data<-data.table::fread("Other Linked Datasets/Other Linked Datasets 2022-04-04.csv")
CHIRPS<-data.table(load.Rdata2("CHIRPS.LT.RData",path=paste0(ClimateDir,"CHIRPS/")))
POWER<-data.table(load.Rdata2("POWER.LT.RData",path=paste0(ClimateDir,"POWER/")))
Soils<-data.table::fread("Soils/SoilGrids Data.csv")

# Subset Data to Site.Keys
Climate<-unique(ERA.Compiled[!is.na(Site.Key),"Site.Key"])

# Add climate data ####
# Add AEZ
Climate[,AEZCode:=Env.Data[match(Climate[,Site.Key],Site.Key),'SSA-aez09.Mode']]
Climate[,AEZ16simple:=AEZ.Mappings[match(Climate[,AEZCode],AEZ16.Value),AEZ.TempHumid.Name]]
Climate[,AEZ16:=AEZ.Mappings[match(Climate[,AEZCode],AEZ16.Value),AEZ16.Name]]
Climate[,AEZCode:=Env.Data[match(Climate[,Site.Key],Site.Key),"AEZ5_CLAS--SSA.Mode"]]
Climate[,AEZ5:=AEZ.Mappings[match(Climate[,AEZCode],AEZ5.Value),AEZ5.Name]]
Climate[,AEZCode:=NULL]

# Add CHIRPS
Climate[,Mean.Annual.Precip:=CHIRPS[match(Climate[,Site.Key],Site.Key),Total.Rain]]

# Add POWER
Climate[,Mean.Annual.Temp:=POWER[match(Climate[,Site.Key],Site.Key),Temp.Mean.Mean]]

# Save climate file ####
fwrite(Climate,file=paste0(ClimateDir,"AEZ_MAP_MAT.csv"))

# Add soil data  ####
Cols<-c("Site.Key",grep("Mean",colnames(Soils),invert = F,value = T))
Soils<-Soils[,..Cols]

VARS<-c("SND","SLT","CLY")

# Calulate weighted mean soil parameter values across depths sl1 (0-5cm),sl2 (5-15cm) & sl3 (15-30cm)
Soils<-rbindlist(lapply(VARS,FUN=function(VAR){
  COLS<-c("Site.Key",colnames(Soils)[grepl(VAR,colnames(Soils))])
  X<-data.table(melt(Soils[,..COLS],"Site.Key"))
  X[,Variable:=VAR]
  X[grepl("sl1",variable),Weight:=5/30]
  X[grepl("sl2",variable),Weight:=10/30]
  X[grepl("sl3",variable),Weight:=15/30]
  X
}))

Soils<-Soils[,weighted.mean(value,Weight),by=c("Variable","Site.Key")]
Soils<-data.table(dcast(Soils,Site.Key~Variable))

rm(Cols,VARS)

# Check for absence of practice comparison ####
ERA.Compiled[is.na(plist)|plist==""|length(plist)==0]

# Check outcomes with NA MeanC values ####
ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency" & Out.SubInd=="Crop Yield" & !grepl("LER",Units),list(Code,Index,DataLoc,MeanC,MeanT,Out.SubInd)]
ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency" & Out.SubInd=="Soil Organic Carbon" & !grepl("LER",Units),list(Code,Index,DataLoc,MeanC,MeanT,Out.SubInd)]

ERA.Compiled<-data.table(ERA.Compiled,Climate[match(ERA.Compiled[,Site.Key],Site.Key),!"Site.Key"],Soils[match(ERA.Compiled[,Site.Key],Site.Key),!"Site.Key"])

ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency",list(C.Descrip,T.Descrip,MeanT,MeanC,C1,C2,C3,T1,T2,T3,Out.SubInd)]
ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency" & !Out.SubInd %in% c("Crop Yield","Soil Organic Carbon"),unique(Out.SubInd)]

# Attempt to standardize text encoding

TextStand<-function(Data){
  Xenc<-uchardet::detect_str_enc(Data) 
  for(FROM in unique(Xenc[!is.na(Xenc) & Xenc!="UTF-8"])){
    N<-which(Xenc==FROM)
    Data[N]<-iconv(Data[N],from=FROM,to="UTF-8")
  }
  return(Data)
}

ERA.Compiled[,C.Descrip:=TextStand(C.Descrip)]
ERA.Compiled[,T.Descrip:=TextStand(T.Descrip)]

# Split off Economic outcomes with no comparison ####

ERA.Compiled.Econ<-ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency" & !Out.SubInd %in% c("Crop Yield","Soil Organic Carbon"),]

ERA.Compiled<-ERA.Compiled[!Index %in% ERA.Compiled.Econ[,Index]]

unique(ERA.Compiled.Econ[,list(Code,C1,C2,C3,Version)])

# Save Data ####
SaveDir<-paste0(getwd(),"/",FilenameCombined,"/")
if(!dir.exists(SaveDir)){
  dir.create(SaveDir)
}


save(ERA.Compiled,file=paste0(SaveDir,"ERA Wide.RData"))
save(ERA.Compiled.Econ,file=paste0(SaveDir,"ERA Wide Econ Only.RData"))


ERA.Compiled[,Buffer:=as.numeric(Buffer)]

View(unique(ERA.Compiled[is.na(AEZ16simple) & Buffer<50000 & !is.na(Latitude),list(Code,Country,Latitude,Longitude,Buffer,Site.Key)]))

# Tidy up
rm(Soils,Climate,POWER,CHIRPS,Env.Data)

`
