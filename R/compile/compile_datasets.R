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
               gsubfn,
               progressr,
               pbapply, 
               stringr,
               ggplot2,
               miceadds,
               openxlsx,
               reshape2,
               rworldmap,
               tidyr,
               arrow,
               viridis)

  # 0.2) Create Functions #####

compile_era<-function(Data,
                      PracticeCodes,
                      OutcomeCodes,
                      EUCodes,
                      cores,       # Number of cores to use for parallel processing
                      VERBOSE=F,
                      AddText=T,   # Add descriptive outcome and practice names as fields? (T/F) 
                      delim="-",   # Specify delimiter using in outcome and practice names (default = "-")
                      Keep.hcode=F, # Consider non-CSA h-codes in practice naming? (T/F)
                      AddBuffer=T, # Harmonize latitude and longitude and add spatial uncertainty? (T/F), Note if Buffer.Manual field is not blank these values will be used.
                      Keep.No.Spatial=T # Keep observations with no latitude or longitude (T/F)
                      ){
  
  if(colnames(EUCodes)[1]!="EU"){colnames(EUCodes)[1]<-"EU"}
  Data<-as.data.table(Data)
  
  N.Cols<-sum(paste0("T",1:30) %in% colnames(Data))
  # Error Checking ####
  STEP<-1
  print(paste0("Step ",STEP,": Running validation checks"))
  STEP<-STEP+1
  
  errors<-list()

  # Check T/C codes match PracticeCodes$Codes
  error_dat<-rbindlist(lapply(c("T","C"),FUN=function(X){
    Y<-rbindlist(lapply(1:N.Cols,FUN=function(j){
      column_name<-paste0(X,j)
      T.Code<-unlist(Data[,..column_name])
      N<-which((!T.Code %in% PracticeCodes$Code) & !is.na(T.Code) & !T.Code=="")
      T.Code<-T.Code[N]
      data.table(field=column_name,Index=N,non_match=T.Code)
    }))
    Y
  }))
  
  error_dat<-merge(error_dat,Data[,.(Index,Code,T.Descrip,Version)],by="Index",all.x=T)[,.(value=paste(unique(paste0(T.Descrip,"-",field,"-",non_match)),collapse="/"),dataset=Version[1]),by=Code
                                                                                        ][,field:="T.Descrip-code field-non matching code"
                                                                                          ][,issue:="A practice code does not match the era_master_codes."]
  errors$practice_code_non_match<-error_dat

  # Check EU codes against EUCodes$Codes
  Data[, EU_split := lapply(as.character(EU), SplitProd)]
  Data[,EU_error:=unlist(lapply(EU_split,function(x){paste(x[!x %in% EUCodes$EU],collapse="/")}))]
  
  error_dat<-Data[EU_error!="",.(value=paste0(unique(EU_error),collapse="/"),dataset=Version[1]),by=Code
                ][,field:="EU"
                  ][,issue:="An EU code does not match the era_master_codes."]
  
  errors$prod_code_non_match<-error_dat
  
  Data[,c("EU_split","EU_error"):=NULL]

  # Check outcome codes against OutcomeCodes$Codes
  error_dat<-Data[!Outcome %in% OutcomeCodes$Code,.(value=paste(unique(Outcome),collapse="/"),dataset=Version[1]),by=Code
  ][,field:="Outcome"
  ][,issue:="An outcome code does not match the era_master_codes."]
  
  errors$outcome_code_non_match<-error_dat
  
  # Check no codes are NA
  NACols<-c("Code","Author","Date","Journal","Country","EU","T1","C1","T.Descrip","C.Descrip","CID","TID","Site.ID")
  Z<-apply(Data[,..NACols],1,FUN=function(X){any(is.na(X)|X==" ")})
  error_dat<-Data[Z,.(value=paste(unique(paste(T.Descrip,"-",Analysis.Function)),collapse="/"),dataset=Version[1]),by=Code
                  ][,field:="T.Descrip-Analysis.Function"
                    ][,issue:=paste0("Missing critical data in one of these fields: ",paste(NACols,collapse=", "))]
  
  errors$crit_field_missing<-error_dat
  
  #Create a unique numeric id for each study
  Data[,ID:=as.numeric(factor(Code))]
  
  # Compile Data ####
  #Calculate number of observations from each study and outcome per study, creates new column obs_count and out_count
  print(paste0("Step ",STEP,": Calculating number of observations from each study and outcome per study"))
  STEP<-STEP+1
  
  Data<-Data[,`:=`(obs_count=.N,out_count=length(unique(Outcome))),by=ID]
  
  # Deal with characters in numeric columns ####
  Cols<-c("LonD","LonM","LonS","LatD","LatM","LatS","MeanT","MeanC","Rep")
  error_dat<-rbindlist(lapply(Cols,FUN=function(i){
    
    A<-unlist(Data[,..i])
    N1<-is.na(A)
    N2<-is.na(as.numeric(A))
    N<-which(N1!=N2)
    
    if(length(N)>0){
      data.table(Index=N,Field=i)
    }else{
      NULL
    }
    
  }))
  
  error_dat<-rbindlist(lapply(error_dat[,unique(Cols)],function(COL){
    subset_Cols<-c("Code","Version","T.Descrip",COL)
    result<-unique(Data[Index %in% error_dat[Field==COL,unique(Index)],..subset_Cols][,field:=COL])
    setnames(result,COL,"value")
    result
  }))
  
  error_dat<-error_dat[,.(value=paste(field,paste0(unique(paste0(T.Descrip," val=",value)),collapse="|")),dataset=Version[1]),by=.(Code,field)]
  error_dat[,field:=NULL
            ][,field:="Column T.Descrip Value"
              ][,issue:="Non-numeric value in numeric field?"]
  
  errors$non_numeric_vals<-error_dat
  
  Data[, (Cols) := lapply(.SD, function(x) as.numeric(as.character(x))), .SDcols = Cols]

  # Convert to DMS locations to decimal degrees ####
  Data[,Lat:=LatD + ((LatS/60)+LatM)/60]
  Data[,Lon:=LonD + ((LonS/60)+LonM)/60]
  Data[LatH=='S',Lat:=Lat*-1]
  Data[LonH=='W',Lon:=Lon*-1]
  
  # Calculate effect size (% change) ####
  #if no reps are given, assume no replication (e.g. a replication of 1)
  Data[is.na(Rep),Rep:=1]
  Data[,yi:=suppressWarnings(log(MeanT/MeanC))] #yi is the response ratio
  Data[,pc:=100*(MeanT - MeanC)/MeanC]  #pc is the percent change
  
  # Deal with LER outcomes
  Data[Outcome==103 & is.na(MeanC),yi:=log(MeanT)]
  Data[Outcome==103 & is.na(MeanC),pc:=100*(MeanT-1)]
  
  # Extract treatments by comparing the treatment codes to the control codes ####
  
  # Fast function to that extract treatments by comparing the treatment codes to the control codes.
  # Function either outputs a list, using parallel processing and lapply, or a vector, using apply.
  # Cores are the number of cores to use in parallel processing, it's a good idea to leave at least one free (so total system cores -1)
  
  print(paste0("Step ",STEP,": Extracting treatments by comparing the treatment codes to the control codes"))
  STEP<-STEP+1
  
  Data<-ExtractTreatment(Data,cores,N.Cols,LIST=T)
  
  setnames(Data,"Outcome","Outcode")
  
  # Unlist lists
  print(paste0("Step ",STEP,": Unlisting Lists"))
  STEP<-STEP+1
  
  Data[,N:=1:.N
  ][,plist:=paste(unlist(plist),collapse="-"),by=N
  ][,N:=NULL]
  
  Data$plist<-unlist(Data$plist)
  
  # Add products ####
  print(paste0("Step ",STEP,": Adding descriptive names for products"))
  STEP<-STEP+1
  
  Data[,EUlist:=paste(sort(unlist(SplitProd(EU))),collapse="-"),by=EU]
  
  # Add Text ####
  
  print(paste0("Step ",STEP,": Adding descriptive names for practice and outcomes"))
  STEP<-STEP+1
  
  if(AddText){
    Data<-Code.2.Text(Data=Data,OutcomeCodes,PracticeCodes,EUCodes,Keep.hcode,delim=delim)
  }
  
  # Add Buffer ####
  print(paste0("Step ",STEP,": Harmonizing spatial data and estimating uncertainty buffers"))
  STEP<-STEP+1
  
  if(AddBuffer){
    Data<-BufferCalc(Data=Data,SaveError=F,Folder,Keep.No.Spatial=T)
  }
  
  # Use Lat, Long and Buffer to create a folder name and a unique spatial ID ####
  Data[,Site.Key:=paste0(sprintf("%07.4f",Latitude)," ",sprintf("%07.4f",Longitude)," B",Buffer),by=list(Latitude,Longitude,Buffer)]
  
  # Harmonize lat/long of any sites with duplicate Site.Keys (differ at 5dp but not 4)
  X<-table(unique(Data[,list(Site.Key,Latitude,Longitude,Buffer)])[,Site.Key])
  X<-names(X[X>1])
  
  unique(Data[Site.Key %in% X,list(Site.Key,Latitude,Longitude,Buffer)])
  
  Data[,Latitude:=round(mean(unique(Latitude)),5),by=Site.Key]
  Data[,Longitude:=round(mean(unique(Longitude)),5),by=Site.Key]
  
  return(list(Data=Data,Errors=errors))
}

# Calculate buffer distances and process spatial coordinates in a dataset
BufferCalc <- function(Data, SaveError = TRUE, Folder, Keep.No.Spatial = TRUE) {
  options(scipen = 999)  # Disable scientific notation for numbers
  
  # Convert Data to a data frame if it's not already
  Data <- as.data.frame(Data)
  
  # Identify and separate observations with missing spatial information
  MissingXY <- Data[is.na(Data$LatD) | is.na(Data$LonD), ]
  
  # Exclude observations with missing spatial data from the main Data
  Data <- Data[!(is.na(Data$LatD) | is.na(Data$LonD)), ]
  
  # Handle cases where decimal degrees are mixed with minutes or seconds
  X <- which(
    (grepl("[.]", as.character(Data$LatD)) & (!is.na(Data$LatM) | !is.na(Data$LatS))) |
      (grepl("[.]", as.character(Data$LonD)) & (!is.na(Data$LonM) | !is.na(Data$LonS)))
  )
  if (length(X) > 0) {
    # Remove minutes and seconds if decimal degrees are present
    Data[X, c("LatM", "LatS", "LonM", "LonS")] <- NA
  }
  
  # Initialize Latitude and Longitude columns for analysis
  Data$Latitude <- Data$LatD
  Data$Longitude <- Data$LonD
  
  # Initialize Buffer columns
  Data$Buffer <- as.numeric("")
  Data$BufferLa <- as.numeric("")
  Data$BufferLo <- as.numeric("")
  
  # Calculate accuracy (buffer) from decimal degree spatial locations
  # Find indices of observations with decimal degrees in latitude
  DD <- grep("[.]", as.character(Data$LatD))
  # Get number of decimal places after the decimal point
  DecimalPlaces <- nchar(unlist(strsplit(as.character(Data$LatD[DD]), "[.]"))[seq(2, length(unlist(strsplit(as.character(Data$LatD[DD]), "[.]"))), 2)])
  # Assign buffer distances based on number of decimal places
  DecimalPlaces[DecimalPlaces == 1] <- 11000 / 2  # Approximate accuracy in meters
  DecimalPlaces[DecimalPlaces == 2] <- 1100 / 2
  DecimalPlaces[DecimalPlaces >= 3 & DecimalPlaces <= 10] <- 500 / 2
  Data$BufferLa[DD] <- DecimalPlaces
  
  # Repeat the same process for longitude
  DD2 <- grep("[.]", as.character(Data$LonD))
  DecimalPlaces <- nchar(unlist(strsplit(as.character(Data$LonD[DD2]), "[.]"))[seq(2, length(unlist(strsplit(as.character(Data$LonD[DD2]), "[.]"))), 2)])
  DecimalPlaces[DecimalPlaces == 1] <- 11000 / 2
  DecimalPlaces[DecimalPlaces == 2] <- 1100 / 2
  DecimalPlaces[DecimalPlaces >= 3 & DecimalPlaces <= 10] <- 500 / 2
  Data$BufferLo[DD2] <- DecimalPlaces
  
  # Calculate accuracy from Degrees, Minutes, Seconds (DMS) spatial locations
  # Degrees Only: No decimal point, no minutes, no seconds
  Data$BufferLa[!grepl("[.]", as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS)] <- 110000 / 2
  Data$BufferLo[!grepl("[.]", as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS)] <- 110000 / 2
  
  # Minutes Only (no seconds)
  Minute <- round(0.5 * 110000 / 60)  # Approximate buffer for minute precision
  # Latitude with minutes but no seconds
  X <- which(!is.na(Data$LatM) & is.na(Data$LatS))
  if (length(X) > 0) {
    Data$BufferLa[X] <- Minute
    Data$Latitude[X] <- Data$LatD[X] + Data$LatM[X] / 60
  }
  # Longitude with minutes but no seconds
  X <- which(!is.na(Data$LonM) & is.na(Data$LonS))
  if (length(X) > 0) {
    Data$BufferLo[X] <- Minute
    Data$Longitude[X] <- Data$LonD[X] + Data$LonM[X] / 60  
  }
  
  # Minutes and Seconds
  Second <- 500 / 2  # Minimum buffer for second precision
  # Latitude with minutes and seconds
  X <- which(!is.na(Data$LatM) & !is.na(Data$LatS))
  if (length(X) > 0) {
    Data$BufferLa[X] <- Second
    Data$Latitude[X] <- Data$LatD[X] + Data$LatM[X] / 60 + Data$LatS[X] / 3600
  }
  # Longitude with minutes and seconds
  X <- which(!is.na(Data$LonM) & !is.na(Data$LonS))
  if (length(X) > 0) {
    Data$BufferLo[X] <- Second
    Data$Longitude[X] <- Data$LonD[X] + Data$LonM[X] / 60 + Data$LonS[X] / 3600
  }
  
  # Handle inconsistent formats where latitude and longitude are recorded differently
  DD3 <- which(!grepl("[.]", as.character(Data$LatD)) & is.na(Data$LatM) & is.na(Data$LatS))
  DD4 <- which(!grepl("[.]", as.character(Data$LonD)) & is.na(Data$LonM) & is.na(Data$LonS))
  
  X <- unique(c(DD[is.na(match(DD, DD2))], DD2[is.na(match(DD2, DD))], DD3[is.na(match(DD3, DD4))], DD4[is.na(match(DD4, DD3))]))
  # Note: The variable 'X' is identified but not utilized further in this context
  
  # Choose minimum buffer width between latitude and longitude buffers
  Data$Buffer <- apply(cbind(Data$BufferLa, Data$BufferLo), 1, min, na.rm = TRUE)
  
  # Adjust sign for southern and western hemispheres
  X <- which(Data$LatH == "S" & Data$Latitude > 0)
  if (length(X) > 0) {
    Data$Latitude[X] <- Data$Latitude[X] * -1
  }
  
  X <- which(Data$LonH == "W" & Data$Longitude > 0)
  if (length(X) > 0) {
    Data$Longitude[X] <- Data$Longitude[X] * -1
  }
  
  # Add accuracy where locations are averaged; 'Lat.Diff' should be in minutes
  Data$Lat.Diff <- as.numeric(Data$Lat.Diff) / 60  # Convert minutes to degrees
  Data$Buffer[!is.na(Data$Lat.Diff)] <- 1500 * Data$Lat.Diff[!is.na(Data$Lat.Diff)] / 2  # Calculate buffer
  
  # Round Latitude and Longitude to a sensible number of decimal places
  Data$Latitude <- round(Data$Latitude, 5)
  Data$Longitude <- round(Data$Longitude, 5)
  
  # Round buffer values to integers
  Data$Buffer <- as.integer(round(Data$Buffer))
  
  # Replace calculated buffer values with manual values if available
  if (!is.null(Data$Buffer.Manual)) {
    Data$Buffer[!is.na(Data$Buffer.Manual)] <- Data$Buffer.Manual[!is.na(Data$Buffer.Manual)]
    Data$Buffer.Manual <- NULL
    if (nrow(MissingXY) > 0) {
      MissingXY$Buffer.Manual <- NULL
    }
  }
  
  # Remove unnecessary fields from Data
  Data[, c("LatD", "LatM", "LatS", "LatH", "LonD", "LonM", "LonS", "LonH", "Lat.Diff", "BufferLa", "BufferLo")] <- NULL
  
  # Create a unique Site Key using Latitude, Longitude, and Buffer
  Data$Site.Key <- paste0(sprintf("%07.4f", Data$Latitude), " ", sprintf("%07.4f", Data$Longitude), " B", Data$Buffer)
  
  # Save error data and optionally add back in sites with missing spatial data
  if (nrow(MissingXY) > 0) {
    if (SaveError) {
      # Create the Errors directory if it doesn't exist
      if (!dir.exists(paste0(Folder, "/Errors"))) {
        dir.create(paste0(Folder, "/Errors"), recursive = TRUE)
      }
      # Save the missing spatial data to a CSV file
      fwrite(
        unique(MissingXY[, c("Code", "Site.ID", "Country", "LatD", "LatM", "LatS", "LatH", "LonD", "LonM", "LonS", "LonH", "Lat.Diff")]),
        file = paste0(Folder, "/Errors/", Folder, "-MissingXY.csv")
      )
    }
    
    if (Keep.No.Spatial) {
      # Remove unnecessary fields from MissingXY
      MissingXY[, c("LatD", "LatM", "LatS", "LatH", "LonD", "LonM", "LonS", "LonH", "Lat.Diff")] <- NULL
      # Set spatial fields to NA
      MissingXY[, c("Latitude", "Longitude", "Buffer", "Site.Key")] <- NA
      # Combine MissingXY back into Data
      Data <- rbind(Data, MissingXY)
    }
  }
  
  # Convert Data back to a data.table
  Data <- data.table(Data)
  
  # Return the processed Data
  return(Data)
}

# Helper function to map data codes to master name, used in Code.2.Text function
AddNames <- function(Data.Code, Master.Name, Master.Code, delim) {
  # Match Data.Code to Master.Code and retrieve corresponding Master.Name
  names <- Master.Name[match(Data.Code, Master.Code)]
  # Return unique, sorted names concatenated with the delimiter
  paste(sort(unique(names)), collapse = delim)
}

# Helper function to remove 'h' codes from code strings, used in Code.2.Text
RmHCodes <- function(Codes, Delim) {
  # Split the codes by delimiter
  X <- unlist(strsplit(Codes, Delim))
  # Remove codes that contain 'h'
  X <- X[!grepl("h", X)]
  # Reassemble the codes with the delimiter
  paste(sort(X), collapse = Delim)
}

# Map coded variables in a dataset to descriptive text labels for outcomes, practices, and products
Code.2.Text <- function(Data, OutcomeCodes, PracticeCodes, EUCodes, Keep.hcode, AddCodes = FALSE,delim) {
  
  # Process Outcome Codes
  Data[, Outcode := as.character(Outcode)]
  delim <- "-"
  
  # Map outcome codes to their respective Pillar, Subpillar, Indicator, and Subindicator names
  Data[, Out.Pillar := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                Master.Name = OutcomeCodes[, Pillar],
                                Master.Code = OutcomeCodes[, Code],
                                delim = delim),
       by = Outcode]
  
  Data[, Out.SubPillar := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                   Master.Name = OutcomeCodes[, Subpillar],
                                   Master.Code = OutcomeCodes[, Code],
                                   delim = delim),
       by = Outcode]
  
  Data[, Out.Ind := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                             Master.Name = OutcomeCodes[, Indicator],
                             Master.Code = OutcomeCodes[, Code],
                             delim = delim),
       by = Outcode]
  
  Data[, Out.SubInd := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                Master.Name = OutcomeCodes[, Subindicator],
                                Master.Code = OutcomeCodes[, Code],
                                delim = delim),
       by = Outcode]
  
  # If AddCodes is TRUE, map additional code information
  if (AddCodes) {
    Data[, Out.SubInd.S := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                    Master.Name = OutcomeCodes[, Subindicator.Short],
                                    Master.Code = OutcomeCodes[, Code],
                                    delim = delim),
         by = Outcode]
    
    delim <- "/"
    
    Data[, Out.Pillar.Code := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                       Master.Name = OutcomeCodes[, Pillar.Code],
                                       Master.Code = OutcomeCodes[, Code],
                                       delim = delim),
         by = Outcode]
    
    Data[, Out.SubPillar.Code := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                          Master.Name = OutcomeCodes[, Subpillar.Code],
                                          Master.Code = OutcomeCodes[, Code],
                                          delim = delim),
         by = Outcode]
    
    Data[, Out.Ind.Code := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                    Master.Name = OutcomeCodes[, Indicator.Code],
                                    Master.Code = OutcomeCodes[, Code],
                                    delim = delim),
         by = Outcode]
    
    Data[, Out.SubInd.Code := AddNames(Data.Code = unlist(strsplit(Outcode[1], "-")),
                                       Master.Name = OutcomeCodes[, Subindicator.Code],
                                       Master.Code = OutcomeCodes[, Code],
                                       delim = delim),
         by = Outcode]
  }
  
  # Process Practice Codes
  if (!Keep.hcode) {
    # Remove 'h' codes from plist and base.list
    Data[, plist := RmHCodes(plist[1], "-"), by = plist]
    Data[, base.list := RmHCodes(base.list[1], "-"), by = base.list]
  }
  
  delim <- "-"
  
  # Map practice codes to their respective Subpractice, Practice, and Theme names
  Data[, SubPrName := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                               Master.Name = PracticeCodes[, Subpractice.S],
                               Master.Code = PracticeCodes[, Code],
                               delim = delim),
       by = plist]
  
  Data[, PrName := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                            Master.Name = PracticeCodes[, Practice],
                            Master.Code = PracticeCodes[, Code],
                            delim = delim),
       by = plist]
  
  Data[, Theme := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                           Master.Name = PracticeCodes[, Theme],
                           Master.Code = PracticeCodes[, Code],
                           delim = delim),
       by = plist]
  
  # If AddCodes is TRUE, map additional practice code information
  if (AddCodes) {
    delim <- "/"
    
    Data[, SubPrName.Code := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                                      Master.Name = PracticeCodes[, Subpractice.Code],
                                      Master.Code = PracticeCodes[, Code],
                                      delim = delim),
         by = plist]
    
    Data[, PrName.Code := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                                   Master.Name = PracticeCodes[, Practice.Code],
                                   Master.Code = PracticeCodes[, Code],
                                   delim = delim),
         by = plist]
    
    Data[, Theme.Code := AddNames(Data.Code = unlist(strsplit(plist[1], "-")),
                                  Master.Name = PracticeCodes[, Theme.Code],
                                  Master.Code = PracticeCodes[, Code],
                                  delim = delim),
         by = plist]
  }
  
  # Process Base Practice Codes if available
  if (!is.null(Data$base.list)) {
    delim <- "-"
    
    Data[, SubPrName.Base := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                      Master.Name = PracticeCodes[, Subpractice.S],
                                      Master.Code = PracticeCodes[, Code],
                                      delim = delim),
         by = base.list]
    
    Data[, PrName.Base := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                   Master.Name = PracticeCodes[, Practice],
                                   Master.Code = PracticeCodes[, Code],
                                   delim = delim),
         by = base.list]
    
    Data[, Theme.Base := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                  Master.Name = PracticeCodes[, Theme],
                                  Master.Code = PracticeCodes[, Code],
                                  delim = delim),
         by = base.list]
    
    if (AddCodes) {
      delim <- "/"
      
      Data[, SubPrName.Base.Code := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                             Master.Name = PracticeCodes[, Subpractice.Code],
                                             Master.Code = PracticeCodes[, Code],
                                             delim = delim),
           by = base.list]
      
      Data[, PrName.Base.Code := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                          Master.Name = PracticeCodes[, Practice.Code],
                                          Master.Code = PracticeCodes[, Code],
                                          delim = delim),
           by = base.list]
      
      Data[, Theme.Base.Code := AddNames(Data.Code = unlist(strsplit(base.list[1], "-")),
                                         Master.Name = PracticeCodes[, Theme.Code],
                                         Master.Code = PracticeCodes[, Code],
                                         delim = delim),
           by = base.list]
    }
  }
  
  # Process Product Codes
  delim <- "-"
  
  Data[, Product := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                             Master.Name = EUCodes[, Product],
                             Master.Code = EUCodes[, EU],
                             delim = delim),
       by = EUlist]
  
  Data[, Product.Type := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                  Master.Name = EUCodes[, Product.Type],
                                  Master.Code = EUCodes[, EU],
                                  delim = delim),
       by = EUlist]
  
  Data[, Product.Subtype := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                     Master.Name = EUCodes[, Product.Subtype],
                                     Master.Code = EUCodes[, EU],
                                     delim = delim),
       by = EUlist]
  
  Data[, Product.Simple := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                    Master.Name = EUCodes[, Product.Simple],
                                    Master.Code = EUCodes[, EU],
                                    delim = delim),
       by = EUlist]
  
  if (AddCodes) {
    delim <- "/"
    
    Data[, Product.Type.Code := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                         Master.Name = EUCodes[, Product.Type.Code],
                                         Master.Code = EUCodes[, EU],
                                         delim = delim),
         by = EUlist]
    
    Data[, Product.Subtype.Code := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                            Master.Name = EUCodes[, Product.Subtype.Code],
                                            Master.Code = EUCodes[, EU],
                                            delim = delim),
         by = EUlist]
    
    Data[, Product.Simple.Code := AddNames(Data.Code = unlist(strsplit(EUlist[1], "-")),
                                           Master.Name = EUCodes[, Product.Simple.Code],
                                           Master.Code = EUCodes[, EU],
                                           delim = delim),
         by = EUlist]
  }
  
  # Return the modified Data
  return(Data)
}

#Split a product code into components, merging single-character elements with preceding elements
SplitProd <- function(X) {
  # Split the input string by periods into a character vector
  X <- unlist(strsplit(X, "[.]"))
  
  # Initialize the result vector Y with the first element of X
  Y <- X[1]
  
  # If there are more elements, process them
  if (length(X) > 1) {
    for (i in 2:length(X)) {
      if (nchar(X[i]) == 1) {
        # If the current element is a single character,
        # append it to the previous element in Y with a period
        Y[i - 1] <- paste0(Y[i - 1], ".", X[i])
      } else {
        # If the current element has more than one character,
        # add it as a new element in Y
        Y <- c(Y, X[i])
      }
    }
  }
  
  # Return the processed vector Y
  return(Y)
}

#Extracts unique treatment codes not present in control codes and identifies common codes between treatments and controls for each row in the dataset.
ExtractTreatment <- function(Data, cores, N.Cols, LIST = TRUE) {
  # Ensure Data is a data.table
  Data <- as.data.table(Data)
  
  # Set up the future plan for parallel processing
  if (cores > 1) {
    # Use multisession plan for parallel execution across multiple R sessions
    plan(multisession, workers = cores)
  } else {
    # Use sequential plan if only one core is specified
    plan(sequential)
  }
  
  if (LIST) {
    # If LIST is TRUE, we want the output in list format
    
    # Extract the treatment codes not present in control for each row
    Data[, plist := future_lapply(.I, function(i) {
      # Get control codes for row i
      cset <- unlist(Data[i, paste0("C", 1:N.Cols), with = FALSE])
      # Get treatment codes for row i
      tset <- unlist(Data[i, paste0("T", 1:N.Cols), with = FALSE])
      # Remove NA values and empty strings from cset and tset
      cset <- cset[!is.na(cset) & cset != ""]
      tset <- tset[!is.na(tset) & tset != ""]
      # Find codes present in treatment but not in control
      Y <- setdiff(tset, cset)
      # Exclude empty strings
      Y[Y != ""]
    }, future.seed = TRUE)]
    
    # Extract the common codes between control and treatment for each row
    Data[, base.list := unlist(future_lapply(.I, function(i) {
      # Get control codes for row i
      cset <- unlist(Data[i, paste0("C", 1:N.Cols), with = FALSE])
      # Get treatment codes for row i
      tset <- unlist(Data[i, paste0("T", 1:N.Cols), with = FALSE])
      # Remove NA values and empty strings
      cset <- cset[!is.na(cset) & cset != ""]
      tset <- tset[!is.na(tset) & tset != ""]
      # Find common codes between control and treatment
      Y <- intersect(tset, cset)
      Y <- Y[Y != ""]
      # Concatenate common codes with '-' separator
      paste(Y, collapse = "-")
    }, future.seed = TRUE))]
    
  } else {
    # If LIST is FALSE, we want the output in vector format
    
    # Extract the treatment codes not present in control for each row
    Data[, plist := future_sapply(.I, function(i) {
      # Get control codes for row i
      cset <- unlist(Data[i, paste0("C", 1:N.Cols), with = FALSE])
      # Get treatment codes for row i
      tset <- unlist(Data[i, paste0("T", 1:N.Cols), with = FALSE])
      # Remove NA values and empty strings
      cset <- cset[!is.na(cset) & cset != ""]
      tset <- tset[!is.na(tset) & tset != ""]
      # Find codes present in treatment but not in control
      Y <- setdiff(tset, cset)
      Y <- Y[Y != ""]
      # Concatenate unique treatment codes with ',' separator
      paste(Y, collapse = ",")
    }, future.seed = TRUE)]
    
    # Extract the common codes between control and treatment for each row
    Data[, base.list := future_sapply(.I, function(i) {
      # Get control codes for row i
      cset <- unlist(Data[i, paste0("C", 1:N.Cols), with = FALSE])
      # Get treatment codes for row i
      tset <- unlist(Data[i, paste0("T", 1:N.Cols), with = FALSE])
      # Remove NA values and empty strings
      cset <- cset[!is.na(cset) & cset != ""]
      tset <- tset[!is.na(tset) & tset != ""]
      # Find common codes between control and treatment
      Y <- intersect(tset, cset)
      Y <- Y[Y != ""]
      # Concatenate common codes with '-' separator
      paste(Y, collapse = "-")
    }, future.seed = TRUE)]
  }
  
  # Reset the future plan to sequential
  plan(sequential)
  
  # Return the updated Data table
  return(Data)
}

  # 0.3) Set Cores for Parallel #####
# Detect the number of cores in your system & set number for parallel processing 
cores<-max(1, parallel::detectCores() - 1)

  # 0.4) Set error dir ####

# 1) Load data ####
  # 1.1) Load era vocab #####
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)

  # 1.2) Read in concept and harmonization sheets ######
# Outcomes
OutcomeCodes<-master_codes$out
if(sum(table(OutcomeCodes[,Code])>1)){print("Duplicate codes present in outcomes")}

# Practice Codes
PracticeCodes<-master_codes$prac
if(sum(table(PracticeCodes[,Code])>1)){print("Duplicate codes present in practices")}

# Products
EUCodes<-master_codes$prod
if(sum(table(EUCodes[,EU])>1)){print("Duplicate codes present in products")}

# Trees
TreeCodes<-master_codes$trees
setnames(TreeCodes, "Tree.Latin.Name", "Latin.Name")
TreeCodes<-na.omit(TreeCodes, cols=c("EU"))

# Duplicate Papers (entered in both 2020 and 2018, or some other reason)
Duplicates<-master_codes$dups


# Unit Harmonization
UnitHarmonization<-master_codes$unit_harmonization
UnitHarmonization[Out.Unit.Correct==""|is.na(Out.Unit.Correct),Out.Unit.Correct:=Out.Unit]

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

  # 1.4) Read in and prepare ERA datasets ####
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
    
    # Set soil texture to lowercase
    era_2018[,Soil.Texture:=tolower(Soil.Texture)]
    
    # Set date to columns to correct class
    era_2018[,Harvest.Start:=as.Date(Harvest.Start,"%d.%m.%Y")
             ][,Harvest.End:=as.Date(Harvest.End,"%d.%m.%Y")
               ][,Plant.Start:=as.Date(Plant.Start,"%d.%m.%Y")
                 ][,Plant.End:=as.Date(Plant.End,"%d.%m.%Y")]
    
    # Check for duplicate row indices
    error_dat<-era_2018[,.(N=.N,Code=unique(Code)),by=Index
    ][N>1,.(value=paste(unique(Index),collapse="/")),by=Code
    ][,dataset:=era_projects$v1.0_2018
    ][,field:="Index"
    ][,issue:="Duplicate index value(s)."]
    
    if(nrow(error_dat)>0){
      errors$duplicate_indices_2018<-error_dat
      warning("Duplicate row indices in 2018 dataset.")
    }
      # 1.4.1.0) BETTER TO FIX THE RAW DATASET - Fix encoding issue with Site.ID ######
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
    future::plan(multisession, workers = cores)
    
    # Recode in parallel with progress reporting
    with_progress({
      # Define the progress bar
      progress <- progressr::progressor(along = indices)
      
      DataX<-rbindlist(future.apply::future_lapply(indices,function(i){
     # DataX<-rbindlist(pblapply(era_2018[Div==T,Index],FUN=function(i){
  
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
      # 1.4.1.4) Change aggregated site & country delimiter from . to .. #######
      era_2018[,Site.ID:=gsub("[.] ",".",Site.ID)]
      era_2018[,Site.ID:=gsub("site.1","Site 1",Site.ID)]
      era_2018[,Site.ID:=gsub("[.]","..",Site.ID)]
      era_2018[,Country:=gsub("[.]","..",Country)]
      # 1.4.1.5) Remove SRI b15 ####
      X<-grepl("b15",era_2018[,N:=1:.N][,list(TCols=paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10),collapse="-")),by=N][,TCols])
      
      SRI.Present<-unique(era_2018[X,list(Index,Code,T1,T2,T3,T4,T5,T6,T7,T8,T9,T10)])
      if(nrow(SRI.Present)>0){
        warning(paste0("SRI present in ",nrow(SRI.Present)," rows of era_2018. These codes will be removed."))
      }
      
      error_dat<-era_2018[X,.(value=paste(unique(T.Descrip),collapse="/")),by=Code
      ][,dataset:=era_projects$v1.0_2018
      ][,field:="T.Descrip"
      ][,issue:="SRI codes are present, can these be converted into component practices?."]
      
      # Remove SRI rows
      era_2018<-era_2018[!X]
      era_2018[,N:=NULL]
      
      # 1.4.1.6) Update outcomes ####
      # Check cost outcomes with costs per kg or similar
      if(F){
        unique(era_2018[Outcome %in% c(150,151,152,152.1) & grepl("/kg|/m3|/sack",Units),list(Code,Outcome,Units,DataLoc)])
      }
      
      # Check & update biomass yield outcomes 
      era_2018[Outcome==102,EUX:=EU
           ][Outcome==102,Product.Subtype:=EUCodes[match(unlist(strsplit(EUX,"[.]")),EU),paste(unique(Product.Subtype),collapse = ".")],by=EUX
             ][Outcome==102,Product.Simple:=EUCodes[match(unlist(strsplit(EUX,"[.]")),EU),paste(unique(Product.Simple),collapse = ".")],by=EUX]
      
      # Change outcomes that are only for fodders to biomass yield
      unique(era_2018[Outcome==102 & Product.Subtype=="Fodders",list(Product.Subtype,Product.Simple)])
      era_2018[Outcome==102 & Product.Subtype=="Fodders",Outcome:=101.1]
      
      era_2018[,c("Product.Subtype","Product.Simple","EUX"):=NULL]
      
    # 1.4.2) 2020 ######
    era_2020<-data.table(arrow::read_parquet(filename20))
    # How many C/T columns are in the 2020 dataset?
    N.Cols<-sum(paste0("C",1:30) %in% colnames(era_2020))
    
    # Add Index to 2020
    era_2020[,Index:=(era_2018[,max(Index)]+1):(era_2018[,max(Index)]+nrow(era_2020))]
    era_2020[,Version:=2020]
    
    # Check for NA values in T1 field, if values are present this indicates an issue with the excel extraction script. In the past issues have been caused by non-matches in concept or harmonization sheets due to character encoding issues.
    era_2020[,list(Len=sum(is.na(T1))),by=list(Code)][Len>0]
    
    # Convert excel date format to R date class
    # https://stackoverflow.com/questions/43230470/how-to-convert-excel-date-format-to-proper-date-in-r/62334132
    era_2020[,Plant.End:=as.Date(Plant.End, origin = "1899-12-30")
             ][,Plant.Start:=as.Date(Plant.Start, origin = "1899-12-30")
               ][,Harvest.Start:=as.Date(Harvest.Start, origin = "1899-12-30")
                 ][,Harvest.End:=as.Date(Harvest.End, origin = "1899-12-30")]
    
    # 1.4.3) skinny_cow_2022 ######
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

era_2022[,Plant.End:=as.Date(Plant.End, origin = "1899-12-30")
][,Plant.Start:=as.Date(Plant.Start, origin = "1899-12-30")
][,Harvest.Start:=as.Date(Harvest.Start, origin = "1899-12-30")
][,Harvest.End:=as.Date(Harvest.End, origin = "1899-12-30")]

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
  

  # 1.6) Remove papers in more recent extractions from old extractions #####
  codes_2020<-era_2020[,unique(Code)]
  codes_2022<-era_2022[,unique(Code)]
  era_2018<-era_2018[!Code %in% c(codes_2020,codes_2022)]
  era_2020<-era_2020[!Code %in% codes_2022 & !Analysis.Function %in% c("NoDietSub","DietSub")]
  # 1.7) Join Datasets ####
  era_merged<-rbindlist(list(era_2018,era_2020,era_2022),use.names = T)
  
  # Use Clean Columns (where various fields in 2018 have been tidied using scripts)
  era_merged[T.Descrip.Clean=="",T.Descrip.Clean:=T.Descrip]
  era_merged[C.Descrip.Clean=="",C.Descrip.Clean:=C.Descrip]
  era_merged[Variety.Clean=="",Variety.Clean:=Variety]
  era_merged[Tree.Clean=="",Variety.Clean:=Tree]
  era_merged[Diversity.Clean=="",Variety.Clean:=Diversity]
  
  era_merged[,c("T.Descrip.Clean","C.Descrip.Clean","Variety.Clean","Tree.Clean","Diversity.Clean"):=NULL]
  
  era_merged[is.na(C13),C13:=""]

  # 1.8) Add Irrigation Present Columns #####
  CCols<-paste0("C",1:13)
  TCols<-paste0("T",1:13)
  
  era_merged[,Irrigation.C:=as.logical(apply(era_merged[,..CCols],1,FUN=function(X){sum(grepl("b37|b54|b34|b72|b36|b53|h7",X))}))]
  era_merged[,Irrigation.T:=as.logical(apply(era_merged[,..TCols],1,FUN=function(X){sum(grepl("b37|b54|b34|b72|b36|b53|h7",X))}))]

  # 1.9) Remove t and s codes from EUCodes and move to Diversity/Tree fields respectively #####

# If biomass outcome with s/t code exclude observation
era_merged<-era_merged[!(grepl("s|t",EU) & Outcome %in% c(101,102,103))]

CoverCrops<-era_merged[(is.na(Diversity)|Diversity=="") & grepl("s",EU),list(Code,EU,Outcome)][,EU]

CoverCrops<-rbindlist(lapply(unique(CoverCrops),FUN=function(CC){
  X<-unlist(strsplit(CC,"[.]"))
  X<-grep("s",X,value = T)
  X<-X[!X %in% c("s1","s2")]
  if(length(X)>0){
    data.table(Code=CC,Div.Col=paste(EUCodes[match(X,EU),Product.Simple],collapse="-"))
  }
}))

for(i in 1:nrow(CoverCrops)){
  era_merged[grepl(CoverCrops[i,Code],EU) & !grepl(CoverCrops[i,Div.Col],Diversity),Diversity:=paste(Diversity,CoverCrops[i,Div.Col])]
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

era_merged[,EU:=RemoveEUCode(X=EU[1],Code="t|s"),by=EU]

error_dat<-era_merged[is.na(EU)|EU==""|nchar(EU)==0
                      ][,.(value=paste(unique(T.Descrip),collapse="/"),dataset=Version[1]),by=Code
                        ][,field:="T.Descrip"
                          ][,issue:="No product (EU) code present after removing t or s codes."]
errors$no_eu_after_ts_removal<-error_dat

era_merged<-era_merged[!(is.na(EU)|EU==""|nchar(EU)==0)]

  # 1.10) Restructure Irrigation Practices #####
pasteNA<-function(X){paste(X[!is.na(X)],collapse="-")}

era_merged[,C.Codes:=paste(c(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13),collapse = "-"),by=Index]

era_merged[grep("b34",C.Codes),Irrig.Meth.C1:=PracticeCodes[Code=="b34",Subpractice]
][grep("b36",C.Codes),Irrig.Meth.C2:=PracticeCodes[Code=="b36",Subpractice]
][grep("b72",C.Codes),Irrig.Meth.C3:=PracticeCodes[Code=="b72",Subpractice]
][grep("b53",C.Codes),Irrig.Meth.C4:=PracticeCodes[Code=="b53",Subpractice]
][,Irrig.Meth.C:=pasteNA(c(Irrig.Meth.C1,Irrig.Meth.C2,Irrig.Meth.C3,Irrig.Meth.C4)),by=Index
][,c("Irrig.Meth.C1","Irrig.Meth.C2","Irrig.Meth.C3","Irrig.Meth.C4","C.Codes"):=NULL]

era_merged[,T.Codes:=paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13),collapse = "-"),by=Index]

era_merged[grep("b34",T.Codes),Irrig.Meth.C1:=PracticeCodes[Code=="b34",Subpractice]
][grep("b36",T.Codes),Irrig.Meth.C2:=PracticeCodes[Code=="b36",Subpractice]
][grep("b72",T.Codes),Irrig.Meth.C3:=PracticeCodes[Code=="b72",Subpractice]
][grep("b53",T.Codes),Irrig.Meth.C4:=PracticeCodes[Code=="b53",Subpractice]
][,c("Irrig.Meth.C1","Irrig.Meth.C2","Irrig.Meth.C3","Irrig.Meth.C4","T.Codes"):=NULL]

# Remove Codes 
TC.Cols<-paste0(c("T","C"),rep(1:13,2))

RemoveIrrigCode<-function(X){
  X[X %in% c("b72","b36","b34","b53")]<-""
  return(X)
}

era_merged[,(TC.Cols):=lapply(.SD, RemoveIrrigCode),.SDcols=TC.Cols]

  # 1.11) Recode Cost Benefit Ratios to Benefit Cost Ratios ####
  X<-data.table(In=c(126,126.1,126.2,126.3),Out=c(125,125.1,125.2,125.3))
  
  for(i in 1:nrow(X)){
    era_merged[Outcome==X[i,In],MeanC:=1/MeanC
    ][Outcome==X[i,In],MeanT:=1/MeanT
    ][Outcome==X[i,In],Outcome:=X[i,Out]]
  }
  # 1.12) Inconsistent or Missing NI/NO numeric 0s ####
# This is where there is inconsistency between a N fertilizer code and columns indicating the amount of N added
# Remove Inconsistent Practices
NI.Pracs<-c("b17|b23")
NO.Pracs<-c("b29|b30|b75|b73|b67")

era_merged[,C.Codes:=trimws(paste(c(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13),collapse=" ")),by=list(C1,C2,C3,C4,C5,C6,C7,C8,C9,C10,C11,C12,C13)]
era_merged[,T.Codes:=trimws(paste(c(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13),collapse=" ")),by=list(T1,T2,T3,T4,T5,T6,T7,T8,T9,T10,T11,T12,T13)]

era_merged[,C.NI:=as.numeric(C.NI)
][,T.NI:=as.numeric(T.NI)
][,C.NO:=as.numeric(C.NO)
][,T.NO:=as.numeric(T.NO)]

# T.NI or C.NI has inorganic fertilizer added, but there are no fertilizer codes in the T/C cols
error_dat<-era_merged[!grepl(NI.Pracs,C.Codes) & C.NI>0,.(value=paste(unique(T.Descrip),collapse="/"),Version=Version[1]),by=Code
                      ][,field:="T.Descrip"
                        ][,issue:="C.NI has inorganic fertilizer added, but there are no fertilizer codes in the C cols"]

error_dat<-rbind(error_dat,era_merged[!grepl(NI.Pracs,T.Codes) & T.NI>0,.(value=paste(unique(T.Descrip),collapse="/"),Version=Version[1]),by=Code
][,field:="T.Descrip"
][,issue:="T.NI has inorganic fertilizer added, but there are no fertilizer codes in the T cols"])

# T.NO or C.NO has a value, but there are no organic input codes in the T/C cols
error_dat<-rbind(error_dat,era_merged[!grepl(NO.Pracs,C.Codes) & C.NO>0,.(value=paste(unique(T.Descrip),collapse="/"),Version=Version[1]),by=Code
][,field:="T.Descrip"
][,issue:="C.NO has a value, but there are no organic input codes in the C cols"])

error_dat<-rbind(error_dat,era_merged[!grepl(NO.Pracs,T.Codes) & T.NO>0,.(value=paste(unique(T.Descrip),collapse="/"),Version=Version[1]),by=Code
][,field:="T.Descrip"
][,issue:="T.NO has a value, but there are no organic input codes in the Tcols"])

errors$fert_amount_no_code<-error_dat

# Remove rows with inconsistencies (Temporary solution until QC on potential errors is complete)
Remove.Indices<-c(era_merged[!grepl(NI.Pracs,C.Codes) & C.NI>0,Index],
                  era_merged[!grepl(NI.Pracs,T.Codes) & T.NI>0,Index],
                  era_merged[!grepl(NO.Pracs,C.Codes) & C.NO>0,Index],
                  era_merged[!grepl(NO.Pracs,T.Codes) & T.NO>0,Index])

length(Remove.Indices)
era_merged<-era_merged[!Index %in% Remove.Indices]

# Update missing NI or NO codes for crop yield outcomes
#era_merged[!grepl(NI.Pracs,C.Codes) & is.na(C.NI) & Outcome %in% c(101,102),C.NI:=0]
#era_merged[!grepl(NI.Pracs,T.Codes) & is.na(T.NI) & Outcome %in% c(101,102),T.NI:=0]
#era_merged[!grepl(NO.Pracs,C.Codes) & is.na(C.NO) & Outcome %in% c(101,102),C.NO:=0]
#era_merged[!grepl(NO.Pracs,T.Codes) & is.na(T.NO) & Outcome %in% c(101,102),T.NO:=0]

era_merged[,C.Codes:=NULL][,T.Codes:=NULL]

  # 1.13) Currency Harmonization #####
# Remove existing cols
era_merged[,c("USD2010.C","USD2010.T"):=NULL]

# Set target year
year_target<-2010

# Read and Process FAO Deflators
fao_deflators_raw <- fread(era_dirs$era_currency_files$deflators)

# Clean and process the FAO deflators data
fao_deflators <- fao_deflators_raw[
  Item == "Value Added Deflator (Agriculture, forestry and fishery)" &
    Element == "Value US$, 2015 prices", 
  .(`Area Code`, Area, Year, Value)
]

# Convert Area.Code to ISO3 country codes
fao_deflators[, ISO3 := countrycode::countrycode(
  `Area Code`, origin = "fao", destination = "iso3c", warn = TRUE
)]

# Remove rows with missing ISO3 codes
fao_deflators <- fao_deflators[!is.na(ISO3)]

# Keep only necessary columns
fao_deflators <- fao_deflators[, .(ISO3, Year, Deflator = Value)]

# Normalize deflators to the target year
# First, get deflator values for the target year for each country
target_deflators <- fao_deflators[Year == year_target, .(ISO3, TargetDeflator = Deflator)]

# Merge target deflators back into the deflators data
fao_deflators <- merge(
  fao_deflators, 
  target_deflators, 
  by = "ISO3", 
  all.x = TRUE
)

# Calculate the adjustment factor for each year
fao_deflators[, AdjustmentFactor := TargetDeflator / Deflator]

# Read in PPP and Exchange Rate Data
# Use wbstats to download the latest data
ppp_data <- fread(era_dirs$era_currency_files$ppp)
xrat_data <-  fread(era_dirs$era_currency_files$exchange_rates)

# Convert date columns to numeric years
ppp_data[, date := as.numeric(date)]
xrat_data[, date := as.numeric(date)]

# Identify Indices for Currency Harmonization
# Define economic codes based on your OutcomeCodes data
economic_codes <- OutcomeCodes[
  Subpillar == "Economics" | Code %in% c(242, 243, 133), Code
]

# Filter observations for currency harmonization
PPP_N <- era_merged[
  ISO.3166.1.alpha.3 %in% unique(xrat_data$iso3c) &
    Outcome %in% economic_codes &
    !is.na(M.Year.Start) &
    !grepl("[.]", ISO.3166.1.alpha.3) &
    !Units %in% c("%", ""),
  Index
]

# Subset and Prepare Data
PPP_era_merged <- era_merged[
  Index %in% PPP_N,
  .(Index, ISO.3166.1.alpha.3, M.Year.Start, M.Year.End, MeanC, MeanT, Units)
]

# Calculate median year for each observation
PPP_era_merged[, M.Year := round((M.Year.Start + M.Year.End) / 2)]

# Add currency codes
PPP_era_merged[, currency_iso3 := countrycode(
  ISO.3166.1.alpha.3, origin = 'iso3c', destination = 'iso4217c'
)]

# Handle cases where Units indicate USD
PPP_era_merged[grepl("USD", Units, ignore.case = TRUE), currency_iso3 := "USD"]

# Merge Observed Exchange Rates
PPP_era_merged <- merge(
  PPP_era_merged,
  xrat_data[, .(iso3c, date, xrat_obs = PA.NUS.FCRF)],
  by.x = c("ISO.3166.1.alpha.3", "M.Year"),
  by.y = c("iso3c", "date"),
  all.x = TRUE
)

# Set exchange rate to 1 for USD currencies
PPP_era_merged[currency_iso3 == "USD", xrat_obs := 1]

# Convert MeanT and MeanC to USD Using Observed Exchange Rates
PPP_era_merged[, MeanT_USD := MeanT / xrat_obs]
PPP_era_merged[, MeanC_USD := MeanC / xrat_obs]

# Merge FAO Deflators and Calculate Adjustment Factors
# Merge deflators for the observation years
PPP_era_merged <- merge(
  PPP_era_merged,
  fao_deflators[, .(ISO3, Year, AdjustmentFactor)],
  by.x = c("ISO.3166.1.alpha.3", "M.Year"),
  by.y = c("ISO3", "Year"),
  all.x = TRUE
)

# Handle missing adjustment factors (if any)
PPP_era_merged[is.na(AdjustmentFactor), AdjustmentFactor := 1]

# Adjust MeanT_USD and MeanC_USD to Target Year Using FAO Deflators
PPP_era_merged[, MeanT_USD_Target := round(MeanT_USD * AdjustmentFactor,2)]
PPP_era_merged[, MeanC_USD_Target := round(MeanC_USD * AdjustmentFactor,2)]

# Update the Main Dataset
# Initialize new columns
new_cols<-paste0("USD",year_target,".",c("C","T"))
era_merged[, (new_cols) := NA_real_]

# Update era_merged with the adjusted values
era_merged[PPP_era_merged, on = .(Index), (new_cols) := .(i.MeanC_USD_Target, i.MeanT_USD_Target)]

  # 1.14) Harmonize data location names #####

patterns <- c(
  "fig", "tab", "Table", "Figure", "txt", " ", "Tab", 
  "A", "B", "C", "D", "E", "G", "H", "\\[", "\\]", 
  "fib", "fi1", "V", "I", "X", ",", "&", "Fig", "page3"
)
replacements <- c(
  "Fig", "Tab", "Tab", "Fig", "Text", "", "Tab ", 
  "a", "b", "c", "d", "e", "g", "h", "", "", 
  "Fig", "Fig 1", "v", "i", "x", ".", ".", "Fig ", " Page 3"
)

substitutions <- setNames(replacements, patterns)

era_merged[, DataLoc := stringr::str_replace_all(DataLoc[1], substitutions),by=DataLoc]
era_merged[DataLoc==0,DataLoc:=NA]

# era_merged[,unique(DataLoc)]

  # 1.15) Remove duplicates ####

# List partial duplicates
Partial.Dup<-Duplicates[CompleteDuplicate=="No",paste(B.Code2,DataLoc2)]
Partial.Dup2<-Duplicates[CompleteDuplicate=="No",list(B.Code2,DataLoc2)]
# Any non-matches?
NonMatch<-Partial.Dup2[!Partial.Dup %in% era_merged[,paste(Code,DataLoc)]]

# List full paper duplicates
Full.Dup<-Duplicates[CompleteDuplicate=="Yes",B.Code2]
# Any non-matches?
Full.Dup[!Full.Dup %in% era_merged[,Code]]

# Remove complete partial duplicate data
unique(era_merged[Code %in% NonMatch[,B.Code2],list(Code,DataLoc)])
era_merged<-era_merged[!paste(Code,DataLoc) %in% Partial.Dup]

# Remove complete paper duplicates
era_merged<-era_merged[!Code %in% Full.Dup]

# Tidy up
rm(Full.Dup,Partial.Dup,Partial.Dup2,NonMatch)


  # 1.16) Process practice codes #####
    # 1.16.1) Simplify complex rotation and intercropping codes ######
    Cols<-c(paste0(rep(c("C","T"),each=N.Cols),1:N.Cols))
    
    # 1.16.2) Complex Rotation - b44 - All are legume/non-legume sequences ######
    FunX<-function(X){gsub("b44","b43",X)}
    Z<-era_merged[,lapply(.SD,FunX),.SDcols=Cols]
    
    # Partial Intercrop - Rotation code is already dealt with so we just need to change intercropping code
    FunX<-function(X){gsub("b55.1","b25",X)}
    Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
    FunX<-function(X){gsub("b55.2","b50.1",X)}
    Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
    FunX<-function(X){gsub("b55.3","b50.2",X)}
    Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
    # Intercrop Rotation & Complex Intercrop Rotation -  Not used
    
    # 1.16.3) Update h-codes ######
    # 1.16.5) Simplify rotation and intercropping controls ######
    FunX<-function(X){gsub("h38","h2",X)}
    Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
    
    Dpracs<-c("Crop Rotation","Intercropping","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
    PC<-PracticeCodes[Practice %in% Dpracs,Code]
    PC_no_h<-PC[!grepl("h",PC)]
    
    # 1.16.6) h2 & h23 codes ######
    
    FunX<-function(data,PC_no_h,verbose){
      X<-data
      
      if(verbose){
        cat('\r                                                                                                                                          ')
        cat('\r',paste0("Processing ",paste(data,collapse = "-")))
        flush.console()
      }
      
      # Add missing control "h2" code if rotation/intercropping/alleycropping present in treatment and absent in control
      dat1<-X[1:N.Cols]
      dat2<-X[(N.Cols+1):length(X)]
      if(any(dat2 %in% PC_no_h) & !"h2" %in% dat1 & ! any(dat1 %in% PC_no_h)){
        X[which(PC_no_h=="")[1]]<-"h2"
      }
      # Add h23 deficit irrigation code where missing from control (mostly 2020 data)
      if(any(dat2=="b54") & all(dat1!="b54") & !any(dat1=="h23")){
        X[which(dat1=="")[1]]<-"h23"
      }
      return(as.data.table(as.list(unlist(X))))
    }
    
    Z[,code2:=apply(Z,1,paste,collapse="-")]
    Z1<-unique(Z)
    Z1[,Code:=apply(Z1[,!c("code2")],1,list)]
    result<-pblapply(Z1[,Code],function(x){FunX(data=x,PC_no_h=PC_no_h,verbose=F)})
    result<-rbindlist(result)
    result[,code2:=Z1$code2]
    
    Z<-merge(Z[,"code2"],result,by="code2",all.x=T,sort=F)[,code2:=NULL]
    
    # 1.16.7) Remove h1, h10 & h10.2 codes ######
    # h1 - Non-CSA codes
    # h10 - Conventional input (fertilizer) control (vs recommended rate)
    # h10.2 - Addition of recommended amount of fertilizer following soil tests
    
    FunX<-function(X,Cols){
      X[X %in% c("h1","h10","h10.2")]<-""
      X
    }
    Z<-Z[,lapply(.SD,FunX),.SDcols=Cols]
    
    # 1.16.8) h7 deficit irrigation code ######
    # Here we are checking the control is present where it should be
    FunX<-function(X){
      any(X[(N.Cols+1):length(X)]=="b37") & all(X[1:N.Cols]!="b37") & !any(X[1:N.Cols]=="h7")
    }
    
    Z1<-apply(Z,1,FunX)
    error_dat<-unique(era_merged[Z1 & Code!="EO0134",list(Code,DataLoc,T.Descrip,C.Descrip,Version)])
    if(nrow(error_dat)>0){
      View(error_dat)
    }
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
    
    # 1.16.9) Combine datasets back together ######
    era_merged<-cbind(era_merged[,!(..Cols)],Z)
    
    rm(Z,FunX,Cols,Dpracs,PC,CC,TT)
    
  # 1.17) Carbon change outcomes - change % to proportion #####
  era_merged[Outcome == 224.1 & Units=="%",MeanC:=(100+MeanC)/100]
  era_merged[Outcome == 224.1 & Units=="%",MeanT:=(100+MeanT)/100]
  era_merged[Outcome == 224.1 & Units=="%",Units:="Proportion"]
  
  # 1.18) Remove product = n1 (Soil) #####
  era_merged[,EU:=gsub("-n1","",EU)]
  era_merged[,EU:=gsub("n1-","",EU)]
  era_merged[,EU:=gsub("n1","",EU)]

  # 1.19) Check for Practice in control not in treatment #####

CCols<-paste0("C",1:13)
TCols<-paste0("T",1:13)

#DataX<-era_merged[!Analysis.Function %in% c("DietSub","Sys.Rot.vs.Mono","Ratios") & is.na(Invalid.Comparison)] # unhash if Invalid.Comparison field is present
DataX<-era_merged[!Analysis.Function %in% c("DietSub","Sys.Rot.vs.Mono","Ratios")]

X<-cbind(DataX[,..CCols],DataX[,..TCols])

NotInT<-pbapply(X,1,FUN=function(Y){
  N<-!Y[1:13] %in% Y[14:26]
  Y<-Y[1:13][N]
  Y<-Y[Y!="" & !grepl("h",Y)]
  if(length(Y)>0){paste(Y,collapse="-")}else{NA}
})

Cols<-c("Index","Code","Version","Analysis.Function","C.Descrip","T.Descrip","Outcome","M.Year",CCols,TCols)
error_dat<-DataX[!is.na(NotInT),..Cols][,NotInT:=NotInT[!is.na(NotInT)]][,.(value=paste(unique(paste0(Outcome,":",T.Descrip," vs ",C.Descrip," NotInT = ",NotInT)),collapse = "/"),dataset=paste0(Version[1],"-",Analysis.Function[1])),by=Code
][,field:="Outcode:T.Descrip vs C.Descrip"][,issue:="Practice is present in control that is not present in treatment."]

errors$cprac_not_in_tpracs<-error_dat

era_merged<-era_merged[!Index %in% DataX[!is.na(NotInT),Index]]

# 2) Compile ERA ####
  system.time(
    result<-compile_era(Data=era_merged,
                       OutcomeCodes=OutcomeCodes,
                       EUCodes=EUCodes,
                       PracticeCodes=PracticeCodes,
                       cores=cores,
                       VERBOSE=F,  
                       AddText=T,
                       delim="-",
                       AddBuffer=T,
                       Keep.No.Spatial=T,
                       Keep.hcode=F)
    )
  
  ERA.Compiled<-result$Data
  errors$compilation_validation_checks<-rbindlist(result$Errors)
  ERA.Compiled[Product.Simple=="NA",Product.Simple:=NA][,Buffer:=as.numeric(Buffer)]

# 3) Add biophysical data ####
  # 3.1) Prepare data ####
  # Note that environmental data scripts must have been run on the compiled ERA dataset, see ERA_dev/R/add_geodata
    # 3.1.1) Load environmental data
    Env.Data<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"era_site_others.parquet"))
    POWER<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"POWER_ltavg.parquet"))
    Soils<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"era_site_soil_af_isda.parquet"))
    
    # !!!!NEED TO CREATE CHIRPS.LT.RData File ####
    # CHIRPS<-data.table(load.Rdata2("CHIRPS.LT.RData",path=paste0(ClimateDir,"CHIRPS/")))
    CHIRPS<-arrow::read_parquet(file.path(era_dirs$era_geodata_dir,"CHIRPS.parquet"))
    CHIRPS<-CHIRPS[,N:=.N,by=.(Site.ID,Latitude,Longitude,Buffer,Year)
                   ][N>=365
                     ][,.(Total.Rain=sum(Rain,na.rm=T),Mean.Annual.Temp=mean(Temp.Mean,na.rm=T)),by=.(Site.ID,Latitude,Longitude,Buffer,Year)
                       ][,.(Total.Rain=mean(Total.Rain,na.rm=T),Mean.Annual.Temp=mean(Mean.Annual.Temp,na.rm=T)),by=.(Site.ID,Latitude,Longitude,Buffer)
                         ][,Site.Key:=paste0(sprintf("%07.4f", Latitude[1]), " ", sprintf("%07.4f", Longitude[1]), " B", Buffer[1]),by=.(Latitude,Longitude,Buffer)]
    
    # Initialize AEZ mappings from era_master_table
    AEZ.Mappings<-master_codes$aez
    
    # Subset Data to Site.Keys
    Climate<-unique(ERA.Compiled[!is.na(Site.Key),"Site.Key"])
    
    # 3.1.2) Prepare climate data ######
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
    
    # Save climate file
    fwrite(Climate,file=file.path(era_dirs$era_geodata_dir,paste0("AEZ_MAP_MAT-",Sys.Date(),".csv")))
    
    # 3.1.3) Prepare soil data  ######
    # Calulate weighted mean soil parameter values weighted by depth interval
    Soils<-Soils[stat=="mean" & variable %in% c("clay.tot.psa","sand.tot.psa","silt.tot.psa")
                 ][,interval:=mean(as.numeric(unlist(strsplit(gsub("cm","",depth[1]),"-")))),by=depth
                   ][,.(value=round(weighted.mean(value,interval),1)),by=.(Site.Key,variable)
                     ][,variable:=gsub("[.]tot[.]psa","",variable)]
    
    Soils<-data.table(dcast(Soils,Site.Key~variable))
    
    col_mappings<-c(sand="SND",silt="SLT",clay="CLY")
    
    setnames(Soils,names(col_mappings),col_mappings)
    
  # 3.2) Merge biophysical data ######
    ERA.Compiled<-cbind(ERA.Compiled,Climate[match(ERA.Compiled[,Site.Key],Site.Key),!"Site.Key"],Soils[match(ERA.Compiled[,Site.Key],Site.Key),!"Site.Key"])

# 4) Split off Economic outcomes with no comparison ####
ERA.Compiled.Econ<-ERA.Compiled[is.na(MeanC) & Out.SubPillar != "Efficiency" & !Out.SubInd %in% c("Crop Yield","Soil Organic Carbon"),]
ERA.Compiled<-ERA.Compiled[!Index %in% ERA.Compiled.Econ[,Index]]

# 4.1) Validation  #####
  # 4.1.1) Check for where no ERA practices are present ######
  error_dat<-ERA.Compiled[plist=="",.(Index=paste(Index,collapse=","),Version=Version[1]),by=.(Code,C.Descrip,T.Descrip)][,issue:="No practices in plist (no valid comparison found)?."]
  unique(error_dat[,.(Code,Version)])
  
  error_file<-file.path(era_dirs$era_masterdata_dir,paste0(filename_comb,"_no_comparions.csv"))
  fwrite(error_dat,error_file)

  # 4.1.2) Check for residual outcomes with NA MeanC values ######
  error_dat<-ERA.Compiled[is.na(MeanC) & 
                            Out.SubPillar != "Efficiency" & 
                            !Out.SubInd %in% c(OutcomeCodes[Subpillar=="Economics",Subindicator],"Fuel Savings","Labour Person Hours"),
                          .(value=paste(unique(C.Descrip),collapse="/"),
                            Version=Version[1]),by=.(Code)
  ][,issue:="No value is present in MeanC"]
  
  errors$no_value_in_MeanC<-error_dat
  
  
  # 4.2) Remove no comparisons
  ERA.Compiled<-ERA.Compiled[plist!=""]


  # 4.1.3) Check for missing climate data ######
  # No AEZ
  unique(ERA.Compiled[is.na(AEZ16simple) & Buffer<50000 & !is.na(Latitude),list(Code,Country,Latitude,Longitude,Buffer,Site.Key,Version)])
  # No CLY
  unique(ERA.Compiled[!is.na(M.Year) & is.na(CLY) & Buffer<50000 & !is.na(Latitude),list(Code,Country,Latitude,Longitude,Buffer,Site.Key,Version)])
  # No Precip
  unique(ERA.Compiled[!is.na(M.Year) & M.Year!="NA" & is.na(Mean.Annual.Precip) & Buffer<50000 & !is.na(Latitude),list(Code,Country,Latitude,Longitude,Buffer,Site.Key,M.Year,Version)])
  # No Temperature
  unique(ERA.Compiled[!is.na(M.Year) & M.Year!="NA" & is.na(Mean.Annual.Temp) & Buffer<50000 & !is.na(Latitude),list(Code,Country,Latitude,Longitude,Buffer,Site.Key,M.Year,Version)])
  
# 5) Save results ####
save_file<-file.path(era_dirs$era_masterdata_dir,paste0(filename_comb,".csv"))
fwrite(ERA.Compiled,save_file)
arrow::write_parquet(ERA.Compiled,gsub("[.]csv",".parquet",save_file))

save_file<-file.path(era_dirs$era_masterdata_dir,paste0(gsub("era_compiled-","era_compiled_econ_only-",filename_comb),".csv"))
fwrite(ERA.Compiled.Econ,save_file)
