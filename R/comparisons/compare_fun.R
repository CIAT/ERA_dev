#' compare_fun: Compare Control vs. Treatment Groups
#'
#' This function automates the comparison of control and treatment groups based 
#' on a dataset containing experimental data. It identifies differences in practices, 
#' validates comparisons, and handles specific exceptions like residue mulching 
#' versus incorporation. This function is designed for use in agricultural or 
#' environmental experiments but can be adapted to other domains with similar 
#' structures.
#'
#' ## Key Features:
#' 1. Identifies differences between treatment and control groups.
#' 2. Handles special cases, such as recoding residue practices for mulching and incorporation.
#' 3. Validates comparisons based on matching practices and linked metadata.
#' 4. Returns either a summary table or detailed debugging information.
#'
#' ## Core Concepts:
#' - **Control Groups**: Represent baseline practices or conditions.
#' - **Treatment Groups**: Variations or experimental conditions applied.
#' - **Practices**: Specific actions or methods used (e.g., crop rotation, mulching).
#' 
#' ## Developer Notes:
#'  1.  Explore improved varieties heat etc. vs other?

#' @import data.table
#'
#' @param Data Data table containing experimental data. Must include columns 
#'   like `Final.Codes` (practice codes), `N` (group identifiers), and others.
#' @param Verbose Logical; if TRUE, prints detailed progress and debugging 
#'   information for monitoring or troubleshooting.
#' @param Debug Logical; if TRUE, returns raw intermediate tables for inspection 
#'   rather than just final results.
#' @param PracticeCodes Data table listing codes for various practices, along with 
#'   metadata for linking them to higher-level categories or descriptions.
#' @param Fert.Method Data table detailing fertilizer applications from the ERA
#'   data model.
#' @param Plant.Method Data table detailing crop planting methods and spacings
#'    from the ERA data model.
#' @param Irrig.Method Data table detailing irrigation methods and applications
#'    from the ERA data model.
#' @param Res.Method  Data table detailing mulching methods and applications
#'    from the ERA data model.
#' @param p_density_similarity_threshold Numeric; minimum similarity needed in planting
#'    density comparisons (0-1, 1 being 100% similar), Default = 0.95.
#' @param Return.Lists Logical; if TRUE, outputs comparisons as lists (useful for 
#'   further programmatic analysis). If FALSE, results are returned as a flat table.
#' @return A data table summarizing valid comparisons between control and treatment 
#'   groups, including matched and unmatched codes. If `Debug = TRUE`, returns 
#'   intermediate outputs for detailed inspection.
#'
#' ## Example:
#' Suppose you have experimental data comparing different farming practices 
#' (e.g., mulching vs. no mulching). You want to determine:
#' - Which treatment groups differ from the control group.
#' - Whether the differences meet specified criteria for a valid comparison.
#'
#' ```R
#' result <- compare_fun(
#'   Data = experimental_data,
#'   Verbose = TRUE,
#'   Debug = FALSE,
#'   PracticeCodes = practice_codes,
#'   Fert.Method=Fert.Method,
#'   Plant.Method=Plant.Method,
#'   Irrig.Method=Irrig.Method,
#'   Res.Method=Res.Method,
#'   p_density_similarity_threshold = 0.95,
#'   Return.Lists = FALSE
#' )
#'
#' head(result)
#' ```
#' The output will be a data table with details on valid treatment-control pairs.
#'
#' ## Notes:
#' - Ensure the input data is cleaned and structured according to the expected format.
#' - Special cases like mulching require predefined codes in the `PracticeCodes` table.
#' 
#' ## Developer Notes:
#' - Potential issue? Diversification residue re-coding is for any residues, whereas we are interested in only experimental crop residues.
#'
compare_fun<-function(Data,Verbose=F,Debug=F,PracticeCodes,Fert.Method,Plant.Method,Irrig.Method,Res.Method,p_density_similarity_threshold=0.95,Return.Lists=F){
  
  if(!all(c("Time","Site.ID") %in% colnames(Fert.Method))){
    do_times<-F
  }else{
    do_times<-T
  }
  
  # Match.Fun: Identifies elements in A not found in B
  Match.Fun<-function(A,B){
    A<-unlist(A)
    B<-unlist(B)
    list(A[!A %in% B])
  }
  
  # Match.Fun2: Maps matched elements from A and B to C
  Match.Fun2<-function(A,B,C){
    A<-unlist(A)
    B<-unlist(B)
    C<-unlist(C)
    list(unique(C[match(A,B)]))
  }
  
  # Mulch.Fun: Custom logic for mulching and residue incorporation
  Mulch.Fun<-function(A,B,X,Z){
    if(A %in% X){
      Z[Mulch.Flag==F,Mulch.Flag:=Z[Mulch.Flag==F,any(unlist(Final.Codes) %in% B),by="N"][,V1]]
      Z[Mulch.Flag==T,Match:=Match+1]
      Z[Mulch.Flag==T,NoMatch:=NoMatch-1]
      Z[Mulch.Flag==T,Mulch.Code:=paste0(na.omit(c(Mulch.Code,A)),collapse = "-"),by="N"]
      
    }
    return(Z)
  }
  
  # Incorporation codes in control (to be removed)
  Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
  # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
  Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
  
  BC<-Data$B.Code[1]
  N<-Data[,N]
  Final.Codes<-Data[,Final.Codes] # Is this redundant?
  k<-N
  Y<-Data[,c("Final.Codes","N","N.Prac")][,Y.N:=1:.N]
  
  # Recode Mulch or Incorporation codes by removing ".n" code suffix
  Dpracs<-c("Crop Rotation","Intercropping","Improved Fallow","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
  if(any(unlist(Y[,Final.Codes]) %in%  PracticeCodes[Practice %in% Dpracs,Code])){
    
    if(Verbose){print(paste0(BC," - Simplifying Residue Codes"))}
    
    PC1<-PracticeCodes[Practice %in% c("Agroforestry Pruning"),Code]
    PC2<-PracticeCodes[Practice %in% c("Mulch","Crop Residue","Crop Residue Incorporation"),Code] 
    
    
    Recode<-function(PC1,PC2,Final.Codes){
      Final.Codes<-unlist(Final.Codes)
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
      list(Final.Codes)
      
    }
    
    Y[,Final.Codes:=list(Recode(PC1,PC2,Final.Codes)),by=Y.N]
    Data[,Final.Codes:=Y$Final.Codes]
    
  }
  
  rbindlist(lapply(1:length(N),FUN=function(j){
    if(Verbose){
      cat(BC," - Group ",Data$Group[1]," - Row =",j,paste0(c("(",N[j],")"),collapse = ""),"       ","\r")
      }
    X<-unlist(Data$Final.Codes[j])
    i<-N[j]
    
    # This section determines how the current treatment (X) can act as a control group.
    # It handles two cases:
    # 1. If the treatment (X) has no associated practices (`is.na(X[1])`), it evaluates
    #    whether it can serve as a baseline control for other treatments based on their practices.
    # 2. If the treatment (X) has associated practices, it identifies treatments where
    #    the current treatment can act as a valid control, considering special cases like
    #    mulching versus incorporation practices.
    if(is.na(X[1])){
      # Check if the current treatment (X) has no associated practices.
      # If X is NA or empty, handle it as a potential "control group".
      
      Z <- Y[N != i & !is.na(Final.Codes)] 
      # Filter the dataset Y to exclude the current treatment (N != i) 
      # and rows where Final.Codes (practice codes) are missing.
      
      Z[, Match := sum(X %in% unlist(Final.Codes)), by = N] 
      # Count how many practices in X (currently empty) match practices in other treatments.
      # Since X is empty, Match will always be 0.
      
      Z[, NoMatch := sum(!unlist(Final.Codes) %in% X), by = N] 
      # Count how many practices in other treatments (Final.Codes) are not in X.
      # Since X is empty, NoMatch will equal the total practices in each other treatment.
      
      Z <- Z[Match >= 0 & NoMatch > 0] 
      # Retain only rows where:
      # - Match is non-negative (always true for empty X).
      # - NoMatch is greater than 0, ensuring meaningful differences exist.
      
      Z[, Mulch.Code := as.character("")][, Mulch.Flag := F] 
      # Initialize columns for mulch-related logic:
      # - Mulch.Code: Empty character column for mulch-specific codes.
      # - Mulch.Flag: Logical flag to track if mulching adjustments apply.
      
      Z[, Control.Code := rep(list(X), nrow(Z))] 
      # Assign the practices of the current control group (X) to all rows in Z.
      # Since X is empty, this column will contain empty lists.
      
      Z[, Prac.Code := list(Match.Fun(Final.Codes, Control.Code)), by = N] 
      # Identify practices unique to each treatment by comparing Final.Codes (treatment practices)
      # against Control.Code (empty for X in this case).
      
      Z[, Linked.Tab := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Tab)), by = N] 
      # Map control practices (Control.Code) to their corresponding linked tables in PracticeCodes.
      
      Z[, Linked.Col := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Col)), by = N] 
      # Map control practices (Control.Code) to their corresponding linked columns in PracticeCodes.
      
    }else{
      
      # Logic for determining which other treatments the current treatment (X) can be a control for.
      Z <- Y[N != i] 
      # Filter dataset Y to exclude the current treatment (N != i) for comparisons.
      
      Z[, Match := sum(X %in% unlist(Final.Codes)), by = N] 
      # For each treatment in Z, count how many practices in the current treatment (X)
      # match practices in Final.Codes (practices of other treatments).
      # The result is stored in a new column, `Match`.
      
      Z[, NoMatch := sum(!unlist(Final.Codes) %in% X), by = N] 
      # For each treatment in Z, count how many practices in Final.Codes
      # do not match any practices in the current treatment (X).
      # The result is stored in a new column, `NoMatch`.
      
      # Handle exceptions for mulched residues compared to incorporated residues.
      Z[, Mulch.Code := as.character(NA)][, Mulch.Flag := F] 
      # Initialize columns for mulch-specific adjustments:
      # - `Mulch.Code`: Placeholder for mulch-specific codes (initially NA).
      # - `Mulch.Flag`: Logical flag to indicate whether mulching adjustments apply (initially FALSE).
      
      for (kk in 1:length(Mulch.C.Codes)) {
        Z <- Mulch.Fun(A = Mulch.C.Codes[kk], B = Mulch.T.Codes[kk], X, Z) 
        # Apply the `Mulch.Fun` function for each pair of control and treatment mulch codes.
        # `A` represents the control mulch codes (`Mulch.C.Codes`).
        # `B` represents the corresponding treatment mulch codes (`Mulch.T.Codes`).
        # Updates `Mulch.Code`, `Mulch.Flag`, and matching logic in Z.
      }
      
      Z <- Z[Match >= length(X) & NoMatch > 0] 
      # Retain only rows in Z where:
      # - The number of matching practices (`Match`) equals or exceeds the length of X.
      # - The number of non-matching practices (`NoMatch`) is greater than 0.
      # This ensures the current treatment (X) can be meaningfully compared as a control.
      
      # Correct potential issue when assigning Control.Code to avoid length mismatch.
      Z[, Control.Code := rep(list(X), nrow(Z))] 
      # Assign the current treatment's practices (X) to a new column, `Control.Code`,
      # replicated for all rows in Z.
      
      # Remove mulch-specific codes from the control practices where mulching adjustments apply.
      Z[, Control.Code := Z[, list(Control.Code = list(unlist(Control.Code)[!unlist(Control.Code) %in% Mulch.Code])), by = "N"][, Control.Code]] 
      # For each row in Z, update `Control.Code` by excluding codes in `Mulch.Code`.
      # This ensures mulch-specific adjustments are properly accounted for in the control practices.
      
      Z[, Prac.Code := list(Match.Fun(Final.Codes, Control.Code)), by = N] 
      # Identify practices in the treatments (`Final.Codes`) that are not in the control practices (`Control.Code`).
      # Store the unmatched practices in a new column, `Prac.Code`.
      
      Z[, Linked.Tab := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Tab)), by = N] 
      # Map practices in the control (`Control.Code`) to their corresponding linked tables (`Linked.Tab`) in `PracticeCodes`.
      # This creates a new column, `Linked.Tab`.
      
      Z[, Linked.Col := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Col)), by = N] 
      # Map practices in the control (`Control.Code`) to their corresponding linked columns (`Linked.Col`) in `PracticeCodes`.
      # This creates a new column, `Linked.Col`.
    }
    
    if(nrow(Z)>0){
      Z$Level.Check<-lapply(1:nrow(Z),FUN=function(ii){
        if(length(unlist(Z[ii,Linked.Tab]))==0){
          !is.na(unlist(Z$Prac.Code)[ii])
        }else{
          unlist(lapply(1:length(unlist(Z[ii,Linked.Tab])),FUN=function(jj){
            # ***FERTILIZER*** Do both treatments have fertilizer? If so the sequences must match 
            if(unlist(Z[ii,Linked.Tab])[jj]=="Fert.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Fert.Out")){
              if(Verbose){cat("Fert: ii = ",ii," | jj = ",jj,"\n")}
              
              Trt<-Data[Z[ii,Y.N],F.Level.Name]
              Control<-Data[j,F.Level.Name]
              
              # ISSUE - Fertilizer codes from rotation sequence with outcome reported for entire sequence
              # Do we need to update Level.Name fields with aggregated names from Int/Rot Sequence? 
              # Guess this would be done through combining MT.Out entries.
              
              if(Trt==Control){
                # If Trt & Control Have the same Fertilizer Level Name then no need for further investigation.
                TRUE
              }else{ 
                # Trt & Control have different names so we need to compare these in more detail
                
                # Extract data from Fert.Method table for Treatment and Control
                # Choose fields to match on 
                Fert.Fields<-c("F.Level.Name","F.Category","F.Type","F.NPK","F.Amount","F.Unit","F.Method","F.Physical",
                               "F.Fate","F.Date","F.Date.Stage","F.Date.DAP","F.Date.DAE")
                
                # Suppress warnings is present as cols Code and F.Level.Name_original may not always be present depending on 
                # where this function is run in the comparison workflow. We must remove any fields that identify a fertilizer
                # treatment beyond the description of an individual element (e.g. anything derived at the fertilizer practice level combining multiple rows)
                if(do_times){
                Trt1<-suppressWarnings(Fert.Method[B.Code==BC & 
                                    F.Level.Name==Trt & 
                                    Time %in% c(Data$Time[1],"All Times") & 
                                    Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                  !c("F.Date.Text","F.Level.Name2","Code","F.Level.Name_original","F.Codes.Level")])
                
                Control1<-suppressWarnings(Fert.Method[B.Code==BC & 
                                        F.Level.Name==Control & 
                                        Time %in% c(Data$Time[1],"All Times") & 
                                        Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                      !c("F.Date.Text","F.Level.Name2","Code","F.Level.Name_original","F.Codes.Level")] )
                }else{
                  Trt1<-suppressWarnings(Fert.Method[B.Code==BC & F.Level.Name==Trt,!c("F.Date.Text","F.Level.Name2","Code","F.Level.Name_original","F.Codes.Level")])
                  Control1<-suppressWarnings(Fert.Method[B.Code==BC &F.Level.Name==Control,!c("F.Date.Text","F.Level.Name2","Code","F.Level.Name_original","F.Codes.Level")] )
                }
                
                # Are there any Fert.Method rows in the Control not in the Treatment? 
                # Note that the fertilizer being applied could have the same identity but still differ in amount applied 
                # Amount can be recorded as total amount applied to plot in the Fert.Out tab and not for each row in the Fert.Method Tab
                N.Control<-data.table(Level=c(rep("Control",nrow(Control1)),rep("Trt",nrow(Trt1))),
                                      Dup=duplicated(rbind(Control1[,!"F.Level.Name"],Trt1[,!"F.Level.Name"])))[Level=="Trt"]
                
                # 1) For rows that match:
                # 1.1) For shared inorganic NPK practices do the NPK values match? (check any 999999 values are set to NA) ####
                #Shared.FCodes<-unique(unlist(Trt1[N.Control$Dup,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                Shared.FCodes<-na.omit(unique(Trt1[N.Control$Dup,unlist(tstrsplit(F.Codes,"-"))]))
                Shared.FCodes<-Shared.FCodes[Shared.FCodes!="NA"]
                
                Trt2<-Fert.Out[B.Code==BC & F.Level.Name==Trt,]  
                Control2<-Fert.Out[B.Code==BC & F.Level.Name==Control,]  
                
                if(any(Shared.FCodes %in% c("b16","b17","b21","b23"))){
                  
                  if("b16" %in% Shared.FCodes){
                    K.Logic<-paste(Trt2$F.KI,Trt2$F.K2O) == paste(Control2$F.KI,Control2$F.K2O)
                  }else{
                    K.Logic<-T
                  }
                  
                  if(any(c("b23","b17") %in% Shared.FCodes)){
                    N.Logic<-Trt2$F.NI == Control2$F.NI
                  }else{
                    N.Logic<-T
                  }
                  
                  if("b21" %in% Shared.FCodes){
                    P.Logic<-paste(Trt2$F.PI,Trt2$F.P2O5) == paste(Control2$F.PI,Control2$F.P2O5)
                  }else{
                    P.Logic<-T
                  }
                  
                  FI.Logic<-all(K.Logic,N.Logic,P.Logic)
                }else{
                  FI.Logic<-T
                }
                
                # 1.2) For shared organic practices do organic NPK amounts match?) ####
                # To compare organic NPK levels there should be one or more organic practices shared and none not shared (if there is a mix then organic NPK could vary)
                #NotShared.FCodes<-unique(unlist(Trt1[!N.Control$Dup,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                NotShared.FCodes<-na.omit(unique(Trt1[!N.Control$Dup,unlist(tstrsplit(F.Codes,"-"))]))
                
                if(any(Shared.FCodes %in% c("b29","b30","b75","b67")) & !any(NotShared.FCodes %in% c("b29","b30","b75","b67"))){
                  FO.Logic<-paste(as.character(unlist(Trt2[,c("F.NO","F.PO","F.KO")])),collapse = " ") == paste(as.character(unlist(Control2[,c("F.NO","F.PO","F.KO")])),collapse = " ")
                }else{
                  FO.Logic<-T
                }
                
                # 2) For rows that don't match do they correspond to a shared practice code? If so these are part of the same practice and cannot be compared) ####
                # The row in the Trt that does not match the control should have a code that is not present in the control
                #F.ControlCode<-unique(unlist(Control1[,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                F.ControlCode<-na.omit(unique(Control1[,unlist(tstrsplit(F.Codes,"-"))]))
                FNoMatch.Logic<-!NotShared.FCodes %in% F.ControlCode
                
                # 3) If control has practices not in the treatment then it is an invalid comparison) ####
                #F.TCode<-unique(unlist(Trt1[,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                F.TCode<-na.omit(unique(Trt1[,unlist(tstrsplit(F.Codes,"-"))]))
                
                # 4) For Practices with NoCode do rows match?
                FCinT.Logic<-all(F.ControlCode %in% F.TCode)
                
                all(FI.Logic,FO.Logic,FNoMatch.Logic,FCinT.Logic)
                
              }
              
            }else{
              # ***ROTATION*** Do both treatments have rotation? If so the sequences must match
              if(unlist(Z[ii,Linked.Tab])[jj]=="Rot.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Rot.Out")){
                if(Verbose){print(paste0("Rotation: ii = ",ii," | jj = ",jj))}
                # Rotation will need to be compared by matching crop sequences
                
                Trt<-Data[Z[ii,Y.N],R.Level.Name]
                Control<-Data[j,R.Level.Name]
                
                if(is.na(Trt)){Trt<-0}
                if(is.na(Control)){Control<-0}
                
                if(Trt==Control){TRUE}else{ 
                  # Do rotation sequences match?
                  Trt<-Data[Z[ii,Y.N],R.Prod.Seq]
                  Control<-Data[j,R.Prod.Seq]
                  
                  # There will not always be values - if both NA for sequence assume comparison is valid by setting both to 0
                  if(is.na(Trt)){Trt<-0}
                  if(is.na(Control)){Control<-0}
                  
                  if(Trt==Control){TRUE}else{FALSE}
                }
                
              }else{
                # ***INTERCROPPING*** Do both treatments have intercropping? If so the intercropping planting must match for them to be compared
                if(unlist(Z[ii,Linked.Tab])[jj]=="Int.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Int.Out")){ 
                  if(Verbose){print(paste0("Intercropping: ii = ",ii," | jj = ",jj))}
                  
                  # If both the treatment and control share intercropping then the planting density of each component needs to be 95% similar
                  # If comparing practices at the treatment level (not the intercropping or rotation system level) 
                  
                  Trt<-Data[Z[ii,Y.N],IN.Level.Name]
                  Control<-Data[j,IN.Level.Name]
                  
                  # Test if crops used are the same.
                  Trt.Comp<-Data[Z[ii,Y.N],IN.Prod]
                  Int.Comp<-Data[j,IN.Prod]
                  
                  if(all(Trt.Comp %in% Int.Comp)){
                    
                    # Test if Planting Density is 95% similar between treatments 
                    Trt1<-Plant.Method[P.Product %in% unlist(strsplit(Trt,"[*][*][*]")) & B.Code == BC
                                       ,c("P.Product","Plant.Density")
                    ][order(P.Product)
                    ][!is.na(Plant.Density)]
                    
                    Control1<-Plant.Method[P.Product %in% unlist(strsplit(Control,"[*][*][*]")) & B.Code == BC
                                           ,c("P.Product","Plant.Density")
                    ][order(P.Product)
                    ][!is.na(Plant.Density)]
                    
                    # Situation where information might be incomplete for one treatment
                    if(nrow(Trt1) != nrow(Control1)){
                      Trt1<-Trt1[P.Product %in% Control1[,P.Product]]
                      Control1<-Control1[P.Product %in% Trt1[,P.Product]]
                    }
                    
                    # Skip if no density information is available
                    if(nrow(Trt1)==0){TRUE}else{
                      
                      # Planting Density needs to be similar in all treatments
                      if(all((Trt1[,Plant.Density]/Control1[,Plant.Density])>=p_density_similarity_threshold)){TRUE}else{FALSE}
                      
                    }
                    
                  }else{FALSE}
                  
                }else{
                  # ***IRRIGATION*** Do both treatments have deficit or supplemental irrigation? 
                  if(unlist(Z[ii,Linked.Tab])[jj]=="Irrig.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Irrig.Out")){
                    if(Verbose){print(paste0("Irrigation: ii = ",ii," | jj = ",jj))}
                    
                    # If deficit, supplemental, APRI or Other irrigation present in both Control and Trt then If so the water applied must match for them to be compared
                    if(any(c("b37","b36","b54","b53") %in% unlist(Z[ii,Control.Code]))){
                      Trt<-Data[Z[ii,Y.N],I.Level.Name]
                      Control<-Data[j,I.Level.Name]
                      
                      if(do_times){
                      Trt1<-Irrig.Method[I.Level.Name==Trt & B.Code == BC & 
                                           Time %in% c(Data$Time[1],"All Times") & 
                                           Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                         c("I.Amount","I.Unit","I.Water.Type")]
                      
                      Control1<-Irrig.Method[I.Level.Name==Control  & 
                                               B.Code == BC & 
                                               Time %in% c(Data$Time[1],"All Times") & 
                                               Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                             c("I.Amount","I.Unit","I.Water.Type")]
                      }else{
                        Trt1<-Irrig.Method[I.Level.Name==Trt & B.Code == BC,c("I.Amount","I.Unit","I.Water.Type")]
                        
                        Control1<-Irrig.Method[I.Level.Name==Control  & B.Code == BC,c("I.Amount","I.Unit","I.Water.Type")]
                      }
                      
                      if(identical(Trt1,Control1)){TRUE}else{FALSE}
                    }else{
                      # Shared practices must be drip or sprinkler and we assume these are the same.
                      TRUE
                    }
                    
                  }else{
                    # ***WATER HARVESTING*** Do both treatment & control have Water Harvesting?
                    if(unlist(Z[ii,Linked.Tab])[jj]=="WH.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="WH.Out")){
                      if(Verbose){print(paste0("Water Harvesting: ii = ",ii," | jj = ",jj))}
                      
                      # This should be OK by default. We can have multiple WH practices, but these do not have levels.
                      # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                      TRUE
                      
                    }else{
                      # ***POST HARVEST*** Do both treatment & control have Post Harvest?
                      if(unlist(Z[ii,Linked.Tab])[jj]=="PO.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="PO.Out")){
                        if(Verbose){print(paste0("Post Harvest: ii = ",ii," | jj = ",jj))}
                        
                        # This should be OK by default. We can have multiple postharvest practices, but these do not have levels.
                        # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                        TRUE
                        
                      }else{
                        # ***ENERGY*** Do both treatment & control have Energy?
                        if(unlist(Z[ii,Linked.Tab])[jj]=="E.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="E.Out")){
                          if(Verbose){print(paste0("Energy: ii = ",ii," | jj = ",jj))}
                          
                          # This should be OK by default. We can have multiple energy practices, but these do not have levels.
                          # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                          TRUE
                          
                        }else{
                          # ***RESIDUES*** Do both treatments have Residues? If so the material, amount, date and method of application must match for them to be compared
                          if(unlist(Z[ii,Linked.Tab])[jj]=="Res.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Res.Out")){
                            if(Verbose){print(paste0("Residues: ii = ",ii," | jj = ",jj))}
                            
                            Trt<-Data[Z[ii,Y.N],M.Level.Name]
                            Control<-Data[j,M.Level.Name]
                            
                            # Check if residues are crop residues that have varying application levels, check if MT.Out$T.Name matches the M.Level.Name field in Res.Method
                            if(is.na(Trt) & is.na(Control)){
                              Trt<-Data[Z[ii,Y.N],T.Name]
                              Control<-Data[j,T.Name]
                              
                              if(Trt %in% unlist(Res.Method[B.Code == BC,M.Level.Name])){
                                Trt
                              }else{
                                NA
                              }
                              
                              if(Control %in% unlist(Res.Method[B.Code == BC,M.Level.Name])){
                                Control
                              }else{
                                NA
                              }
                            }
                            
                            if(is.na(Trt) & is.na(Control)){
                              TRUE # Assume levels must be the same
                            }else{
                              
                              if(do_times){
                              Trt1<-Res.Method[M.Level.Name==Trt & 
                                                 B.Code == BC & 
                                                 Time %in% c(Data$Time[1],"All Times") & 
                                                 Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                               c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
                              
                              Control1<-Res.Method[M.Level.Name==Control  & 
                                                     B.Code == BC & 
                                                     Time %in% c(Data$Time[1],"All Times") & 
                                                     Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                                   c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
                              }else{
                                Trt1<-Res.Method[M.Level.Name==Trt & B.Code == BC,c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
                                
                                Control1<-Res.Method[M.Level.Name==Control& B.Code == BC,c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
                              }
                              
                              if(identical(Trt1,Control1)){TRUE}else{FALSE}
                              
                            }
                            
                          }else{
                            
                            if(Verbose){print(paste0("Simple: ii = ",ii," | jj = ",jj))}
                            
                            # Simple cases where the treatment level names need be identical for practices shared between control and treatment
                            # Control Field
                            COL<-unlist(Z[ii,Linked.Col])[jj]
                            if(!is.na(COL)){
                              # Does control value (left) equal treament value (right)
                              Data[N==i,..COL] == Data[N== Z[ii,N],..COL]
                            }else{
                              # If control codes are NA (no ERA practice) then it could be comparible to other treatments that are not NA and have an ERA practice
                              !is.na(unlist(Z$Prac.Code)[ii])
                            }
                          }}}}}}}}
            
          }))
        }
        
      })
      
      # All Level.Checks must be true for comparison to be valid
      Z[,Level.Check:=all(unlist(Level.Check)),by="Y.N"]
      
      if(Debug){
        Z
      }else{
        # Return rows from master table that are valid treatments for this control (Level.Check==T)
        
        Z<-Z[Level.Check==T,]
        if(nrow(Z)>0){
          if(!Return.Lists){
            data.table(N = Y[j,N],Control.For=Z[,N],Mulch.Code=Z[,Mulch.Code],B.Code=BC)
          }else{
            data.table(N = Y[j,N],Control.For=list(Z[,N]),Mulch.Code=list(Z[,Mulch.Code]),B.Code=BC)
          }
        }else{}
      }
    }else{}
    
  }))
} # Setting Debug to T prints comparison table rather than row numbers
