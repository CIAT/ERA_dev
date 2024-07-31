# *****Comparison logic***** =====

# Required packages ####
require(data.table)
require(miceadds)
require(doSNOW)
require(pbapply)

Cores<-14

# Read in data ####

# Read imported excel data
Tables_2020<-miceadds::load.Rdata2(file="ERA V1.1 Tables 2023-06-22.RData",path="Data/Compendium Master Database")

AF.Out<-Tables_2020$AF.Out
Animals.Out<-Tables_2020$Animals.Out
Animals.Diet<-Tables_2020$Animals.Diet
Animals.Diet.Comp<-Tables_2020$Animals.Diet.Comp
Data.Out<-Tables_2020$Data.Out
Fert.Method<-Tables_2020$Fert.Method
Fert.Out<-Tables_2020$Fert.Out
Int.Out<-Tables_2020$Int.Out
Irrig.Out<-Tables_2020$Irrig.Out
MT.Out2<-Tables_2020$MT.Out2
Plant.Out<-Tables_2020$Plant.Out
Res.Method<-Tables_2020$Res.Method
Rot.Levels<-Tables_2020$Rot.Levels
Rot.Out<-Tables_2020$Rot.Out
Rot.Seq<-Tables_2020$Rot.Seq
Rot.Seq.Summ<-Tables_2020$Rot.Seq.Summ
Site.Out<-Tables_2020$Site.Out
Soil.Out<-Tables_2020$Soil.Out
Times<-Tables_2020$Times
Var.Out<-Tables_2020$Var.Out


Rot.Seq2<-Rot.Seq[,list(Time=paste(Time,collapse="|||"),
                        Treatment=paste(`Rotation Component`,collapse="|||"),
                        Products=paste(R.Prod,collapse="|||")),by=ID]


# Read in codes and harmonization datasets
EUCodes<-fread("Concept Scheme/EU.csv",header=T,strip.white=F,encoding="Latin-1")
MasterLevels<-fread("Concept Scheme/Levels.csv",header=T,strip.white=F,encoding="Latin-1")
PracticeCodes<-fread("Concept Scheme/Practices.csv",header=T,strip.white=F,encoding="Latin-1")
PracticeCodes1<-data.table::copy(PracticeCodes)
TreeCodes<-fread("Concept Scheme/Trees.csv",header=T,strip.white=F,encoding="Latin-1")

# ***Automated Comparison of Control vs Treatments*** ####
# 1) Basic ####
# Remove Controls for Ratio Comparisons
Data.Out.No.Agg<-Data.Out[ED.Ratio.Control!=T ]
# Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
# Data.Out.No.Agg<-Data.Out.No.Agg[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
# Remove Aggregated Observations (dealt with in section 1.1)
Data.Out.No.Agg<-Data.Out.No.Agg[!grepl("[.][.][.]",T.Name2)]
# Remove outcomes aggregated over rot/int entire sequence or system (dealt with sections 1.2 and 1.3)
Data.Out.No.Agg<-Data.Out.No.Agg[!is.na(ED.Treatment)]
# Remove animal data (dealt with in section 2)
Data.Out.No.Agg<-Data.Out.No.Agg[!Data.Out.No.Agg$Out.Subind %in% c("Weight Gain","Meat Yield","Milk Yield","Egg Yield","Other Animal Product Yield","Reproductive Yield",
                                                                    "Feed Conversion Ratio (Out In)","Feed Conversion Ratio (In Out)","Protein Conversion Ratio (Out In)",
                                                                    "Protein Conversion Ratio (In Out)")]

Data.Out.No.Agg<-Data.Out.No.Agg[!B.Code %in% Animals.Out$B.Code]

# Combine all Practice Codes together & remove h-codes
Join.T<-function(A,B,C,D){
  X<-c(A,B,C,D)
  X<-unlist(strsplit(X,"-"))
  X<-unique(X[!is.na(X)])
  if(length(grep("h",X))>0){
    X<-X[-grep("h",X)]
    if(length(X)==0){list(NA)}else{list(X)}
  }else{
    if(length(X)==0){list(NA)}else{list(X)}
  }
}

X<-Data.Out.No.Agg[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N"]
Data.Out.No.Agg[,Final.Codes:=X$Final.Codes]
rm(X)

# Calculate Number of ERA Practices
Data.Out.No.Agg[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]

# Extract B.Codes
B.Codes<-unique(Data.Out.No.Agg$B.Code)

DATA<-Data.Out.No.Agg

  # Basic: Select fields that need to match for comparisons to be valid ####
  CompareWithin<-c("ED.Site.ID","ED.Product.Simple","ED.Product.Comp","ED.M.Year", "ED.Outcome","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
                   "ED.Harvest.End","ED.Harvest.DAS","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","C.Structure","P.Structure","O.Structure",
                   "W.Structure","B.Code","Country","ED.Comparison")
  # Basic: Create Comparison function ####
  # ISSUE: Diversification residue re-coding is for any residues, whereas we are interested in only experimental crop residues.
  Compare.Fun<-function(Data,Verbose,Debug,PracticeCodes1,Return.Lists){
    
    Match.Fun<-function(A,B){
      A<-unlist(A)
      B<-unlist(B)
      list(A[!A %in% B])
    }
    
    Match.Fun2<-function(A,B,C){
      A<-unlist(A)
      B<-unlist(B)
      C<-unlist(C)
      list(unique(C[match(A,B)]))
    }
    
    # Exception for residue mulching being compared to residue incorporation
    Mulch.Fun<-function(A,B,X,Z){
      if(A %in% X){
        Z[Mulch.Flag==F,Mulch.Flag:=Z[,any(unlist(Final.Codes) %in% B),by="N"][,V1]]
        Z[Mulch.Flag==T,Match:=Match+1]
        Z[Mulch.Flag==T,NoMatch:=NoMatch-1]
        Z[Mulch.Flag==T,Mulch.Code:=paste0(Mulch.Code,A,collapse = "-"),by="N"]
        
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
    
    # Deal with residues codes in diversification comparisons ####
    # Recode AF prunings to a15 codes
    # Recode Mulch or Incorporation codes by removing ".n" code suffix
    Dpracs<-c("Crop Rotation","Intercropping","Improved Fallow","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
    if(any(unlist(Y[,Final.Codes]) %in%  PracticeCodes1[Practice %in% Dpracs,Code])){
      
      if(Verbose){print(paste0(BC," - Simplifying Residue Codes"))}
      
      PC1<-PracticeCodes1[Practice %in% c("Agroforestry Pruning"),Code]
      PC2<-PracticeCodes1[Practice %in% c("Mulch","Crop Residue","Crop Residue Incorporation"),Code] 
      
      
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
      if(Verbose){print(paste(BC," - Group ",Data$Group[1]," - N =",j))}
      X<-unlist(Data$Final.Codes[j])
      i<-N[j]
      
      if(is.na(X[1])){
        Z<-Y[N!=i & !is.na(Final.Codes)
        ][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
        ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
        ][Match>=0 & NoMatch>0] # Keep instances for which this treatment can be a control for other treatments
        
        Z[,Mulch.Code:=as.character("")][,Mulch.Flag:=F]
        
        Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
        
        Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N  # Add in column for the codes that are in treatment but not control
        ][,Linked.Tab:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
        ][,Linked.Col:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Col)),by=N]
        
        
      }else{
        
        
        
        # Here we are working from the logic of looking at what other treatments can treatment[i] be a control for?
        Z<-Y[N!=i][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
        ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
        ]
        
        # Exception for comparison of mulched residues to incorporated residues.
        Z[,Mulch.Code:=as.character("")][,Mulch.Flag:=F]
        
        for(kk in 1:length(Mulch.C.Codes)){
          Z<-Mulch.Fun(Mulch.C.Codes[kk],Mulch.T.Codes[kk],X,Z)
          
        }
        
        # Keep instances for which this treatment can be a control for other treatments
        Z<-Z[Match>=length(X) & NoMatch>0] 
        
        # There was a bug here where Control.Code was set to :=list(X), but if X was the same length as nrow(Z) this led to issues
        Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
        
        # Remove Mulch Code from Control Code where Mulch.Flag == T
        Z[,Control.Code:=Z[,list(Control.Code=list(unlist(Control.Code)[!unlist(Control.Code) %in% Mulch.Code])),by="N"][,Control.Code]]
        
        
        Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N  # Add in column for the codes that are in treatment but not control
        ][,Linked.Tab:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
        ][,Linked.Col:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Col)),by=N]
        
      }
      
      if(nrow(Z)>0){
        Z$Level.Check<-lapply(1:nrow(Z),FUN=function(ii){
          if(length(unlist(Z[ii,Linked.Tab]))==0){
            !is.na(unlist(Z$Prac.Code)[ii])
          }else{
            unlist(lapply(1:length(unlist(Z[ii,Linked.Tab])),FUN=function(jj){
              # ***FERTILIZER*** Do both treatments have fertilizer? If so the sequences must match 
              if(unlist(Z[ii,Linked.Tab])[jj]=="Fert.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Fert.Out")){
                if(Verbose){print(paste0("Fert: ii = ",ii," | jj = ",jj))}
                
                Trt<-Data[Z[ii,Y.N],F.Level.Name]
                Control<-Data[j,F.Level.Name]
                
                # ISSUE - Fertilizer codes from rotation sequence with outcome reported for entire sequence
                # Do we need to update Level.Name fields with aggregated names from Int/Rot Sequence? 
                # Guess this would be done through combining MT.Out2 entries.
                
                if(Trt==Control){
                  # If Trt & Control Have the same Fertilizer Level Name then no need for further investigation.
                  TRUE
                }else{ 
                  # Trt & Control have different names so we need to compare these in more detail
                  
                  # Extract data from Fert.Method table for Treatment and Control
                  # Choose fields to match on 
                  Fert.Fields<-c("F.Level.Name","F.Category","F.Type","F.NPK","F.Amount","F.Unit","F.Method",
                                 "F.Fate","F.Date","F.Date.Stage","F.Date.DAP","F.Date.DAE")
                  
                  Trt1<-Fert.Method[B.Code==BC & F.Level.Name==Trt,!c("F.Date.Text","F.Level.Name2")]  
                  Control1<-Fert.Method[B.Code==BC & F.Level.Name==Control,!c("F.Date.Text","F.Level.Name2")]  
                  
                  # Are there any Fert.Method rows in the Control not in the Treatment? 
                  # Note that the fertilizer being applied could have the same identity but still differ in amount applied 
                  # Amount can be recorded as total amount applied to plot in the Fert.Out tab and not for each row in the Fert.Method Tab
                  N.Control<-data.table(Level=c(rep("Control",nrow(Control1)),rep("Trt",nrow(Trt1))),Dup=duplicated(rbind(Control1[,!"F.Level.Name"],Trt1[,!"F.Level.Name"])))[Level=="Trt"]
                  
                  # 1) For rows that match:
                  # 1.1) For shared inorganic NPK practices do the NPK values match? (check any 999999 values are set to NA) ####
                  Shared.FCodes<-unique(unlist(Trt1[N.Control$Dup,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                  Shared.FCodes<-Shared.FCodes[!is.na(Shared.FCodes)]
                  
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
                  NotShared.FCodes<-unique(unlist(Trt1[!N.Control$Dup,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                  NotShared.FCodes<-NotShared.FCodes[!is.na(NotShared.FCodes)]
                  
                  if(any(Shared.FCodes %in% c("b29","b30","b75","b67")) & !any(NotShared.FCodes %in% c("b29","b30","b75","b67"))){
                    FO.Logic<-paste(as.character(unlist(Trt2[,c("F.NO","F.PO","F.KO")])),collapse = " ") == paste(as.character(unlist(Control2[,c("F.NO","F.PO","F.KO")])),collapse = " ")
                  }else{
                    FO.Logic<-T
                  }
                  
                  # 2) For rows that don't match do they correspond to a shared practice code? If so these are part of the same practice and cannot be compared) ####
                  # The row in the Trt that does not match the control should have a code that is not present in the control
                  F.ControlCode<-unique(unlist(Control1[,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                  F.ControlCode<-F.ControlCode[!is.na(F.ControlCode)]
                  
                  FNoMatch.Logic<-!NotShared.FCodes %in% F.ControlCode
                  
                  # 3) If control has practices not in the treatment then it is an invalid comparison) ####
                  F.TCode<-unique(unlist(Trt1[,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                  F.TCode<-F.TCode[!is.na(F.TCode)]
                  
                  # 4) For Practices with NoCode do rows match?
                  FCinT.Logic<-all(F.ControlCode %in% F.TCode)
                  
                  all(FI.Logic,FO.Logic,FNoMatch.Logic,FCinT.Logic)
                  
                }
                
              }else{
                # ***ROTATION*** Do both treatments have rotation? If so the sequences must match
                if(unlist(Z[ii,Linked.Tab])[jj]=="Rot.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="Rot.Out")){
                  if(Verbose){print(paste0("Rotation: ii = ",ii," | jj = ",jj))}
                  # Rotation will need to be compared by matching crop sequences
                  
                  Trt<-Data[Z[ii,Y.N],ED.Rot]
                  Control<-Data[j,ED.Rot]
                  
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
                    
                    Trt<-Data[Z[ii,Y.N],IN.Level.Name2]
                    Control<-Data[j,IN.Level.Name2]
                    
                    # Test if crops used are the same.
                    Trt.Comp<-Data[Z[ii,Y.N],IN.Prod]
                    Int.Comp<-Data[j,IN.Prod]
                    
                    if(all(Trt.Comp %in% Int.Comp)){
                      
                      # Test if Planting Density is 95% similar between treatments 
                      Trt1<-Plant.Out[match(unlist(strsplit(Trt,"[*][*][*]")),Plant.Out[,P.Level.Name])
                      ][,c("P.Product","Plant.Density","B.Code")][order(P.Product)][B.Code==BC,-"B.Code"][!is.na(Plant.Density)]
                      
                      Control1<-Plant.Out[match(unlist(strsplit(Control,"[*][*][*]")),Plant.Out[,P.Level.Name])
                      ][,c("P.Product","Plant.Density","B.Code")][order(P.Product)][B.Code==BC,-"B.Code"][!is.na(Plant.Density)]
                      
                      # Situation where information might be incomplete for one treatment
                      if(nrow(Trt1) != nrow(Control1)){
                        Trt1<-Trt1[P.Product %in% Control1[,P.Product]]
                        Control1<-Control1[P.Product %in% Trt1[,P.Product]]
                      }
                      
                      # Skip if no density information is available
                      if(nrow(Trt1)==0){TRUE}else{
                        
                        # Planting Density needs to be 95% similar in all treatments
                        if(all((Trt1[,Plant.Density]/Control1[,Plant.Density])>=0.95)){TRUE}else{FALSE}
                        
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
                        
                        Trt1<-Irrig.Out[I.Name==Trt  & B.Code == BC,c("I.Amount","I.Unit","I.Water.Type")]
                        Control1<-Irrig.Out[I.Name==Control  & B.Code == BC,c("I.Amount","I.Unit","I.Water.Type")]
                        
                        if(identical(Trt1,Control1)){TRUE}else{FALSE}
                      }else{
                        # Shared practices must be drip or sprinkler and we assume these are the same.
                        TRUE
                      }
                      
                    }else{
                      # ***Water Harvesting*** Do both treatment & control have Water Harvesting?
                      if(unlist(Z[ii,Linked.Tab])[jj]=="WH.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="WH.Out")){
                        if(Verbose){print(paste0("Water Harvesting: ii = ",ii," | jj = ",jj))}
                        
                        # This should be OK by default. We can have multiple WH practices, but these do not have levels.
                        # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                        TRUE
                        
                      }else{
                        # ***Post Harvest*** Do both treatment & control have Post Harvest?
                        if(unlist(Z[ii,Linked.Tab])[jj]=="PO.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="PO.Out")){
                          if(Verbose){print(paste0("Post Harvest: ii = ",ii," | jj = ",jj))}
                          
                          # This should be OK by default. We can have multiple postharvest practices, but these do not have levels.
                          # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                          TRUE
                          
                        }else{
                          # ***Energy*** Do both treatment & control have Energy?
                          if(unlist(Z[ii,Linked.Tab])[jj]=="E.Out" & !is.na(unlist(Z[ii,Linked.Tab])[jj]=="E.Out")){
                            if(Verbose){print(paste0("Energy: ii = ",ii," | jj = ",jj))}
                            
                            # This should be OK by default. We can have multiple energy practices, but these do not have levels.
                            # This could be an issue if we have mutilple levels of a base practice shared btw control and treatment
                            TRUE
                            
                          }else{
                            # ***Residues*** Do both treatments have Residues? If so the material, amount, date and method of application must match for them to be compared
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
                                
                                Trt1<-Res.Method[M.Level.Name==Trt & B.Code == BC ,c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
                                Control1<-Res.Method[M.Level.Name==Control  & B.Code == BC,c("M.Tree","M.Material","M.Amount","M.Fate","M.Date","M.Cover","M.Date.Stage","M.Date.DAP")]
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
  
  # Basic: Run Comparisons ####
  # ISSUE: IMPROVED VARIETIES IV heat etc. vs IV other? ####
  Verbose<-F
  
  cl<-makeCluster(Cores)
  clusterEvalQ(cl, list(library(data.table)))
  clusterExport(cl,list("DATA","CompareWithin","Verbose","B.Codes","Compare.Fun","PracticeCodes","PracticeCodes1","Fert.Method","Plant.Out",
                        "Fert.Out","Int.Out","Irrig.Out","Res.Method","MT.Out2","Rot.Seq.Summ"),envir=environment())
  registerDoSNOW(cl)
  
  Comparisons<-parLapply(cl,unique(DATA$B.Code),fun=function(BC){
    
    #Comparisons<-pblapply(B.Codes,FUN=function(BC){
    
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(Verbose){print(paste0(BC," Subgroup = ", i))}
      
      Compare.Fun(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F,PracticeCodes1,Return.Lists=F)
      
    }))
    
  })
  
  stopCluster(cl)
  
  Comparisons<-unique(rbindlist(Comparisons))
  
  # Basic: Validation - list studies with no comparisons at all (could be studies with system or aggregated outcomes) ####
  Comparisons.Simple.All.NA<-DATA[!B.Code %in% Comparisons[,B.Code],unique(B.Code)]
  if(F){
    if(length(Comparisons.Simple.All.NA)!=0){
      View(Comparisons.Simple.All.NA)
      write.table(Comparisons.Simple.All.NA,"clipboard",row.names = F,sep="\t")
    }
  }
  rm(Comparisons.Simple.All.NA,X)
  
  # Basic: Restructure and save
  # Remove NA values
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  
  Cols<-c("ED.Treatment","ED.Int","ED.Rot")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.No.Agg[match(Comparisons[,N],N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
  setnames(Comparisons1,"ED.Treatment","Control.Trt")
  setnames(Comparisons1,"ED.Int","Control.Int")
  setnames(Comparisons1,"ED.Rot","Control.Rot")
  
  Comparisons1[,Compare.Trt:=Data.Out.No.Agg[match(Control.For,N),ED.Treatment]]
  Comparisons1[,Compare.Int:=Data.Out.No.Agg[match(Control.For,N),ED.Int]]
  Comparisons1[,Compare.Rot:=Data.Out.No.Agg[match(Control.For,N),ED.Rot]]
  
  # fwrite(Comparisons1,paste0(choose.dir(),"\\Basic_Comparisons_V1.8.csv"),row.names = F)
  
  Comparison.List<-list(Simple=Comparisons1)
  
  # 1.1) Aggregated Treatments (Not Animals) ####
  
  # Extract Aggregated Observations
  Data.Out.Agg<-Data.Out[grep("[.][.][.]",T.Name2)]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Agg<-Data.Out.Agg[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Agg<-Data.Out.Agg[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
  # Ignore outcomes aggregated over rot/int entire sequence or system
  Data.Out.Agg<-Data.Out.Agg[!is.na(ED.Treatment)]
  
  # Ignore animal data
  Data.Out.Agg<-Data.Out.Agg[!Data.Out.Agg$Out.Subind %in% c("Weight Gain","Meat Yield","Milk Yield","Egg Yield","Other Animal Product Yield","Reproductive Yield",
                                                             "Feed Conversion Ratio (Out In)","Feed Conversion Ratio (In Out)","Protein Conversion Ratio (Out In)",
                                                             "Protein Conversion Ratio (In Out)")]
  
  Data.Out.Agg<-Data.Out.Agg[!B.Code %in% Animals.Out$B.Code]
  
  X<-Data.Out.Agg[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N"]
  Data.Out.Agg[,Final.Codes:=X$Final.Codes]
  rm(X)
  
  Data.Out.Agg[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N]
  
  # Calculate Number of ERA Practices
  Data.Out.Agg[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
  
  # Explore Data
  if(F){
    X<-unique(Data.Out.Agg[,c("B.Code","T.Codes.No.Agg","T.Codes.Agg","Final.Codes2","T.Agg.Levels2","T.Agg.Levels3")])
    X[,N.Pracs:=length(Final.Codes2),by=c("B.Code","T.Agg.Levels3")]
    write.table(X,"clipboard-256000",row.names = F,sep="\t")
    
    Y<-unique(Data.Out.Agg[,c("B.Code","T.Name2","T.Codes.No.Agg","T.Codes.Agg","Final.Codes2","T.Agg.Levels2","T.Agg.Levels3")])
    
    Y[,N.Pracs:=X[match(Y[,paste(B.Code,T.Codes.No.Agg,T.Codes.Agg,Final.Codes2,T.Agg.Levels2,T.Agg.Levels3)],
                        X[,paste(B.Code,T.Codes.No.Agg,T.Codes.Agg,Final.Codes2,T.Agg.Levels2,T.Agg.Levels3)]),N.Pracs]]
    
    write.table(Y[N.Pracs<2],"clipboard-256000",row.names = F,sep="\t")
    
    write.table(Y,"clipboard-256000",row.names = F,sep="\t")
  }
  
  
  CompareWithin<-c("ED.Site.ID","ED.Product.Simple","ED.Product.Comp","ED.M.Year", "ED.Outcome","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
                   "ED.Harvest.End","ED.Harvest.DAS","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","C.Structure","P.Structure","O.Structure",
                   "W.Structure","B.Code","Country","T.Agg.Levels3","ED.Comparison")
  
  DATA<-Data.Out.Agg
  
  Verbose<-F
  
  cl<-makeCluster(Cores)
  clusterEvalQ(cl, list(library(data.table)))
  clusterExport(cl,list("DATA","CompareWithin","Verbose","B.Codes","Compare.Fun","PracticeCodes","PracticeCodes1","Fert.Method","Plant.Out",
                        "Fert.Out","Int.Out","Irrig.Out","Res.Method","MT.Out2","Rot.Seq.Summ"),envir=environment())
  registerDoSNOW(cl)
  
  # Fert is separated into columns now.....so how to deal with? remove F.Level from CompareFun?
  
  Comparisons<-parLapply(cl,unique(DATA[,B.Code]),fun=function(BC){
    
    #Comparisons<-pblapply(unique(DATA[,B.Code]),FUN=function(BC){
    
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(Verbose){print(paste0(BC," Subgroup = ", i))}
      
      Compare.Fun(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F,PracticeCodes1,Return.Lists=F)
      
    }))
    
  })
  
  stopCluster(cl)
  
  Comparisons<-unique(rbindlist(Comparisons))
  
  # Restructure and save
  # Remove NA values
  Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  
  Cols<-c("ED.Treatment","ED.Int","ED.Rot")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Agg[match(Comparisons[,N],N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
  setnames(Comparisons1,"ED.Treatment","Control.Trt")
  setnames(Comparisons1,"ED.Int","Control.Int")
  setnames(Comparisons1,"ED.Rot","Control.Rot")
  
  Comparisons1[,Compare.Trt:=Data.Out.Agg[match(Control.For,N),ED.Treatment]]
  Comparisons1[,Compare.Int:=Data.Out.Agg[match(Control.For,N),ED.Int]]
  Comparisons1[,Compare.Rot:=Data.Out.Agg[match(Control.For,N),ED.Rot]]
  
  # write.table(Comparisons1,"clipboard-256000",row.names = F,sep="\t")
  
  Comparison.List[["Aggregated"]]<-Comparisons1
  
  # 1.2) Intercropping System Outcomes ####
  
  # Extract outcomes aggregated over rot/int entire sequence or system
  Data.Out.Int<-Data.Out[is.na(ED.Treatment) & !is.na(ED.Int)]
  
  # Replace T-Codes with Intercrop T-Codes
  Data.Out.Int[,T.Codes:=IN.T.Codes]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Int<-Data.Out.Int[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Int<-Data.Out.Int[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
    # 1.2.1) Scenario 1: Intercrop vs Intercrop ####
    
    # Replace Treatment Name with Intercropping Name
    Data.Out.Int2<-data.table::copy(Data.Out.Int)
    Data.Out.Int2[,ED.Treatment:=ED.Int]
    
    # Combine intercropped treatment information using a similar process to aggregated trts in the MT.Out2 table ####
    F.Master.Codes<-PracticeCodes[Theme=="Nutrient Management" | Subpractice =="Biochar",Code]
    F.Master.Codes<-F.Master.Codes[!grepl("h",F.Master.Codes)]
    
    Fields<-data.table(Levels=c("T.Residue.Prev",colnames(MT.Out2)[grep("Level.Name",colnames(MT.Out2))]),
                       Codes =c("T.Residue.Code","AF.Codes","A.Codes",NA,"E.Codes","H.Codes","I.Codes","M.Codes","F.Codes",
                                "pH.Codes",NA,"PO.Codes","Till.Codes","V.Codes","WH.Codes",NA,NA,NA))
    Fields<-Fields[!grepl("F.Level.Name",Levels)]
    Fields<-rbind(Fields,data.table(Levels=F.Master.Codes,Codes=paste0(F.Master.Codes,".Code")))
    
    N<-which(!is.na(Data.Out.Int[,ED.Int]))
    
    # Annoying code to correct structure in MT.Out2, this could be done earlier in the script with MT.Out2
    # but I am concerned about having to debug cascading effects
    
    MT.Out3<-MT.Out2
    MT.Out2[!is.na(P.Structure),P.Structure:=P.Level.Name]
    MT.Out2[!is.na(O.Structure),O.Structure:=O.Level.Name]
    MT.Out2[!is.na(W.Structure),W.Structure:=W.Level.Name]
    MT.Out2[!is.na(C.Structure),C.Structure:=C.Level.Name]
    MT.Out2[is.na(P.Structure),P.Structure:=NA]
    MT.Out2[is.na(O.Structure),O.Structure:=NA]
    MT.Out2[is.na(W.Structure),W.Structure:=NA]
    MT.Out2[is.na(C.Structure),C.Structure:=NA]
    MT.Out2[,P.Level.Name:=P.Structure]
    MT.Out2[,O.Level.Name:=O.Structure]
    MT.Out2[,W.Level.Name:=W.Structure]
    MT.Out2[,C.Level.Name:=C.Structure]
    
    # SLOW ADD PARALLEL
    Data.Out.Int2<-rbindlist(pblapply(1:nrow(Data.Out.Int2),FUN=function(i){
      
      if(i %in% N){
        
        # Deal with ".." delim used in Fert tab and Varieties tab that matches ".." delim used to aggregate treatments in Data.Out.Int2 tab
        # Above should not be required anyone as Var delim changed to "$$" and combined fertilizers dis-aggregated.
        
        Trts<-Data.Out.Int2[i,c("IN.Level.Name2","F.Level.Name","V.Level.Name")]  
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
        
        Trts<-unlist(strsplit(Trts$IN.Level.Name2,"[*][*][*]")) 
        
        Trts2<-Trts
        Trts<-gsub("---","..",Trts)
        Study<-Data.Out.Int2[i,B.Code]
        
        Y<-MT.Out2[T.Name2 %in% Trts & B.Code == Study]
        
        # Aggregated Treatments: Split T.Codes & Level.Names into those that are the same and those that differ between treatments
        # This might need some more nuance for fertilizer treatments?
        Fields1<-Fields
        
        # Exclude Other, Chemical, Weeding or Planting Practice Levels if they do no structure outcomes.
        Exclude<-c("O.Level.Name","P.Level.Name","C.Level.Name","W.Level.Name")[is.na(apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure")],2,unique))]
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
        }),collapse="***")
        
        if("F.Level.Name" %in% COLS){
          
          COLS2<-gsub("F.Level.Name","F.Level.Name2",COLS)
          
          Agg.Levels3<-paste(apply(Y[,..COLS2],1,FUN=function(X){
            X[as.vector(is.na(X))]<-"NA"
            paste(X,collapse="---")
          }),collapse="***")
          
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
          CODES.IN<-paste0(CODES.IN,collapse="***")
        }
        
        # Collapse into a single row using "***" delim to indicate a treatment aggregation
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
            X<-paste0(X,collapse = "***")
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
        Y$T.Name2<-Y$IN.Level.Name2
        Y$T.Name<-Data.Out.Int2[i,ED.Int]
        Y[,N2:=NULL]
        
        
        Y<-data.frame(Y)
        Z<-data.frame(Data.Out.Int2[i])
        Y.Cols<-match(colnames(Y),colnames(Z))
        Z[,Y.Cols]<-Y
        Z<-data.table(Z)
        
        Z$IN.Agg.Levels<-Agg.Levels
        Z$IN.Agg.Levels2<-Agg.Levels2
        Z$IN.Agg.Levels3<-Agg.Levels3
        Z$IN.Codes.No.Agg<-CODES.OUT
        Z$IN.Codes.Agg<-CODES.IN
        Z
      }else{
        Y<-Data.Out.Int2[i]
        Y$IN.Agg.Levels<-NA
        Y$IN.Agg.Levels2<-NA
        Y$IN.Agg.Levels3<-NA
        Y$IN.Codes.No.Agg<-NA
        Y$IN.Codes.Agg<-NA
        Y
      }
      
    }))
    
    MT.Out2<-MT.Out3
    rm(MT.Out3)
    
    
    # Combine all Practice Codes together & remove h-codes
    # In intercropping we consider any practice present in components to be present
    
    Data.Out.Int2[,N2:=1:.N]
    X<-Data.Out.Int2[,list(Final.Codes=Join.T(IN.T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N2"]
    Data.Out.Int2[,Final.Codes:=X$Final.Codes]
    Data.Out.Int2[,N2:=NULL]
    rm(X)
    
    Data.Out.Int2[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N]
    
    # Calculate Number of ERA Practices
    Data.Out.Int2[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
    
    # Explore Data
    if(F){
      X<-unique(Data.Out.Int2[,c("B.Code","IN.Codes.No.Agg","IN.Codes.Agg","Final.Codes2","IN.Agg.Levels2","IN.Agg.Levels3")])
      X[,N.Pracs:=length(Final.Codes2),by=c("B.Code","IN.Agg.Levels3")]
      write.table(X,"clipboard-256000",row.names = F,sep="\t")
      
      Y<-unique(Data.Out.Int2[,c("B.Code","T.Name2","IN.Codes.No.Agg","IN.Codes.Agg","Final.Codes2","IN.Agg.Levels2","IN.Agg.Levels3")])
      
      Y[,N.Pracs:=X[match(Y[,paste(B.Code,IN.Codes.No.Agg,IN.Codes.Agg,Final.Codes2,IN.Agg.Levels2,IN.Agg.Levels3)],
                          X[,paste(B.Code,IN.Codes.No.Agg,IN.Codes.Agg,Final.Codes2,IN.Agg.Levels2,IN.Agg.Levels3)]),N.Pracs]]
      
      write.table(Y[N.Pracs<2],"clipboard-256000",row.names = F,sep="\t")
      
      write.table(Y,"clipboard-256000",row.names = F,sep="\t")
    }
    
    # Run Comparisons ####
    
    # We cannot compare different types of intercrop so Levels and cropping sequence should be the same
    # The basic comparison grouping variables on Data.Out.Int dataset should work.
    CompareWithin<-c("ED.Site.ID","ED.Product.Simple","ED.Product.Comp","ED.M.Year", "ED.Outcome","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
                     "ED.Harvest.End","ED.Harvest.DAS","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","C.Structure","P.Structure","O.Structure",
                     "W.Structure","B.Code","Country","IN.Agg.Levels3","ED.Comparison")
    
    DATA<-Data.Out.Int2
    
    Verbose<-F
    
    cl<-makeCluster(Cores)
    clusterEvalQ(cl, list(library(data.table)))
    clusterExport(cl,list("DATA","CompareWithin","Verbose","B.Codes","Compare.Fun","PracticeCodes","PracticeCodes1","Fert.Method","Plant.Out",
                          "Fert.Out","Int.Out","Irrig.Out","Res.Method","MT.Out2","Rot.Seq.Summ"),envir=environment())
    registerDoSNOW(cl)
    
    # Fert is separated into columns now.....so how to deal with? remove F.Level from CompareFun?
    
    Comparisons<-parLapply(cl,unique(DATA[,B.Code]),fun=function(BC){
      
      #Comparisons<-pblapply(unique(DATA[,B.Code]),FUN=function(BC){
      
      Data.Sub<-DATA[B.Code==BC]
      CW<-unique(Data.Sub[,..CompareWithin])
      CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
      Data.Sub[,Group:=CW]
      
      rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
        if(Verbose){print(paste0(BC," Subgroup = ", i))}
        
        Compare.Fun(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F,PracticeCodes1,Return.Lists=F)
        
      }))
      
    })
    
    stopCluster(cl)
    
    
    Comparisons<-unique(rbindlist(Comparisons))
    
    # Basic: Restructure and save
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    
    # Restructure and save
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    
    Cols<-c("ED.Treatment","ED.Int","ED.Rot")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Int2[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"ED.Treatment","Control.Trt")
    setnames(Comparisons1,"ED.Int","Control.Int")
    setnames(Comparisons1,"ED.Rot","Control.Rot")
    
    Comparisons1[,Compare.Trt:=Data.Out.Int2[match(Control.For,N),ED.Treatment]]
    Comparisons1[,Compare.Int:=Data.Out.Int2[match(Control.For,N),ED.Int]]
    Comparisons1[,Compare.Rot:=Data.Out.Int2[match(Control.For,N),ED.Rot]]
    
    # write.table(Comparisons1,"clipboard-256000",row.names = F,sep="\t")
    
    Comparison.List[["Sys.Int.vs.Int"]]<-Comparisons1
    
    
    # 1.2.2) Scenario 2: Intercrop vs Monocrop ####
    
    # Extract outcomes aggregated over rot/int entire sequence or system
    Data.Out.Int<-Data.Out[is.na(ED.Treatment) & !is.na(ED.Int)]
    # Replace T-Codes with Intercrop T-Codes
    Data.Out.Int[,T.Codes:=IN.T.Codes]
    # Exclude Ratios
    # Remove Controls for Ratio Comparisons
    Data.Out.Int<-Data.Out.Int[ED.Ratio.Control!=T]
    # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
    # Data.Out.Int<-Data.Out.Int[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
    
    Data.Out.Int3<-data.table::copy(Data.Out.Int2)
    Data.Out.Int3[,ED.Treatment:=Data.Out.Int[,ED.Treatment]]
    
    # We will need to consider T.Agg.Levels3 & Structure Cols
    
    CompareWithinInt<-c("ED.Site.ID","ED.M.Year","ED.Outcome","B.Code","Country")
    
    # Create group codes
    Data.Out.Int3[,CodeTemp:=apply(Data.Out.Int3[,..CompareWithinInt],1,paste,collapse = " ")]
    
    # In intercropping we consider any practice present in components to be present
    
    # Update Data.Out Columns to Match Data.Out.Int3
    Data.Out[,N2:=1:.N]
    X<-Data.Out[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N2"]
    Data.Out[,Final.Codes:=X$Final.Codes]
    Data.Out[,N2:=NULL]
    rm(X)
    
    # Calculate Number of ERA Practices
    Data.Out[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
    
    Data.Out[,IN.Agg.Levels:=NA]
    Data.Out[,IN.Agg.Levels2:=NA]
    Data.Out[,IN.Agg.Levels3:=NA]
    Data.Out[,IN.Codes.No.Agg:=NA]
    Data.Out[,IN.Codes.Agg:=NA]
    
    Data.Out[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N]
    
    Data.Out[,CodeTemp:=apply(Data.Out[,..CompareWithinInt],1,paste,collapse = " ")]
    
    # Add in (potential) monoculture controls ####
    Data.Out.Int3<-rbindlist(pblapply(unique(Data.Out.Int3[,CodeTemp]),FUN=function(X){
      
      # Find observations without intercropping or rotation that match the code of the intercropping system outcome
      Y1<-Data.Out[CodeTemp == X & is.na(ED.Int) & is.na(ED.Rot) & !is.na(ED.Treatment)]
      Y2<-Data.Out.Int3[CodeTemp == X]
      
      #print(X)
      
      # Does a match exist in main data? is there a potential control?
      if(nrow(Y1)>0){
        # Match product in monoculture to a product in the intercrop
        W<-rbindlist(lapply(Y1[,unique(ED.Product.Simple)],FUN=function(PROD){
          Z<-Y2[grep(PROD,ED.Product.Simple),]
          # Is there a product match?
          if(nrow(Z)>0){
            # Match mono crop to intercrop and bind rows
            MONO<-Y1[ED.Product.Simple==PROD]
            INT<-Z[,ED.Product.Simple:=PROD]
            rbind(INT,MONO)
          }else{
          }
          
        }))
        
      }else{
        
      }
      
    }))
    
    Data.Out.Int3<-unique(Data.Out.Int3[,!"Final.Codes"])
    Data.Out.Int3[,N3:=1:.N]
    Data.Out.Int3[,Final.Codes:=list(strsplit(Final.Codes2,"-")),by=N3]
    
    # Reset Data.Out
    Data.Out[,CodeTemp:=NULL]
    Data.Out[,Final.Codes:=NULL]
    Data.Out[,Final.Codes2:=NULL]
    Data.Out[,N.Prac:=NULL]
    Data.Out[,IN.Agg.Levels:=NULL]
    Data.Out[,IN.Agg.Levels2:=NULL]
    Data.Out[,IN.Agg.Levels3:=NULL]
    Data.Out[,IN.Codes.No.Agg:=NULL]
    Data.Out[,IN.Codes.Agg:=NULL]
    
    # Update Level/ID Fields
    
    # Function to simplify crop residue codes (nature of residues is ignored, e.g. legume vs non-legume is no longer important)
    Update.Res<-function(Codes){
      if(!is.na(Codes[1])){
        IS.LIST<-is.list(Codes)
        Codes<-unlist(Codes)
        Y<-unlist(strsplit(Codes,"-"))
        if(length(unique(Y))>1){
          Y[grepl("b41",Y)]<-"b41"
          Y[grepl("b40",Y)]<-"b40"
          Y[grepl("b27",Y)]<-"b27"
          Y[Y %in% c("a16","a17")] <- "a15"
          Y[Y %in% c("a16.1","a17.1")] <- "a15.1"
          Y[Y %in% c("a16.2","a17.2")] <- "a15.2"
          Y<-unique(Y)
        }
        if(IS.LIST){
          Y<-list(Y)
        }else{
          Y<-paste(Y[order(Y)],collapse="-")
        }
      }else{
        Y<-NA
      }
      return(Y)
    }
    
    Data.Out.Int3[,Final.Codes:=list(Update.Res(Final.Codes)),by=N3]
    Data.Out.Int3[,Final.Codes2:=Update.Res(Final.Codes2),by=N3]
    Data.Out.Int3[,Final.Residue.Code:=Update.Res(Final.Residue.Code),by=N3]
    Data.Out.Int3[,T.Codes:=Update.Res(T.Codes),by=N3]
    Data.Out.Int3[,IN.T.Codes:=Update.Res(IN.T.Codes),by=N3]
    Data.Out.Int3[,N3:=NULL]
    
    # Run Comparisons ####
    # 1) All T-Codes in the Monocrop should be in the Intercrop
    # 2) Test if all Level/Fields in the Monocrop are in the intercrop
    
    Data.Out.Int3[is.na(ED.Treatment),N2:=1:.N]
    
    # Add crop var column that refers to EU x variety in question, use this a level instead of variety so matching is done on product 
    # monocrop only.
    
    X<-rbindlist(pblapply(1:nrow(Data.Out.Int3),FUN=function(i){
      if(!is.na(Data.Out.Int3[i,V.Level.Name])){
        X<-Data.Out.Int3[i,c("B.Code","ED.Product.Simple","V.Level.Name","V.Codes")]
        VAR<-unlist(strsplit(X[,V.Level.Name],"[*][*][*]"))
        VAR<-VAR[!(is.na(VAR)|VAR=="NA")]
        if(length(VAR)>0){
          Z<-Var.Out[B.Code==X[,B.Code] & V.Product==X[,ED.Product.Simple] & V.Var %in% VAR,c("V.Codes","Join")]
          if(nrow(Z)==0){
            # print(X)
            data.table(V.Codes=NA,Join=NA)
          }else{
            Z
          }
          
        }else{
          data.table(V.Codes=NA,Join=NA)
        }
      }else{
        data.table(V.Codes=NA,Join=NA)
      }
    }))
    
    setnames(X,"Join","V.Level.Name")
    
    # Remove & Replace columns
    Y<-Data.Out.Int3[,c("V.Codes","V.Level.Name")]
    Data.Out.Int3[,V.Codes:=NULL]
    Data.Out.Int3[,V.Level.Name:=NULL]
    Data.Out.Int3<-cbind(Data.Out.Int3,X)
    
    # Incorporation codes in control (to be removed)
    Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
    # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
    Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
    
    Comparisons<-unique(rbindlist(pblapply(Data.Out.Int3[!is.na(N2),N2],FUN=function(i){
      INT<-Data.Out.Int3[N2==i]
      MONOS<-Data.Out.Int3[!is.na(ED.Treatment) & CodeTemp == INT[,CodeTemp] & ED.Product.Simple == INT[,ED.Product.Simple]]
      
      INT.Codes<-unlist(strsplit(c(INT$IN.T.Codes,INT$IN.Residue.Code),"-"))
      INT.Codes<-INT.Codes[!grepl("h",INT.Codes)]
      INT.Codes<-INT.Codes[!is.na(INT.Codes)]
      
      # Are crop residue and practice codes in monoculture in intercrop?
      X<-unlist(lapply(1:nrow(MONOS),FUN=function(j){
        MONO<-MONOS[j]
        MONO.Codes<-unlist(strsplit(c(MONO$T.Codes,MONO$T.Residue.Code),"-"))
        MONO.Codes<-MONO.Codes[!is.na(MONO.Codes)]
        
        # Deal with exception for mulch vs. incorporation
        M.Int<-Mulch.C.Codes[Mulch.C.Codes %in% MONO.Codes & Mulch.T.Codes %in% INT.Codes]
        if(length(M.Int)>0){
          MONO.Codes<-MONO.Codes[MONO.Codes!=M.Int]
        }
        
        
        MONO.Codes<-MONO.Codes[!grepl("h",MONO.Codes)]
        
        if(length(MONO.Codes)==0){
          grepl("[.][.][.]",INT[,IN.Level.Name2]) == grepl("[.][.][.]",MONOS[j,T.Name2])
        }else{
          if(all(MONO.Codes %in% INT.Codes)){
            # If aggregated treatments are present they should be in both monocrop and intercrop
            grepl("[.][.][.]",INT[,IN.Level.Name2]) == grepl("[.][.][.]",MONOS[j,T.Name2])
            
            
          }else{
            F
          }
        }}))
      
      # To deal with situations where different crops in the intercrop have different tillage, force tillage comparisons to
      # use only the tillage data from the intercrop crop that matches the monocrop
      INT.Till<-data.table(Product=unlist(INT[,strsplit(IN.Prod,"***",fixed=T)]),
                           Till.Level.Name=unlist(INT[,strsplit(Till.Level.Name,"***",fixed=T)]),
                           Till.Codes=unlist(INT[,strsplit(Till.Codes,"***",fixed=T)]))
      
      INT.Till.Lev<-unique(INT.Till[Product==MONOS[1,ED.Product.Simple],Till.Level.Name])
      INT.Till.Code<-unique(INT.Till[Product==MONOS[1,ED.Product.Simple],Till.Codes])
      
      INT[,Till.Level.Name:=INT.Till.Lev][,Till.Codes:=INT.Till.Code]
    
      
      if(sum(X)>0){
        
        MONOS<-MONOS[X]
        
        Y<-unlist(lapply(1:nrow(MONOS),FUN=function(k){
          MONO<-MONOS[k]
          COLS<-Fields$Levels
          COLS<-COLS[!COLS=="T.Residue.Prev"]
          
          VALS.M<-Fields[Levels %in% COLS & !is.na(Codes),Codes]
          VALS.M<-strsplit(unlist(MONO[,..VALS.M]),"-")
          VALS.M<-lapply(VALS.M,FUN=function(X){
            if(is.na(X[1])){NA}else{
              X<-X[!grepl("h",X)]
              if(length(X)==0){NA}else{X}
            }
          })
          
          VALS.I<-Fields[Levels %in% COLS & !is.na(Codes),Codes]
          VALS.I<-strsplit(unlist(INT[,..VALS.I]),"-")
          VALS.I<-lapply(VALS.I,FUN=function(X){
            if(!is.na(X[1])){
              X<-unique(unlist(strsplit(X,"***",fixed=T)))
              X<-X[X!="NA"]
              
              if(is.na(X[1])){NA}else{
                X<-X[!grepl("h",X)]
                if(length(X)==0){NA}else{X}
              }
              
            }else{
              NA
            }
          })
          
          V.MATCH<-which(unlist(lapply(1:length(VALS.I),FUN=function(k){
            if(is.na(VALS.I[[k]][1])){
              NA
            }else{
              any(VALS.I[[k]] %in% VALS.M[[k]])
            }
          })))
          
          # Levels that refer to shared ERA codes
          COLS1<-Fields[Codes %in% names(VALS.I)[V.MATCH],Levels]
          
          # ERA Codes in Treatment that are not shared
          VALS.I<-unlist(lapply(VALS.I[-V.MATCH],paste,collapse="-"))
          
          # Levels for non-shared ERA codes
          COLS<-Fields[Codes %in% names(VALS.I),Levels]
          
          LEV.M<-MONOS[k,..COLS]
          
          # Add columns that do not have an ERA code in the Trt, but have a level?
          COLS1<-c(COLS1,colnames(LEV.M)[(!is.na(unlist(LEV.M))) & is.na(VALS.I)])
          
          STRUC<-c("O.Structure","P.Structure","C.Structure","W.Structure")
          Include<-STRUC[!is.na(apply(MONO[,..STRUC],2,unique))]
          COLS1<-c(COLS1,Include)
          
          if(length(COLS1)==0){T}else{
            all(unlist(lapply(COLS1,FUN=function(COL1){
              
              if(COL1=="Till.Level.Name"){
                ILEVEL<-INT.Till
              }else{
                ILEVEL<-unlist(INT[,..COL1])
              }
              
              grepl(unlist(MONO[,..COL1]),ILEVEL,fixed=T)
            })))
          }
          
        }))
        
        Comparison<-data.table(N=MONOS[,N],Control.For=rep(INT[,N],nrow(MONOS)),B.Code=MONOS[,B.Code],Mulch.Code="Int System",Codes.Match=T)
        Comparison[,Level.Match:=Y]
        
      }else{
        Comparison<-data.table(N=MONOS[,N],Control.For=NA,B.Code=MONOS[,B.Code],Mulch.Code=NA,Codes.Match=NA,Level.Match=NA)
        
      }
      Comparison
      
    })))
    
    Data.Out.Int3[,V.Codes:=NULL]
    Data.Out.Int3[,V.Level.Name:=NULL]
    Data.Out.Int3<-cbind(Data.Out.Int3,Y)
    rm(X,Y,Mulch.C.Codes,Mulch.T.Codes)
    
    
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)]
    
    CompareWithin<-c("ED.Site.ID","ED.M.Year","ED.Outcome","B.Code","Country","C.Structure","P.Structure","O.Structure",
                     "W.Structure","ED.Comparison")
    
    Cols<-c("ED.Treatment","ED.Int","ED.Rot")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Int3[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"ED.Treatment","Control.Trt")
    setnames(Comparisons1,"ED.Int","Control.Int")
    setnames(Comparisons1,"ED.Rot","Control.Rot")
    Comparisons1[,Compare.Trt:=Data.Out.Int3[match(Control.For,N),ED.Treatment]]
    Comparisons1[,Compare.Int:=Data.Out.Int3[match(Control.For,N),ED.Int]]
    Comparisons1[,Compare.Rot:=Data.Out.Int3[match(Control.For,N),ED.Rot]]
    Comparisons1[,Codes.Match:=Comparisons[,Codes.Match]]
    Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
    
    Comparison.List[["Sys.Int.vs.Mono"]]<-Comparisons1
    
  # 1.3) Rotation System Outcomes ####
  # Extract outcomes aggregated over rot/int entire sequence or system
  Data.Out.Rot<-Data.Out[is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot)]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Rot<-Data.Out.Rot[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Rot<-Data.Out.Rot[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
    # 1.3.1) Scenario 1: Rotation vs Rotation ####
    # Replace Treatment Name with Rotation Name
    Data.Out.Rot2<-Data.Out.Rot
    Data.Out.Rot2[,ED.Treatment:=ED.Rot]
    
    # Improved vs Unimproved Fallow - the latter is the control for the former, but the rotation sequences differ
    # this is an issues are comparisons in this section are made within rotation sequences.
    # Solution: similar to Rot vs Monoculture, duplicate the control for each comparable improved fallow giving the same rotation sequence
    
    # 1) Subset to unimproved fallow
    
    Data.Out.Rot2[,Group:=paste(ED.Site.ID,ED.M.Year,ED.Outcome,R.All.Structure,B.Code)][,Group:=as.numeric(as.factor(Group))]
    U.Fallow<-Data.Out.Rot2[grepl("h24",R.Code)]
    Data.Out.Rot2[,U.Fallow.Dup:=F]
    
    U.Fallow<-rbindlist(pblapply(1:nrow(U.Fallow),FUN=function(i){
      Trts<-Data.Out.Rot2[Group==U.Fallow[i,Group] & grepl("b60",R.Code)]
      if(nrow(Trts)>0){
        Control<-U.Fallow[rep(i,nrow(Trts)),][,R.Prod.Seq:=Trts[,R.Prod.Seq]]
        Control
      }else{
        print(paste("No Improved Treatment for Control: ",i," - ",U.Fallow[i,paste(B.Code,ED.Rot)]))
        NULL
      }
    }))
    
    U.Fallow[,U.Fallow.Dup:=T]
    
    Data.Out.Rot2<-rbind(Data.Out.Rot2,U.Fallow)
    Data.Out.Rot2[,Group:=NULL]
    
    rm(U.Fallow)
    
    # Combine all Practice Codes together & remove h-codes
    # In rotation we consider any practice present in components to be present
    
    Data.Out.Rot2[,N2:=1:.N]
    X<-Data.Out.Rot2[,list(Final.Codes=Join.T(R.T.Codes.Sys,R.IN.Codes.Sys,R.Res.Codes.Sys,R.Code)),by=N2]
    Data.Out.Rot2[,Final.Codes:=X$Final.Codes]
    Data.Out.Rot2[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N2]
    
    # Calculate Number of ERA Practices
    Data.Out.Rot2[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N2"]
    Data.Out.Rot2[,N2:=NULL]
    rm(X)
    
    CompareWithin<-c("ED.Site.ID","ED.M.Year", "ED.Outcome","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS",
                     "R.All.Structure","B.Code","Country","R.Prod.Seq")
    
    X<-as.numeric(factor(apply(Data.Out.Rot2[,..CompareWithin],1,paste,collapse="-")))
    Data.Out.Rot2[,Group:=X]
    Data.Out.Rot2[,Group.Len:=.N,by=Group]
    
    Return.Lists<-F
    Verbose<-F
    Match.Fun<-function(A,B){
      A<-unlist(A)
      B<-unlist(B)
      A<-A[!A %in% B]
      A<-A[!is.na(A)]
      if(length(A)==0){A<-NA}
      list(A)
    }
    Match.Fun2<-function(A,B,C){
      A<-unlist(A)
      B<-unlist(B)
      C<-unlist(C)
      A<-C[match(A,B)]
      A<-A[!is.na(A)]
      if(length(A)==0){A<-NA}
      list(unique(A))
    }
    # Exception for residue mulching being compared to residue incorporation
    Mulch.Fun<-function(A,B,X,Z){
      if(A %in% X){
        Z[Mulch.Flag==F,Mulch.Flag:=Z[,any(unlist(Final.Codes) %in% B),by="N"][,V1]]
        Z[Mulch.Flag==T,Match:=Match+1]
        Z[Mulch.Flag==T,NoMatch:=NoMatch-1]
        Z[Mulch.Flag==T,Mulch.Code:=paste0(Mulch.Code,A,collapse = "-"),by="N"]
        
      }
      return(Z)
    }
    # Incorporation codes in control (to be removed)
    Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
    # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
    Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
    
    Levels<-PracticeCodes1[!Linked.Col %in% c("F.Level.Name","ED.Int","ED.Rot"),c("Code","Linked.Col")]
    Levels<-rbind(Levels,data.table(Code=F.Master.Codes,Linked.Col=F.Master.Codes))
    
    # Run Comparisons ####
    Comparisons<-unique(rbindlist(pblapply(Data.Out.Rot2[,Group],FUN=function(GROUP){
      
      Data<-Data.Out.Rot2[Group==GROUP]
      
      if(nrow(Data)<2){
        NULL
      }else{
        
        
        BC<-Data$B.Code[1]
        N<-Data[,N]
        Final.Codes<-Data[,Final.Codes] # Is this redundant?
        #k<-N
        Y<-Data[,c("Final.Codes","N","N.Prac")][,Y.N:=1:.N]
        
        rbindlist(lapply(1:nrow(Y),FUN=function(j){
          if(Verbose){print(paste(BC," - Group ",GROUP," - Row =",j))}
          
          X<-unlist(Data$Final.Codes[j])
          i<-N[j]
          
          if(is.na(X[1])){
            
            Z<-Y[N!=i & !is.na(Final.Codes)
            ][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
            ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
            ][Match>=0 & NoMatch>0] # Keep instances for which this treatment can be a control for other treatments
            
            Z[,N2:=1:.N]
            
            Z[,Mulch.Code:=as.character("")][,Mulch.Flag:=F]
            
            Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
            
            Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N2]  # Add in column for the codes that are in treatment but not control
            
            Z[,Linked.Col:=list(Match.Fun2(Control.Code,Levels$Code,Levels$Linked.Col)),by=N2]
            
            Z[,N2:=NULL]
            
            
          }else{
            
            # Here we are working from the logic of looking at what other treatments can treatment[i] be a control for?
            Z<-Y[N!=i][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
            ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N # How many practices in this treatment do not match practices in other treatments?
            ]
            
            Z[,N2:=1:.N]
            
            # Exception for comparison of mulched residues to incorporated residues.
            Z[,Mulch.Code:=as.character("")][,Mulch.Flag:=F]
            
            for(kk in 1:length(Mulch.C.Codes)){
              Z<-Mulch.Fun(Mulch.C.Codes[kk],Mulch.T.Codes[kk],X,Z)
            }
            
            # Keep instances for which this treatment can be a control for other treatments
            Z<-Z[Match>=length(X) & NoMatch>0] 
            
            # There was a bug here where Control.Code was set to :=list(X), but if X was the same length as nrow(Z) this led to issues
            Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
            
            # Remove Mulch Code from Control Code where Mulch.Flag == T
            
            Z[,Control.Code:=Z[,list(Control.Code=list(unlist(Control.Code)[!unlist(Control.Code) %in% Mulch.Code])),by="N2"][,Control.Code]]
            
            Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N2]  # Add in column for the codes that are in treatment but not control
            
            Z[,Linked.Col:=list(Match.Fun2(Control.Code,Levels$Code,Levels$Linked.Col)),by=N2]
            Z[,N2:=NULL]
            
          }
          
          
          
          if(nrow(Z)>0){
            for(k in 1:nrow(Z)){
              L.Cols<-unlist(Z[k,Linked.Col])
              if(!is.na(L.Cols[1])){
                TID<-Data[Y[k,Y.N],R.ID]
                CID<-Data[j,R.ID]
                L.Match<-all(unlist(Rot.Levels[R.ID==TID,..L.Cols]) %in% unlist(Rot.Levels[R.ID==CID,..L.Cols]))
                Z[k,Level.Match:=as.logical(L.Match)]
              }else{
                Z[k,Level.Match:=T]
              }
              
            }
            
            if(!Return.Lists){
              data.table(N = Y[j,N],Control.For=Z[,N],Mulch.Code=Z[,Mulch.Code],B.Code=BC,Level.Match=Z[,Level.Match])
            }else{
              data.table(N = Y[j,N],Control.For=list(Z[,N]),Mulch.Code=list(Z[,Mulch.Code]),B.Code=BC,Level.Match=Z[,Level.Match])
            }
          }else{}
          
          
        }))
        
      }
      
    })))
    
    Data.Out.Rot2<-Data.Out.Rot2[!U.Fallow.Dup==T,]
    Data.Out.Rot2[,U.Fallow.Dup:=NULL]
    
    rm(Return.Lists,Verbose,Match.Fun,Match.Fun2,Mulch.C.Codes,Mulch.T.Codes,Levels)
    
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)]
    
    Cols<-c("ED.Treatment","ED.Int","ED.Rot")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Rot2[match(Comparisons[,N],Data.Out.Rot2$N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
    setnames(Comparisons1,"ED.Treatment","Control.Trt")
    setnames(Comparisons1,"ED.Int","Control.Int")
    setnames(Comparisons1,"ED.Rot","Control.Rot")
    Comparisons1[,Compare.Trt:=Data.Out.Rot2[match(Control.For,N),ED.Treatment]]
    Comparisons1[,Compare.Int:=Data.Out.Rot2[match(Control.For,N),ED.Int]]
    Comparisons1[,Compare.Rot:=Data.Out.Rot2[match(Control.For,N),ED.Rot]]
    
    Comparison.List[["Sys.Rot.vs.Rot"]]<-Comparisons1
    
    # 1.3.2) Scenario 2: Rotation vs Monocrop ####
    # Extract outcomes aggregated over rot/int entire sequence or system
    Data.Out.Rot<-Data.Out[is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot)]
    
    # Exclude Ratios
    # Remove Controls for Ratio Comparisons
    Data.Out.Rot<-Data.Out.Rot[ED.Ratio.Control!=T]
    # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
    # Data.Out.Rot<-Data.Out.Rot[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
    
    Data.Out.Rot3<-Data.Out.Rot2[,!c("Group","Group.Len")]
    Data.Out.Rot3[,ED.Treatment:=Data.Out.Rot[,ED.Treatment]]
    
    CompareWithinInt<-c("ED.Site.ID","ED.M.Year","ED.Outcome","B.Code","Country")
    
    # Create group codes
    CT<-apply(Data.Out.Rot3[,..CompareWithinInt],1,paste,collapse = " ")
    Data.Out.Rot3[,CodeTemp:=CT]
    rm(CT)
    
    # In rotation we consider any practice present in the majority of components to be present in the system
    
    # Update Data.Out Columns to Match Data.Out.Rot3
    Data.Out[,N2:=1:.N]
    X<-Data.Out[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N2"]
    Data.Out[,Final.Codes:=X$Final.Codes]
    Data.Out[,N2:=NULL]
    rm(X)
    
    # Calculate Number of ERA Practices
    Data.Out[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
    
    Data.Out[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N]
    
    CT<-Data.Out[,..CompareWithinInt]
    Data.Out[,CodeTemp:=apply(CT,1,paste,collapse = " ")]
    rm(CT)
    
    # Add in (potential) monoculture controls ####
    # Also duplicate rotation observation replacing the EU with a simplified EU that matches the potential control
    Data.Out.Rot3<-rbindlist(pblapply(unique(Data.Out.Rot3[,CodeTemp]),FUN=function(X){
      
      Y1<-Data.Out[CodeTemp == X & is.na(ED.Int) & is.na(ED.Rot) & !is.na(ED.Treatment)]
      Y2<-Data.Out.Rot3[CodeTemp == X]
      
      #print(X)
      
      if(nrow(Y1)>0){
        W<-rbindlist(lapply(Y1[,unique(ED.Product.Simple)],FUN=function(PROD){
          Z<-Y2[grep(PROD,ED.Product.Simple),]
          if(nrow(Z)>0){
            MONO<-Y1[ED.Product.Simple==PROD]
            INT<-Z[,ED.Product.Simple:=PROD]
            rbind(INT,MONO)
          }else{
          }
          
        }))
        
      }else{
        
      }
      
    }))
    
    Data.Out.Rot3<-unique(Data.Out.Rot3[,!"Final.Codes"])
    Data.Out.Rot3[,N3:=1:.N]
    Data.Out.Rot3[,Final.Codes:=list(strsplit(Final.Codes2,"-")),by=N3]
    
    # Reset Data.Out
    Data.Out[,CodeTemp:=NULL]
    Data.Out[,Final.Codes:=NULL]
    Data.Out[,Final.Codes2:=NULL]
    Data.Out[,N.Prac:=NULL]
    
    # Update Level/ID Fields
    
    Update.Res<-function(Codes){
      if(!is.na(Codes[1])){
        IS.LIST<-is.list(Codes)
        Codes<-unlist(Codes)
        Y<-unlist(strsplit(Codes,"-"))
        if(length(unique(Y))>1){
          Y[grepl("b41",Y)]<-"b41"
          Y[grepl("b40",Y)]<-"b40"
          Y[grepl("b27",Y)]<-"b27"
          Y[Y %in% c("a16","a17")] <- "a15"
          Y[Y %in% c("a16.1","a17.1")] <- "a15.1"
          Y[Y %in% c("a16.2","a17.2")] <- "a15.2"
          Y<-unique(Y)
        }
        if(IS.LIST){
          Y<-list(Y)
        }else{
          Y<-paste(Y[order(Y)],collapse="-")
        }
      }else{
        Y<-NA
      }
      return(Y)
    }
    
    Data.Out.Rot3[,Final.Codes:=list(Update.Res(Final.Codes)),by=N3]
    Data.Out.Rot3[,Final.Codes2:=Update.Res(Final.Codes2),by=N3]
    Data.Out.Rot3[,Final.Residue.Code:=Update.Res(Final.Residue.Code),by=N3]
    Data.Out.Rot3[,T.Codes:=Update.Res(T.Codes),by=N3]
    Data.Out.Rot3[,IN.T.Codes:=Update.Res(IN.T.Codes),by=N3]
    Data.Out.Rot3[,N3:=NULL]
    
    
    # Run Comparisons ####
    Data.Out.Rot3[is.na(ED.Treatment),N2:=1:.N]
    
    # Update Rot Levels with the aggregated data in Rot.Levels 
    CNAMES<-colnames(Rot.Levels)[!colnames(Rot.Levels) %in% c("B.Code","R.Level.Name","R.ID")]
    Data.Out.Rot3<-data.frame(Data.Out.Rot3)
    N<-match(Data.Out.Rot3[,"R.ID"],Rot.Levels[,R.ID])
    Data.Out.Rot3[!is.na(N),CNAMES]<-data.frame(Rot.Levels[N[!is.na(N)],..CNAMES])
    Data.Out.Rot3<-data.table(Data.Out.Rot3)
    
    # Add crop var column that refers to EU x variety in question, use this a level instead of variety so matching is done on product 
    # monocrop only.
    
    X<-rbindlist(pblapply(1:nrow(Data.Out.Rot3),FUN=function(i){
      if(!is.na(Data.Out.Rot3[i,V.Level.Name])){
        X<-Data.Out.Rot3[i,c("B.Code","ED.Product.Simple","V.Level.Name","V.Codes")]
        VAR<-unlist(strsplit(X[,V.Level.Name],"---"))
        VAR<-VAR[!is.na(VAR)]
        if(length(VAR)>0){
          Z<-Var.Out[B.Code==X[,B.Code] & V.Product==X[,ED.Product.Simple] & V.Var %in% VAR,c("V.Codes","Join")]
          if(nrow(Z)>1){
            Z1<-unlist(apply(Z,2,FUN=function(W){paste(unique(W),collapse = "---")}))
            Z<-data.table(V.Codes=Z1[1],Join=Z1[2])
          }
          
          if(nrow(Z)==0){
            # print(X)
            data.table(V.Codes=NA,Join=NA)
          }else{
            Z
          }
          
        }else{
          data.table(V.Codes=NA,Join=NA)
        }
      }else{
        data.table(V.Codes=NA,Join=NA)
      }
    }))
    
    setnames(X,"Join","V.Level.Name")
    
    # Remove & Replace columns
    Y<-Data.Out.Rot3[,c("V.Codes","V.Level.Name")]
    Data.Out.Rot3[,V.Codes:=NULL]
    Data.Out.Rot3[,V.Level.Name:=NULL]
    Data.Out.Rot3<-cbind(Data.Out.Rot3,X)
    
    # Incorporation codes in control (to be removed)
    Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
    # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
    Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
    
    Comparisons<-unique(rbindlist(pblapply(Data.Out.Rot3[!is.na(N2),N2],FUN=function(i){
    
      ROT<-Data.Out.Rot3[N2==i]
      MONOS<-Data.Out.Rot3[!is.na(ED.Treatment) & CodeTemp == ROT[,CodeTemp] & ED.Product.Simple == ROT[,ED.Product.Simple]]
      
      ROT.Codes<-unlist(strsplit(c(ROT$R.T.Codes.All,ROT$R.Residue.Codes.All),"-"))
      ROT.Codes<-ROT.Codes[!grepl("h",ROT.Codes)]
      ROT.Codes<-ROT.Codes[!is.na(ROT.Codes)]
      
      # Are crop residue and practice codes in monoculture in rotation?
      X<-unlist(lapply(1:nrow(MONOS),FUN=function(j){
        MONO<-MONOS[j]
        MONO.Codes<-unlist(strsplit(c(MONO$T.Codes,MONO$T.Residue.Code),"-"))
        MONO.Codes<-MONO.Codes[!grepl("h",MONO.Codes)]
        MONO.Codes<-MONO.Codes[!is.na(MONO.Codes)]
        
        # Deal with exception for mulch vs. incorporation
        M.Int<-Mulch.C.Codes[Mulch.C.Codes %in% MONO.Codes & Mulch.T.Codes %in% ROT.Codes]
        if(length(M.Int)>0){
          MONO.Codes<-MONO.Codes[MONO.Codes!=M.Int]
        }
        
        if(length(MONO.Codes)==0){
          grepl("[.][.][.]",ROT[,IN.Level.Name2]) == grepl("[.][.][.]",MONOS[j,T.Name2])
        }else{
          if(all(MONO.Codes %in% ROT.Codes)){
            # If aggregated treatments are present they should be in both monocrop and rotation
            grepl("[.][.][.]",ROT[,IN.Level.Name2]) == grepl("[.][.][.]",MONOS[j,T.Name2])
            
            
          }else{
            F
          }
        }}))
      
      
      if(sum(X)>0){
        
        MONOS<-MONOS[X]
        
        Y<-unlist(lapply(1:nrow(MONOS),FUN=function(k){
          
          MONO<-MONOS[k]
          
          # Ensure for shared crops comparison is to the monocrop at the time of measurement
          
          # Using rotation sequence data create sequences of treatments, products and times
          ROT.Levels<-Rot.Seq2[ID==ROT[,R.ID],unlist(strsplit(unlist(strsplit(Treatment,"|||",fixed=T)),"***",fixed=T))]
          ROT.Crops<-Rot.Seq2[ID==ROT[,R.ID],unlist(strsplit(unlist(strsplit(Products,"|||",fixed=T)),"***",fixed=T))]
          
          N<-Rot.Seq2[ID==ROT[,R.ID],unlist(lapply(strsplit(unlist(strsplit(Products,"|||",fixed=T)),"***",fixed=T),length))]
          
          ROT.Times<-rep(Rot.Seq2[ID==ROT[,R.ID],unlist(strsplit(unlist(strsplit(Time,"|||",fixed=T)),"***",fixed=T))],N)
          
          # Find the matching treatment to the monocrop based on product and time
          ROT.Levels<-unique(ROT.Levels[ROT.Crops %in% MONO[,ED.Product.Simple] & ROT.Times %in% MONO[,unlist(strsplit(ED.M.Year,"..",fixed=T))]])
          
          # We will still run into issues where same crop treatments differ over time and the period of measurement is across multiple years
          # So exclude where treatments for a crop differ over time
          if(length(ROT.Levels)!=1){
            Logic<-F
          }else{
          
            ROT2<-MT.Out2[T.Name==ROT.Levels]
            
            # Set practices of interest
            COLS<-c("A.Level.Name","E.Level.Name","H.Level.Name","I.Level.Name","M.Level.Name","pH.Level.Name","PO.Level.Name","Till.Level.Name",
            "V.Level.Name","WH.Level.Name","F.Level.Name")
            
            
            RCodes<-ROT[,unlist(Final.Codes)]
            MCodes<-MONO[,unlist(Final.Codes)]
            
            SharedCodes<-RCodes[RCodes %in% MCodes]
            ContCodes<-MCodes[!MCodes %in% RCodes]
            
            # There should be no ERA practices in control not in treatment
            if(length(ContCodes)>0){
              Logic<-F
            }else{
              SharedCols<-PracticeCodes[Code %in% SharedCodes,unique(Linked.Col)]
              SharedCols<-SharedCols[SharedCols %in% COLS]
             
              Logic<-all(MONO[,..SharedCols] %in% ROT2[,..SharedCols])
            
              STRUC<-c("O.Structure","P.Structure","C.Structure","W.Structure")
              Include<-STRUC[!is.na(apply(MONO[,..STRUC],2,unique))]
              
              if(length(Include)>0){
                Logic<-all(c(Logic,unlist(lapply(Include,FUN=function(COL1){
                  grepl(unlist(MONO[,..COL1]),ROT[,R.All.Structure],fixed=T)
                }))))
              }
            }
          }
          Logic
          
        }))
        
        Comparison<-data.table(N=MONOS[,N],Control.For=rep(ROT[,N],nrow(MONOS)),B.Code=MONOS[,B.Code],Mulch.Code="ROT System",Codes.Match=T)
        Comparison[,Level.Match:=Y]
        
      }else{
        Comparison<-data.table(N=MONOS[,N],Control.For=NA,B.Code=MONOS[,B.Code],Mulch.Code=NA,Codes.Match=NA,Level.Match=NA)
        
      }
      Comparison
      
    })))
    
    Data.Out.Rot3[,V.Codes:=NULL]
    Data.Out.Rot3[,V.Level.Name:=NULL]
    Data.Out.Rot3<-cbind(Data.Out.Rot3,Y)
    rm(X,Y,Mulch.C.Codes,Mulch.T.Codes)
    
    
    Comparisons<-Comparisons[!is.na(Control.For)]
    
    
    CompareWithin<-c("ED.Site.ID","ED.M.Year","ED.Outcome","B.Code","Country","R.All.Structure")
    
    Cols<-c("ED.Treatment","ED.Int","ED.Rot")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Rot3[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"ED.Treatment","Control.Trt")
    setnames(Comparisons1,"ED.Int","Control.Int")
    setnames(Comparisons1,"ED.Rot","Control.Rot")
    Comparisons1[,Compare.Trt:=Data.Out.Rot3[match(Control.For,N),ED.Treatment]]
    Comparisons1[,Compare.Int:=Data.Out.Rot3[match(Control.For,N),ED.Int]]
    Comparisons1[,Compare.Rot:=Data.Out.Rot3[match(Control.For,N),ED.Rot]]
    Comparisons1[,Codes.Match:=Comparisons[,Codes.Match]]
    Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
    
    Comparison.List[["Sys.Rot.vs.Mono"]]<-Comparisons1
    
    
  # 1.4) Save Data for QC ####
  if(F){
    A<-Comparison.List$Sys.Int.vs.Int
    B<-Comparison.List$Sys.Int.vs.Mono
    C<-Comparison.List$Aggregated
    D<-Comparison.List$Sys.Rot.vs.Rot
    E<-Comparison.List$Sys.Rot.vs.Mono
    
    write.table(A,"clipboard-256000",row.names = F,sep="\t")
    write.table(B,"clipboard-256000",row.names = F,sep="\t")
    write.table(C,"clipboard-256000",row.names = F,sep="\t")
    write.table(D,"clipboard-256000",row.names = F,sep="\t")
    write.table(E,"clipboard-256000",row.names = F,sep="\t")
    
    rm(A,B,C,D,E)
  }
# 2) Animals ####
# Remove Controls for Ratios
Data.Out.Animals<-Data.Out[ED.Ratio.Control!=T]
# Ignore Aggregated Observations (dealt with in section 2.1 for additions)
Data.Out.Animals<-Data.Out.Animals[-grep("[.][.][.]",T.Name2)]
# Subset to Animal Data
Data.Out.Animals<-Data.Out.Animals[!(is.na(A.Level.Name) & is.na(V.Animal.Practice)),]

  # Validation: Check instances where there is no A.Level.Name specified ####
  # Could be crop in animal section of Varieties tab
  # Paper comparing animal breeds with an identical diet (base diet)
  # Paper looking a postharvest of animals (e.g. fish drying)
  if(F){
    unique(Data.Out.Animals[is.na(A.Level.Name),c("B.Code","ED.Treatment","V.Animal.Practice","V.Crop.Practice","A.Level.Name")])
  }
  # Combine all Practice Codes together & remove h-codes ####
  Join.T<-function(A.Codes,V.Codes,WH.Codes,AF.Codes){
    X<-c(A.Codes,V.Codes,WH.Codes,AF.Codes)
    X<-unlist(strsplit(X,"-"))
    X<-X[X!=0]
    X<-unique(X[!is.na(X)])
    if(length(grep("h",X))>0){
      X<-X[-grep("h",X)]
      if(length(X)==0){list(NA)}else{list(X)}
    }else{
      if(length(X)==0){list(NA)}else{list(X)}
    }
  }
  
  Data.Out.Animals[,Final.Codes:=list(Join.T(A.Codes,V.Codes,WH.Codes,AF.Codes)),by=N]
  
  # Validation: Are there any non-animal codes?
  if(F){
    Data.Out.Animals[,NonA:=sum(!unlist(Final.Codes) %in% PracticeCodes1[Theme == "Animals",Code]),by="N"]
    
    Animals_NonA.Codes<-Data.Out.Animals[NonA>0 & !is.na(Final.Codes),c("B.Code","A.Level.Name","V.Animal.Practice","Final.Codes","NonA")]
    if(nrow(Animals_NonA.Codes)>0){
      View(Animals_NonA.Codes)
    }
    rm(Animals_NonA.Codes)
    Data.Out.Animals[,NonA:=NULL]
  }
  
  # Add in Control Columns from Animal.Out Column
  MATCH<-match(Data.Out.Animals[,paste(B.Code,A.Level.Name)],Animals.Out[,paste(B.Code,A.Level.Name)])
  Data.Out.Animals[,A.Feed.Add.C:=Animals.Out[MATCH,A.Feed.Add.C]]
  Data.Out.Animals[,A.Feed.Sub.C:=Animals.Out[MATCH,A.Feed.Sub.C]]
  
  # TO DO: Add validation for Feed.Addition Control any rows it contained should also be in the Treatment
  
  # Calculate Number of ERA Practices
  Data.Out.Animals[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
  
  # Is Feed Substitution Present?
  Data.Out.Animals[,Feed.Sub:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Substitution",Code]),by=N]
  
  # Is Feed Addition Present?
  Data.Out.Animals[,Feed.Add:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Addition",Code]),by=N]
  
  # Weight Gain Outcomes need non-essential info removing
  X<-Data.Out.Animals[grepl("Weight Gain",ED.Outcome),ED.Outcome]
  X<-lapply(X,FUN=function(Y){
    Y<-unlist(strsplit(Y,"[.][.]"))
    paste(Y[-length(Y)],collapse="..")
  })
  
  Data.Out.Animals[grepl("Weight Gain",ED.Outcome),ED.Outcome:=X]
  rm(X)
  
  # Meat Yield Outcomes need weight info removing
  X<-Data.Out.Animals[grepl("Meat Yield",ED.Outcome),ED.Outcome]
  X<-lapply(X,FUN=function(Y){
    Y<-unlist(strsplit(Y,"[.][.]"))
    paste(Y[-length(Y)],collapse="..")
  })
  
  Data.Out.Animals[grepl("Meat Yield",ED.Outcome),ED.Outcome:=X]
  rm(X)
  
  # 2.1) Feed Addition: Everything but Feed Substitution ####
  DATA<-Data.Out.Animals[(Feed.Add==T|A.Feed.Add.C=="Yes")]
  
  Compare.Fun.Ani<-function(Data,Verbose,Debug){
    
    Match.Fun<-function(A,B){
      A<-unlist(A)
      B<-unlist(B)
      list(A[!A %in% B])
    }
    
    Match.Fun2<-function(A,B,C){
      A<-unlist(A)
      B<-unlist(B)
      C<-unlist(C)
      list(unique(C[match(A,B)]))
    }
    
    BC<-Data$B.Code[1]
    N<-Data[,N]
    Final.Codes<-Data[,Final.Codes] # Is this redundant?
    k<-N # Is this redundant?
    Y<-Data[,c("Final.Codes","N","N.Prac")][,Y.N:=1:.N]
    
    lapply(1:length(N),FUN=function(j){
      if(Verbose){print(paste("N =",j))}
      X<-unlist(Data$Final.Codes[j])
      i<-N[j]
      
      if(is.na(X[1])){
        Z<-Y[N!=i & !is.na(Final.Codes)
        ][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
        ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
        ][Match>=0 & NoMatch>0] # Keep instances for which this treatment can be a control for other treatments
        
        Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
        
        Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N  # Add in column for the codes that are in treatment but not control
        ][,Linked.Tab:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
        ][,Linked.Col:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Col)),by=N]
        
        
      }else{
        
        # Here we are working from the logic of looking at what other treatments can treatment[i] be a control for?
        Z<-Y[N!=i][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
        ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
        ][Match>=length(X) & NoMatch>0] # Keep instances for which this treatment can be a control for other treatments
        
        # There was a bug here where Control.Code was set to :=list(X), but if X was the same length as nrow(Z) this led to issues
        Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
        
        
        Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N  # Add in column for the codes that are in treatment but not control
        ][,Linked.Tab:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
        ][,Linked.Col:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Col)),by=N]
        
      }
      
      if(nrow(Z)>0){
        Z$Level.Check<-lapply(1:nrow(Z),FUN=function(ii){
          unlist(lapply(1:length(unlist(Z[ii,Linked.Tab])),FUN=function(jj){
            
            if(is.na(Z[ii,Linked.Tab])){TRUE}else{
              
              if(unlist(Z[ii,Linked.Tab])[jj]=="Animal.Out" & any(Z$Prac.Code[ii] %in% PracticeCodes1[Practice=="Feed Addition",Code])){
                if(Verbose){print(paste0("Feed Add: ii = ",ii," | jj = ",jj))}
                # Is the potential control listed as a control in the Animal tab?
                Data.Sub[j,A.Feed.Add.C]=="Yes"
                # TO DO Advanced Logic: Control should contain all rows of treatment
              }else{
                
                if(unlist(Z[ii,Linked.Tab])[jj]=="Var.Out"){
                  if(Verbose){print(paste0("Improved Breed: ii = ",ii," | jj = ",jj))}
                  
                  COL<-unlist(Z[ii,Linked.Col])[jj]
                  if(!is.na(COL)){
                    # Does control value (left) equal treatment value (right)
                    Data[N==i,..COL] == Data[N== Z[ii,N],..COL]
                  }else{
                    # If control codes are NA (no ERA practice) then it could be comparible to other treatments that are not NA and have an ERA practice
                    !is.na(unlist(Z$Prac.Code)[ii])
                  }
                }else{
                  if(Verbose){print(paste0("Simple: ii = ",ii," | jj = ",jj))}
                  
                  # Otherwise we assume comparison is valid
                  TRUE
                }}}
            
            
          }))
          
        })
        
        # All Level.Checks must be true for comparison to be valid
        Z[,Level.Check:=all(unlist(Level.Check)),by="Y.N"]
        
        if(Debug){
          Z
        }else{
          # Return rows from master table that are valid treatments for this control (Level.Check==T)
          Z[Level.Check==T,N]
        }
      }else{NA}
      
    })
  } # Setting Debug to T prints comparison table rather than row numbers
  
  Verbose<-F
  Debug<-F
  
  # Set Grouping Variables
  CompareWithin<-c("ED.Site.ID","ED.Product.Simple","ED.Product.Comp","ED.M.Year", "ED.Outcome","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
                   "ED.Harvest.End","ED.Harvest.DAS","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","O.Structure","B.Code","Country")
  
  
  Comparisons<-pblapply(unique(DATA$B.Code),FUN=function(BC){
    
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    DS<-rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(Verbose){print(paste0(BC," Subgroup = ", i))}
      
      Control.For<-Compare.Fun.Ani(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F)
      data.table(Control.For=Control.For,N=Data.Sub[Group==i,N])
    }))
    
    DS[,B.Code:=BC]
    DS
  })
  
  # Feed.Add: Validation ####
  # list studies with no comparisons
  Feed.Add.No.Comparison<-unique(rbindlist(Comparisons[unlist(lapply(Comparisons, FUN=function(X){all(is.na(X$Control.For))}))])$B.Code)
  if(length(Feed.Add.No.Comparison)>0){
    View(Feed.Add.No.Comparison)
  }
  rm(Feed.Add.No.Comparison)
  
  Comparisons<-rbindlist(Comparisons)
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  sum(Comparisons$Len)
  
  Cols<-c("ED.Treatment")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=as.character("")]
  setnames(Comparisons1,"ED.Treatment","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals[match(unlist(Control.For),N),ED.Treatment],collapse="//"),by=Control.N]
  
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  
  #fwrite(Comparisons1,paste0(choose.dir(),"\\Animal_NoDietSub_V1.5.csv"),row.names = F)
  
  Comparison.List[["Animal.NoDietSub"]]<-Comparisons1
  
  # 2.2) Feed.Add: Aggregated Practices ####
  # Remove Controls for Ratios
  Data.Out.Animals.Agg<-Data.Out[ED.Ratio.Control!=T]
  # Ignore Aggregated Observations for now
  Data.Out.Animals.Agg<-Data.Out.Animals.Agg[grep("[.][.][.]",T.Name2)]
  # Subset to Animal Data
  Data.Out.Animals.Agg<-Data.Out.Animals.Agg[!(is.na(A.Level.Name) & is.na(V.Animal.Practice)),]
  
  # Split codes into list
  Data.Out.Animals.Agg[,Final.Codes:=list(strsplit(T.Codes,"-")),by="N"]
  
  # Add in Control Columns from Animal.Out Column
  MATCH<-match(paste(Data.Out.Animals.Agg$B.Code,Data.Out.Animals.Agg$A.Level.Name),paste(Animals.Out$B.Code,Animals.Out$A.Level.Name))
  Data.Out.Animals.Agg[,A.Feed.Add.C:=Animals.Out[MATCH,A.Feed.Add.C]]
  Data.Out.Animals.Agg[,A.Feed.Sub.C:=Animals.Out[MATCH,A.Feed.Sub.C]]
  
  # Calculate Number of ERA Practices
  Data.Out.Animals.Agg[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
  
  # Is Feed Substitution Present?
  Data.Out.Animals.Agg[,Feed.Sub:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Substitution",Code]),by=N]
  
  # Is Feed Addition Present?
  Data.Out.Animals.Agg[,Feed.Add:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Addition",Code]),by=N]
  
  # Weight Gain Outcomes need non-essential info removing
  X<-Data.Out.Animals.Agg[grepl("Weight Gain",ED.Outcome),ED.Outcome]
  X<-lapply(X,FUN=function(Y){
    Y<-unlist(strsplit(Y,"[.][.]"))
    paste(Y[-length(Y)],collapse="..")
  })
  
  Data.Out.Animals.Agg[grepl("Weight Gain",ED.Outcome),ED.Outcome:=X]
  rm(X)
  
  # Meat Yield Outcomes need weight info removing
  X<-Data.Out.Animals.Agg[grepl("Meat Yield",ED.Outcome),ED.Outcome]
  X<-lapply(X,FUN=function(Y){
    Y<-unlist(strsplit(Y,"[.][.]"))
    paste(Y[-length(Y)],collapse="..")
  })
  
  Data.Out.Animals.Agg[grepl("Meat Yield",ED.Outcome),ED.Outcome:=X]
  rm(X)
  
  
  # Set Grouping Variables
  CompareWithin<-c("ED.Site.ID","ED.Product.Simple","ED.Product.Comp","ED.M.Year", "ED.Outcome","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start",
                   "ED.Harvest.End","ED.Harvest.DAS","ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","O.Structure","B.Code","Country","T.Agg.Levels3")
  
  Verbose<-F
  
  DATA<-Data.Out.Animals.Agg
  
  Comparisons<-rbindlist(pblapply(unique(DATA$B.Code),FUN=function(BC){
    
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    DS<-rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(Verbose){print(paste0(BC," Subgroup = ", i))}
      
      Control.For<-Compare.Fun.Ani(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F)
      data.table(Control.For=Control.For,N=Data.Sub[Group==i,N])
    }))
    
    DS[,B.Code:=BC]
    DS
  }))
  
  
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  Cols<-c("ED.Treatment")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals.Agg[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=as.character("")]
  setnames(Comparisons1,"ED.Treatment","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals.Agg[match(unlist(Control.For),N),ED.Treatment],collapse="//"),by=Control.N]
  
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  
  Comparison.List[["Animal.NoDietSub.Agg"]]<-Comparisons1
  
  # 2.3) Feed Substitution ####
  DATA<-Data.Out.Animals[Feed.Sub==T |A.Feed.Sub.C=="Yes"]
  
  Compare.Fun.Ani.DSub<-function(Data,Verbose,PracticeCodes1){
    if(Verbose){print(paste0(Data$B.Code[1], " | Group = ",unique(Data$Group)))}
    
    X<-which(Data$A.Feed.Sub.C=="Yes")
    
    if(length(X)==0){
      data.table(Control.For="No Feed Substitution Control Present",N=Data$N,B.Code=Data$B.Code[1])
    }else{
      
      if(length(unique(Data$V.Level.Name))>1){
        
        GI.Fun=function(FC){
          FC<-FC[FC %in% PracticeCodes1[Practice == "Genetic Improvement",Code]]
          if(length(FC)==0){"Unimproved"}else{FC}
        }
        Data[,GI.Code:=GI.Fun(unlist(Final.Codes)),by=N]
        Data[is.na(V.Level.Name),V.Level.Name:="Blank"]
        
        Control.For<-lapply(X,FUN=function(i){
          # If control is unimproved it can be compared to any improved practice or unimproved if it has same breed
          Control<-Data[i]
          
          Control.For<-unlist(lapply((1:nrow(Data))[-i],FUN=function(j){
            Trt<-Data[j]
            
            # Variety codes are the same - comparison must be for Feed substitution only
            if(Trt$GI.Code == Control$GI.Code){
              # Check breeds are identical & feed sub is not a control
              if((Trt$V.Level.Name == Control$V.Level.Name) & (is.na(Trt$A.Feed.Sub.C)|Trt$A.Feed.Sub.C=="No")){
                Data[j,N]
              }else{
                NA
              }
              
            }else{
              if(Control$GI.Code == "Unimproved"){
                Data[j,N]
              }else{
                NA
              }
              
            }
            
          }))
          Control.For<-Control.For[!is.na(Control.For)]
          if(length(Control.For)==0){
            Control.For<-NA
          }
          Control.For
          
        })
        
        Z<-data.table(Control.For=Control.For,N=Data$N[X],B.Code=Data$B.Code[1])
        
      }else{
        
        Y<-Data[is.na(A.Feed.Sub.C)|A.Feed.Sub.C=="No",N]
        
        Z<-data.table(Control.For=list(as.character(NA)),N=Data$N[X],B.Code=Data$B.Code[1])
        Z[,Control.For:=list(Y)]
        
        
      }
      Z
    }
    
  }
  
  Verbose<-F
  
  Comparisons<-pblapply(unique(DATA$B.Code),FUN=function(BC){
    
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    DS<-rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(nrow(Data.Sub[Group==i])<=1){
        DS<-data.table(Control.For=NA,N=Data.Sub[Group==i,N],B.Code=BC)
      }else{
        
        DS<-Compare.Fun.Ani.DSub(Verbose = Verbose,Data = Data.Sub[Group==i],PracticeCodes1)
      }
      DS
    }))
    
    
    DS
    
  })
  
  # Output data ####
  Comparisons<-rbindlist(Comparisons)
  
  # Validation: No Feed Sub Control Present ####
  DATA[N %in% Comparisons[Control.For=="No Feed Substitution Control Present",N],c("B.Code","ED.Treatment","ED.Outcome","ED.M.Year")]
  
  # Validation: No Comparisons Present ####
  Comparisons[Control.For=="No Feed Substitution Control Present",Control.For:=NA]
  
  Feed.Sub.No.Comparisons<-Comparisons[,list(N.Comp=length(!is.na(Control.For))),by="B.Code"][N.Comp==0]
  if(nrow(Feed.Sub.No.Comparisons)>0){
    View(Feed.Sub.No.Comparisons)
  }
  rm(Feed.Sub.No.Comparisons)
  
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  Cols<-c("ED.Treatment")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=as.character("")]
  setnames(Comparisons1,"ED.Treatment","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals[match(unlist(Control.For),N),ED.Treatment],collapse="//"),by=Control.N]
  
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  
  # fwrite(Comparisons1,paste0(choose.dir(),"\\Animal_DietSub_V1.5.csv"),row.names = F)
  
  Comparison.List[["Animal.DietSub"]]<-Comparisons1
  
  
  
# 3) Combine Data ####
Comparison.List$Simple$Analysis.Function<-"Simple"
Comparison.List$Animal.NoDietSub$Analysis.Function<-"NoDietSub"
Comparison.List$Animal.DietSub$Analysis.Function<-"DietSub"
Comparison.List$Animal.NoDietSub.Agg$Analysis.Function<-"NoDietSub.Agg"

Comparison.List$Animal.NoDietSub$Control.Int<-NA
Comparison.List$Animal.DietSub$Control.Int<-NA
Comparison.List$Animal.NoDietSub.Agg$Control.Int<-NA

Comparison.List$Animal.NoDietSub$Compare.Int <-NA
Comparison.List$Animal.DietSub$Compare.Int <-NA
Comparison.List$Animal.NoDietSub.Agg$Compare.Int <-NA

Comparison.List$Animal.NoDietSub$Control.Rot<-NA
Comparison.List$Animal.DietSub$Control.Rot<-NA
Comparison.List$Animal.NoDietSub.Agg$Control.Rot<-NA

Comparison.List$Animal.NoDietSub$Compare.Rot <-NA
Comparison.List$Animal.DietSub$Compare.Rot <-NA
Comparison.List$Animal.NoDietSub.Agg$Compare.Rot <-NA

Comparison.List$Aggregated$Analysis.Function<-"Aggregated"
Comparison.List$Sys.Int.vs.Int$Analysis.Function<-"Sys.Int.vs.Int"
Comparison.List$Sys.Int.vs.Mono$Analysis.Function<-"Sys.Int.vs.Mono"
Comparison.List$Sys.Rot.vs.Rot$Analysis.Function<-"Sys.Rot.vs.Rot"
Comparison.List$Sys.Rot.vs.Mono$Analysis.Function<-"Sys.Rot.vs.Mono"

# Get Columns
B<-Comparison.List$Sys.Int.vs.Mono
B<-B[Level.Match==T][,Codes.Match:=NULL][,Level.Match:=NULL]
COLS<-colnames(B)
COLS<-COLS[!grepl("Structure|ED.Comparison",COLS)]

# Subset Int/Rot vs Mono to Match.Levels = T
Comparison.List$Sys.Rot.vs.Mono<- Comparison.List$Sys.Rot.vs.Mono[Level.Match==T]
Comparison.List$Sys.Int.vs.Mono<- Comparison.List$Sys.Int.vs.Mono[Level.Match==T]

Comparisons<-rbindlist(lapply(Comparison.List,FUN=function(X){X[,..COLS]}),use.names = T,fill=T)
Comparisons[Mulch.Code %in% c("Int System","ROT System"),Mulch.Code:=NA]
rm(B,COLS,Comparisons1)

# 4) Prepare Main Dataset ####
Data<-Data.Out
  # 4.1) Ignore Aggregated Observations for now ####
  # Data<-Data.Out[-grep("[.][.][.]",T.Name2)]
  # 4.2) Ignore outcomes aggregated over rot/int entire sequence or system ####
  #Data<-Data[!is.na(ED.Treatment)]
  
  # 4.3) Combine all Practice Codes together & remove h-codes ####
  Join.T<-function(A,B,C,D){
    X<-c(A,B,C,D)
    X<-unlist(strsplit(X,"-"))
    X<-unique(X[!is.na(X)])
    if(length(X)==0){list(NA)}else{list(X)}
  }
  
  # Simple/Animal/Aggregated
  X<-Data[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N"]
  Data[,Final.Codes:=X$Final.Codes]
  
  # Intercrop System
  X<-Data[is.na(ED.Treatment) & !is.na(ED.Int),list(Final.Codes=Join.T(IN.T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N"]
  Data[is.na(ED.Treatment) & !is.na(ED.Int),Final.Codes:=X$Final.Codes]
  
  # Rotation System
  X<-Data[is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot),list(Final.Codes=Join.T(R.T.Codes.Sys,R.IN.Codes.Sys,R.Res.Codes.Sys,R.Code)),by="N"]
  Data[is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot),Final.Codes:=X$Final.Codes]
  
  rm(X)
  
  # 4.4) Add filler cols ####
  
  Data$LatM<-NA # All values converted to DD in excel
  Data$LatS<-NA # All values converted to DD in excel
  Data$LatH<-NA # All values converted to DD in excel
  Data$LonM<-NA # All values converted to DD in excel
  Data$LonS<-NA # All values converted to DD in excel
  Data$LonH<-NA # All values converted to DD in excel
  Data$LatDiff<-NA # I don't think this is needed as the Buffer.Manual field is calculated in the excel
  Data$Soil.Classification<-NA # Not collected deliberately
  Data$Soil.Type<-NA # Not collected deliberately
  Data[,USD2010.T:=NA] # TO DO: Needs function to calculate this automatically
  Data[,USD2010.C:=NA] # TO DO: Needs function to calculate this automatically
  Data[,MeanFlip:=NA] # Not sure what this is for, but all values in the Master dataset seem to say N
  
  # 4.5) MSP & MAT ####
  Data[,Season:=unlist(lapply(strsplit(Data[,ED.M.Year],"[.][.]"),FUN=function(X){
    X<-unlist(strsplit(X,"[.]"))
    if(all(is.na(X))|is.null(X)){
      NA
    }else{
      if(any(X==1) & !any(X==2)){
        1
      }else{
        if(any(X==2) & !any(X==1)){
          2
        }else{
          NA
        }}}
  }))]
  Data[Season==1 | is.na(Season),MSP:=Site.MSP.S1]
  Data[Season==2 | is.na(Season),MSP:=Site.MSP.S2]
  
  # 4.6) Soils ####
  #Soil.Out$Soil.pH.Method<-NA # Bug in Excels that needs fixing
  Soil.Out[,Depth.Interval:=Soil.Upper-Soil.Lower]
  Soil.Out[Depth.Interval==0,Depth.Interval:=1]
  X<-Soil.Out[,list(SOC=round(weighted.mean(Soil.SOC,Depth.Interval,na.rm=T),2),Lower=max(Soil.Lower),Upper=min(Soil.Upper),SOC.Unit=unique(Soil.SOC.Unit),
                    Soil.pH=round(weighted.mean(Soil.pH,Depth.Interval,na.rm=T),2),Soil.pH.Method=unique(Soil.pH.Method)),by=c("B.Code","Site.ID")]
  Y<-Soil.Out[Soil.Lower<=50,list(SOC=round(weighted.mean(Soil.SOC,Depth.Interval,na.rm=T),2),Soil.pH=round(weighted.mean(Soil.pH,Depth.Interval,na.rm=T),2),
                                  Lower=max(Soil.Lower),Upper=min(Soil.Upper)),by=c("B.Code","Site.ID")]
  X[match(Y[,paste(B.Code,Site.ID)],paste(B.Code,Site.ID)),SOC:=Y[,SOC]]
  X[match(Y[,paste(B.Code,Site.ID)],paste(B.Code,Site.ID)),Soil.pH:=Y[,Soil.pH]]
  X[match(Y[,paste(B.Code,Site.ID)],paste(B.Code,Site.ID)),Upper:=Y[,Upper]]
  X[match(Y[,paste(B.Code,Site.ID)],paste(B.Code,Site.ID)),Lower:=Y[,Lower]]
  N.Match<-match(Data[,paste(B.Code,Site.ID)],X[,paste(B.Code,Site.ID)])
  Data[,SOC:=X[N.Match,SOC]]
  Data[,SOC.Depth:=X[,(Lower+Upper)/2][N.Match]]
  Data[,SOC.Unit:=X[N.Match,SOC.Unit]]
  Data[,Soil.pH:=X[N.Match,Soil.pH]]
  Data[,Soil.pH.Method:=X[N.Match,Soil.pH.Method]]
  rm(N.Match,X,Y)
  
  # 4.7.1) Update treatment names for intercrop and rotation ####
  
  # Component of Intercrop without rotation
  Data[!is.na(ED.Treatment) & !is.na(ED.Int) & is.na(ED.Rot),ED.Treatment:=paste0(ED.Treatment,">>",ED.Int)]
  # Component of Rotation without Intercrop
  Data[!is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot),ED.Treatment:=paste0(ED.Treatment,"<<",ED.Rot)]
  # Component of Intercrop in Rotation
  Data[!is.na(ED.Treatment) & !is.na(ED.Int) & !is.na(ED.Rot),ED.Treatment:=paste0(ED.Treatment,">>",ED.Int,"<<",ED.Rot)]
  
  # Intercrop only
  Data[is.na(ED.Treatment) & !is.na(ED.Int) & is.na(ED.Rot),ED.Treatment:=ED.Int]
  # Rotation only
  Data[is.na(ED.Treatment) & is.na(ED.Int) & !is.na(ED.Rot),ED.Treatment:=ED.Rot]
  # Intercrop part of rotation 
  Data[is.na(ED.Treatment) & !is.na(ED.Int) & !is.na(ED.Rot),ED.Treatment:=paste0(ED.Int,"<<",ED.Rot)]
  
  # 4.7.2) Create TID code ####
  Data[,TID:=paste0("T",as.numeric(as.factor(ED.Treatment))),by="B.Code"]
  
  # 4.8) Make "C1:Cmax" & Add Base Codes ####
  Data[,Base.Codes:=strsplit(Base.Codes,"-")]
  Join.Fun<-function(X,Y){
    X<-unique(c(unlist(X),unlist(Y)))
    X<-X[order(X,decreasing = T)]
    if(all(is.na(X))){
      NA
    }else{
      return(X[!is.na(X)])
    }
  }
  
  Data[,All.Codes:=Data[,list(All.Codes=list(Join.Fun(Base.Codes,Final.Codes))),by=N][,All.Codes]]
  
  X<-rbindlist(pblapply(Data[,All.Codes],FUN=function(X){data.table(t(data.table(X)))}),fill=T)
  colnames(X)<-paste0("C",1:ncol(X))
  
  Data<-cbind(Data,X)
  
  # TO DO - Which studies have >11 practices - Consider simplifying residues codes when Int/Rot are present (as per Basic Comparisons)
  Data[apply(X,1,FUN=function(Y){sum(!is.na(Y))})>10]
  
  if(!"C10" %in%colnames(Data)){
    Data[,C10:=NA]
  }
  
    # 4.8.1) How many C columns are there? ####
  NCols<-sum(paste0("C",1:30) %in% colnames(Data))
  
  # 4.9) Add Rotation Seq & Intercropping Species ####
  N.R<-match(paste0(Data$B.Code,Data$R.Level.Name),paste0(Rot.Seq.Summ$B.Code,Rot.Seq.Summ$R.Level.Name))
  Data[,R.Seq:=Rot.Seq.Summ[N.R,Seq]]
  rm(N.R)
  # Intercropping
  N.I<-match(Data[is.na(R.Seq),paste0(B.Code,ED.Int)],paste0(Int.Out$B.Code,Int.Out$IN.Level.Name))
  Data[is.na(R.Seq),R.Seq:=Int.Out[N.I,IN.Prod]]
  rm(N.I)
  # Reformat sequence
  Data[,R.Seq:=gsub("[*][*][*]","-",R.Seq)][,R.Seq:=gsub("[|][|][|]","/",R.Seq)]
  
  # 4.10) Add in Trees ####
  # Trees in rotation sequence
  X<-Rot.Out[,list(P.List=unlist(strsplit(R.Prod.Seq,"[|][|][|]"))),by=N]
  X<-X[,list(P.List=list(unlist(strsplit(unlist(P.List),"[.][.]")))),by=N]
  Rot.Out[,Trees:=lapply(X$P.List,FUN=function(X){
    X<-unlist(X)
    Y<-unique(X[X %in% TreeCodes$Product.Simple])
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Product.Simple]]))
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Latin.Name]]))
    
    if(length(Y)==0){
      NA
    }else{
      Y
    }
  })]
  
  # Trees in intercropping
  X<-Int.Out[,list(P.List=unlist(strsplit(IN.Prod,"[*][*][*]"))),by=N]
  X<-X[,list(P.List=list(unlist(strsplit(unlist(P.List),"[.][.]")))),by=N]
  Int.Out[X$N,Trees:=lapply(X$P.List,FUN=function(X){
    unlist(X)
    Y<-unique(X[X %in% TreeCodes$Product.Simple])
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Product.Simple]]))
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Latin.Name]]))
    
    if(length(Y)==0){
      NA
    }else{
      Y
    }
  })]
  
  # Add Trees to Data
  X<-match(Data[,paste(B.Code,ED.Int)],Int.Out[,paste(B.Code,IN.Level.Name)])
  Data[!is.na(X),Int.Tree:=Int.Out[X[!is.na(X)],Trees]]
  
  X<-match(Data[,paste(B.Code,ED.Rot)],Rot.Out[,paste(B.Code,R.Level.Name)])
  Data[!is.na(X),Rot.Tree:=Rot.Out[X[!is.na(X)],Trees]]
  
  Y<-AF.Out[,list(Trees=list(AF.Tree[!is.na(AF.Tree)])),by=c("AF.Level.Name","B.Code")]
  Y<-Y[,list(Trees=if(length(unlist(Trees))==0){as.character(NA)}else{unlist(Trees)}),by=c("AF.Level.Name","B.Code")]
  X<-match(Data[,paste(B.Code,AF.Level.Name)],Y[,paste(B.Code,AF.Level.Name)])
  Data[!is.na(X),AF.Tree:=Y[X[!is.na(X)],Trees]]
  
  X<-Data[,list(Trees=list(c(unlist(Int.Tree),unlist(Rot.Tree),unlist(AF.Tree)))),by=N]
  X<-X[,list(Trees=list(if(!all(is.na(unlist(Trees)))){unique(unlist(Trees)[!is.na(unlist(Trees))])}else{NA})),by=N]
  X[,Trees:=paste0(unlist(Trees),collapse="-"),by=N]
  
  Data[,Tree:=X[,Trees]][,Tree:=unlist(Tree)]
  rm(X,Y)
  # 4.11) M.Year/Season - Translate to old system or code start/end year and season ####
  # We have some instances of 2000-2020 and some of ".." and some of "-5" between seasons
  
  Data[,M.Year:=gsub("..",".",ED.M.Year,fixed=T)]
  Data[,M.Year:=gsub("-5","",M.Year)]
  Data[,M.Year:=gsub("-",".",M.Year)]
  
  M.Year.Fun<-function(M.Year){
    X<-unlist(strsplit(M.Year,"[.]"))
    if(length(X)>4 & nchar(X[1])==4 & nchar(X[2])==1){
      X<-paste0(c(X[1],X[2],X[(length(X)-1)],X[length(X)]),collapse = ".")
    }else{
      if(length(X)>1 & nchar(X[1])==4 & nchar(X[2])==4){
        X<-paste0(c(X[1],X[length(X)]),collapse = ".")
      }else{
        X<-paste0(X,collapse=".")
      }
    }
    
    return(X)
  }
  
  Data[,M.Year:=M.Year.Fun(M.Year),by=N]
  
  rm(M.Year.Fun)
  
  M.Start.Fun<-function(M.Year){
    X<-unlist(strsplit(M.Year,"[.]"))
    X<-as.integer(X[1])
    return(X)
  }
  Data[,M.Year.Start:=M.Start.Fun(M.Year),by=N]
  
  rm(M.Start.Fun)
  
  M.End.Fun<-function(M.Year){
    X<-unlist(strsplit(M.Year,"[.]"))
    X<-X[nchar(X)==4]
    X<-as.integer(X[length(X)])
    return(X)
  }
  
  Data[,M.Year.End:=M.End.Fun(M.Year),by=N]
  
  rm(M.End.Fun)
  
  M.S.Start.Fun<-function(M.Year){
    X<-unlist(strsplit(M.Year,"[.]"))
    X<-X[nchar(X)==1]
    X<-as.integer(X[1])
    return(X)
  }
  
  Data[,M.Season.Start:=M.S.Start.Fun(M.Year),by=N]
  
  rm(M.S.Start.Fun)
  
  M.S.End.Fun<-function(M.Year){
    X<-unlist(strsplit(M.Year,"[.]"))
    X<-X[nchar(X)==1]
    X<-as.integer(X[length(X)])
    return(X)
  }
  
  Data[,M.Season.End:=M.S.End.Fun(M.Year),by=N]
  
  rm(M.S.End.Fun)
  
  Data[,Max.Season:=max(M.Season.Start),by=c("B.Code","Site.ID")]
  
  # 4.12) Add Duration ####
    # 4.12.1) Update start season ####
    # If start season is unknown but 2 seasons are present in the same year then the start season should be 1
    
    Updates.SS<-function(M.Season.End,Max.Season,M.Year.End,Final.Start.Year.Code){
      M.Season.End<-M.Season.End[1]
      Max.Season<-Max.Season[1]
      M.Year.End<-M.Year.End[1]
      Final.Start.Year.Code<-Final.Start.Year.Code[1]
      X<-NA
      if(is.na(Final.Start.Year.Code)|is.na(M.Year.End)|is.na(M.Year.End)){Final.Start.Year.Code}else{
        N<-M.Year.End==Final.Start.Year.Code[1]
        if(sum(N)>1 & unique(Max.Season)==2){
          M.Season.End<-M.Season.End[N]
          if(all(c(1,2) %in% M.Season.End)){
            X<-1
          }}
        return(X)
      }
    }
    
    # Here are assuming that all aspects of a Treatment within a Site & Study start at the same time.
    Data[is.na(Final.Start.Season.Code),
         Final.Start.Season.Code:=Updates.SS(M.Season.End,Max.Season,M.Year.End,Final.Start.Year.Code),
         by=list(ED.Site.ID, ED.Treatment, B.Code, Country, Final.Start.Year.Code)]
    
    
    # 4.12.2) Calculate duration  ####
    Dur.Fun<-function(M.Year.Start,M.Year.End,M.Season.Start,M.Season.End,Max.Season,Final.Start.Year.Code,Final.Start.Season.Code){
      
      M.Season<-mean(c(M.Season.Start,M.Season.End))
      M.Year<-mean(c(M.Year.Start,M.Year.End))
      
      A<-M.Year-suppressWarnings(as.numeric(Final.Start.Year.Code))
      B<-(M.Season-suppressWarnings(as.numeric(Final.Start.Season.Code))*(M.Season/Max.Season))
      
      if(is.na(A) & is.na(B)){
        C<-NA
      }else{
        
        if(is.na(B) & !is.na(A)){
          C<-A
        }
        
        if(!is.na(A) & !is.na(B)){
          C<-A+B
        }
        
        if(is.na(Max.Season)){Max.Season<-1}
        
        # If bimodal system first division = 0.5
        if(C==0 & Max.Season==2){
          C<-0.5
        }
        
        # If unimodal system first division = 1
        if(C==0 & Max.Season==1){
          C<-1
        }
      }
      
      return(round(C,2))
    }
    
    Data[,Duration:=Dur.Fun(M.Year.Start,M.Year.End,M.Season.Start,M.Season.End,Max.Season,Final.Start.Year.Code,Final.Start.Season.Code),by=N]
    
    rm(Dur.Fun)
    
    # 4.12.3) Calculate duration for animal or postharvest experiments ####
    Data[,ED.Plant.Start:=as.numeric(as.character(ED.Plant.Start))]
    Data[,ED.Plant.End:=as.numeric(as.character(ED.Plant.End))]
    Data[,ED.Harvest.Start:=as.numeric(as.character(ED.Harvest.Start))]
    Data[,ED.Harvest.End:=as.numeric(as.character(ED.Harvest.End))]
    Data[,ED.Sample.Start:=as.numeric(as.character(ED.Sample.Start))]
    Data[,ED.Sample.End:=as.numeric(as.character(ED.Sample.End))]
    Data[,ED.Sample.DAS:=as.numeric(as.character(ED.Sample.DAS))]
    
    Data[is.na(Duration) & (!is.na(A.Level.Name)|!is.na(V.Animal.Practice)),Duration:=round(((ED.Harvest.Start+ED.Harvest.End)/2-(ED.Plant.Start+ED.Plant.End)/2)/365,3)]
    Data[is.na(Duration) & (!is.na(A.Level.Name)|!is.na(V.Animal.Practice)),Duration:=round(ED.Sample.DAS/365,3)]
    
    Data[is.na(Duration) & !is.na(PO.Level.Name),Duration:=round(((ED.Harvest.Start+ED.Harvest.End)/2-(ED.Plant.Start+ED.Plant.End)/2)/365,3)]
    Data[is.na(Duration) & !is.na(PO.Level.Name),Duration:=round(ED.Sample.DAS/365,3)]
    
    if(F){
      Data[Out.Code %in% c(209,209.1,212),list(B.Code,ED.Plant.Start,ED.Plant.End,ED.Harvest.Start,ED.Sample.Start,ED.Sample.End,ED.Sample.DAS,Duration)]
      write.table(Data[is.na(Duration) & (!is.na(A.Level.Name)|!is.na(V.Animal.Practice)),unique(B.Code)],"clipboard",row.names = F,sep="\t")
      write.table(Data[is.na(Duration) & !is.na(PO.Level.Name),unique(B.Code)],"clipboard",row.names = F,sep="\t")
    }
    # 4.12.4) Duration: Validation checks ####
    # 1) Observation date before start of experiment
    Negative.Duration<-unique(Data[Duration<0,c("B.Code","Duration","Final.Start.Year.Code","Final.Start.Season.Code","ED.M.Year")])
    if(nrow(Negative.Duration)>0){
      View(Negative.Duration)
    }
    rm(Negative.Duration)
    
    # 2) Start Date is NA but reporting year is not
    
    NA.Duration<-unique(Data[is.na(Duration) & !is.na(ED.M.Year) & !ED.Ratio.Control==T,c("B.Code","ED.Treatment","ED.Rot","ED.Int","Final.Start.Year.Code","Final.Start.Season.Code","M.Year","Duration","M.Year.Start","M.Year.End","M.Season.Start","M.Season.End","Max.Season")])
    if(nrow(NA.Duration)>0){
      View(NA.Duration)
      write.table(NA.Duration,"clipboard-256000",row.names=F,sep="\t")
    }
    rm(NA.Duration)
    
  # 4.13) EUs: Remove agroforestry tree codes (t) & change delimiters ####
  
  Rm.T<-function(A,Delim){
    A<-unlist(strsplit(A,Delim,fixed = T))
    A<-A[!grepl("t",A)]
    if(length(A)==0){
      NA
    }else{
      paste(A[order(A)],collapse = Delim)
    }
  }
  
  Data[,EU:=Rm.T(EU,"**"),by=N]
  
  Data[,EU:=gsub("**",".",EU,fixed = T)]
  unique(Data[grep("..",ED.Product.Simple,fixed = T),c("ED.Product.Simple","EU")])
  
  # 4.14) Animal Diet Source ####
  X<-Animals.Diet[,list(Source=unique(D.Source)),by=c("B.Code","A.Level.Name")]
  NX<-match(Data[,paste(B.Code,A.Level.Name)],X[,paste(B.Code,A.Level.Name)])
  Data[,Feed.Source:=X[NX,Source]]
  rm(X,NX)
  # 4.15) TSP & TAP ####
  NX<-match(Data[,paste(B.Code,ED.Site.ID,M.Year)],Times[,paste(B.Code,Site.ID,Time)])
  Data[,TSP:=Times[NX,TSP]]
  Data[,TAP:=Times[NX,TAP]]
  rm(NX)
  # 4.16) ISO.3166.1.alpha.3 ####
  Countries<-MasterLevels[,c("Country","ISO.3166-1.alpha-3")]
  Data[,ISO.3166.1.alpha.3:=as.character(NA)]
  for(C in Data[,unique(Country)]){
    ISO<-paste(unlist(Countries[match(unlist(strsplit(C,"[.][.]")),Countries[,Country]),"ISO.3166-1.alpha-3"]),collapse = "..")
    Data[Country == C,ISO.3166.1.alpha.3:=ISO]
  }
  
  rm(Countries)
  
  # 4.17) Update Journal Codes ####
  Journals<-MasterLevels[,c("B.Journal","Journal")]
  NX<-match(Data[,B.Journal],Journals[,B.Journal])
  
  if(length(is.na(NX))>0){
    Journal.No.Match<-unique(Data[is.na(NX),c("B.Journal","B.Code")])[order(B.Journal)]
    View(Journal.No.Match)
    write.table(Journal.No.Match,"clipboard",row.names = F,sep="\t")
    rm(Journal.No.Match)
  }
  
  Data[!is.na(NX),B.Journal:=Journals[NX[!is.na(NX)],Journal]]
  
  rm(Journals,NX)
  
  
  # 4.18) Aggregate data for averaged site locations ####
  A.Sites<-Data[grepl("[.][.]",ED.Site.ID),N]
  
  
  
  Agg.Sites<-function(B.Code,ED.Site.ID){
    
    Sites<-unlist(strsplit(ED.Site.ID,"[.][.]"))
    Study<-B.Code
    
    coords<-Site.Out[Site.ID %in% Sites & B.Code==Study,list(Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Site.Buffer.Manual)]
    
    coords[,N:=1:.N][is.na(Site.Buffer.Manual),Site.Buffer.Manual:=max(c(Site.Lat.Unc,Site.Lon.Unc)),by=N]
    
    coords<-coords[!(is.na(Site.LatD)|is.na(Site.LonD))]
    
    if(nrow(coords)==0){
      
      return(data.table(Site.LonD=as.numeric(NA),Site.LatD=as.numeric(NA),Site.Buffer.Manual=as.numeric(NA)))
      
    }else{
      
      points <- SpatialPoints(coords[,list(Site.LonD,Site.LatD)],proj4string=CRS("+init=epsg:4326"))
      points<-spTransform(points,CRS("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"))
      
      pbuf1<-lapply(1:nrow(coords),FUN=function(i){
        pbuf<- gBuffer(points[i], widt=coords[i,Site.Buffer.Manual])
        pbuf<- spChFIDs(pbuf, paste(i, row.names(pbuf), sep="."))
        
      })
      
      pbuf1<-SpatialPolygons(lapply(pbuf1, function(x){x@polygons[[1]]}),proj4string=CRS("+proj=merc +lon_0=0 +k=1 +x_0=0 +y_0=0 +ellps=WGS84 +datum=WGS84 +units=m +no_defs"))
      
      X<-bbox(pbuf1)
      Buff<-max(X[1,2]-X[1,1],X[2,2]-X[2,1])/2
      
      pbuf<-spTransform(pbuf1,CRS("+init=epsg:4326"))
      Y<-gCentroid(pbuf)
      
      return(data.table(Site.LonD=round(Y@coords[1],5),Site.LatD=round(Y@coords[2],5),Site.Buffer.Manual=round(Buff,0)))
    } 
  }
  
  
  
  X<-Data[A.Sites,Agg.Sites(B.Code[1],ED.Site.ID[1]),by=c("B.Code","ED.Site.ID")]
  
  A.Sites<-match(Data[,paste(B.Code,ED.Site.ID)],X[,paste(B.Code,ED.Site.ID)])
  
  Data[!is.na(A.Sites),Site.LonD:=X[A.Sites[!is.na(A.Sites)],Site.LonD]]
  Data[!is.na(A.Sites),Site.LatD:=X[A.Sites[!is.na(A.Sites)],Site.LatD]]
  Data[!is.na(A.Sites),Site.Buffer.Manual:=X[A.Sites[!is.na(A.Sites)],Site.Buffer.Manual]]
  
  rm(A.Sites,Agg.Sites,X)
  
  unique(Data[is.na(Site.Buffer.Manual),list(B.Code,Site.ID,Site.LatD,Site.LonD,Site.Lat.Unc,Site.Lon.Unc,Site.Buffer.Manual)])
  
  
  # 4.19) Update DOI field
  Data[is.na(B.DOI),B.DOI:=B.Url]
# 5) Reconfigure to ERA v1.0 format ####
# This could be integrated earlier in the script for better efficiency 

Knit.V1<-function(Control.N,Control.For,Data,Mulch,Analysis.Function,NCols){
  
  C.Descrip.Col<-"ED.Treatment"
  T.Descrip.Col<-"ED.Treatment"
  
  
  Control.N<-as.numeric(Control.N)
  C.Data.Cols<-c("B.Code","B.Author.Last","B.Date","B.Journal","B.DOI","Site.LatD","LatM","LatS","LatH","Site.LonD","LonM","LonS","LonH","LatDiff","Site.Elevation",
                 "Country","ISO.3166.1.alpha.3","Site.Type","Site.ID","Site.MAT","Site.MAP","TAP","MSP","TSP","Soil.Type","Soil.Classification","Site.Soil.Texture",
                 "SOC","SOC.Unit","SOC.Depth","Soil.pH","Soil.pH.Method","ED.Plant.Start","ED.Plant.End","ED.Harvest.Start","ED.Harvest.End","Final.Reps",
                 "EX.Plot.Size","TID",C.Descrip.Col,paste0("C",1:NCols),"F.NO","F.NI","ED.Mean.T","ED.Error","ED.Error.Type",C.Descrip.Col,
                 "Site.Buffer.Manual","MeanFlip","Feed.Source","Out.Partial.Outcome.Name","Out.Partial.Outcome.Code")
  Ctrl<-Data[N==Control.N,..C.Data.Cols]
  
  C.Cols<-c("Code","Author","Date","Journal","DOI","LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff","Elevation","Country","ISO.3166.1.alpha.3","Site.Type",
            "Site.ID","MAT","MAP","TAP","MSP","TSP","Soil.Type","Soil.Classification","Soil.Texture","SOC","SOC.Unit","SOC.Depth","Soil.pH","Soil.pH.Method",
            "Plant.Start","Plant.End","Harvest.Start","Harvest.End","Rep","Plot.Size","CID","C.Descrip",
            paste0("C",1:NCols),"C.NO","C.NI","MeanC","MeanC.Error","Mean.Error.Type","C.Descrip.Clean","Buffer.Manual","MeanFlip","C.Feed.Source","Partial.Outcome.Name","Partial.Outcome.Code")
  
  colnames(Ctrl)<-C.Cols
  
  
  T.Data.Cols<-c("TID",T.Descrip.Col,paste0("C",1:NCols),"F.NI","F.NO","R.Seq","V.Level.Name","Tree","Duration",
                 "M.Year","M.Year.Start","M.Year.End","M.Season.Start","M.Season.End","EU","Out.Code","Out.Unit","ED.Mean.T","ED.Error","Out.Depth.Upper","Out.Depth.Lower",
                 "ED.Data.Loc","USD2010.C","USD2010.T",T.Descrip.Col,"V.Level.Name","Tree","R.Seq","Feed.Source","V.Species")
  
  T.Cols<-c("TID","T.Descrip",paste0("T",1:NCols),"T.NI","T.NO","Diversity","Variety","Tree","Duration",
            "M.Year","M.Year.Start","M.Year.End","Season.Start","Season.End","EU","Outcome","Units","MeanT","MeanT.Error","Upper","Lower","DataLoc",
            "USD2010.C","USD2010.T","T.Descrip.Clean","Variety.Clean","Tree.Clean","Diversity.Clean","T.Feed.Source","Species")
  
  Control.For<-as.numeric(unlist(strsplit(as.character(Control.For),"[/][/]")))
  
  Trt<-rbindlist(lapply(Control.For,FUN=function(i){
    X<-Data[N==i,..T.Data.Cols]
    colnames(X)<-T.Cols
    X
  }))
  
  Ctrl<-Ctrl[rep(1,nrow(Trt))]
  Ctrl[,Mulch.Code:=Mulch]
  X<-cbind(Ctrl,Trt)
  X[,Analysis.Function:=Analysis.Function]
  X
  
}
Verbose<-F

cl<-makeCluster(Cores)
clusterEvalQ(cl, list(library(data.table)))
clusterExport(cl,list("Comparisons","Knit.V1","Data","Verbose","NCols"),envir=environment())
registerDoSNOW(cl)

ERA.Reformatted<-rbindlist(
  parLapply(cl,1:nrow(Comparisons),fun = function(i){
    #pblapply(1:nrow(Comparisons),FUN = function(i){
    if(Verbose){print(paste(Comparisons[i,paste(B.Code,Control.N)]," | i = ",i))}
    
    Knit.V1(Control.N=Comparisons[i,Control.N],Control.For=Comparisons[i,Control.For],Data,
            Mulch=Comparisons[i,Mulch.Code],Analysis.Function = Comparisons[i,Analysis.Function],NCols=NCols)
  }))

stopCluster(cl)



  # 5.1.1) Add in outcomes that are ratios of Trt/Cont already ####
  Data.R<-Data[(!is.na(ED.Comparison) & !is.na(ED.Treatment)) & Out.Subind!="Land Equivalent Ratio",c("ED.Treatment","ED.Int","ED.Rot","ED.Comparison","B.Code","N","ED.Outcome")]
  
  
  # Use this to check outcomes are appropriate
  if(F){
    Data.R[order(ED.Outcome),unique(paste(B.Code,ED.Outcome))]
  }
  
  # Outcomes where the Treatment and Control are the same
  Data.R<-Data.R[!(ED.Treatment==ED.Comparison)]
  
  # Check & remove entries where ED.Treatment == ED.Comparison
  if(nrow(Data.R[ED.Treatment==ED.Comparison])>0){
    View(Data.R[ED.Treatment==ED.Comparison])
  }
  
  Data.R<-Data.R[ED.Treatment!=ED.Comparison]
  
  # Hack to fix DK0056 issue where the comparison is a different rotation sequence
  N<-Data[B.Code=="DK0056",stringr::str_count(ED.Treatment,"<<")]
  X<-unlist(lapply(strsplit(Data[B.Code=="DK0056",ED.Treatment][N>1],"<<"),FUN=function(X){
    paste(c(X[1],X[2]),collapse="<<")
  }))
  
  
  Data[B.Code=="DK0056",Temp:=stringr::str_count(ED.Treatment,"<<")>1]
  Data[B.Code=="DK0056" & Temp==T,ED.Treatment:=X]
  
  Data[B.Code=="DK0056",ED.Treatment]
  
  
  # Need to add in Analysis Function
  Verbose=T
  ERA.Reformatted.Ratios<-rbindlist(pblapply(1:nrow(Data.R),FUN = function(i){
    if(Verbose){print(paste(Data.R[i,paste(B.Code,N)]," i = ",i))}
    Control.N<-Data[match(Data.R[i,paste(B.Code,ED.Comparison)],Data[,paste(B.Code,ED.Treatment)]),N][1]
    
    if(is.na(Control.N)|length(Control.N)==0){
      print(paste0("No Match - ",Data.R[i,paste(B.Code,N)]," | i = ",i))
      NULL
    }else{
      Control.For<-Data.R[i,N]
      
      AF<-"Simple"
      
      A<-Data[c(Control.N,Control.For),is.na(ED.Treatment)]
      if(any(A)){
        B<-Data[c(Control.N,Control.For),is.na(ED.Int)]
        if(any(B)){
          if(sum(B)==2){
            AF<-"Sys.Int.vs.Int"
          }else{
            AF<-"Sys.Int.vs.Mono"
          }
        }else{
          C<-Data[c(Control.N,Control.For),is.na(ED.Rot)]
          if(sum(C)==2){
            AF<-"Sys.Rot.vs.Rot"
          }else{
            AF<-"Sys.Rot.vs.Mono"
          }
        }}
      
      
      Knit.V1(Control.N=Control.N,Control.For=Control.For,Data=Data,Mulch="",Analysis.Function = AF,NCols=NCols)
    }
  }))
  
  ERA.Reformatted.Ratios[,MeanC:=NA][,MeanC.Error:=NA][,MeanC.Error.Type:=NA][,Analysis.Function:="Ratios"]
  nrow(ERA.Reformatted.Ratios[is.na(C.Descrip)])
  
  # Check valid comparison is present
  C.COLS<-paste0("C",1:NCols)
  T.COLS<-paste0("T",1:NCols)
  
  N.Log<-unlist(pblapply(1:nrow(ERA.Reformatted.Ratios),FUN=function(i){
    A<-unlist(ERA.Reformatted.Ratios[i,..C.COLS])
    A<-A[!(grepl("h",A)|is.na(A))]
    B<-unlist(ERA.Reformatted.Ratios[i,..T.COLS])
    B<-B[!(grepl("h",B)|is.na(B))]
    Valid<-sum(!B %in% A)>0
  }))
  
  if(sum(!N.Log)>1){
    Ratios.No.Comparison<-ERA.Reformatted.Ratios[!N.Log,]
    View(Ratios.No.Comparison)
    rm(Ratios.No.Comparison)
    # ERA.Reformatted.Ratios[!N.Log]
  }
  
  ERA.Reformatted.Ratios<-ERA.Reformatted.Ratios[N.Log]
  
  ERA.Reformatted.Ratios[,MeanC.Error.Type:=NULL]
  
  rm(C.COLS,T.COLS,N.Log)
  
  # 5.1.2) Add in LER ####
  # Generate comparison by removing diversification components
  
  Data.R<-Data[Out.Code==103,]
  Data.R<-Data.R[!grepl("sole",ED.Treatment)]
  Data.R[,N:=1:.N]
  Data.R[is.na(T.Name2),T.Name2:=IN.Level.Name2]
  Data.R[is.na(T.Name2),T.Name2:=R.Level.Name]
  
  # Perhaps best to create a new "Data" object to use in Knit.V1
  Data.R.Cont<-Data.R
  Data.R.Cont<-data.frame(Data.R.Cont)
  
  Div.Codes<-PracticeCodes1[Practice %in% c("Intercropping","Rotation","Alleycropping","Scattered Trees","Silvopasture",
                                            "Parklands","Agroforestry Fallow","Green Manure","Improved Fallow"),Code]
  
  
  Data.R.Cont<-rbindlist(pblapply(1:nrow(Data.R.Cont),FUN=function(i){
    X<-Data.R.Cont[i,paste0("C",1:10)]
    
    if("h2" %in% X){
      print(paste("Error - h2 present:",paste(Data.R.Cont[i,c("ED.Treatment","ED.Int","ED.Rot","B.Code")],collapse = "-")))
    }
    N<-which(X %in% Div.Codes)
    
    if(length(N) == 0){
      print(paste("Error - no diversification present:",i,"-",paste(Data.R.Cont[i,c("ED.Treatment","ED.Int","ED.Rot","B.Code")],collapse = "-")))
    }else{
      
      X[N[1]]<-"h2"
      N<-N[-1]
      if(length(N)>0){
        X[N]<-NA
      }
    }
    
    Y<-Data.R.Cont[i,]
    Y[,paste0("C",1:10)]<-X
    Y$All.Codes<-list(unique(X[!is.na(X)]))
    
    data.table(Y)
    
    
  }))
  Data.R.Cont[,R.Seq:=NA]
  Data.R.Cont[,Int.Tree:=NA]
  Data.R.Cont[,Rot.Tree:=NA]
  Data.R.Cont[,TID:=paste0(TID,".LER.Control")]
  Data.R.Cont[,T.Name2:="LER Control"]
  Data.R.Cont[,IN.Level.Name2:="LER Control"]
  Data.R.Cont[,R.Level.Name:="LER Control"]
  Data.R.Cont[,ED.Mean.T:=NA]
  Data.R.Cont[,ED.Error:=NA]
  Data.R.Cont[,ED.Error.Type:=NA]
  
  Data.R.Cont[,N:=(nrow(Data.R)+1):(nrow(Data.R)*2)]
  
  
  Data2<-rbind(Data.R,Data.R.Cont)
  
  Verbose=F
  ERA.Reformatted.LER<-rbindlist(pblapply(1:nrow(Data.R),FUN = function(i){
    
    if(Verbose){print(paste(Data.R[i,paste(B.Code,N)]," i = ",i))}
    
    Control.N<-Data.R.Cont[i,N]
    Control.For<-Data.R[i,N]
    
    AF<-"Simple"
    
    A<-Data2[c(Control.N,Control.For),is.na(ED.Treatment)]
    if(any(A)){
      B<-Data[c(Control.N,Control.For),is.na(ED.Int)]
      if(any(B)){
        if(sum(B)==2){
          AF<-"Sys.Int.vs.Int"
        }else{
          AF<-"Sys.Int.vs.Mono"
        }
      }else{
        C<-Data[c(Control.N,Control.For),is.na(ED.Rot)]
        if(sum(C)==2){
          AF<-"Sys.Rot.vs.Rot"
        }else{
          AF<-"Sys.Rot.vs.Mono"
        }
      }}
    
    Knit.V1(Control.N=Control.N,Control.For=Control.For,Data=Data2,Mulch="",Analysis.Function = AF,NCols=NCols)
    
  }))
  
  ERA.Reformatted.LER[,Analysis.Function:="LER"]
  
  # Set MeanC to equal 1 where MeanC is NA
  ERA.Reformatted.LER[is.na(MeanC),MeanC.Error:=NA][is.na(MeanC),MeanC:=1]
  
  rm(Data.R,Data.R.Cont,Data2)
  # 5.2) Combine reformatted datasets ####
  ERA.Reformatted<-rbind(ERA.Reformatted,ERA.Reformatted.Ratios,ERA.Reformatted.LER)
  rm(ERA.Reformatted.Ratios,ERA.Reformatted.LER,Knit.V1)
  
  # 5.3) Recode residues + incorporation as a control for mulching + feed substitution control  ####
  # Simplest option might be where we have imbalanced residues (i.e. where different residues codes are present in control vs treatment)
  # to recode these.  
  # Where we have one mulch code in control vs >1 in treatment what to do?
  
  Dpracs<-c("Crop Rotation","Intercropping","Improved Fallow","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
  Dpracs<-PracticeCodes1[!grepl("h",Code)][Practice %in% Dpracs,Code]
  
  Rpracs<-c("Agroforestry Pruning","Mulch","Crop Residue","Crop Residue Incorporation")
  X<-lapply(Rpracs,FUN=function(R){
    PracticeCodes1[!grepl("h",Code)][Practice %in% R,Code]
  })
  names(X)<-Rpracs
  Rpracs<-X
  rm(X)
  
  # Mulch codes present in Control & Treatment + Dprac should be present (as per CompareFun) in Trt else we are not in a complex situation
  # Recode either where Control has Rprac category code that Trt does not, or Trt has >1 and control has 1 practice.
  # ***ISSUE*** Should Agroforestry Prunings be excluded?? (here and in CompareFun?)
  
  C.Cols<-which(colnames(ERA.Reformatted) %in% paste0("C",1:NCols))
  
  Verbose<-F
  
  cl<-makeCluster(Cores)
  clusterEvalQ(cl, list(library(data.table)))
  clusterExport(cl,list("ERA.Reformatted","Dpracs","Rpracs","C.Cols","Verbose","NCols"),envir=environment())
  registerDoSNOW(cl)
  
  
  ERA.Reformatted<-rbindlist(parLapply(cl,1:nrow(ERA.Reformatted),fun=function(i){
    
    #ERA.Reformatted<-rbindlist(pblapply(1:nrow(ERA.Reformatted),FUN =function(i){
    
    X<-data.frame(ERA.Reformatted[i,])
    
    T.Code<-as.vector(unlist(X[,paste0("T",1:NCols)]))
    T.Code<-T.Code[!is.na(T.Code)]
    
    if(Verbose){print(paste0("row = ",i))}
    
    if(any(T.Code %in% Dpracs)){
      C.Code<-as.vector(unlist(X[,paste0("C",1:NCols)]))
      C.Code<-C.Code[!is.na(C.Code)]
      for(j in 1:length(Rpracs)){
        if(Verbose){print(paste0("row = ",i," | RPracs = ",names(Rpracs)[j]," - ",j))}
        Ts<-T.Code[T.Code %in% Rpracs[[j]]]
        Cs<-C.Code[C.Code %in% Rpracs[[j]]]
        CnT<-Cs[!Cs %in% Ts]
        
        if(length(CnT)>0 | (length(Ts)>1 & length(Cs)==1)){
          T.Code<-T.Code[!T.Code %in% Ts]
          C.Code<-C.Code[!C.Code %in% Cs]
          
          if(names(Rpracs)[j] != "Agroforestry Pruning"){
            
            T.Code<-c(T.Code,unique(unlist(lapply(Ts,FUN=function(X){unlist(strsplit(X,"[.]"))[1]}))))
            T.Code<-c(T.Code,rep(NA,NCols-length(T.Code)))
            
            C.Code<-c(C.Code,unique(unlist(lapply(Cs,FUN=function(X){unlist(strsplit(X,"[.]"))[1]}))))
            C.Code<-c(C.Code,rep(NA,NCols-length(C.Code)))
            
          }else{
            T.Code<-unique(c(T.Code,gsub("a17|a16","a15",Ts)))
            
            C.Code<-unique(c(C.Code,gsub("a17|a16","a15",Cs)))
          }
          
          X[,paste0("T",1:NCols)]<-T.Code
          X[,paste0("C",1:NCols)]<-C.Code
          
        }
      }
      
      T.Code<-as.vector(unlist(X[,paste0("T",1:NCols)]))
      C.Code<-as.vector(unlist(X[,paste0("C",1:NCols)]))
      # if incorporation is present in the control but not the treatment and mulch is present in the treatment
      # we recode the incorporation code be a h37 control code
      
      if(any(grepl("b41",C.Code)) & !any(grepl("b41",T.Code)) & any(T.Code %in% Rpracs$Mulch)){
        X[grepl("b41",X)]<-"h37"
      }
      
    }
    
    # Change mulch ERA codes to h37 where Mulch.Code is not blank (mulch vs incorporation)
    
    M.Code<-X[,"Mulch.Code"]
    
    if(M.Code!="" & !is.na(M.Code)){
      N<-C.Cols[grep(M.Code,unlist(X[,C.Cols]))]
      X[,N]<-"h37"
    }
    
    return(data.table(X))
    
  }))
  
  stopCluster(cl)
  
  rm(Dpracs,Rpracs,C.Cols)
  
  # 5.4) Remove other Fert, Mulch, Water Harvesting and Ash codes ####
  # Need to remove then check comparisons are still present
  
  # Rm.Codes<-c("b74","b73","b99")
  Cols<-paste0(c("C","T"),rep(1:10,each=2))
  
  X<-data.frame(ERA.Reformatted)
  
  for(Col in Cols){
    X[,Col]<-gsub("b74",NA,X[,Col])
    X[,Col]<-gsub("b73",NA,X[,Col])
    X[,Col]<-gsub("b99",NA,X[,Col])
  }
  
  X<-data.table(X)
  
  # Check for any comparisons that now have no ERA practices extra in the Treatment 
  Y<-unlist(pblapply(1:nrow(X),FUN=function(i){
    T.Codes<-unlist(X[i,paste0("T",1:10)])
    T.Codes<-T.Codes[!is.na(T.Codes)]
    T.Codes<-T.Codes[!grepl("h",T.Codes)]
    
    C.Codes<-unlist(X[i,paste0("C",1:10)])
    C.Codes<-C.Codes[!is.na(C.Codes)]
    C.Codes<-C.Codes[!grepl("h",C.Codes)]
    
    # Are any there ERA practices in the treatment not in the control?
    Z<-length(T.Codes[!T.Codes %in% C.Codes])>0 
    Z
  }))
  
  ERA.Reformatted<-X[Y]
  rm(X,Y,Cols)
  
  # 5.5) Reconstitution of Animal Diet codes ####
  # This section could be moved to Animals.Out then integrated into MT.Out2
  # Update Diet Item Names and add Codes
  A.Corrections<-fread("Concept Scheme/Harmonization/Animal_Lists.csv",header=T,strip.white=F,encoding="Latin-1")
  
  A.Corrections.Process<-A.Corrections[,38:44]
  colnames(A.Corrections.Process)<-unlist(lapply(strsplit(colnames(A.Corrections.Process),"[.][.][.]"),"[",1))
  A.Corrections.Process<-A.Corrections.Process[!is.na(B.Code)]
  
  A.Corrections.Unit<-A.Corrections[,15:16][D.Unit.Amount!="" & !is.na(D.Unit.Amount)]
  
  A.Corrections<-A.Corrections[,1:13]
  colnames(A.Corrections)<-unlist(lapply(strsplit(colnames(A.Corrections),"[.][.][.]"),"[",1))
  
  # Check for non-matches
  
  # Check for any missing items in the corrections table
  A.Diet.Missing<-  Animals.Diet[!(D.Item %in% A.Corrections[,D.Item]|D.Item=="Entire Diet"),unique(D.Item)]
  if(length(A.Diet.Missing[!is.na(A.Diet.Missing)])>0){
    View(A.Diet.Missing[!is.na(A.Diet.Missing)])
  }

  # Change "Yes" in Animals.Out control columns to logical
  Animals.Out[A.Feed.Add.C=="Yes",A.Feed.Add.C:=T][,A.Feed.Add.C:=as.logical(A.Feed.Add.C)]
  Animals.Out[A.Feed.Sub.C=="Yes",A.Feed.Sub.C:=T][,A.Feed.Sub.C:=as.logical(A.Feed.Sub.C)]
  
  # Add Logic column for if this is feed substitution or addition
  Animals.Out[,A.Feed.Add:=any(!is.na(A.Feed.Add.1)|!is.na(A.Feed.Add.2)|!is.na(A.Feed.Add.3)|!is.na(A.Feed.Add.C)),by=c("A.Level.Name","B.Code")]
  Animals.Out[,A.Feed.Sub:=any(!is.na(A.Feed.Sub.1)|!is.na(A.Feed.Sub.2)|!is.na(A.Feed.Sub.3)|!is.na(A.Feed.Sub.C)),by=c("A.Level.Name","B.Code")]
  
  # Add Logic column for if this is feed substitution or addition
  N<-match(Animals.Diet[,paste(B.Code,A.Level.Name)],Animals.Out[,paste(B.Code,A.Level.Name)])
  
  Animals.Diet[,A.Feed.Add:=Animals.Out[N,A.Feed.Add]]
  Animals.Diet[,A.Feed.Add.C:=Animals.Out[N,A.Feed.Add.C]]
  
  Animals.Diet[,A.Feed.Sub:=Animals.Out[N,A.Feed.Sub]]
  Animals.Diet[,A.Feed.Sub.C:=Animals.Out[N,A.Feed.Sub.C]]
  
  # Add Codes for Diet Items
  N<-match(Animals.Diet[,D.Item],A.Corrections[,D.Item])
  
  Animals.Diet[,I.Code.Add:=A.Corrections[N,Add.Codes]
  ][,I.Code.Sub:=A.Corrections[N,Sub.Codes]]
  
  # Update Type and Item Names
  Animals.Diet[!is.na(N),D.Type:=A.Corrections[N[!is.na(N)],D.Type.Correct]]
  Animals.Diet[!is.na(N),D.Item:=A.Corrections[N[!is.na(N)],D.Item.Correct]]
  rm(A.Corrections)
  
  # Add Item Diet Codes
  Animals.Diet[A.Feed.Sub==T,D.Item.Codes:=I.Code.Sub]
  Animals.Diet[A.Feed.Add==T,D.Item.Codes:=I.Code.Add]
  Animals.Diet[is.na(A.Feed.Add) & is.na(A.Feed.Sub),D.Item.Codes:=I.Code.Add]
  
  # Update Process
  # Check for any missing items x processes in the corrections table
  A.Diet.Missing.Process<-Animals.Diet[!is.na(D.Process)][is.na(match(Animals.Diet[!is.na(D.Process),paste(D.Item,D.Process)],A.Corrections.Process[,paste(D.Item.Correct,D.Process)]))]
  if(nrow(A.Diet.Missing.Process)>0){
    View(A.Diet.Missing.Process)
  }
  rm(A.Diet.Missing.Process)
  
  N<-match(Animals.Diet[!is.na(D.Process),paste(D.Item,D.Process)],A.Corrections.Process[,paste(D.Item.Correct,D.Process)])
  
  Animals.Diet[!is.na(D.Process),D.Process.Code:=A.Corrections.Process[N,D.Process.Code]]
  Animals.Diet[!is.na(D.Process),D.Process:=A.Corrections.Process[N,D.Process.Correct]]
  
  rm(A.Corrections.Process)
  
  # Update Units
  N<-match(Animals.Diet[,D.Unit.Amount],A.Corrections.Unit[,D.Unit.Amount])
  Animals.Diet[!is.na(N),D.Unit.Amount:=A.Corrections.Unit[N[!is.na(N)],D.Unit.Amount.Correct]]
  rm(A.Corrections.Unit,N)
  
  # Join Codes and remove duplicates
  D.Join<-function(A,B){
    X<-c(unlist(strsplit(A,"-")),B)
    unique(X[!is.na(X)])
  }
  
  Animals.Diet[,N:=1:.N]
  Animals.Diet[,D.Codes.Final:=Animals.Diet[,list(D.Codes.Final=list(D.Join(D.Item.Codes,D.Process.Code))),by=N][,D.Codes.Final]]
  rm(D.Join)
  
  # Change Kg to g
  Animals.Diet[,D.Amount:=as.numeric(D.Amount)]
  Animals.Diet[D.Unit.Amount=="kg",D.Amount:=D.Amount*1000]
  Animals.Diet[D.Unit.Amount=="kg",D.Unit.Amount:="g"]
  
  # Round amounts to 0 dp 
  Animals.Diet[,D.Amount.Round:=round(D.Amount)]
  
  # Create a column of all non-diet animal codes in Animals.Out
  A.Codes<-fread("Concept Scheme/Harmonization/Animal_Lists.csv",header=T,encoding="Latin-1")
  A.Codes<-A.Codes[D.Prac!="" & !is.na(D.Prac),list(D.Prac,Code,Descrip)]
  
  A<-A.Codes[match(Animals.Out[,A.Pasture.Man],A.Codes[,Descrip]),Code]
  B<-A.Codes[match(Animals.Out[,A.Manure.Man],A.Codes[,Descrip]),Code]
  C<-A.Codes[match(Animals.Out[,A.Aquasilvaculture],A.Codes[,Descrip]),Code]
  
  # Would be faster to combine into 3 level matrix and apply across 3rd level of matrix
  
  AA<-A.Codes[match(Animals.Out[,A.Feed.Pro.1],A.Codes[,Descrip]),Code]
  BB<-A.Codes[match(Animals.Out[,A.Feed.Pro.2],A.Codes[,Descrip]),Code]
  CC<-A.Codes[match(Animals.Out[,A.Feed.Pro.3],A.Codes[,Descrip]),Code]
  
  for(i in 1:nrow(Animals.Out)){
    X<-unique(c(A[i],B[i],C[i]))
    if(length(X)>1){
      X<-paste(X[!is.na(X)],collapse = "-")
    }
    
    # Technically this is a diet code so Non.Diet.Codes field would be better named Non.Feed.Codes
    if(Animals.Out[i,!is.na(A.Grazing) & A.Grazing=="Yes"]){
      X<-paste0(c(X[!is.na(X)],"h60"),collapse = "-")
    }
    
    Y<-unique(c(AA[i],BB[i],CC[i]))
    if(length(Y)>1){
      Y<-paste(Y[!is.na(Y)],collapse = "-")
    }
    
    Animals.Out[i,Non.Diet.Codes:=X]
    Animals.Out[i,Process.Codes:=Y]
  }
  
  rm(A,B,C,AA,BB,CC,A.Codes,X,Y)
  
  # This could adjusted with logic when amounts are low (e.g. 1 dp when amounts are <1)
  #Animals.Diet[D.Amount>=1,D.Amount.Round:=round(D.Amount)]
  #Animals.Diet[D.Amount<1,D.Amount.Round:=round(D.Amount,1)]
  
  # Entire Diets
  # write.table(Animals.Diet[D.Type=="Entire Diet",c("B.Code","A.Level.Name")],"clipboard",row.names = F,sep="\t")
  
  # Deal with Diet Amount is na
  Animals.Diet[is.na(D.Amount),D.Amount.Round:=999999]
  # Deal with Diet Amount Rounded == 0  and Diet Amount Rounded is not zero
  Animals.Diet[D.Amount>0 & D.Amount.Round==0,D.Amount.Round:=999999]
  
  # Check Papers where there is nothing in the diet table but a practice is listed
  if(F){
    N<-match(Animals.Out[,paste0(A.Level.Name,B.Code)],Animals.Diet[,paste0(A.Level.Name,B.Code)])
    write.table(Animals.Out[is.na(N) & is.na(A.Feed.Add.C) & is.na(A.Feed.Sub.C),c("B.Code","A.Level.Name","A.Feed.Add.C","A.Feed.Sub.C")],"clipboard",row.names = F,sep="\t")
  }
  
    # *** ISSUE *** How to add in base codes? - Not exactly sure what this issue refers to. Investigate further
    # Run lapply ####  
    
    Verbose<-F
    
    # ISSUE? - LM0102, this paper had a control with just Base codes (so no entry of the control name in the Diet Table) this caused a
    # problem with the lapply below. We need to add a line to the Animal.Diet for where the control is the same as the Base practice, but
    # is not mentioned in the Diet Table (Diet.Out), but is present in the animal diet practices names (Animals.Out)
    
    # Function below currently does not account for structuring variables that constrain the comparisons that can be made
    # Another reason to move to Animals.Out/MT.Out
    
    Animals.Recode<-rbindlist(pblapply(Animals.Diet[,unique(B.Code)],FUN=function(BC){
      
      if(Verbose)(print(BC))
      
      Y<-Animals.Diet[B.Code==BC  & D.Type != "Entire Diet" & D.Amount.Round!=0]
      Y2<-Animals.Out[B.Code==BC]
      
      if(nrow(Y)==0 & nrow(Y2)==0){
        cat(paste0("\n Check: ", BC," - Non-diet Base Practice? \n"))
        Trt<-paste(Y2[!is.na(Non.Diet.Codes) & A.Level.Name!="Base",unlist(strsplit(Non.Diet.Codes,"-"))],collapse="-")
        Base<-paste(Y2[A.Level.Name=="Base",unlist(strsplit(Non.Diet.Codes,"-"))],collapse="-")
        
        data.frame(B.Code=BC,A.T.Name="Base",T.Codes=NA,A.C.Name=as.character(NA),
                   C.Codes=as.character(NA),Type="Non-diet Practice?",N.Control=NA,Error.Flag=if(is.null(Trt)){"No Practice Codes"}else{NA},
                   Base.Codes=Base)
      }else{
        # Are at least 2 practices present excluding the base? or is there no control practice (so not a diet comparison)?
        if(Y2[,sum(A.Level.Name!="Base")]<=1 | (Y2[,all(is.na(A.Feed.Add.C))] & Y2[,all(is.na(A.Feed.Sub.C))])){
          
          if((Y2[,sum(A.Level.Name!="Base")]>1 & Y2[,all(is.na(A.Feed.Add.C))] & Y2[,all(is.na(A.Feed.Sub.C))])){
            cat(paste0("\n No Control Diet Practice for ",BC," \n"))
          }
          
          A.NAME<-Y[A.Level.Name!="Base",unique(A.Level.Name)]
          
          Trt<-Y[A.Level.Name!="Base",list(Trt=list(unique(unlist(D.Codes.Final)))),by=A.Level.Name]
          Trt2<-Y2[!is.na(Non.Diet.Codes) & A.Level.Name!="Base",list(Trt=list(unlist(strsplit(Non.Diet.Codes,"-")))),by=A.Level.Name]
          
          Base<-unique(c(Y2[A.Level.Name=="Base",unlist(strsplit(Non.Diet.Codes,"-"))],Y[A.Level.Name=="Base",unlist(D.Codes.Final)]))
          Base<-Base[!is.na(Base)]
          for(jj in A.NAME){
            Trt[A.Level.Name==jj,Trt:=unique(c(unlist(Trt[A.Level.Name==jj]),unlist(Trt2[A.Level.Name==jj])))]
            Trt[A.Level.Name==jj,Trt:=paste0(unlist(Trt)[!unlist(Trt) %in% Base],collapse="-")]
          }
          
          Trt<-unlist(Trt[,Trt])
          
          if(length(A.NAME)==0){
            A.NAME<-NA
            Trt<-NA
          }
          
          
          # Note this includes any base practice present
          data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=Trt,A.C.Name=as.character(NA),
                     C.Codes=as.character(NA),Type="Single Diet Practice",N.Control=NA,Error.Flag=if(is.null(Trt)){"No Practice Codes"}else{NA},
                     Base.Codes=paste0(Base,collapse = "-"))
          
        }else{
          
          # 1) Additions
          # Todd: Addition code cannot be shared with control?
          
          XA<-Y[A.Feed.Add==T & A.Level.Name!="Base" & is.na(A.Feed.Add.C),]
          
          if(nrow(XA)>0){
            ZA<-rbindlist(lapply(Y2[A.Feed.Add.C==T,A.Level.Name],FUN=function(C.NAME){
              XC<-Y[A.Level.Name %in% c("Base",C.NAME)]
              XC<-XC[,list(D.Amount.Round=sum(D.Amount.Round)),by=c("D.Item","D.Process")]
              XC[D.Amount.Round>999999,D.Amount.Round:=999999]
              
              # Add back fields
              XC<-cbind(XC,Y[match(XC[,paste(D.Item,D.Process)],Y[,paste(D.Item,D.Process)]),c("D.Codes.Final","D.Item.Codes","A.Level.Name")])
              
              rbindlist(lapply(XA[is.na(A.Feed.Add.C),unique(A.Level.Name)],FUN=function(A.NAME){
                if(Verbose){cat(paste0("\n ",BC,": Addition: Control = ",C.NAME," vs Trt = ",A.NAME," \n"))}
                
                XA.Diet<-Y[A.Level.Name %in% c("Base",A.NAME),c("D.Item","D.Process","D.Amount.Round")] 
                
                # Combine duplicate items
                XA.Diet<-XA.Diet[,list(D.Amount.Round=sum(D.Amount.Round)),by=c("D.Item","D.Process")]
                XA.Diet[D.Amount.Round>999999,D.Amount.Round:=999999]
                
                # Add in Control Diet
                XA.Diet[,D.Amount.C:=XC[match(XA.Diet[,paste(D.Item,D.Process)],XC[,paste(D.Item,D.Process)]),D.Amount.Round]]
                
                # Add in Codes
                XA.Diet[,D.Codes.Final:=Y[match(XA.Diet[,paste(D.Item,D.Process)],Y[,paste(D.Item,D.Process)]),D.Codes.Final]]
                
                # Add in logic columns for shared/decreasing/increasing items between control and treatment
                XA.Diet[,Shared:=paste(D.Item,D.Process) %in% XC[,paste(D.Item,D.Process)]]
                XA.Diet[is.na(D.Amount.C),D.Amount.C:=0]
                XA.Diet[,Increase:=D.Amount.Round>D.Amount.C][,Decrease:=D.Amount.Round<D.Amount.C]
                
                if(nrow(XA.Diet)==0){
                  data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=as.character(NA),A.C.Name=C.NAME,C.Codes=as.character(NA),Type="Addition",
                             N.Control=XC[A.Level.Name!="Base",length(unique(A.Level.Name))],Error.Flag="No Rows in XA",Base.Codes=NA)
                }else{
                  
                  if(any(XA.Diet[,Decrease])){Error.Flag<-"Decreasing Item - Sub not Add?"}else{Error.Flag<-NA}
                  
                  # Items being added to diet
                  Trt<-XA.Diet[Shared==F,unique(unlist(D.Codes.Final))]
                  
                  # Items in the control
                  Con<-XA.Diet[Shared==T,unique(unlist(D.Codes.Final))]
                  
                  # Add h-codes
                  Base<-Con[Con %in% Trt]
                  
                  if(length(Base[Base!="h60"])>0){
                    Con<-c(Con,"h60","h61")
                    Trt<-c(Trt,"h60")
                  }else{
                    Con<-c(Con,"h60")
                  }
                  
                  # Add in non-diet codes
                  A<-Y2[A.Level.Name %in% c(C.NAME,"Base") & !is.na(Non.Diet.Codes),unlist(strsplit(Non.Diet.Codes,"-"))]
                  B<-Y2[A.Level.Name %in% c(A.NAME,"Base") & !is.na(Non.Diet.Codes),unlist(strsplit(Non.Diet.Codes,"-"))]
                  Con<-unique(c(Con,A))
                  Trt<-unique(c(Trt,B))
                  
                  # Check if grazing is one practice, but not the other
                  if(sum(c("h60" %in% A,"h60" %in% B))==1){
                    cat(paste0("\n Addition: Grazing present in only practice - ",BC,": ",C.NAME," vs ",A.NAME," \n"))
                  }
                  
                  Base<-Con[Con %in% Trt]
                  Con<-Con[!Con %in% Base]
                  Trt<-Trt[!Trt %in% Base]
                  
                  data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=paste0(Trt,collapse="-"),A.C.Name=C.NAME,C.Codes=paste0(Con,collapse="-"),Type="Addition",
                             N.Control=XC[A.Level.Name!="Base",length(unique(A.Level.Name))],
                             Error.Flag=
                               if(length(Trt)==0){paste0(c(Error.Flag[!is.na(Error.Flag)],"No Addition Code in Trt"),sep="/")
                               }else{
                                 if(length(Trt[!Trt %in% Con])==0){paste0(c(Error.Flag[!is.na(Error.Flag)],"All Trt Codes in Con"),sep="/")
                                 }else{Error.Flag}
                               },
                             Base.Codes=paste0(Base,collapse="-"))
                }
                
              }))
              
            }),use.names = T)
          }else{ZA<-NULL}
          
          
          # 2) Substitutions
          # Keep Codes for Substituted Item in Trt & Any shared codes that are not in Trt
          XS<-Y[A.Feed.Sub==T & A.Level.Name!="Base" & is.na(A.Feed.Sub.C),]
          
          if(nrow(XS)==0 & nrow(XA)==0){
            # Not a study about diet - probably pasture management (probably something that is fine and does not need amending in the main dataset)
            
            A.NAME<-Y2[A.Level.Name!="Base",A.Level.Name]
            Trt<-Y2[A.Level.Name!="Base",c("A.Level.Name","A.Codes")]
            
            Base<-unique(c(Y2[A.Level.Name=="Base",unlist(strsplit(Non.Diet.Codes,"-"))],Y[A.Level.Name=="Base",unlist(D.Codes.Final)]))
            
            for(jj in A.NAME){
              Trt[A.Level.Name==jj,A.Codes:=paste0(unlist(A.Codes)[!unlist(A.Codes) %in% Base],collapse="-")]
            }
            
            data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=Trt[,A.Codes],A.C.Name=as.character(NA),
                       C.Codes=as.character(NA),Type="Non-diet Animal Practices",N.Control=NA,Error.Flag=NA,Base.Codes=paste0(Base,collapse = "-"))
            
            
          }else{ 
            if(nrow(XS)>0){
              
              ZS<-rbindlist(lapply(Y2[A.Feed.Sub.C==T,A.Level.Name],FUN=function(C.NAME){
                XC<-Y[A.Level.Name %in% c(C.NAME,"Base")]
                # Combine duplicate items
                XC<-XC[,list(D.Amount.Round=sum(D.Amount.Round)),by=c("D.Item","D.Process")]
                XC[D.Amount.Round>999999,D.Amount.Round:=999999]
                
                # Add in Codes
                XC<-cbind(XC,Y[match(XC[,paste(D.Item,D.Process)],Y[,paste(D.Item,D.Process)]),c("D.Codes.Final","D.Item.Codes","A.Level.Name")])
                
                rbindlist(lapply(unique(XS[,A.Level.Name]),FUN=function(A.NAME){
                  if(Verbose){cat(paste("\n Substitution",C.NAME,"vs",A.NAME," \n"))}
                  
                  
                  XS.Diet<-Y[A.Level.Name %in% c("Base",A.NAME),c("D.Item","D.Process","D.Amount.Round")] 
                  
                  # Combine duplicate items
                  XS.Diet<-XS.Diet[,list(D.Amount.Round=sum(D.Amount.Round)),by=c("D.Item","D.Process")]
                  XS.Diet[D.Amount.Round>999999,D.Amount.Round:=999999]
                  
                  # Add in Control Diet
                  XS.Diet[,D.Amount.C:=XC[match(XS.Diet[,paste(D.Item,D.Process)],XC[,paste(D.Item,D.Process)]),D.Amount.Round]]
                  
                  # Add in Codes
                  XS.Diet[,D.Codes.Final:=Y[match(XS.Diet[,paste(D.Item,D.Process)],Y[,paste(D.Item,D.Process)]),D.Codes.Final]]
                  
                  # Add in logic columns for shared/decreasing/increasing items between control and treatment
                  XS.Diet[,Shared:=paste(D.Item,D.Process) %in% XC[,paste(D.Item,D.Process)]]
                  XS.Diet[is.na(D.Amount.C),D.Amount.C:=0]
                  XS.Diet[,Increase:=D.Amount.Round>D.Amount.C][,Decrease:=D.Amount.Round<D.Amount.C]
                  
                  A<-XC[!paste(D.Item,D.Process) %in% XS.Diet[,paste(D.Item,D.Process)],c("D.Item","D.Process","D.Amount.Round","D.Codes.Final")][,Shared:=F][,D.Amount.C:=D.Amount.Round][,Increase:=F][,Decrease:=T][,D.Amount.Round:=0]
                  
                  XS.Diet<-rbind(XS.Diet,A)
                  
                  
                  if(!(any(XS.Diet[,Increase])& any(XS.Diet[,Decrease]))){
                    
                    data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=as.character(NA),A.C.Name=C.NAME,C.Codes=as.character(NA),Type="Substitution",
                               N.Control=XC[A.Level.Name!="Base",length(unique(A.Level.Name))],Error.Flag="Increase + Decrease Absent",Base.Codes=NA)
                    
                  }else{
                    
                    Con<-XS.Diet[Increase==F & D.Amount.C!=0,unique(unlist(D.Codes.Final))]
                    
                    # Remove Con Codes Shared With Increasing Diet Items
                    # Note this is in a different position to addition meaning shared codes are removed rather than becoming part of the Base
                    Con<-Con[!Con %in% XS.Diet[Increase==T,unique(unlist(D.Codes.Final))]]
                    
                    # Trt
                    Trt<-XS.Diet[D.Amount.Round!=0,unique(unlist(D.Codes.Final))]
                    
                    # Add h-codes
                    Base<-Con[Con %in% Trt]
                    
                    if(length(Base[Base!="h60"])>0){
                      Con<-c(Con,"h61","h60")
                      Trt<-c(Trt,"h60")
                    }else{
                      Con<-c(Con,"h60")
                    }
                    
                    # Add in non-diet codes
                    A<-Y2[A.Level.Name %in% c(C.NAME,"Base") & !is.na(Non.Diet.Codes),unlist(strsplit(Non.Diet.Codes,"-"))]
                    B<-Y2[A.Level.Name %in% c(A.NAME,"Base") & !is.na(Non.Diet.Codes),unlist(strsplit(Non.Diet.Codes,"-"))]
                    Con<-unique(c(Con,A))
                    Trt<-unique(c(Trt,B))
                    
                    # Check if grazing is one practice, but not the other
                    if(sum(c("h60" %in% A,"h60" %in% B))==1){
                      cat(paste0("\n Addition: Grazing present in only practice - ",B.CODE,": ",C.NAME," vs ",A.NAME," \n"))
                    }
                    
                    # Base Codes = Control values that are also in Trt
                    Base<-Con[Con %in% Trt]
                    
                    # Remove Con & Trt values that are shared with Base
                    Con<-Con[!Con %in% Base]
                    Trt<-Trt[!Trt %in% Base]
                    
                    data.frame(B.Code=BC,A.T.Name=A.NAME,T.Codes=paste0(Trt,collapse = "-"),A.C.Name=C.NAME,C.Codes=paste0(Con,collapse="-"),Type="Substitution",
                               N.Control=XC[A.Level.Name!="Base",length(unique(A.Level.Name))],Error.Flag=as.character(NA),Base.Codes=paste0(Base,collapse="-"))
                  }
                  
                }))
              }),use.names = T)
              
              if(is.null(ZA)){ZS}else{rbind(ZA,ZS)} 
            }else{
              ZA
            }}}}
      
    }),use.names = T)
    
    Animal.Recode.ErrorFlags<-Animals.Recode[!is.na(Error.Flag)]
    # Ignore studies that have structuring variables
    Animal.Recode.ErrorFlags<-Animal.Recode.ErrorFlags[!B.Code %in% c("DK0007","EO0080")]
    if(nrow(Animal.Recode.ErrorFlags)>0){
      # For the flagged studies from the function below AG0097 & NH0036 are OK (Base practice of grazing and hay feed)
      Animals.Recode[!B.Code %in% c("AG0097","NH0036")]
      View(Animal.Recode.ErrorFlags)
    }
    rm(Animal.Recode.ErrorFlags)
    
    ERA.Reformatted[,N:=1:.N]
    
    CodeNames<-c("Final.Residue.Code","AF.Codes","E.Codes","H.Codes","I.Codes","M.Codes","F.Codes","pH.Codes","PO.Codes","Till.Codes","WH.Codes",
                 "V.Codes", "IN.Code","R.Code","Base.Codes.No.Animals")
    
    # TO DO - extract non-valid comparisons from Y below to QC why these are occurring
    # NOTE - sytem cannot currently deal with aggregated practices
    Verbose<-F
    Y<-pblapply(unique(ERA.Reformatted[Analysis.Function!="NoDietSub.Agg" & Code %in% Animals.Recode[,B.Code],Code]),FUN=function(BC){
      # Reconstituted Animal Code Table
      X<-Animals.Recode[B.Code==BC]
      X[,A.CT.Name:=paste(A.C.Name[!is.na(A.C.Name)],A.T.Name)]
      # Reformatted ERA table
      Y<-ERA.Reformatted[Code==BC]
      # Unmodified ERA Data Table
      Z<-Data.Out[B.Code==BC]
      # Get all non-animal codes from the unmodified ERA Data Table
      Non.Animal.Codes<-lapply(1:nrow(Z),FUN=function(i){
        X<-unlist(Z[,..CodeNames][i])
        unique(X[!is.na(X)])
      })
      Z<-Z[,c("ED.Treatment","A.Level.Name")][,Non.Animal.Codes:=list(Non.Animal.Codes)]
      
      # Deal with NAs
      Z[is.na(A.Level.Name),A.Level.Name:=ED.Treatment]
      
      # Add Animal Practice Names to Y (Reformatted ERA table)
      Y[,A.Lev.C:=Z[match(Y[,C.Descrip],Z[,ED.Treatment]),A.Level.Name]]
      Y[,A.Lev.T:=Z[match(Y[,T.Descrip],Z[,ED.Treatment]),A.Level.Name]]
      Y[,A.Lev.CT:=paste(A.Lev.C,A.Lev.T)]
      # Add Non-animal diet practices to Y (Reformatted ERA table)
      Y[,Non.Animal.Codes.C:=Z[match(Y[,C.Descrip],Z[,ED.Treatment]),Non.Animal.Codes]]
      Y[,Non.Animal.Codes.T:=Z[match(Y[,T.Descrip],Z[,ED.Treatment]),Non.Animal.Codes]]
      
      # How many C/T cols are there in the Reformatted ERA table
      NCols<-sum(paste0("C",1:15) %in% colnames(Y))
      A<-lapply(1:nrow(Y), FUN=function(i){
        if(Verbose){cat(paste("\n",BC,"-",i))}
        
        # Match Reformatted ERA table to Reconstituted Animal Code Table
        N<-match(Y[i,A.Lev.CT], X[,A.CT.Name])
        
        if(!is.na(N)){
          C.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.C)],X[N,unlist(strsplit(c(C.Codes,Base.Codes),"-"))]))
          T.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.T)],X[N,unlist(strsplit(c(T.Codes,Base.Codes),"-"))],C.Codes[!grepl("h",C.Codes)]))
        }else{
          if(Y[i,A.Lev.T] == Y[i,A.Lev.C]){ # If practices are the same then derive codes directly from the Animals.Out & Animals.Diet tab
            C.Codes<-c(Animals.Diet[A.Level.Name == Y[i,A.Lev.T],unique(unlist(D.Codes.Final[!is.na(D.Codes.Final)]))],
                       Animals.Out[A.Level.Name == Y[i,A.Lev.T],unique(unlist(strsplit(Non.Diet.Codes[!is.na(Non.Diet.Codes)],"-")))])
            C.Codes<-C.Codes[C.Codes!="h60"]
            T.Codes<-c(Y[i,unlist(Non.Animal.Codes.T)],C.Codes)
            C.Codes<-c(Y[i,unlist(Non.Animal.Codes.C)],C.Codes)
          }else{
            
            # CT match is NA then there might not be any Diet Comparisons in the Animal Table, but there still could be codes, here we match on the Treatment 
            # Level name in the 
            N1<-match(Y[i,A.Lev.C], X[,A.T.Name])
            N2<-match(Y[i,A.Lev.T], X[,A.T.Name])
            
            if(!is.na(N1) & !is.na(N2)){
              C.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.C)],X[N1,unlist(strsplit(c(T.Codes,Base.Codes),"-"))]))
              T.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.T)],X[N2,unlist(strsplit(c(T.Codes,Base.Codes),"-"))]))
            }else{
              if(is.na(N1) & is.na(N2) & nrow(X)<=1){
                C.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.C)],X[,unlist(strsplit(c(T.Codes,Base.Codes),"-"))]))
                T.Codes<-unique(c(Y[i,unlist(Non.Animal.Codes.T)],X[,unlist(strsplit(c(T.Codes,Base.Codes),"-"))]))
              }else{
                cat(paste0("\n Error in ",BC," - ",i)," \n")
                C.Codes<-NA
                T.Codes<-NA
              }
            }
          }
        }
        
        # Add in NA values to the length of the C/T ERA Code Cols
        C.Codes<-c(C.Codes,rep(NA,NCols-length(C.Codes)))
        T.Codes<-c(T.Codes,rep(NA,NCols-length(T.Codes)))
        
        # Check there is still a valid comparison
        A<-C.Codes[!(grepl("h",C.Codes) | is.na(C.Codes))]
        B<-T.Codes[!(grepl("h",T.Codes) | is.na(T.Codes))]
        Valid<-sum(!B %in% A)>0
        
        list(C.Codes=C.Codes,T.Codes=T.Codes,Valid.Comparison=Valid)
        
      })
      
      
      # Replace C & T Codes
      Y<-data.frame(Y)
      Y[,paste0("C",1:NCols)]<-do.call("rbind",lapply(A,"[[","C.Codes"))
      Y[,paste0("T",1:NCols)]<-do.call("rbind",lapply(A,"[[","T.Codes"))
      Y<-data.table(Y)
      
      # Remove any non-comparisons
      list(
        Valid=Y[!unlist(lapply(A,"[[","Valid.Comparison")),c("Code","C.Descrip","T.Descrip")],
        Data=Y[unlist(lapply(A,"[[","Valid.Comparison")),!c("A.Lev.C","A.Lev.T","A.Lev.CT","Non.Animal.Codes.C","Non.Animal.Codes.T")]
      )
    })
    rm(CodeNames)
    
    Invalid<-unique(rbindlist(lapply(Y,"[[","Valid")))
    Invalid[,Existing.Error:=Code %in% Animals.Recode[!is.na(Error.Flag),B.Code]]
    if(nrow(Invalid)>0){
      View(Invalid)
      write.table(Invalid,"clipboard",row.names = F,sep="\t")
    }
    rm(Invalid)
    
    Y<-rbindlist(lapply(Y,"[[","Data"))
    
    # Check for use of "***" in Treatment names of Animal Diet Practices - correct to "-Sub" or "-Add" in excels
    
    Y[,N:=NULL]
    ERA.Reformatted[,N:=NULL]
    
    # Combine into final dataset ####
    ERA.Reformatted<-rbind(ERA.Reformatted[Analysis.Function=="NoDietSub.Agg" | !Code %in% Animals.Recode[,B.Code]],Y)
    
    # There are still issues with duplication, so only allow unique rows
    # I don't think this warrants spending a lot of tweaking the comparison code
    dim(ERA.Reformatted)
    table(ERA.Reformatted$Analysis.Function)
    ERA.Reformatted<-unique(ERA.Reformatted)
    table(ERA.Reformatted$Analysis.Function)
    dim(ERA.Reformatted)
  # 5.6) Update Mulch vs Incorporation Codes ####
  # Basic Comparisons function adds a Mulch.Code column which shows which code in the Control should be changed to a h37 code
  # However this is not fully implemented for system outcomes yet so we will recode the comparison directly
  # Incorporation codes in control (to be removed)
  Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
  # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
  Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
  
  Cols<-paste0(rep(c("C","T"),each=NCols),1:NCols)
  
  Z<-ERA.Reformatted[,..Cols]
  
  Z<-rbindlist(apply(Z,1,FUN=function(X){
    A<-Mulch.C.Codes[Mulch.C.Codes %in% X[1:NCols] & Mulch.T.Codes %in% X[(NCols+1):length(X)]]
    if(length(A)>0){
      X[which(X[1:NCols] == A)] <-"h37"
    }
    data.table(t(X))
  }))
  
  ERA.Reformatted<-cbind(ERA.Reformatted[,!..Cols],Z)
  
  # 5.7) Save Output ####
  fwrite(ERA.Reformatted,paste0("Data/Compendium Master Database/ERA V1.1 ",as.character(Sys.Date()),".csv"),row.names = F)
  
  