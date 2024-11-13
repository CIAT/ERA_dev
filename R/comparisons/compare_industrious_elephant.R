# Make sure you have set the era working directory using the 0_set_env.R script ####
# This script requires the compiled dataset created in import/import_industrious_elephant_2023.R ####

# 0) Load packages ####
pacman::p_load(data.table,miceadds,pbapply,future.apply,progressr)

worker_n<-14

# 1) Read in data ####
  # 1.1) Load tables from era data model #####
data_dir<-era_dirs$era_masterdata_dir
file_local<-tail(list.files(data_dir,"industrious_elephant"),1)
data<-miceadds::load.Rdata2(filename=file_local,data_dir)

Data.Out<-data$Data.Out
setnames(Data.Out,"Rot.Seq","R.Prod.Seq",skip_absent = T)
Data.Out[,R.ID:=paste(B.Code,R.Level.Name)]

Fert.Method<-data$Fert.Method
Fert.Out<-data$Fert.Out
Int.Out<-data$Int.Out
Irrig.Codes<-data$Irrig.Codes
Irrig.Method<-data$Irrig.Method
setnames(Irrig.Method,"Times","Time",skip_absent = T)

MT.Out2<-data$MT.Out2
Plant.Out<-data$Plant.Out
Plant.Method<-data$Plant.Method
Plant.Method<-merge(Plant.Method,Plant.Out[,.(B.Code,P.Product,P.Level.Name)],all.x=T)

Res.Method<-data$Res.Method
setnames(Res.Method,"Times","Time",skip_absent = T)
Rot.Levels<-data$Rot.Levels
Rot.Out<-data$Rot.Out
setnames(Rot.Out,"Seq","R.Prod.Seq",skip_absent = T)
Rot.Seq<-data$Rot.Seq
Rot.Seq.Summ<-data$Rot.Seq.Summ
Site.Out<-data$Site.Out
Soil.Out<-data$Soil.Out
Times<-data$Times
Var.Out<-data$Var.Out

AF.Trees<-data$AF.Trees

Rot.Seq2<-Rot.Seq[,list(Time=paste(Time,collapse="|||"),
                        Treatment=paste(R.Treatment,collapse="|||"),
                        Products=paste(R.Prod,collapse="|||")),by=ID]

  # 1.2) Load era vocab #####
# Get names of all sheets in the workbook
sheet_names <- readxl::excel_sheets(era_vocab_local)
sheet_names <- sheet_names[!grepl("sheet|Sheet",sheet_names)]

# Read each sheet into a list
master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)

# Read in codes and harmonization datasets
EUCodes<-master_codes$prod
MasterLevels<-master_codes$lookup_levels
PracticeCodes<-master_codes$prac
PracticeCodes[,Linked.Col:=gsub("ED.Int","IN.Level.Name",Linked.Col)]
PracticeCodes[,Linked.Col:=gsub("ED.Rot","R.Level.Name",Linked.Col)]
PracticeCodes1<-copy(PracticeCodes)
TreeCodes<-master_codes$trees

# 2) Automated Comparison of Control vs Treatments ####
  # 2.1) Basic #####
  # Remove Controls for Ratio Comparisons
  Data.Out.No.Agg<-Data.Out[ED.Ratio.Control!=T ]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.No.Agg<-Data.Out.No.Agg[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  # Remove Aggregated Observations (dealt with in section 1.1)
  Data.Out.No.Agg<-Data.Out.No.Agg[!grepl("[.][.][.]",T.Name)]
  # Remove outcomes aggregated over rot/int entire sequence or system (dealt with sections 1.2 and 1.3)
  Data.Out.No.Agg<-Data.Out.No.Agg[!is.na(T.Name)]
  
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
  
    # 2.1.2) Select fields that need to match for comparisons to be valid ####
    CompareWithin<-c("Site.ID","Product.Simple","ED.Product.Comp","Time", "Out.Code.Joined",
                     "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Sample.DAE","ED.Sample.Stage",
                     "C.Structure","P.Structure","O.Structure","W.Structure","PD.Structure",
                     "B.Code","Country","ED.Comparison1","ED.Comparison2")
    # 2.1.3) Create Comparison function ####
    # ISSUE: Diversification residue re-coding is for any residues, whereas we are interested in only experimental crop residues.
    Compare.Fun<-function(Data,Verbose,Debug,PracticeCodes,Return.Lists){
      
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
      
      # Deal with residues codes in diversification comparisons
      # Recode AF prunings to a15 codes
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
          Z[,Mulch.Code:=as.character(NA)][,Mulch.Flag:=F]
          
          for(kk in 1:length(Mulch.C.Codes)){
            Z<-Mulch.Fun(A=Mulch.C.Codes[kk],B=Mulch.T.Codes[kk],X,Z)
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
                    Fert.Fields<-c("F.Level.Name","F.Category","F.Type","F.NPK","F.Amount","F.Unit","F.Method","F.Physical",
                                   "F.Fate","F.Date","F.Date.Stage","F.Date.DAP","F.Date.DAE")
                    
                    Trt1<-Fert.Method[B.Code==BC & 
                                        F.Level.Name==Trt & 
                                        Time %in% c(Data$Time[1],"All Times") & 
                                        Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                      !c("F.Date.Text","F.Level.Name2")]  
                    
                    Control1<-Fert.Method[B.Code==BC & 
                                            F.Level.Name==Control & 
                                            Time %in% c(Data$Time[1],"All Times") & 
                                            Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                          !c("F.Date.Text","F.Level.Name2")]  
  
                    # Are there any Fert.Method rows in the Control not in the Treatment? 
                    # Note that the fertilizer being applied could have the same identity but still differ in amount applied 
                    # Amount can be recorded as total amount applied to plot in the Fert.Out tab and not for each row in the Fert.Method Tab
                    N.Control<-data.table(Level=c(rep("Control",nrow(Control1)),rep("Trt",nrow(Trt1))),
                                          Dup=duplicated(rbind(Control1[,!"F.Level.Name"],Trt1[,!"F.Level.Name"])))[Level=="Trt"]
                    
                    # 1) For rows that match:
                    # 1.1) For shared inorganic NPK practices do the NPK values match? (check any 999999 values are set to NA) ####
                    #Shared.FCodes<-unique(unlist(Trt1[N.Control$Dup,c("Fert.Code1","Fert.Code2","Fert.Code3")]))
                    Shared.FCodes<-na.omit(unique(Trt1[N.Control$Dup,unlist(tstrsplit(F.Codes,"-"))]))
  
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
                          
                          Trt1<-Irrig.Method[I.Level.Name==Trt & 
                                            B.Code == BC & 
                                            Time %in% c(Data$Time[1],"All Times") & 
                                            Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                          c("I.Amount","I.Unit","I.Water.Type")]
                          
                          Control1<-Irrig.Method[I.Level.Name==Control  & 
                                                B.Code == BC & 
                                                Time %in% c(Data$Time[1],"All Times") & 
                                                Site.ID %in% c(Data$Site.ID[1],"All Sites"),
                                              c("I.Amount","I.Unit","I.Water.Type")]
                          
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
    
    # 2.1.3) Run Comparisons ####
      # ISSUE?: IMPROVED VARIETIES IV heat etc. vs IV other? ####
  
  # Set up future plan for parallel execution with the specified number of workers
  plan(multisession, workers = worker_n)
  
  # Progress bar setup
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Start parallelized process with progress bar
  b_codes<-DATA[,unique(B.Code)]
  
  Verbose<-F
  
  Comparisons <- with_progress({
    # Progress indicator
    p <- progressor(steps = length(b_codes))
    
    # Apply function across unique B.Codes with future_lapply in parallel
    future_lapply(1:length(b_codes), function(j) {
    #lapply(1:length(b_codes), function(j){
      BC<-b_codes[j]
      
      if (Verbose) {
        cat(BC,"-",j,"\n")
      }
      
      # Filter subset of data for current B.Code
      Data.Sub <- DATA[B.Code == BC]
      CW <- unique(Data.Sub[, ..CompareWithin])
      CW <- match(apply(Data.Sub[, CompareWithin, with = FALSE], 1, paste, collapse = "-"),
                  apply(CW, 1, paste, collapse = "-"))
      Data.Sub[, Group := CW]
      
      # Report groups that have no comparisons
      group_n<-Data.Sub[,table(Group)]
      no_comparison <- Data.Sub[Group %in% names(group_n)[group_n == 1], c(CompareWithin,"T.Name","IN.Level.Name","R.Level.Name"), with = FALSE]
      
      Data.Sub<-Data.Sub[Group %in% names(group_n)[group_n>1]]
      
      # Apply Compare.Fun to each group
      if(nrow(Data.Sub)>0){
        
      comp_dat<-rbindlist(lapply(unique(Data.Sub$Group), function(i) {
        if (Verbose) {
          cat(BC,"-",j, " Subgroup = ", i,"\n")
        }
        Compare.Fun(Verbose = Verbose, 
                    Data = Data.Sub[Group == i], 
                    Debug = FALSE,
                    PracticeCodes=PracticeCodes, 
                    Return.Lists = FALSE)
      }))
      }else{
        comp_dat<-NULL
      }
      
      # Update progress bar after each iteration
      p() 
      
      return(list(data=comp_dat,
                  no_comparison_rows=no_comparison,
                  zero_comparisons=if(is.null(comp_dat)){BC}else{NULL}))
    })
  })
  
  # Reset to sequential plan after parallel tasks are complete
  plan(sequential)
  
    no_comparison_rows<-rbindlist(lapply(Comparisons,"[[","no_comparison_rows"))
    zero_comp_basic<-unlist(lapply(Comparisons,"[[","zero_comparisons"))
    Comparisons<-rbindlist(lapply(Comparisons,"[[","data"))
    
    # 2.1.4) Restructure and save ######
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)
    ][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    
    Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.No.Agg[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"T.Name","Control.Trt")
    setnames(Comparisons1,"IN.Level.Name","Control.Int")
    setnames(Comparisons1,"R.Level.Name","Control.Rot")
    
    Comparisons1[,Compare.Trt:=Data.Out.No.Agg[match(Control.For,N),T.Name]]
    Comparisons1[,Compare.Int:=Data.Out.No.Agg[match(Control.For,N),IN.Level.Name]]
    Comparisons1[,Compare.Rot:=Data.Out.No.Agg[match(Control.For,N),R.Level.Name]]
    
    # fwrite(Comparisons1,paste0(choose.dir(),"\\Basic_Comparisons_V1.8.csv"),row.names = F)
    
    Comparison.List<-list(Simple=Comparisons1)
    
  # 2.2) Aggregated Treatments ####
  
  # Extract Aggregated Observations
  Data.Out.Agg<-Data.Out[grep("[.][.][.]",T.Name)]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Agg<-Data.Out.Agg[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Agg<-Data.Out.Agg[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
  # Ignore outcomes aggregated over rot/int entire sequence or system
  Data.Out.Agg<-Data.Out.Agg[!is.na(T.Name)]

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

  CompareWithin<-c("Site.ID","Product.Simple","ED.Product.Comp","Time", "Out.Code.Joined",
                   "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Sample.DAE","ED.Sample.Stage",
                   "C.Structure","P.Structure","O.Structure","W.Structure","PD.Structure",
                   "B.Code","Country","ED.Comparison1","ED.Comparison2","T.Agg.Levels3")
  
  DATA<-Data.Out.Agg
  
  Verbose<-F

  # Set up future plan for parallel execution with the specified number of workers
  plan(multisession, workers = worker_n)
  
  # Enable progress bar
  handlers(global = TRUE)
  handlers("progress")
  
  # Set up future plan for parallel execution
  plan(multisession, workers = worker_n)
  
  # Initialize progress bar
  Comparisons <- with_progress({
    p <- progressor(steps = length(unique(DATA[, B.Code])))
    
    # Run future_lapply with progress bar
    future_lapply(unique(DATA[, B.Code]), function(BC) {
      p()  # Update progress bar
      
      # Filter subset of data for current B.Code
      Data.Sub <- DATA[B.Code == BC]
      CW <- unique(Data.Sub[, CompareWithin, with = FALSE])  # No `..` prefix needed with `with = FALSE`
      CW <- match(apply(Data.Sub[, CompareWithin, with = FALSE], 1, paste, collapse = "-"),
                  apply(CW, 1, paste, collapse = "-"))
      Data.Sub[, Group := CW]
      
      # Report groups that have no comparisons
      group_n<-Data.Sub[,table(Group)]
      no_comparison <- Data.Sub[Group %in% names(group_n)[group_n == 1], CompareWithin, with = FALSE]
      
      Data.Sub<-Data.Sub[Group %in% names(group_n)[group_n>1]]
      
      # Apply Compare.Fun to each group
      if(nrow(Data.Sub)>0){
      
      # Apply Compare.Fun to each group
      comp_dat<-rbindlist(lapply(unique(Data.Sub$Group), function(i) {
        if (Verbose) {
          print(paste0(BC, " Subgroup = ", i))
        }
        
        Compare.Fun(
          Verbose = Verbose,
          Data = Data.Sub[Group == i],
          Debug = FALSE,
          PracticeCodes = PracticeCodes,
          Return.Lists = FALSE
        )
      }))
      }else{
        comp_dat<-NULL
      }
      
      return(list(data=comp_dat,
                  no_comparison_rows=no_comparison,
                  zero_comparisons=if(is.null(comp_dat)){BC}else{NULL}))
      
    })
  })
  
  no_comparison_rows_agg<-rbindlist(lapply(Comparisons,"[[","no_comparison_rows"))
  zero_comp_agg<-unlist(lapply(Comparisons,"[[","zero_comparisons"))
  Comparisons<-rbindlist(lapply(Comparisons,"[[","data"))
  
  # Restructure and save
  # Remove NA values
  Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  
  Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Agg[match(Comparisons[,N],N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
  setnames(Comparisons1,"T.Name","Control.Trt")
  setnames(Comparisons1,"IN.Level.Name","Control.Int")
  setnames(Comparisons1,"R.Level.Name","Control.Rot")
  
  Comparisons1[,Compare.Trt:=Data.Out.Agg[match(Control.For,N),T.Name]]
  Comparisons1[,Compare.Int:=Data.Out.Agg[match(Control.For,N),IN.Level.Name]]
  Comparisons1[,Compare.Rot:=Data.Out.Agg[match(Control.For,N),R.Level.Name]]
  
  # write.table(Comparisons1,"clipboard-256000",row.names = F,sep="\t")
  
  Comparison.List[["Aggregated"]]<-Comparisons1
  
  # 2.3) Intercropping System Outcomes ####
  
  # Extract outcomes aggregated over rot/int entire sequence or system
  Data.Out.Int<-Data.Out[is.na(T.Name) & !is.na(IN.Level.Name)]
  
  # Replace T-Codes with Intercrop T-Codes
  Data.Out.Int[,T.Codes:=IN.T.Codes]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Int<-Data.Out.Int[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Int<-Data.Out.Int[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
    # 2.3.1) Scenario 1: Intercrop vs Intercrop ####
    
    # Replace Treatment Name with Intercropping Name
    Data.Out.Int2<-data.table::copy(Data.Out.Int)
    Data.Out.Int2[,T.Name:=IN.Level.Name]
    
      # 2.3.1.1) Combine intercropped treatment information using a similar process to aggregated trts in the MT.Out table ########
    F.Master.Codes<-PracticeCodes[Theme=="Nutrient Management" | Subpractice =="Biochar",Code]
    F.Master.Codes<-F.Master.Codes[!grepl("h",F.Master.Codes)]
    
       # !!TO DO !!! In original script Fertilizer tab has columns for each practice code #####
    Fields<-data.table(Levels=c("T.Residue.Prev",colnames(MT.Out)[grep("Level.Name$",colnames(MT.Out))]))
    Fields[,Codes:=gsub("Level.Name","Codes",Levels)
           ][,Codes:=gsub("Prev","Code",Levels)
             ][grep("O[.]|PD[.]|C[.]|P[.]|W[.]",Codes),Codes:=NA]
    
    N<-which(!is.na(Data.Out.Int[,IN.Level.Name]))
    
    MT.Out[!is.na(P.Structure),P.Structure:=P.Level.Name]
    MT.Out[!is.na(O.Structure),O.Structure:=O.Level.Name]
    MT.Out[!is.na(W.Structure),W.Structure:=W.Level.Name]
    MT.Out[!is.na(C.Structure),C.Structure:=C.Level.Name]
    MT.Out[!is.na(PD.Structure),PD.Structure:=PD.Level.Name]
    MT.Out[is.na(P.Structure),P.Structure:=NA]
    MT.Out[is.na(O.Structure),O.Structure:=NA]
    MT.Out[is.na(W.Structure),W.Structure:=NA]
    MT.Out[is.na(C.Structure),C.Structure:=NA]
    MT.Out[is.na(PD.Structure),PD.Structure:=NA]
    MT.Out[,P.Level.Name:=P.Structure]
    MT.Out[,O.Level.Name:=O.Structure]
    MT.Out[,W.Level.Name:=W.Structure]
    MT.Out[,C.Level.Name:=C.Structure]
    MT.Out[,PD.Level.Name:=PD.Structure]
    
    Data.Out.Int2<-rbindlist(pblapply(1:nrow(Data.Out.Int2),FUN=function(i){
      if(i %in% N){
        
        Trts<-unlist(strsplit(Data.Out.Int2[i,IN.Level.Name],"[*][*][*]")) 
        
        Trts2<-Trts
        #Trts<-gsub("[.][.][.]","..",Trts)
        Study<-Data.Out.Int2[i,B.Code]
        
        Y<-MT.Out[T.Name %in% Trts & B.Code == Study]
        
        # Aggregated Treatments: Split T.Codes & Level.Names into those that are the same and those that differ between treatments
        # This might need some more nuance for fertilizer treatments?
        Fields1<-Fields
        
        # Exclude Other, Chemical, Weeding or Planting Practice Levels if they do no structure outcomes.
        Exclude<-c("O.Level.Name","P.Level.Name","C.Level.Name","W.Level.Name","PD.Level.Name")[is.na(apply(Y[,c("O.Structure","P.Structure","C.Structure","W.Structure","PD.Structure")],2,unique))]
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
          
          COLS2<-COLS
          COLS2[COLS2=="F.Level.Name"]<-"F.Level.Name2"
            
          
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
        #Y$T.Name2<-Y$IN.Level.Name2
        Y$T.Name<-Data.Out.Int2[i,IN.Level.Name]
        #Y[,N2:=NULL]
        
        
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
    
      # 2.3.1.2) Run Comparisons #######
    
    # We cannot compare different types of intercrop so Levels and cropping sequence should be the same
    # The basic comparison grouping variables on Data.Out.Int dataset should work.
    CompareWithin<-c("Site.ID","Product.Simple","ED.Product.Comp","Time", "Out.Code.Joined",
                     "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Sample.DAE","ED.Sample.Stage",
                     "C.Structure","P.Structure","O.Structure","W.Structure","PD.Structure",
                     "B.Code","Country","ED.Comparison1","ED.Comparison2","IN.Agg.Levels3")
    
    DATA<-Data.Out.Int2
    
    Verbose<-F
   
    # Initialize progress bar
    Comparisons <-pblapply(unique(DATA[, B.Code]), function(BC) {
        # Filter subset of data for current B.Code
        Data.Sub <- DATA[B.Code == BC]
        CW <- unique(Data.Sub[, CompareWithin, with = FALSE])  # No `..` prefix needed with `with = FALSE`
        CW <- match(apply(Data.Sub[, CompareWithin, with = FALSE], 1, paste, collapse = "-"),
                    apply(CW, 1, paste, collapse = "-"))
        Data.Sub[, Group := CW]
        
        # Report groups that have no comparisons
        group_n<-Data.Sub[,table(Group)]
        no_comparison <- Data.Sub[Group %in% names(group_n)[group_n == 1], CompareWithin, with = FALSE]
        
        Data.Sub<-Data.Sub[Group %in% names(group_n)[group_n>1]]
        
        # Apply Compare.Fun to each group
        if(nrow(Data.Sub)>0){
          
          # Apply Compare.Fun to each group
          comp_dat<-rbindlist(lapply(unique(Data.Sub$Group), function(i) {
            if (Verbose) {
              print(paste0(BC, " Subgroup = ", i))
            }
            
            Compare.Fun(
              Verbose = Verbose,
              Data = Data.Sub[Group == i],
              Debug = FALSE,
              PracticeCodes = PracticeCodes,
              Return.Lists = FALSE
            )
          }))
        }else{
          comp_dat<-NULL
        }
        
        return(list(data=comp_dat,
                    no_comparison_rows=no_comparison,
                    zero_comparisons=if(is.null(comp_dat)){BC}else{NULL}))
        
      })
    
    no_comparison_rows_int<-rbindlist(lapply(Comparisons,"[[","no_comparison_rows"))
    zero_comp_int<-unlist(lapply(Comparisons,"[[","zero_comparisons"))
    Comparisons<-rbindlist(lapply(Comparisons,"[[","data"))
    
    if(F){
    cl<-makeCluster(worker_n)
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
    }
    
    # Basic: Restructure and save
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    
      # 2.3.1.3) Restructure and save ######
    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    
    Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Int2[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"T.Name","Control.Trt")
    setnames(Comparisons1,"IN.Level.Name","Control.Int")
    setnames(Comparisons1,"R.Level.Name","Control.Rot")
    
    Comparisons1[,Compare.Trt:=Data.Out.Int2[match(Control.For,N),T.Name]]
    Comparisons1[,Compare.Int:=Data.Out.Int2[match(Control.For,N),IN.Level.Name]]
    Comparisons1[,Compare.Rot:=Data.Out.Int2[match(Control.For,N),R.Level.Name]]
    
    # write.table(Comparisons1,"clipboard-256000",row.names = F,sep="\t")
    
    Comparison.List[["Sys.Int.vs.Int"]]<-Comparisons1
    
    # 2.3.2) Scenario 2: Intercrop vs Monocrop ####
    
    # Extract outcomes aggregated over rot/int entire sequence or system
    Data.Out.Int<-Data.Out[is.na(T.Name) & !is.na(IN.Level.Name)]
    # Replace T-Codes with Intercrop T-Codes
    Data.Out.Int[,T.Codes:=IN.T.Codes]
    # Exclude Ratios
    # Remove Controls for Ratio Comparisons
    Data.Out.Int<-Data.Out.Int[ED.Ratio.Control!=T]
    Data.Out.Int3<-data.table::copy(Data.Out.Int2)
    Data.Out.Int3[,T.Name:=Data.Out.Int[,T.Name]]
    
    # We will need to consider T.Agg.Levels3 & Structure Cols
    CompareWithinInt<-c("Site.ID","Time","Out.Code.Joined","B.Code","Country")
    
    # Create group codes
    Data.Out.Int3[,CodeTemp:=apply(Data.Out.Int3[,CompareWithinInt,with=F],1,paste,collapse = " ")]
    
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
    Data.Out[,CodeTemp:=apply(Data.Out[,CompareWithinInt,with=F],1,paste,collapse = " ")]
    
      # 2.3.2.1) Add in (potential) monoculture controls ####
    Data.Out.Int3<-rbindlist(pblapply(unique(Data.Out.Int3[,CodeTemp]),FUN=function(X){
      
      # Find observations without intercropping or rotation that match the code of the intercropping system outcome
      Y1<-Data.Out[CodeTemp == X & is.na(IN.Level.Name) & is.na(R.Level.Name) & !is.na(T.Name)]
      Y2<-Data.Out.Int3[CodeTemp == X]
      
      # Does a match exist in main data? is there a potential control?
      if(nrow(Y1)>0){
        # Match product in monoculture to a product in the intercrop
        W<-rbindlist(lapply(Y1[,unique(Product.Simple)],FUN=function(PROD){
          Z<-Y2[grep(PROD,Product.Simple),]
          # Is there a product match?
          if(nrow(Z)>0){
            # Match mono crop to intercrop and bind rows
            MONO<-Y1[Product.Simple==PROD]
            INT<-Z[,Product.Simple:=PROD]
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
    
      # 2.3.2.2) Run Comparisons ####
    # 1) All T-Codes in the Monocrop should be in the Intercrop
    # 2) Test if all Level/Fields in the Monocrop are in the intercrop
    
    Data.Out.Int3[is.na(T.Name),N2:=1:.N]
    
    # Add crop var column that refers to EU x variety in question, use this a level instead of variety so matching is done on product 
    # monocrop only.
    
    X<-rbindlist(pblapply(1:nrow(Data.Out.Int3),FUN=function(i){
      if(!is.na(Data.Out.Int3[i,V.Level.Name])){
        X<-Data.Out.Int3[i,c("B.Code","Product.Simple","V.Level.Name","V.Codes")]
        VAR<-unlist(strsplit(X[,V.Level.Name],"[*][*][*]"))
        VAR<-VAR[!(is.na(VAR)|VAR=="NA")]
        if(length(VAR)>0){
          Z<-Var.Out[B.Code==X[,B.Code] & V.Product==X[,Product.Simple] & V.Var %in% VAR,c("V.Codes","V.Level.Name")]
          if(nrow(Z)==0){
            # print(X)
            data.table(V.Codes=NA,V.Level.Name=NA)
          }else{
            Z
          }
          
        }else{
          data.table(V.Codes=NA,V.Level.Name=NA)
        }
      }else{
        data.table(V.Codes=NA,V.Level.Name=NA)
      }
    }))
    
    # Remove & Replace columns
    Y<-Data.Out.Int3[,c("V.Codes","V.Level.Name")]
    Data.Out.Int3[,V.Codes:=NULL]
    Data.Out.Int3[,V.Level.Name:=NULL]
    Data.Out.Int3<-cbind(Data.Out.Int3,X)
    
    # Incorporation codes in control (to be removed)
    Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
    # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
    Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
    
    # Temporary for column name mismatch
    setnames(Data.Out.Int3,"Till.Code","Till.Codes", skip_absent = T)
    
    Comparisons<-unique(rbindlist(pblapply(Data.Out.Int3[!is.na(N2),N2],FUN=function(i){
      INT<-Data.Out.Int3[N2==i]
      MONOS<-Data.Out.Int3[!is.na(T.Name) & CodeTemp == INT[,CodeTemp] & Product.Simple == INT[,Product.Simple]]
      
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
          grepl("[.][.][.]",INT[,IN.Level.Name]) == grepl("[.][.][.]",MONOS[j,T.Name])
        }else{
          if(all(MONO.Codes %in% INT.Codes)){
            # If aggregated treatments are present they should be in both monocrop and intercrop
            grepl("[.][.][.]",INT[,IN.Level.Name]) == grepl("[.][.][.]",MONOS[j,T.Name])
            
            
          }else{
            F
          }
        }}))
      
      # To deal with situations where different crops in the intercrop have different tillage, force tillage comparisons to
      # use only the tillage data from the intercrop crop that matches the monocrop
      INT.Till<-data.table(Product=unlist(INT[,strsplit(IN.Prod,"***",fixed=T)]),
                           Till.Level.Name=unlist(INT[,strsplit(Till.Level.Name,"***",fixed=T)]),
                           Till.Codes=unlist(INT[,strsplit(Till.Codes,"***",fixed=T)]))
      
      INT.Till.Lev<-unique(INT.Till[Product==MONOS[1,Product.Simple],Till.Level.Name])
      INT.Till.Code<-unique(INT.Till[Product==MONOS[1,Product.Simple],Till.Codes])
      
      if(length(INT.Till.Lev)==0){
        INT.Till.Lev<-NA
        INT.Till.Code<-NA
      }
      
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
          
          STRUC<-c("O.Structure","P.Structure","C.Structure","W.Structure","PD.Structure")
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
    
    #Data.Out.Int3[,V.Codes:=NULL]
    #Data.Out.Int3[,V.Level.Name:=NULL]
    Data.Out.Int3<-cbind(Data.Out.Int3,Y)

    # Remove NA values
    Comparisons<-Comparisons[!is.na(Control.For)]

      # 2.3.2.3) Restructure and save ######    
    CompareWithin<-c("Site.ID","Time","Out.Code.Joined","B.Code","Country","C.Structure","P.Structure","O.Structure",
                     "W.Structure","PD.Structure","IN.Agg.Levels3","ED.Comparison1","ED.Comparison2")
    
    Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Int3[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"T.Name","Control.Trt")
    setnames(Comparisons1,"IN.Level.Name","Control.Int")
    setnames(Comparisons1,"R.Level.Name","Control.Rot")
    Comparisons1[,Compare.Trt:=Data.Out.Int3[match(Control.For,N),T.Name]]
    Comparisons1[,Compare.Int:=Data.Out.Int3[match(Control.For,N),IN.Level.Name]]
    Comparisons1[,Compare.Rot:=Data.Out.Int3[match(Control.For,N),R.Level.Name]]
    Comparisons1[,Codes.Match:=Comparisons[,Codes.Match]]
    Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
    
    Comparison.List[["Sys.Int.vs.Mono"]]<-Comparisons1
    
  # 2.4) Rotation System Outcomes ####
  # Extract outcomes aggregated over rot/int entire sequence or system
  Data.Out.Rot<-Data.Out[is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name)]
  
  # Exclude Ratios
  # Remove Controls for Ratio Comparisons
  Data.Out.Rot<-Data.Out.Rot[ED.Ratio.Control!=T]
  # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
  # Data.Out.Rot<-Data.Out.Rot[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
  
    # 2.4.1) Scenario 1: Rotation vs Rotation ####
    # Replace Treatment Name with Rotation Name
    Data.Out.Rot2<-Data.Out.Rot
    Data.Out.Rot2[,T.Name:=R.Level.Name]
    
    # Improved vs Unimproved Fallow - the latter is the control for the former, but the rotation sequences differ
    # this is an issues are comparisons in this section are made within rotation sequences.
    # Solution: similar to Rot vs Monoculture, duplicate the control for each comparable improved fallow giving the same rotation sequence
    
    # 1) Subset to unimproved fallow
    
    Data.Out.Rot2[,Group:=paste(Site.ID,Time,Out.Code.Joined,R.All.Structure,B.Code)][,Group:=as.numeric(as.factor(Group))]
    U.Fallow<-Data.Out.Rot2[grepl("h24",R.Code)]
    Data.Out.Rot2[,U.Fallow.Dup:=F]
    
    U.Fallow<-rbindlist(pblapply(1:nrow(U.Fallow),FUN=function(i){
      Trts<-Data.Out.Rot2[Group==U.Fallow[i,Group] & grepl("b60",R.Code)]
      if(nrow(Trts)>0){
        Control<-U.Fallow[rep(i,nrow(Trts)),][,R.Prod.Seq:=Trts[,R.Prod.Seq]]
        Control
      }else{
        print(paste("No Improved Treatment for Control: ",i," - ",U.Fallow[i,paste(B.Code,R.Level.Name)]))
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
    
    CompareWithin<-c("Site.ID","Time", "Out.Code.Joined",
                     "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","ED.Sample.DAE","ED.Sample.Stage",
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
    
        # TO DO !!! In original script fertilizer tab has codes as columns ####
      Levels<-PracticeCodes[!Linked.Col %in% c("IN.Level.Name","R.Level.Name"),c("Code","Linked.Col")]
      #Levels<-PracticeCodes[!Linked.Col %in% c("F.Level.Name","IN.Level.Name","R.Level.Name"),c("Code","Linked.Col")]
      #Levels<-rbind(Levels,data.table(Code=F.Master.Codes,Linked.Col=F.Master.Codes))
      
      Verbose<-F
      
      # 2.4.1.1) Run Comparisons ####
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
                if(Verbose){print(paste(BC," - Group ",GROUP," - j =",j,", k =",k))}
                
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
      
      # Remove NA values
      Comparisons<-Comparisons[!is.na(Control.For)]
      
      # 2.4.1.2) Restructure and save ######
      Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
      Cols1<-c(CompareWithin,Cols)
      
      Comparisons1<-Data.Out.Rot2[match(Comparisons[,N],Data.Out.Rot2$N),..Cols1]
      Comparisons1[,Control.For:=Comparisons[,Control.For]]
      Comparisons1[,Control.N:=Comparisons[,N]]
      Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
      Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
      setnames(Comparisons1,"T.Name","Control.Trt")
      setnames(Comparisons1,"IN.Level.Name","Control.Int")
      setnames(Comparisons1,"R.Level.Name","Control.Rot")
      Comparisons1[,Compare.Trt:=Data.Out.Rot2[match(Control.For,N),T.Name]]
      Comparisons1[,Compare.Int:=Data.Out.Rot2[match(Control.For,N),IN.Level.Name]]
      Comparisons1[,Compare.Rot:=Data.Out.Rot2[match(Control.For,N),R.Level.Name]]
      
      Comparison.List[["Sys.Rot.vs.Rot"]]<-Comparisons1
      
    # 2.4.2) Scenario 2: Rotation vs Monocrop ####
    # Extract outcomes aggregated over rot/int entire sequence or system
    Data.Out.Rot<-Data.Out[is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name)]
    
    # Exclude Ratios
    # Remove Controls for Ratio Comparisons
    Data.Out.Rot<-Data.Out.Rot[ED.Ratio.Control!=T]
    # Remove observations where ED.Comparison is present (so we are not comparing ratios to ratios)
    # Data.Out.Rot<-Data.Out.Rot[is.na(ED.Comparison) & Out.Subind!="Land Equivalent Ratio"]
    
    Data.Out.Rot3<-Data.Out.Rot2[,!c("Group","Group.Len")]
    Data.Out.Rot3[,T.Name:=Data.Out.Rot[,T.Name]]
    
    CompareWithinInt<-c("Site.ID","Time","Out.Code.Joined","B.Code","Country")
    
    # Create group codes
    CT<-apply(Data.Out.Rot3[,CompareWithinInt,with=F],1,paste,collapse = " ")
    Data.Out.Rot3[,CodeTemp:=CT]

    # In rotation we consider any practice present in the majority of components to be present in the system
    
    # Update Data.Out Columns to Match Data.Out.Rot3
    Data.Out[,N2:=1:.N]
    X<-Data.Out[,list(Final.Codes=Join.T(T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N2"]
    Data.Out[,Final.Codes:=X$Final.Codes]
    Data.Out[,N2:=NULL]

    # Calculate Number of ERA Practices
    Data.Out[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]
    Data.Out[,Final.Codes2:=paste(unlist(Final.Codes),collapse = "-"),by=N]
    
    CT<-Data.Out[,CompareWithinInt,with=F]
    Data.Out[,CodeTemp:=apply(CT,1,paste,collapse = " ")]
    
      # 2.4.2.1) Add in (potential) monoculture controls ####
    # Also duplicate rotation observation replacing the EU with a simplified EU that matches the potential control
    Data.Out.Rot3<-rbindlist(pblapply(unique(Data.Out.Rot3[,CodeTemp]),FUN=function(X){
      
      Y1<-Data.Out[CodeTemp == X & is.na(IN.Level.Name) & is.na(R.Level.Name) & !is.na(T.Name)]
      Y2<-Data.Out.Rot3[CodeTemp == X]
      
      if(nrow(Y1)>0){
        W<-rbindlist(lapply(Y1[,unique(Product.Simple)],FUN=function(PROD){
          Z<-Y2[grep(PROD,Product.Simple),]
          if(nrow(Z)>0){
            MONO<-Y1[Product.Simple==PROD]
            INT<-Z[,Product.Simple:=PROD]
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
    
      # 2.4.2.2) Run Comparisons ####
    Data.Out.Rot3[is.na(T.Name),N2:=1:.N]
    
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
        X<-Data.Out.Rot3[i,c("B.Code","Product.Simple","V.Level.Name","V.Codes")]
        VAR<-unlist(strsplit(X[,V.Level.Name],"---"))
        VAR<-VAR[!is.na(VAR)]
        if(length(VAR)>0){
          Z<-Var.Out[B.Code==X[,B.Code] & V.Product==X[,Product.Simple] & V.Var %in% VAR,c("V.Codes","V.Level.Name")]
          if(nrow(Z)>1){
            Z1<-unlist(apply(Z,2,FUN=function(W){paste(unique(W),collapse = "---")}))
            Z<-data.table(V.Codes=Z1[1],V.Level.Name=Z1[2])
          }
          
          if(nrow(Z)==0){
            # print(X)
            data.table(V.Codes=NA,V.Level.Name=NA)
          }else{
            Z
          }
          
        }else{
          data.table(V.Codes=NA,V.Level.Name=NA)
        }
      }else{
        data.table(V.Codes=NA,V.Level.Name=NA)
      }
    }))
    
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
      MONOS<-Data.Out.Rot3[!is.na(T.Name) & CodeTemp == ROT[,CodeTemp] & Product.Simple == ROT[,Product.Simple]]
      
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
          grepl("[.][.][.]",ROT[,IN.Level.Name]) == grepl("[.][.][.]",MONOS[j,T.Name])
        }else{
          if(all(MONO.Codes %in% ROT.Codes)){
            # If aggregated treatments are present they should be in both monocrop and rotation
            grepl("[.][.][.]",ROT[,IN.Level.Name]) == grepl("[.][.][.]",MONOS[j,T.Name])
            
            
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
          ROT.Levels<-unique(ROT.Levels[ROT.Crops %in% MONO[,Product.Simple] & ROT.Times %in% MONO[,unlist(strsplit(Time,"..",fixed=T))]])
          
          # We will still run into issues where same crop treatments differ over time and the period of measurement is across multiple years
          # So exclude where treatments for a crop differ over time
          if(length(ROT.Levels)!=1){
            Logic<-F
          }else{
          
            ROT2<-MT.Out[T.Name==ROT.Levels]
            
            # Set practices of interest
            COLS<-c("A.Level.Name","H.Level.Name","I.Level.Name","M.Level.Name","pH.Level.Name","Till.Level.Name",
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
            
              STRUC<-c("O.Structure","P.Structure","C.Structure","W.Structure","PD.Structure")
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
    
    #Data.Out.Rot3[,V.Codes:=NULL]
    Data.Out.Rot3[,V.Level.Name:=NULL]
    Data.Out.Rot3<-cbind(Data.Out.Rot3,Y)
    rm(X,Y,Mulch.C.Codes,Mulch.T.Codes)
    
    
    Comparisons<-Comparisons[!is.na(Control.For)]
    
      # 2.4.2.3) Restructure and save ######
    CompareWithin<-c("Site.ID","Time","Out.Code.Joined","B.Code","Country","R.All.Structure")
    
    Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
    Cols1<-c(CompareWithin,Cols)
    
    Comparisons1<-Data.Out.Rot3[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
    setnames(Comparisons1,"T.Name","Control.Trt")
    setnames(Comparisons1,"IN.Level.Name","Control.Int")
    setnames(Comparisons1,"R.Level.Name","Control.Rot")
    Comparisons1[,Compare.Trt:=Data.Out.Rot3[match(Control.For,N),T.Name]]
    Comparisons1[,Compare.Int:=Data.Out.Rot3[match(Control.For,N),IN.Level.Name]]
    Comparisons1[,Compare.Rot:=Data.Out.Rot3[match(Control.For,N),R.Level.Name]]
    Comparisons1[,Codes.Match:=Comparisons[,Codes.Match]]
    Comparisons1[,Level.Match:=Comparisons[,Level.Match]]
    
    Comparison.List[["Sys.Rot.vs.Mono"]]<-Comparisons1
    

    # 2..5) Combine Data ####
  Comparison.List$Simple$Analysis.Function<-"Simple"
  Comparison.List$Aggregated$Analysis.Function<-"Aggregated"
  Comparison.List$Sys.Int.vs.Int$Analysis.Function<-"Sys.Int.vs.Int"
  Comparison.List$Sys.Int.vs.Mono$Analysis.Function<-"Sys.Int.vs.Mono"
  Comparison.List$Sys.Rot.vs.Rot$Analysis.Function<-"Sys.Rot.vs.Rot"
  Comparison.List$Sys.Rot.vs.Mono$Analysis.Function<-"Sys.Rot.vs.Mono"
  
  # Get Columns
  B<-Comparison.List$Sys.Int.vs.Mono
  B<-B[Level.Match==T][,Codes.Match:=NULL][,Level.Match:=NULL]
  COLS<-colnames(B)
  COLS<-COLS[!grepl("Structure|ED.Comparison|IN.Agg.Levels3",COLS)]
  
  # Subset Int/Rot vs Mono to Match.Levels = T
  Comparison.List$Sys.Rot.vs.Mono<- Comparison.List$Sys.Rot.vs.Mono[Level.Match==T]
  Comparison.List$Sys.Int.vs.Mono<- Comparison.List$Sys.Int.vs.Mono[Level.Match==T]
  
  Comparisons<-rbindlist(lapply(Comparison.List,FUN=function(X){X[,COLS,with=F]}),use.names = T,fill=T)
  Comparisons[Mulch.Code %in% c("Int System","ROT System"),Mulch.Code:=NA]
  
  # 2.6) QAQC #####
    # Paths to save to project and local dirs
    qaqc_dir<-file.path(era_dirs$era_dataentry_prj,project,"comparison_logic_qaqc")
    if(!dir.exists(qaqc_dir)){dir.create(qaqc_dir)}
  
    qaqc_dir2<-file.path(era_dirs$era_dataentry_prj,project,"comparison_logic_qaqc")
    if(!dir.exists(qaqc_dir2)){dir.create(qaqc_dir2)}
  
    # 2.6.1) Find studies that have no comparisons ######
    all_bcodes<-Data.Out[,unique(B.Code)] 
    no_match<-sort(all_bcodes[!all_bcodes %in% Comparisons[,unique(B.Code)]])
    
    error_dat<-Plant.Out[B.Code %in% no_match,.(value=if(any(na.omit(P.Structure)=="No")){"Answer -No- is present in planting comparison row."}else{""}),by=B.Code]
    error_dat<-rbind(data.table(B.Code=no_match[!no_match %in% error_dat$B.Code],value=""),error_dat)
    
    error_dat<-error_dat[order(value,B.Code)
                ][!is.na(value),table:="Plant.Out"
                  ][!is.na(value),field:="P.Structure"
                    ][,issue:="Comparison logic does find any comparisons for this paper."]
    
    error_list<-error_tracker(errors=error_dat,filename = "no_comparisons",error_dir=qaqc_dir,error_list = NULL)
    
    error_dat[value!="",.N]
    
    # 2.6.2) Save comparison logic file ######
    arrow::write_parquet(Comparisons,file.path(qaqc_dir,"comparison_results.parquet"))
    fwrite(Comparisons,file.path(qaqc_dir2,"comparison_results.csv"))
    
# 3) Prepare Main Dataset ####
Data<-Data.Out
  # 3.1) Ignore Aggregated Observations for now ####
  # Data<-Data.Out[-grep("[.][.][.]",T.Name2)]
  # 3.2) Ignore outcomes aggregated over rot/int entire sequence or system ####
  #Data<-Data[!is.na(T.Name)]
  
  # 3.3) Combine all Practice Codes together & remove h-codes ####
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
  X<-Data[is.na(T.Name) & !is.na(IN.Level.Name),list(Final.Codes=Join.T(IN.T.Codes,IN.Code,Final.Residue.Code,R.Code)),by="N"]
  Data[is.na(T.Name) & !is.na(IN.Level.Name),Final.Codes:=X$Final.Codes]
  
  # Rotation System
  X<-Data[is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name),list(Final.Codes=Join.T(R.T.Codes.Sys,R.IN.Codes.Sys,R.Res.Codes.Sys,R.Code)),by="N"]
  Data[is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name),Final.Codes:=X$Final.Codes]
  
  rm(X)
  
  # 3.4) Add filler cols ####
  
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
  
  # 3.5) MSP & MAT ####
  Data[,Season:=unlist(lapply(strsplit(Data[,Time],"[.][.]"),FUN=function(X){
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
  
  # 3.6) Soils ####
  #Soil.Out$Soil.pH.Method<-NA # Bug in Excels that needs fixing
  Soil.Out[,Depth.Interval:=Soil.Upper-Soil.Lower]
  Soil.Out[variable=="Soil.pH",Soil.pH.Method:=Method]
  Soil.Out[variable=="Soil.SOC",Soil.SOC.Unit:=Unit]
  Soil.Out<-dcast(Soil.Out[,!c("Method","Unit")],B.Code+Site.ID+Soil.Upper+Soil.Lower+Depth.Interval+Soil.pH.Method+Soil.SOC.Unit~variable,value.var = "value")
  
  Soil.Out[Depth.Interval==0,Depth.Interval:=1]
  X<-Soil.Out[,list(SOC=round(weighted.mean(Soil.SOC,Depth.Interval,na.rm=T),2),
                    Lower=max(Soil.Lower),
                    Upper=min(Soil.Upper),
                    SOC.Unit=unique(Soil.SOC.Unit),
                    Soil.pH=round(weighted.mean(Soil.pH,Depth.Interval,na.rm=T),2),
                    Soil.pH.Method=unique(Soil.pH.Method)),by=c("B.Code","Site.ID")]
  Y<-Soil.Out[Soil.Lower<=50,list(SOC=round(weighted.mean(Soil.SOC,Depth.Interval,na.rm=T),2),
                                  Soil.pH=round(weighted.mean(Soil.pH,Depth.Interval,na.rm=T),2),
                                  Lower=max(Soil.Lower),
                                  Upper=min(Soil.Upper)),by=c("B.Code","Site.ID")]
  
  N<-X[,match(Y[,paste(B.Code,Site.ID)],paste(B.Code,Site.ID))]
  
  X[N,SOC:=Y[,SOC]]
  X[N,Soil.pH:=Y[,Soil.pH]]
  X[N,Upper:=Y[,Upper]]
  X[N,Lower:=Y[,Lower]]
  N.Match<-match(Data[,paste(B.Code,Site.ID)],X[,paste(B.Code,Site.ID)])
  Data[,SOC:=X[N.Match,SOC]]
  Data[,SOC.Depth:=X[,(Lower+Upper)/2][N.Match]]
  Data[,SOC.Unit:=X[N.Match,SOC.Unit]]
  Data[,Soil.pH:=X[N.Match,Soil.pH]]
  Data[,Soil.pH.Method:=X[N.Match,Soil.pH.Method]]
  
  # 3.7) Update treatment names for intercrop and rotation ####
  
  # Component of Intercrop without rotation
  Data[!is.na(T.Name) & !is.na(IN.Level.Name) & is.na(R.Level.Name),T.Name:=paste0(T.Name,">>",IN.Level.Name)]
  # Component of Rotation without Intercrop
  Data[!is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name),T.Name:=paste0(T.Name,"<<",R.Level.Name)]
  # Component of Intercrop in Rotation
  Data[!is.na(T.Name) & !is.na(IN.Level.Name) & !is.na(R.Level.Name),T.Name:=paste0(T.Name,">>",IN.Level.Name,"<<",R.Level.Name)]
  
  # Intercrop only
  Data[is.na(T.Name) & !is.na(IN.Level.Name) & is.na(R.Level.Name),T.Name:=IN.Level.Name]
  # Rotation only
  Data[is.na(T.Name) & is.na(IN.Level.Name) & !is.na(R.Level.Name),T.Name:=R.Level.Name]
  # Intercrop part of rotation 
  Data[is.na(T.Name) & !is.na(IN.Level.Name) & !is.na(R.Level.Name),T.Name:=paste0(IN.Level.Name,"<<",R.Level.Name)]
  
  # 3.8) Create TID code ####
  Data[,TID:=paste0("T",as.numeric(as.factor(T.Name))),by="B.Code"]
  
  # 3.9) Make "C1:Cmax" & Add Base Codes ####
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
  Data[apply(X,1,FUN=function(Y){sum(!is.na(Y))})>13]
  
  if(!"C10" %in%colnames(Data)){
    Data[,C10:=NA]
  }
  
    # 3.9.1) How many C columns are there? ####
  NCols<-sum(paste0("C",1:30) %in% colnames(Data))
  
  # 3.10) Add Rotation Seq & Intercropping Species ####
  N.R<-match(paste0(Data$B.Code,Data$R.Level.Name),paste0(Rot.Seq.Summ$B.Code,Rot.Seq.Summ$R.Level.Name))
  Data[,R.Seq:=Rot.Seq.Summ[N.R,R.Prod.Seq]]

  # Intercropping
  N.I<-match(Data[is.na(R.Seq),paste0(B.Code,IN.Level.Name)],paste0(Int.Out$B.Code,Int.Out$IN.Level.Name))
  Data[is.na(R.Seq),R.Seq:=Int.Out[N.I,IN.Prod]]

  # Reformat sequence
  Data[,R.Seq:=gsub("[*][*][*]","-",R.Seq)][,R.Seq:=gsub("[|][|][|]","/",R.Seq)]
  
  # 3.11) Add in Trees ####
  # Trees in rotation sequence
  X<-Rot.Out[,N:=1:.N][,.(P.List=unlist(strsplit(R.Prod.Seq,"[|][|][|]"))),by=N]
  X<-X[,list(P.List=list(unlist(strsplit(unlist(P.List),"[*][*][*]")))),by=N]
  X<-X[,.(Trees=lapply(P.List,FUN=function(X){
    X<-unlist(X)
    Y<-unique(X[X %in% TreeCodes$Product.Simple])
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Product.Simple]]))
    Y<-c(Y,unique(X[X %in% EUCodes[Tree=="Yes",Latin.Name]]))
    
    if(length(Y)==0){
      NA
    }else{
      paste(sort(unique(Y)),collapse="-")
    }
  })),by=N]
  n_missing<-Rot.Out[!N %in% X[,N],N]
  X<-rbind(X,data.table(N=n_missing,Trees=NA))[order(N)]
  Rot.Out[,Trees:=X$Trees]
  
  
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
  X<-match(Data[,paste(B.Code,IN.Level.Name)],Int.Out[,paste(B.Code,IN.Level.Name)])
  Data[!is.na(X),Int.Tree:=Int.Out[X[!is.na(X)],Trees]]
  
  X<-match(Data[,paste(B.Code,R.Level.Name)],Rot.Out[,paste(B.Code,R.Level.Name)])
  Data[!is.na(X),Rot.Tree:=Rot.Out[X[!is.na(X)],Trees]]

  Y<-AF.Trees[,list(Trees=list(AF.Tree[!is.na(AF.Tree)])),by=c("AF.Level.Name","B.Code")]
  Y<-Y[,list(Trees=if(length(unlist(Trees))==0){as.character(NA)}else{unlist(Trees)}),by=c("AF.Level.Name","B.Code")]
  X<-match(Data[,paste(B.Code,AF.Level.Name)],Y[,paste(B.Code,AF.Level.Name)])
  Data[!is.na(X),AF.Tree:=Y[X[!is.na(X)],Trees]]
  
  X<-Data[,list(Trees=list(c(unlist(Int.Tree),unlist(Rot.Tree),unlist(AF.Tree)))),by=N]
  X<-X[,list(Trees=list(if(!all(is.na(unlist(Trees)))){unique(unlist(Trees)[!is.na(unlist(Trees))])}else{NA})),by=N]
  X[,Trees:=paste0(unlist(Trees),collapse="-"),by=N]
  
  Data[,Tree:=X[,Trees]][,Tree:=unlist(Tree)]
  
  # 3.12) M.Year/Season - Translate to old system or code start/end year and season ####
  # We have some instances of 2000-2020 and some of ".." and some of "-5" between seasons
  setnames(Data,
           c("Time","Time.Start.Year","Time.End.Year","Time.Season.Start","Time.Season.End"),
           c("M.Year","M.Year.Start","M.Year.End","M.Season.Start","M.Season.End"))

  # Variable from majestic hippo, probably redundant
  Data[,Max.Season:=NA]
  
  # 3.13) Rename Duration ####
  setnames(Data,"Exp.Duration","Duration")

  # 3.14) EUs: change delimiters ####
  Data[,EU:=gsub("**",".",EU,fixed = T)]

  # 3.15) Rename TSP & TAP ####
  setnames(Data,c("Time.Clim.SP","Time.Clim.TAP"),c("TSP","TAP"))

  # 3.16) Update Journal Codes ####
  Journals<-master_codes$journals[,c("B.Journal","Journal")]
  
  Data<-merge(Data,Journals,by="B.Journal",all.x=T,sort=F)
  Data[!is.na(Journal),B.Journal:=Journal]

  # 3.17) Update DOI field
  Data[is.na(B.DOI),B.DOI:=B.Url]
  
  # 3.18) Partial economic outcomes ####
  # Note these were not encoded directly in 2023, I think we hoped to be able to derived this from the economic indicators recorded, but we will probably needed to go back and add this information.
  Data[,c("Out.Partial.Outcome.Name","Out.Partial.Outcome.Code"):="Not assessed"]
  
  # 3.19 Species (!!! TO DO !!!) ####
  # I think this was used for products like "Fish" for which the "Variety" was the species.
  # This requires harmonization across extractions to reconfigure the fish species to be the product as per crops and other animals
  Data[,V.Species:=NA]
  
# 4) Reconfigure to ERA v1.0 format ####
  C.Descrip.Col<-"T.Name"
  C.Data.Cols<-c("B.Code","B.Author.Last","B.Date","B.Journal","B.DOI","Site.LatD","LatM","LatS","LatH","Site.LonD","LonM","LonS","LonH","LatDiff","Site.Elevation",
                 "Country","ISO.3166.1.alpha.3","Site.Type","Site.ID","Site.MAT","Site.MAP","TAP","MSP","TSP","Soil.Type","Soil.Classification","Site.Soil.Texture",
                 "SOC","SOC.Unit","SOC.Depth","Soil.pH","Soil.pH.Method","PD.Plant.Start","PD.Plant.End","PD.Harvest.Start","PD.Harvest.End","Final.Reps",
                 "EX.Plot.Size","TID",C.Descrip.Col,paste0("C",1:NCols),"F.NO","F.NI","ED.Mean.T","ED.Error","ED.Error.Type",C.Descrip.Col,
                 "Buffer.Manual","MeanFlip","Out.Partial.Outcome.Name","Out.Partial.Outcome.Code")
  
  C.Cols<-c("Code","Author","Date","Journal","DOI","LatD","LatM","LatS","LatH","LonD","LonM","LonS","LonH","Lat.Diff","Elevation","Country","ISO.3166.1.alpha.3","Site.Type",
            "Site.ID","MAT","MAP","TAP","MSP","TSP","Soil.Type","Soil.Classification","Soil.Texture","SOC","SOC.Unit","SOC.Depth","Soil.pH","Soil.pH.Method",
            "Plant.Start","Plant.End","Harvest.Start","Harvest.End","Rep","Plot.Size","CID","C.Descrip",
            paste0("C",1:NCols),"C.NO","C.NI","MeanC","MeanC.Error","Mean.Error.Type","C.Descrip.Clean","Buffer.Manual","MeanFlip","Partial.Outcome.Name","Partial.Outcome.Code")
  
  names(C.Data.Cols)<-C.Cols
  
  T.Descrip.Col<-"T.Name"
  T.Data.Cols<-c("TID",T.Descrip.Col,paste0("C",1:NCols),"F.NI","F.NO","R.Seq","V.Level.Name","Tree","Duration",
                 "M.Year","M.Year.Start","M.Year.End","M.Season.Start","M.Season.End","EU","Out.Code","Out.Unit","ED.Mean.T","ED.Error","Out.Depth.Upper","Out.Depth.Lower",
                 "ED.Data.Loc","USD2010.C","USD2010.T",T.Descrip.Col,"V.Level.Name","Tree","R.Seq","V.Species")
  
    T.Cols<-c("TID","T.Descrip",paste0("T",1:NCols),"T.NI","T.NO","Diversity","Variety","Tree","Duration",
            "M.Year","M.Year.Start","M.Year.End","Season.Start","Season.End","EU","Outcome","Units","MeanT","MeanT.Error","Upper","Lower","DataLoc",
            "USD2010.C","USD2010.T","T.Descrip.Clean","Variety.Clean","Tree.Clean","Diversity.Clean","Species")
  
  names(T.Data.Cols)<-T.Cols
  
  
  Knit.V1<-function(Control.N,Control.For,Data,Analysis.Function,NCols,C.Data.Cols,T.Data.Cols,Mulch){
    Control.N<-as.numeric(Control.N)
    
    Ctrl<-Data[N==Control.N,..C.Data.Cols]
    
    colnames(Ctrl)<-names(C.Data.Cols)
    
    Control.For<-as.numeric(unlist(strsplit(as.character(Control.For),"[/][/]")))
    
    Trt<-rbindlist(lapply(Control.For,FUN=function(i){
      X<-Data[N==i,..T.Data.Cols]
      colnames(X)<-names(T.Data.Cols)
      X
    }))
    
    Ctrl<-Ctrl[rep(1,nrow(Trt))]
    Ctrl[,Mulch.Code:=Mulch]
    X<-cbind(Ctrl,Trt)
    X[,Analysis.Function:=Analysis.Function]
    X
    
  }
  
  # Set up future plan for parallel execution with the specified number of workers
  plan(multisession, workers = worker_n)
  
  # Progress bar setup
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  # Start parallelized process with progress bar
  b_codes<-DATA[,unique(B.Code)]
  
  Verbose<-F
  
  Comparisons <- with_progress({
    # Progress indicator
    p <- progressor(steps = nrow(Comparisons))
  
  ERA.Reformatted <-rbindlist(
    future_lapply(1:nrow(Comparisons), function(i) {
    #pblapply(1:nrow(Comparisons), function(i) {
      if (Verbose) {
        output <- paste(Comparisons[i, paste(B.Code, Control.N)], " | i = ", i,"/",nrow(Comparions))
        cat(paste(output, strrep(" ", 100 - nchar(output)), "\r"))
        flush.console()  
      }
      
      result <- Knit.V1(
        Control.N = Comparisons[i, Control.N],
        Control.For = Comparisons[i, Control.For],
        Data = Data,
        Analysis.Function = Comparisons[i, Analysis.Function],
        Mulch=Comparisons[i,Mulch.Code],
        NCols = NCols,
        T.Data.Cols = T.Data.Cols,
        C.Data.Cols = C.Data.Cols
      )
      
      # Update progress bar after each iteration
      p() 
      
      return(result)
    })
  )
  })
  
  plan(sequential)

  # 4.1) Add in outcomes that are ratios of Trt/Cont already ####
  Data.R<-Data[(!is.na(ED.Comparison1) & !is.na(T.Name)) & Out.Subind!=c("Land Equivalent Ratio","Area Time Equivalent Ratio"),c("T.Name","IN.Level.Name","R.Level.Name","ED.Comparison1","ED.Comparison2","B.Code","N","Out.Code.Joined")]
  
  # Use this to check outcomes are appropriate
  error_dat<-Data.R[!grepl("Efficiency|Ratio",Out.Code.Joined)
                      ][,.(value=paste0(unique(Out.Code.Joined),collapse = "/")),by=B.Code
                                                 ][,table:="Data.Out"
                                                   ][,field:="ED.Comparison1"
                                                     ][,issue:="Check this is a ratio or efficiency outcome that is calculated from a treatment AND control. Crop yield, biomass yield and gross return should not be associated with ED.Comparison."]
  error_list<-list(error_dat)
  
  # Remove crop yield or biomass yield
  Data.R<-Data.R[!grepl("Crop Yield|Biomass Yield|Gross Return",Out.Code.Joined)]
  
  # Check & remove entries where T.Name == ED.Comparison
    error_dat<-Data.R[T.Name==ED.Comparison1
                      ][,.(value=paste(unique(paste0(Out.Code.Joined,"-",T.Name)),collapse="/")),by=B.Code
                        ][,table:="Data.Out"
                          ][,field:="ED.Comparison1"
                            ][,issue:="T.Name and ED.Comparison1 are the same."]
    
    error_list<-c(error_list,list(error_dat))
    
  Data.R<-Data.R[T.Name!=ED.Comparison1]
  
  # Add control information for ratios
  Data[,Code:=paste(B.Code,T.Name)]
  Data.R[,Code:=paste(B.Code,ED.Comparison1)]
  Verbose=F
  
  results<-pblapply(1:nrow(Data.R),FUN = function(i){
    if(Verbose){print(paste(Data.R[i,paste(B.Code,N)]," i = ",i))}
    data.r_code<-Data.R[i,Code]
    Control.N<-Data[Code == data.r_code,N][1]
    
    if(is.na(Control.N)|length(Control.N)==0){
      if(Verbose){
      print(paste0("No Match - ",Data.R[i,paste(B.Code,N)]," | i = ",i))
      }
      error<-Data.R[i,.(B.Code,ED.Comparison1)]
      return(list(data=NULL,error=error))
      
    }else{
      Control.For<-Data.R[i,N]
      
      AF<-"Simple"
      
      A<-Data[c(Control.N,Control.For),is.na(T.Name)]
      if(any(A)){
        B<-Data[c(Control.N,Control.For),is.na(IN.Level.Name)]
        if(any(B)){
          if(sum(B)==2){
            AF<-"Sys.Int.vs.Int"
          }else{
            AF<-"Sys.Int.vs.Mono"
          }
        }else{
          C<-Data[c(Control.N,Control.For),is.na(R.Level.Name)]
          if(sum(C)==2){
            AF<-"Sys.Rot.vs.Rot"
          }else{
            AF<-"Sys.Rot.vs.Mono"
          }
        }}
      
       data<-Knit.V1(
        Control.N = Control.N,
        Control.For = Control.For,
        Data = Data,
        Analysis.Function = AF,
        NCols = NCols,
        Mulch="",
        T.Data.Cols = T.Data.Cols,
        C.Data.Cols = C.Data.Cols
      )
       
       return(list(data=data,error=NULL))
      
    }
  })
  
  error_dat<-rbindlist(lapply(results,"[[","error"))
  error_dat<-error_dat[,.(value=paste(unique(ED.Comparison1),collapse="/")),by=B.Code
                       ][,table:="Data.Out"
                         ][,field:="ED.Comparison1"
                           ][,issue:="There is no match for ED.Comparison1 in the T.Name column, this means we cannot describe the comparison from another row in the table."]
  
  error_list<-c(error_list,list(error_dat))

  ERA.Reformatted.Ratios<-rbindlist(lapply(results,"[[","data"))
  
  # Add in Analysis Function
  ERA.Reformatted.Ratios[,MeanC:=NA][,MeanC.Error:=NA][,MeanC.Error.Type:=NA][,Analysis.Function:="Ratios"]
  nrow(ERA.Reformatted.Ratios[is.na(C.Descrip)])
  
    # 4.1.2) Remove non-comparisons ######
  
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

    error_dat<-ERA.Reformatted.Ratios[!N.Log,.(value=paste(unique(paste0("T =",T.Descrip,", C = ",C.Descrip,", Out = ",Outcome)),collapse="/")),by=Code
                                      ][,table:="Compiled Dataset"
                                        ][,field:="T.Descrip, C.Descrip, Out.Code"
                                          ][,issue:="There appears to be no comparison between the control and treatment for a efficiency or ratio outcome derived from ED.Comparison1."]
    setnames(error_dat,"Code","B.Code")
    error_list<-c(error_list,list(error_dat))

    ERA.Reformatted.Ratios<-ERA.Reformatted.Ratios[N.Log]
    ERA.Reformatted.Ratios[,MeanC.Error.Type:=NULL]
  
  # 4.2) Add in LER ####
  # Generate comparison by removing diversification components
  
  Data.R<-Data[Out.Subind %in% c("Land Equivalent Ratio","Area Time Equivalent Ratio")]
  Data.R[,N:=1:.N]
  Data.R[is.na(T.Name),T.Name:=IN.Level.Name]
  Data.R[is.na(T.Name),T.Name:=R.Level.Name]
  
  Data.R.Cont<-Data.R
  Data.R.Cont<-data.frame(Data.R.Cont)
  
  Div.Codes<-PracticeCodes1[Practice %in% c("Intercropping","Rotation","Alleycropping","Scattered Trees","Silvopasture",
                                            "Parklands","Agroforestry Fallow","Green Manure","Improved Fallow"),Code]
  
  Verbose<-F
  results<-pblapply(1:nrow(Data.R.Cont),FUN=function(i){
    X<-Data.R.Cont[i,paste0("C",1:NCols)]
    
    if("h2" %in% X){
      if(Verbose){
      print(paste("Error - h2 present:",paste(Data.R.Cont[i,c("T.Name","IN.Level.Name","R.Level.Name","B.Code")],collapse = "-")))
      }
      error1<-data.table(B.Code=Data.R.Cont[i,"B.Code"],value=paste(Data.R.Cont[i,c("T.Name","IN.Level.Name","R.Level.Name")],collapse = "||"),issue="LER outcome treatment has h2 code present")
    }else{
      error1<-NULL
    }
    N<-which(X %in% Div.Codes)
    
    if(length(N) == 0){
      if(Verbose){
      print(paste("Error - no diversification present:",i,"-",paste(Data.R.Cont[i,c("T.Name","IN.Level.Name","R.Level.Name","B.Code")],collapse = "-")))
      }
      error2<-data.table(B.Code=Data.R.Cont[i,"B.Code"],value=paste(Data.R.Cont[i,c("T.Name","IN.Level.Name","R.Level.Name")],collapse = "||"),issue="LER outcome treatment has no diversification code present?")
          }else{
      error2<-NULL
      
      X[N[1]]<-"h2"
      N<-N[-1]
      if(length(N)>0){
        X[N]<-NA
      }
    }
    
    Y<-Data.R.Cont[i,]
    Y[,paste0("C",1:NCols)]<-X
    Y$All.Codes<-list(unique(X[!is.na(X)]))
    
    return(list(data=data.table(Y),error1=error1,error2=error2))
    
  })
  
  error_dat1<-rbindlist(lapply(results,"[[","error1"))
  error_dat2<-rbindlist(lapply(results,"[[","error2"))
  error_dat<-unique(rbind(error_dat1,error_dat2))[,table:="Data.Out"][,field:="T.Name||IN.Level.Name||R.Level.Name"]
  
  error_list<-c(error_list,list(error_dat))
  error_list<-rbindlist(error_list,use.names = T)[order(B.Code)]
  error_list<-error_tracker(errors=error_dat,filename = "Data.Out - issues with enter data comparisons field",error_dir=error_dir,error_list = NULL)
  
  Data.R.Cont<-rbindlist(lapply(results,"[[","data"))
  
  Data.R.Cont[,R.Seq:=NA]
  Data.R.Cont[,Int.Tree:=NA]
  Data.R.Cont[,Rot.Tree:=NA]
  Data.R.Cont[,TID:=paste0(TID,".LER.Control")]
  Data.R.Cont[,T.Name:="LER Control"]
  Data.R.Cont[,IN.Level.Name:="LER Control"]
  Data.R.Cont[,R.Level.Name:="LER Control"]
  Data.R.Cont[,ED.Mean.T:=NA]
  Data.R.Cont[,ED.Error:=NA]
  Data.R.Cont[,ED.Error.Type:=NA]
  
  Data.R.Cont[,N:=(nrow(Data.R)+1):(nrow(Data.R)*2)]
  Data2<-rbind(Data.R,Data.R.Cont)
  
  Verbose=F
  
  # Set up future plan for parallel execution with the specified number of workers
  plan(multisession, workers = worker_n)
  
  # Progress bar setup
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  ERA.Reformatted.LER <- with_progress({
    # Progress indicator
    p <- progressor(steps = nrow(Data.R))
    
    # Apply function across unique B.Codes with future_lapply in parallel
    future_lapply(1:nrow(Data.R), function(i) {
  
  #ERA.Reformatted.LER<-rbindlist(pblapply(1:nrow(Data.R),FUN = function(i){
    
    if(Verbose){print(paste(Data.R[i,paste(B.Code,N)]," i = ",i))}
      # Update progress bar after each iteration
      p() 
    
    Control.N<-Data.R.Cont[i,N]
    Control.For<-Data.R[i,N]
    
    AF<-"Simple"
    
    A<-Data2[c(Control.N,Control.For),is.na(T.Name)]
    if(any(A)){
      B<-Data[c(Control.N,Control.For),is.na(IN.Level.Name)]
      if(any(B)){
        if(sum(B)==2){
          AF<-"Sys.Int.vs.Int"
        }else{
          AF<-"Sys.Int.vs.Mono"
        }
      }else{
        C<-Data[c(Control.N,Control.For),is.na(R.Level.Name)]
        if(sum(C)==2){
          AF<-"Sys.Rot.vs.Rot"
        }else{
          AF<-"Sys.Rot.vs.Mono"
        }
      }}
    
    data<-Knit.V1(
      Control.N = Control.N,
      Control.For = Control.For,
      Data = Data2,
      Analysis.Function = AF,
      NCols = NCols,
      Mulch="",
      T.Data.Cols = T.Data.Cols,
      C.Data.Cols = C.Data.Cols
    )
    return(data)
  })
  })
  
  ERA.Reformatted.LER<-rbindlist(ERA.Reformatted.LER)
  
  plan(sequential)
  
  ERA.Reformatted.LER[,Analysis.Function:="LER"]
  
  # Set MeanC to equal 1 where MeanC is NA
  ERA.Reformatted.LER[is.na(MeanC),MeanC.Error:=NA][is.na(MeanC),MeanC:=1]
  
  # 4.3) Combine reformatted datasets ####
  ERA.Reformatted<-rbind(ERA.Reformatted,ERA.Reformatted.Ratios,ERA.Reformatted.LER)

  # 4.3) Recode residues + incorporation as a control for mulching + feed substitution control  ####
  # Simplest option might be where we have imbalanced residues (i.e. where different residues codes are present in control vs treatment)
  # to recode these.  
  # Where we have one mulch code in control vs >1 in treatment what to do?
  
  Dpracs<-c("Crop Rotation","Intercropping","Improved Fallow","Green Manure","Agroforestry Fallow","Intercropping or Rotation")
  Dpracs<-master_codes$prac[!grepl("h",Code)][Practice %in% Dpracs,Code]
  
  Rpracs<-c("Agroforestry Pruning","Mulch","Crop Residue","Crop Residue Incorporation")
  X<-lapply(Rpracs,FUN=function(R){
    master_codes$prac[!grepl("h",Code)][Practice %in% R,Code]
  })
  names(X)<-Rpracs
  Rpracs<-X
  
  # Mulch codes present in Control & Treatment + Dprac should be present (as per CompareFun) in Trt else we are not in a complex situation
  # Recode either where Control has Rprac category code that Trt does not, or Trt has >1 and control has 1 practice.
  # ***ISSUE*** Should Agroforestry Prunings be excluded?? (here and in CompareFun?) #####
  
  C.Cols<-which(colnames(ERA.Reformatted) %in% paste0("C",1:NCols))
  
  Verbose<-F
  
  # Set up future plan for parallel execution with the specified number of workers
  plan(multisession, workers = worker_n)
  
  # Progress bar setup
  progressr::handlers(global = TRUE)
  progressr::handlers("progress")
  
  ERA.Reformatted <- with_progress({
    # Progress indicator
    p <- progressor(steps = nrow(ERA.Reformatted))
    
    # Apply function across unique B.Codes with future_lapply in parallel
    future_lapply(1:nrow(ERA.Reformatted), function(i) {
    #pblapply(1:nrow(ERA.Reformatted),FUN =function(i){
    
      # Update progress bar after each iteration
      p() 
      
    X<-data.frame(ERA.Reformatted[i,])
    
    T.Code<-as.vector(unlist(X[,paste0("T",1:NCols)]))
    T.Code<-T.Code[!is.na(T.Code)]
    
    if(Verbose){print(paste0("row (i) = ",i))}
    
    if(any(T.Code %in% Dpracs)){
      C.Code<-as.vector(unlist(X[,paste0("C",1:NCols)]))
      C.Code<-C.Code[!is.na(C.Code)]
      for(j in 1:length(Rpracs)){
        if(Verbose){print(paste0("row (i) = ",i," | RPracs (j) = ",names(Rpracs)[j]," - ",j))}
        Ts<-T.Code[T.Code %in% Rpracs[[j]]]
        Cs<-C.Code[C.Code %in% Rpracs[[j]]]
        CnT<-Cs[!Cs %in% Ts]
        
        if(length(CnT)>0 | (length(Ts)>1 & length(Cs)==1)){
          T.Code<-T.Code[!T.Code %in% Ts]
          C.Code<-C.Code[!C.Code %in% Cs]
          
          if(names(Rpracs)[j] != "Agroforestry Pruning"){
            
            T.Code<-c(T.Code,unique(unlist(lapply(Ts,FUN=function(X){unlist(strsplit(X,"[.]"))[1]}))))
            C.Code<-c(C.Code,unique(unlist(lapply(Cs,FUN=function(X){unlist(strsplit(X,"[.]"))[1]}))))
            
          }else{
            T.Code<-unique(c(T.Code,gsub("a17|a16","a15",Ts)))
            C.Code<-unique(c(C.Code,gsub("a17|a16","a15",Cs)))
          }
          
          T.Code<-c(T.Code,rep(NA,NCols-length(T.Code)))
          C.Code<-c(C.Code,rep(NA,NCols-length(C.Code)))
          
          
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
    
  })
  })
  
  plan(sequential)
  
  ERA.Reformatted<-rbindlist(ERA.Reformatted)

  # 4.4) Remove other Fert, Mulch, Water Harvesting and Ash codes ####
  # Need to remove then check comparisons are still present
  
  # Rm.Codes<-c("b74","b73","b99")
  Cols<-paste0(c("C","T"),rep(1:NCols,each=2))
  
  X<-data.frame(ERA.Reformatted)
  
  for(Col in Cols){
    X[,Col]<-gsub("b74",NA,X[,Col])
    X[,Col]<-gsub("b73",NA,X[,Col])
    X[,Col]<-gsub("b99",NA,X[,Col])
  }
  
  X<-data.table(X)
  
  # Check for any comparisons that now have no ERA practices extra in the Treatment 
  Y<-unlist(pblapply(1:nrow(X),FUN=function(i){
    T.Codes<-unlist(X[i,paste0("T",1:NCols)])
    T.Codes<-T.Codes[!is.na(T.Codes)]
    T.Codes<-T.Codes[!grepl("h",T.Codes)]
    
    C.Codes<-unlist(X[i,paste0("C",1:NCols)])
    C.Codes<-C.Codes[!is.na(C.Codes)]
    C.Codes<-C.Codes[!grepl("h",C.Codes)]
    
    # Are any there ERA practices in the treatment not in the control?
    Z<-length(T.Codes[!T.Codes %in% C.Codes])>0 
    Z
  }))
  
  ERA.Reformatted<-X[Y]
  
  # 4.5) There are still issues with duplication, so only allow unique rows ####
    # I don't think this warrants spending a lot of tweaking the comparison code
    dim(ERA.Reformatted)
    table(ERA.Reformatted$Analysis.Function)
    ERA.Reformatted<-unique(ERA.Reformatted)
    table(ERA.Reformatted$Analysis.Function)
    dim(ERA.Reformatted)
  # 4.6) Update Mulch vs Incorporation Codes ####
  # Basic Comparisons function adds a Mulch.Code column which shows which code in the Control should be changed to a h37 code
  # However this is not fully implemented for system outcomes yet so we will recode the comparison directly
  # Incorporation codes in control (to be removed)
  Mulch.C.Codes<-c("a15.2","a16.2","a17.2","b41","b41.1","b41.2","b41.3")
  # Corresponding mulch code required in treatment (order matches Mulch.C.Codes)
  Mulch.T.Codes<-c("a15.1","a16.1","a17.1","b27","b27.1","b27.2","b27.3")
  
  Cols<-paste0(rep(c("C","T"),each=NCols),1:NCols)
  
  Z<-ERA.Reformatted[,..Cols]
  
  Z<-rbindlist(pbapply(Z,1,FUN=function(X){
    A<-Mulch.C.Codes[Mulch.C.Codes %in% X[1:NCols] & Mulch.T.Codes %in% X[(NCols+1):length(X)]]
    if(length(A)>0){
      X[which(X[1:NCols] == A)] <-"h37"
    }
    data.table(t(X))
  }))
  
  ERA.Reformatted<-cbind(ERA.Reformatted[,!..Cols],Z)
  
  # 5.7) Save Output ####
  save_name<-file.path(era_dirs$era_masterdata_dir, gsub("[.]RData","_comparisons.parquet",file_local))
  arrow::write_parquet(ERA.Reformatted,save_name)