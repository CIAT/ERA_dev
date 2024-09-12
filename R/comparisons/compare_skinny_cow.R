# First run R/0_set_env.R & R/import/import_skinny_cow_2022.R
# 0.0) Install and load packages ####
if (!require(pacman)) install.packages("pacman")  # Install pacman if not already installed
pacman::p_load(data.table, 
               miceadds,
               pbapply, 
               future.apply,
               future,
               miceadds)

# 0.1) Set cores for parallel ####
Cores<-14

# 0.2) Set project & directories
project<-era_projects$skinny_cow_2022

# Set directory for error and harmonization tasks
error_dir<-file.path(era_dirs$era_dataentry_prj,project,"data_issues")
if(!dir.exists(error_dir)){
  dir.create(error_dir,recursive=T)
}

# 1) Load data ####
  # 1.1) Load compiled excel data #####
  (files<-list.files(era_dirs$era_masterdata_dir,project,full.names = T))
  (files_s3<-grep(project,s3$dir_ls(era_dirs$era_masterdata_s3),value=T))
  
  # If most recent compilation of the livestock is not available locally download it from the s3
  if(basename(tail(files,1))!=basename(tail(files_s3,1))){
    local_path<-gsub(era_dirs$era_masterdata_s3,era_dirs$era_masterdata_dir,tail(files_s3,1))
    s3$file_download(tail(files_s3,1),local_path)
    files<-list.files(era_dirs$era_masterdata_dir,project,full.names = T)
  }
  
  # Load data
  data_list<-miceadds::load.Rdata2(file=basename(tail(files,1)),path=dirname(tail(files,1)))
  
  AF.Out<-data_list$AF.Out
  Animals.Out<-data_list$Animals.Out
  Animals.Diet<-data_list$Animals.Diet
  Animals.Diet.Comp<-data_list$Animals.Diet.Comp
  Animals.Diet.Comp<-data_list$Animals.Diet.Digest
  Data.Out<-data_list$Data.Out[,N:=1:.N]
  MT.Out<-data_list$MT.Out
  Site.Out<-data_list$Site.Out
  Soil.Out<-data_list$Soil.Out
  Var.Out<-data_list$Var.Out
  
  # Remove h-codes from T.Codes
  rm_code_fun<-function(x,delim,remove_pattern){
    x<-unlist(strsplit(x,delim))
    x<-x[!grepl(remove_pattern,x)]
    x<-x[!is.na(x)]
    x<-paste(sort(x),collapse = delim)
    return(x)
  }
  
  Data.Out[,Final.Codes:=T.Codes
  ][,Final.Codes:=rm_code_fun(Final.Codes[1],delim = "-",remove_pattern="h"),by=Final.Codes]
  Data.Out$Final.Codes<-strsplit(Data.Out$Final.Codes,"-")
  
  # Is Feed Substitution Present?
  check_codes<-master_codes$prac[Practice=="Feed Substitution",Code]
  Data.Out[,Feed.Sub:=any(unlist(Final.Codes) %in% check_codes),by=N]
  
  # Is Feed Addition Present?
  check_codes<-master_codes$prac[Practice=="Feed Addition",Code]
  Data.Out[,Feed.Add:=any(unlist(Final.Codes) %in% check_codes),by=N]
  
  # Remove NAs in diet controls
  Data.Out<-Data.Out[is.na(A.Feed.Sub.C),A.Feed.Sub.C:="No"][is.na(A.Feed.Add.C),A.Feed.Add.C:="No"]
  
  # 1.2) Load era vocab #####
  # Get names of all sheets in the workbook
  sheet_names <- readxl::excel_sheets(era_vocab_local)
  sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]
  
  # Read each sheet into a list
  master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)

  # Read in codes and harmonization datasets
  EUCodes<-master_codes$prod
  MasterLevels<-master_codes$lookup_levels
  PracticeCodes<-master_codes$prac
  PracticeCodes1<-master_codes$prac
  TreeCodes<-master_codes$trees

# 2) Automated Comparison of Control vs Treatments ####
  Comparison.List<-list()
  Verbose<-F
  Debug<-F
  # Ignore Aggregated Observations (dealt with in section 2.1 for additions)
  Data.Out.Animals<-Data.Out[!grepl("[.][.][.]",T.Name)]

  # Paper comparing animal breeds with an identical diet (base diet)
  if(F){
    unique(Data.Out.Animals[is.na(A.Level.Name),c("B.Code","T.Name","V.Animal.Practice","A.Level.Name")])
  }
  # Calculate Number of ERA Practices
  Data.Out.Animals[,N.Prac:=unlist(lapply(Final.Codes,length)),by=N]
  
  # Weight Gain Outcomes need non-essential info removing
  Data.Out.Animals[!is.na(Out.WG.Start) |
                     !is.na(Out.WG.Days)|
                     !is.na(Out.WG.Unit),
                   Out.Code.Joined:=paste(unlist(tstrsplit(Out.Code.Joined[1],"[.][.]",keep=1:2)),collapse=".."),by=Out.Code.Joined]

  # 2.1) Feed Addition: Everything but Feed Substitution ####
  DATA<-Data.Out.Animals[(Feed.Add==T|A.Feed.Add.C=="Yes")]
  
  Match.Fun <- function(A, B) {
    A <- unlist(A)
    B <- unlist(B)
    result <- setdiff(A, B)
    if (length(result) == 0) {
      result <- NA
    }
    return(list(result))
  }
  Match.Fun2 <- function(A, B, C) {
    A <- unlist(A)
    B <- unlist(B)
    C <- unlist(C)
    result <- unique(C[match(A, B)])
    if (length(result) == 0) {
      result <- NA
    }
    return(list(result))
  }
    
  Compare.Fun.Ani<-function(Data,Verbose,Debug,PracticeCodes){
      
      BC<-Data$B.Code[1]
      N<-Data[,N]
      #Final.Codes<-Data[,Final.Codes] # Is this redundant?
      #k<-N # Is this redundant?
      Y<-Data[,c("Final.Codes","N","N.Prac")][,Y.N:=1:.N]
      
      result<-lapply(1:length(N),FUN=function(j){
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
          Z[,Level.Check:=all(unlist(Level.Check)),by=Y.N]
          
          if(Debug){
            Z
          }else{
            # Return rows from master table that are valid treatments for this control (Level.Check==T)
            Z[Level.Check==T,N]
          }
        }else{NA}
        
      })
      return(result)
    } # Setting Debug to T prints comparison table rather than row numbers
    
  # Set Grouping Variables
  CompareWithin<-c("Site.ID","P.Product","ED.Product.Comp","Time", "Out.Code.Joined",
                   "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","O.Structure","C.Structure","B.Code","Country")
  
  B.Codes<-DATA[,unique(B.Code)]
  Comparisons <- pblapply(1:length(B.Codes), FUN = function(j) {
    
    BC <- B.Codes[j]
    
    # Filter Data.Sub once using data.table's fast subset
    Data.Sub <- DATA[B.Code == BC]
    
    # Vectorized comparison for the "CompareWithin" columns
    CW <- unique(Data.Sub[, ..CompareWithin])
    CW.Match <- match(do.call(paste, Data.Sub[, ..CompareWithin]), do.call(paste, CW))
    
    # Assign group by vectorized assignment
    Data.Sub[, Group := CW.Match]
    
    # Process each unique group using rbindlist and lapply
    DS <- rbindlist(lapply(unique(Data.Sub$Group), FUN = function(i) {
      
      if (Verbose) cat(paste0(BC, " Subgroup = ", i, "\n"))
      
      # Call the Compare.Fun.Ani function on each group
      Control.For <- Compare.Fun.Ani(Verbose = Verbose, Data = Data.Sub[Group == i], Debug = FALSE, PracticeCodes = master_codes$prac)
      
      # Return the results as a data.table
      data.table(Control.For = Control.For, N = Data.Sub[Group == i, N])
    }))
    
    # Add B.Code column after processing
    DS[, B.Code := BC]
    
    return(DS)
  })  
   
  # list studies with no comparisons
  error_dat<-data.table(B.Code=unique(rbindlist(Comparisons[unlist(lapply(Comparisons, FUN=function(X){all(is.na(X$Control.For))}))])$B.Code))
  error_dat<-error_dat[,value:=NA
                       ][,table:="Ani.Out"
                         ][,field:=NA
                           ][,issue:="Feed addition is present but there appear to be no valid comparisons."]
  errors<-list(error_dat)
  
  Comparisons<-rbindlist(Comparisons)
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  Cols<-c("T.Name")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  setnames(Comparisons1,"T.Name","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals[match(unlist(Control.For),N),T.Name],collapse="//"),by=Control.N]
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  
  Comparisons1$Analysis.Function<-"NoDietSub"
  
  Comparison.List[["Animal.NoDietSub"]]<-Comparisons1
  
  # 2.2) Feed.Add: Aggregated Practices ####
  # Ignore Aggregated Observations for now
  Data.Out.Animals.Agg<-Data.Out[grep("[.][.][.]",T.Name)]
  # Subset to Animal Data
  Data.Out.Animals.Agg<-Data.Out.Animals.Agg[!(is.na(A.Level.Name) & is.na(V.Animal.Practice)),]
  
  # Split codes into list
  Data.Out.Animals.Agg[,Final.Codes:=.(strsplit(T.Codes,"-")),by=N]
  
  # Calculate Number of ERA Practices
  Data.Out.Animals.Agg[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by=N]
  
  # Is Feed Substitution Present?
  Data.Out.Animals.Agg[,Feed.Sub:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Substitution",Code]),by=N]
  
  # Is Feed Addition Present?
  Data.Out.Animals.Agg[,Feed.Add:=any(unlist(Final.Codes) %in% PracticeCodes1[Practice=="Feed Addition",Code]),by=N]
  
  #  # Weight Gain Outcomes need non-essential info removing
  Data.Out.Animals.Agg[!is.na(Out.WG.Start) |
                     !is.na(Out.WG.Days)|
                     !is.na(Out.WG.Unit),
                   Out.Code.Joined:=paste(unlist(tstrsplit(Out.Code.Joined[1],"[.][.]",keep=1:2)),collapse=".."),by=Out.Code.Joined]
  
  # Set Grouping Variables
  CompareWithin<-c("Site.ID","P.Product","ED.Product.Comp","Time", "Out.Code.Joined",
                   "ED.Sample.End","ED.Sample.DAS","O.Structure","C.Structure","B.Code","Country","T.Agg.Levels_name")
  
  Verbose<-F
  
  DATA<-Data.Out.Animals.Agg
  
  B.Codes<-DATA[,unique(B.Code)]
  Comparisons<-rbindlist(pblapply(1:length(B.Codes),FUN=function(i){
    BC<-B.Codes[i]
    Data.Sub<-DATA[B.Code==BC]
    CW<-unique(Data.Sub[,..CompareWithin])
    CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
    Data.Sub[,Group:=CW]
    
    DS<-rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
      if(Verbose){print(paste0(BC," Subgroup = ", i))}
      
      Control.For<-Compare.Fun.Ani(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F,PracticeCodes = master_codes$prac)
      data.table(Control.For=Control.For,N=Data.Sub[Group==i,N])
    }))
    
    DS[,B.Code:=BC]
    DS
  }))
  
  Comparisons<-Comparisons[!is.na(Control.For)
  ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  Cols<-c("T.Name")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals.Agg[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  setnames(Comparisons1,"T.Name","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals.Agg[match(unlist(Control.For),N),T.Name],collapse="//"),by=Control.N]
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  
  Comparisons1$Analysis.Function<-"NoDietSub.Agg"
  Comparison.List[["Animal.NoDietSub.Agg"]]<-Comparisons1
  
  # 2.3) Feed Substitution ####
  DATA<-Data.Out.Animals[Feed.Sub==T |A.Feed.Sub.C=="Yes"]
  
  Compare.Fun.Ani.DSub<-function(Data,Verbose,PracticeCodes){
    if(Verbose){print(paste0(Data$B.Code[1], " | Group = ",unique(Data$Group)))}
    
    X<-which(Data$A.Feed.Sub.C=="Yes")
    
    if(length(X)==0){
      data.table(Control.For="No Feed Substitution Control Present",N=Data$N,B.Code=Data$B.Code[1])
    }else{
      
      if(length(unique(Data$V.Level.Name))>1){
        
        GI.Fun=function(FC){
          FC<-FC[FC %in% PracticeCodes[Practice == "Genetic Improvement",Code]]
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
  
  
  # Set Grouping Variables
  CompareWithin<-c("Site.ID","P.Product","ED.Product.Comp","Time", "Out.Code.Joined",
                   "ED.Sample.End","ED.Sample.DAS","O.Structure","C.Structure","B.Code","Country","T.Agg.Levels_name")
  
  B.Codes<-DATA[,unique(B.Code)]
  Comparisons<-rbindlist(pblapply(1:length(B.Codes),FUN=function(BC){
    BC<-B.Codes[i]
    
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
    
  }))

  Comparisons[Control.For=="No Feed Substitution Control Present",Control.For:=NA]
  
  error_data<-Comparisons[,N.Comp:=length(!is.na(Control.For)),by=B.Code
                                       ][N.Comp==0,
                                         ][,.(value=NA),by=B.Code
                                           ][,table:="Ani.Out"
                                             ][,field:="A.Level.Name"
                                               ][,issue:="Feed substitution is present, but there appear to be no valid comparisons."]
  errors<-c(errors,list(error_dat))
  
  Comparisons<-Comparisons[!is.na(Control.For)
                           ][,Len:=length(unlist(Control.For)),by=N]
  
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  Cols<-c("T.Name")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-Data.Out.Animals[match(Comparisons$N,N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=as.character("")]
  setnames(Comparisons1,"T.Name","Control.Trt")
  
  Comparisons1[,Compare.Trt:=paste0(Data.Out.Animals[match(unlist(Control.For),N),T.Name],collapse="//"),by=Control.N]
  Comparisons1[,Control.For:=paste0(unlist(Control.For),collapse="//"),by=Control.N][,Control.For:=as.character(Control.For)]
  Comparisons1$Analysis.Function<-"DietSub"
  Comparison.List[["Animal.DietSub"]]<-Comparisons1
  
  # 2.4) Simple Comparisons #####
  Data.Out.No.Agg<-Data.Out[!grepl("[.][.][.]",T.Name)]
  # Remove outcomes aggregated over rot/int entire sequence or system (dealt with sections 1.2 and 1.3)
  Data.Out.No.Agg<-Data.Out.No.Agg[!is.na(T.Name)]
  # Remove animal data (dealt with in section 2)
  Data.Out.No.Agg<-Data.Out.No.Agg[Feed.Add==F & Feed.Sub==F]

  # Calculate Number of ERA Practices
  Data.Out.No.Agg[,N.Prac:=length(unlist(Final.Codes)[!is.na(unlist(Final.Codes))]),by="N"]

  # Weight Gain Outcomes need non-essential info removing
  Data.Out.No.Agg[!is.na(Out.WG.Start) |
                     !is.na(Out.WG.Days)|
                     !is.na(Out.WG.Unit),
                   Out.Code.Joined:=paste(unlist(tstrsplit(Out.Code.Joined[1],"[.][.]",keep=1:2)),collapse=".."),by=Out.Code.Joined]
  
  
  DATA<-Data.Out.No.Agg
  
  CompareWithin<-c("Site.ID","P.Product","ED.Product.Comp","Time", "Out.Code.Joined",
                     "ED.Sample.Start","ED.Sample.End","ED.Sample.DAS","C.Structure","O.Structure",
                     "B.Code")
  
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
  
  Compare.Fun<-function(Data,Verbose,Debug,PracticeCodes1,Return.Lists){
      BC<-Data$B.Code[1]
      N<-Data[,N]
      #Final.Codes<-Data[,Final.Codes] # Is this redundant?
      #k<-N
      Y<-Data[,c("Final.Codes","N","N.Prac")][,Y.N:=1:.N]
      
      rbindlist(lapply(1:length(N),FUN=function(j){
        if(Verbose){print(paste(BC," - Group ",Data$Group[1]," - N =",j))}
        X<-unlist(Data$Final.Codes[j])
        i<-N[j]
        
        if(is.na(X[1])){
          Z<-Y[N!=i & !is.na(Final.Codes)
          ][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
          ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
          ][Match>=0 & NoMatch>0] # Keep instances for which this treatment can be a control for other treatments
          }else{
            # Here we are working from the logic of looking at what other treatments can treatment[i] be a control for?
          Z<-Y[N!=i][,Match:=sum(X %in% unlist(Final.Codes)),by=N # How many practices in this treatment match practices in other treatments?
          ][,NoMatch:=sum(!unlist(Final.Codes) %in% X),by=N  # How many practices in this treatment do not match practices in other treatments?
          ]
          
          # Keep instances for which this treatment can be a control for other treatments
          Z<-Z[Match>=length(X) & NoMatch>0] 
          }
        
        if(nrow(Z)>0){

          Z[,Control.Code:=rep(list(X),nrow(Z))] # Add codes that are present in the control
          
          Z[,Prac.Code:=list(Match.Fun(Final.Codes,Control.Code)),by=N  # Add in column for the codes that are in treatment but not control
          ][,Linked.Tab:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
          ][,Linked.Tab_prac:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Tab)),by=N
          ][,Linked.Col:=list(Match.Fun2(Control.Code,PracticeCodes$Code,PracticeCodes$Linked.Col)),by=N]
          
          # Allow for control practices that are in the animal tab being compared to other practices in the animal tab
          # to have different A.Level.Names
          
          X<-Z[grepl("Animals.Out",Linked.Tab) & grepl("Animals.Out",Linked.Tab_prac),unlist(Linked.Col)]
          X<-list(X[X!="A.Level.Name"])
          Z[grepl("Animals.Out",Linked.Tab) & grepl("Animals.Out",Linked.Tab_prac),Linked.Col:=X]
          
          X<-Z[grepl("Animals.Out",Linked.Tab) & grepl("Animals.Out",Linked.Tab_prac),unlist(Linked.Tab)]
          X<-list(X[X!="Animals.Out"])
          Z[grepl("Animals.Out",Linked.Tab) & grepl("Animals.Out",Linked.Tab_prac),Linked.Tab:=X]
          
          Z[,Linked.Tab_prac:=NULL]
          
          Z$Level.Check<-lapply(1:nrow(Z),FUN=function(ii){
            if(length(unlist(Z[ii,Linked.Tab]))==0){
              !is.na(unlist(Z$Prac.Code)[ii])
            }else{
              unlist(lapply(1:length(unlist(Z[ii,Linked.Tab])),FUN=function(jj){
                                
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
                data.table(N = Y[j,N],Control.For=Z[,N],B.Code=BC)
              }else{
                data.table(N = Y[j,N],Control.For=list(Z[,N]),B.Code=BC)
              }
            }else{}
          }
        }else{}
        
      }))
    } 

  B.Codes<-Data.Out.No.Agg[,unique(B.Code)]
  
  Comparisons<-pblapply(1:length(B.Codes),FUN=function(i){
      BC<-B.Codes[i]
      Data.Sub<-DATA[B.Code==BC]
      CW<-unique(Data.Sub[,..CompareWithin])
      CW<-match(apply(Data.Sub[,..CompareWithin], 1, paste,collapse = "-"),apply(CW, 1, paste,collapse = "-"))
      Data.Sub[,Group:=CW]
      
      results<-rbindlist(lapply(unique(Data.Sub$Group),FUN=function(i){
        if(Verbose){print(paste0(BC," Subgroup = ", i))}
        Compare.Fun(Verbose = Verbose,Data = Data.Sub[Group==i],Debug=F,PracticeCodes1=master_codes$prac,Return.Lists=F)
        }))
      
      return(results)
      
    })

  Comparisons<-unique(rbindlist(Comparisons))
  
  error_dat<-DATA[!B.Code %in% Comparisons[,B.Code] & A.Feed.Add.C!="Yes" & A.Feed.Sub.C!="Yes",.(value=paste(unique(T.Name),collapse = "/")),by=B.Code
                    ][,table:="MT.Out"
                      ][,field:="T.Name"
                        ][,issue:="No Comparisons available in treatments that have no addition or substitution role (i.e. will not be used in comparisons)."]
  
    errors<-c(errors,list(error_dat))
    
    # Restructure and save
    Comparisons<-Comparisons[!is.na(Control.For)
    ][,Len:=length(unlist(Control.For)),by=N]
    
    Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
    
    Cols<-c("T.Name")
    Cols1<-c(CompareWithin,"T.Name")
    
    Comparisons1<-Data.Out.No.Agg[match(Comparisons[,N],N),..Cols1]
    Comparisons1[,Control.For:=Comparisons[,Control.For]]
    Comparisons1[,Control.N:=Comparisons[,N]]
    setnames(Comparisons1,"T.Name","Control.Trt")

    Comparisons1[,Compare.Trt:=Data.Out.No.Agg[match(Control.For,N),T.Name]]
    Comparisons1$Analysis.Function<-"Simple"
    Comparison.List<-list(Simple=Comparisons1)
    
  # 2.5) Save Errors #####
    errors<-rbindlist(errors)
    error_list<-error_tracker(errors=errors,
                              filename = paste0("comparison_logic_errors"),
                              error_dir=error_dir,
                              error_list = list())
    
# 3) Combine Data ####
  Comparisons<-rbindlist(Comparison.List)

# 4) Prepare Main Dataset ####
  Data<-Data.Out
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
  Data[Season==1|is.na(Season),MSP:=Site.MSP.S1]
  Data[Season==2,MSP:=Site.MSP.S2]
  
  # 4.6) Soils ####
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
  # 4.7.2) Create TID code ####
  Data[,TID:=paste0("T",as.numeric(as.factor(T.Name))),by="B.Code"]
  
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
  
    # 4.8.1) How many C columns are there? ####
  NCols<-sum(paste0("C",1:30) %in% colnames(Data))
  
  if(NCols<10){
    cols<-paste0("C",(NCols+1):10)
    Data[,(cols):=NA]
  }
  
  NCols<-sum(paste0("C",1:30) %in% colnames(Data))
  
  # 4.10) Add in Trees ####
  # Trees in animal feeds
  # PICK UP FROM HERE! adding trees to feeds #####

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
  
  Data[,M.Year:=gsub("..",".",Time,fixed=T)]
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
         by=list(Site.ID, T.Name, B.Code, Country, Final.Start.Year.Code)]
    
    
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
    Negative.Duration<-unique(Data[Duration<0,c("B.Code","Duration","Final.Start.Year.Code","Final.Start.Season.Code","Time")])
    if(nrow(Negative.Duration)>0){
      View(Negative.Duration)
    }
    rm(Negative.Duration)
    
    # 2) Start Date is NA but reporting year is not
    
    NA.Duration<-unique(Data[is.na(Duration) & !is.na(Time) & !ED.Ratio.Control==T,c("B.Code","T.Name","ED.Rot","ED.Int","Final.Start.Year.Code","Final.Start.Season.Code","M.Year","Duration","M.Year.Start","M.Year.End","M.Season.Start","M.Season.End","Max.Season")])
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
  unique(Data[grep("..",P.Product,fixed = T),c("P.Product","EU")])
  
  # 4.14) Animal Diet Source ####
  X<-Animals.Diet[,list(Source=unique(D.Source)),by=c("B.Code","A.Level.Name")]
  NX<-match(Data[,paste(B.Code,A.Level.Name)],X[,paste(B.Code,A.Level.Name)])
  Data[,Feed.Source:=X[NX,Source]]
  rm(X,NX)
  # 4.15) TSP & TAP ####
  NX<-match(Data[,paste(B.Code,Site.ID,M.Year)],Times[,paste(B.Code,Site.ID,Time)])
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
  A.Sites<-Data[grepl("[.][.]",Site.ID),N]
  
  
  
  Agg.Sites<-function(B.Code,Site.ID){
    
    Sites<-unlist(strsplit(Site.ID,"[.][.]"))
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
  
  
  
  X<-Data[A.Sites,Agg.Sites(B.Code[1],Site.ID[1]),by=c("B.Code","Site.ID")]
  
  A.Sites<-match(Data[,paste(B.Code,Site.ID)],X[,paste(B.Code,Site.ID)])
  
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
  
  C.Descrip.Col<-"T.Name"
  T.Descrip.Col<-"T.Name"
  
  
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
  Data.R<-Data[(!is.na(ED.Comparison) & !is.na(T.Name)) & Out.Subind!="Land Equivalent Ratio",c("T.Name","ED.Int","ED.Rot","ED.Comparison","B.Code","N","Out.Code.Joined")]
  
  
  # Use this to check outcomes are appropriate
  if(F){
    Data.R[order(Out.Code.Joined),unique(paste(B.Code,Out.Code.Joined))]
  }
  
  # Outcomes where the Treatment and Control are the same
  Data.R<-Data.R[!(T.Name==ED.Comparison)]
  
  # Check & remove entries where T.Name == ED.Comparison
  if(nrow(Data.R[T.Name==ED.Comparison])>0){
    View(Data.R[T.Name==ED.Comparison])
  }
  
  Data.R<-Data.R[T.Name!=ED.Comparison]
  
  # Hack to fix DK0056 issue where the comparison is a different rotation sequence
  N<-Data[B.Code=="DK0056",stringr::str_count(T.Name,"<<")]
  X<-unlist(lapply(strsplit(Data[B.Code=="DK0056",T.Name][N>1],"<<"),FUN=function(X){
    paste(c(X[1],X[2]),collapse="<<")
  }))
  
  
  Data[B.Code=="DK0056",Temp:=stringr::str_count(T.Name,"<<")>1]
  Data[B.Code=="DK0056" & Temp==T,T.Name:=X]
  
  Data[B.Code=="DK0056",T.Name]
  
  
  # Need to add in Analysis Function
  Verbose=T
  ERA.Reformatted.Ratios<-rbindlist(pblapply(1:nrow(Data.R),FUN = function(i){
    if(Verbose){print(paste(Data.R[i,paste(B.Code,N)]," i = ",i))}
    Control.N<-Data[match(Data.R[i,paste(B.Code,ED.Comparison)],Data[,paste(B.Code,T.Name)]),N][1]
    
    if(is.na(Control.N)|length(Control.N)==0){
      print(paste0("No Match - ",Data.R[i,paste(B.Code,N)]," | i = ",i))
      NULL
    }else{
      Control.For<-Data.R[i,N]
      
      AF<-"Simple"
      
      A<-Data[c(Control.N,Control.For),is.na(T.Name)]
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
  Data.R<-Data.R[!grepl("sole",T.Name)]
  Data.R[,N:=1:.N]
  Data.R[is.na(T.Name),T.Name:=IN.Level.Name2]
  Data.R[is.na(T.Name),T.Name:=R.Level.Name]
  
  # Perhaps best to create a new "Data" object to use in Knit.V1
  Data.R.Cont<-Data.R
  Data.R.Cont<-data.frame(Data.R.Cont)
  
  Div.Codes<-PracticeCodes1[Practice %in% c("Intercropping","Rotation","Alleycropping","Scattered Trees","Silvopasture",
                                            "Parklands","Agroforestry Fallow","Green Manure","Improved Fallow"),Code]
  
  
  Data.R.Cont<-rbindlist(pblapply(1:nrow(Data.R.Cont),FUN=function(i){
    X<-Data.R.Cont[i,paste0("C",1:10)]
    
    if("h2" %in% X){
      print(paste("Error - h2 present:",paste(Data.R.Cont[i,c("T.Name","ED.Int","ED.Rot","B.Code")],collapse = "-")))
    }
    N<-which(X %in% Div.Codes)
    
    if(length(N) == 0){
      print(paste("Error - no diversification present:",i,"-",paste(Data.R.Cont[i,c("T.Name","ED.Int","ED.Rot","B.Code")],collapse = "-")))
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
  Data.R.Cont[,T.Name:="LER Control"]
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
    
    A<-Data2[c(Control.N,Control.For),is.na(T.Name)]
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
  # This section could be moved to Animals.Out then integrated into MT.Out
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
      Z<-Z[,c("T.Name","A.Level.Name")][,Non.Animal.Codes:=list(Non.Animal.Codes)]
      
      # Deal with NAs
      Z[is.na(A.Level.Name),A.Level.Name:=T.Name]
      
      # Add Animal Practice Names to Y (Reformatted ERA table)
      Y[,A.Lev.C:=Z[match(Y[,C.Descrip],Z[,T.Name]),A.Level.Name]]
      Y[,A.Lev.T:=Z[match(Y[,T.Descrip],Z[,T.Name]),A.Level.Name]]
      Y[,A.Lev.CT:=paste(A.Lev.C,A.Lev.T)]
      # Add Non-animal diet practices to Y (Reformatted ERA table)
      Y[,Non.Animal.Codes.C:=Z[match(Y[,C.Descrip],Z[,T.Name]),Non.Animal.Codes]]
      Y[,Non.Animal.Codes.T:=Z[match(Y[,T.Descrip],Z[,T.Name]),Non.Animal.Codes]]
      
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
  
  