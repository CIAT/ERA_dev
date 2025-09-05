#' Prepare Fertilizer Reduction/Substitution Comparisons
#'
#' Scans fertilizer outputs to detect reductions or substitutions (organic vs inorganic),
#' then builds paired datasets (control vs treatment) for each study and practice comparison.
#'
#' @param Fert.Out data.table. Fertilizer outputs with columns: B.Code, F.Level.Name, F.Codes,
#'   F.NI, F.PI, F.P2O5, F.KI, F.K2O.
#' @param Fert.Method data.table. Fertilizer methods table (levels, amounts, units).
#' @param Other.Out data.table. Other practice codes (e.g. non-fertilizer practices).
#' @param MT.Out data.table. Management treatments table with T.Codes, O.Structure, etc.
#' @param Data.Out data.table. Main experimental outcomes table (T.Name, IN.Level.Name, R.Level.Name).
#' @param min_reduction numeric. Minimum reduction (e.g. 0.05 for 5%) to flag a nutrient reduction.
#'   Defaults to 0.05 for a proportional reduction. Absolute values assumes unit is "kg/ha".
#' @param reduction_type character.If `a` absolute difference is used, if `p` proportional difference is used.
#'   Defaults to `p`. Absolute reduction subsets `Fert.Out` input to `F.I.Unit==kg/ha`.
#'
#' @return A named list of data.tables with elements:
#'   - `Fert.Out`: modified output table
#'   - `Fert.Method`: modified method table
#'   - `Other.Out`: modified other practices
#'   - `MT.Out`: modified management treatments
#'   - `Data.Out`: modified experimental outcomes (only simple treatments)
#'
#' @import data.table
#' @importFrom pbapply pblapply
#' @export
prepare_fert_sub<-function(Fert.Out,Fert.Method,Other.Out,MT.Out,Data.Out,min_reduction=0.05,reduction_type="p",n_start,verbose=F){

  fert<-copy(Fert.Out)[,.(B.Code,F.Level.Name,F.Codes,F.NI,F.PI,F.P2O5,F.KI,F.K2O,F.I.Unit)]
  fert_method<-copy(Fert.Method)
  
  # 1 - identify the fertilizer practices that could be compared from the fertilizer tab
  
  # Check for reducing fertilizers
  fert[,F.P:=F.PI
  ][,F.K:=F.KI
  ][!is.na(F.P2O5) & !is.na(F.PI),F.P:=F.PI+F.P2O5*0.436
  ][!is.na(F.P2O5) & is.na(F.PI),F.P:=F.P2O5*0.436
  ][!is.na(F.K2O) & !is.na(F.KI),F.K:=F.KI+F.K2O*0.83
  ][!is.na(F.K2O) & is.na(F.KI),F.K:=F.K2O*0.83]
  
  fert<-fert[F.Level.Name!="Base"][,N:=.N,by=B.Code][N>1]
  
  max_fert <- fert[, .(
    maxN = max(F.NI, na.rm = TRUE),
    maxP = max(F.P, na.rm = TRUE),
    maxK = max(F.K, na.rm = TRUE)
  ), by = .(B.Code)]
  
  fert<-merge(fert,max_fert,all.x=T)
  fert<-fert[!is.na(maxN)|!is.na(maxP)|!is.na(maxK)]
  
  fert[,organic:=F][grep("b29|b30|b73|b75|b67",F.Codes),organic:=T]
  
  fert[!grepl("b16",F.Codes) & is.na(F.K),F.K:=0]
  fert[!grepl("b23|b17",F.Codes) & is.na(F.NI),F.NI:=0]
  fert[!grepl("b21",F.Codes) & is.na(F.P),F.P:=0]
  
  fert[,n_cont:=F.NI==maxN]
  fert[,p_cont:=F.P==maxP]
  fert[,k_cont:=F.K==maxK]

  if(reduction_type=="p"){  
    fert[is.na(n_cont)|n_cont==F,n_red:=F.NI<(maxN*(1+min_reduction))] 
    fert[is.na(p_cont)|p_cont==F,p_red:=F.P<(maxP*(1+min_reduction))] 
    fert[is.na(k_cont)|k_cont==F,k_red:=F.K<(maxK*(1+min_reduction))] 
  }
  
  if(reduction_type=="a"){
    fert<-fert[F.I.Unit=="kg/ha"]
    fert[is.na(n_cont)|n_cont==F,n_red:=(maxN-F.NI)>min_reduction] 
    fert[is.na(p_cont)|p_cont==F,p_red:=(maxP-F.P)>min_reduction] 
    fert[is.na(k_cont)|k_cont==F,k_red:=(maxK-F.K)>min_reduction] 
  }
  
  fert[,any_red:=any(!is.na(n_red))|any(!is.na(p_red))|any(!is.na(k_red)),by=B.Code]
  fert[,any_org:=any(organic) & any(!organic),by=B.Code]
  
  fert<-fert[any_red==T|any_org==T][,!c("F.PI","F.P2O5","F.K2O","F.KI")]
  
  b_codes<-fert[,unique(B.Code)]
  
  fert_sub_dat<-pblapply(1:length(b_codes),function(b){
    
    b_code<-b_codes[b]
    
    dat<-fert[B.Code==b_code,.(B.Code,F.Level.Name,F.NI,F.P,F.K,organic)]
    
    results<-lapply(1:nrow(dat),FUN=function(i){
      
      focus<-dat[i]
      compare_set<-dat[-i]
      
      # Comparisons must have less or equal fertilizer to control, never more
      compare_set<-compare_set[F.NI<=focus$F.NI & F.P <=focus$F.P & F.K <=focus$F.K]
      
      # Where an element is completely removed this is just the inverse of addition, so exclude
      # This could modified to allow complete removal if organic is added
      compare_set<-compare_set[!(focus$F.NI>0 & F.NI==0)
                               ][!(focus$F.P>0 & F.P==0)
                                 ][!(focus$F.K>0 & F.K==0)]
      
      if(nrow(compare_set)>1){
        compare_set[,c("n_red","p_red","k_red","org_sub"):=F]
        
        if(reduction_type=="p"){  
          compare_set[,n_red:=F.NI<(focus$F.NI *(1+min_reduction))] 
          compare_set[,p_red:=F.P<(focus$F.P *(1+min_reduction))] 
          compare_set[,k_red:=F.K<(focus$F.K *(1+min_reduction))] 
        }
        
        if(reduction_type=="a"){
          compare_set[,n_red:=(focus$F.NI -F.NI)>min_reduction] 
          compare_set[,p_red:=(focus$F.P -F.P)>min_reduction] 
          compare_set[,k_red:=(focus$F.K -F.K)>min_reduction] 
        }
        

        results<-lapply(1:nrow(compare_set),function(j){
           if(verbose){
             cat(b,"/",length(b_codes),b_code,"| i=",i,"| j =",j,"     \n")
          }
          
          pair<-c(focus$F.Level.Name,compare_set$F.Level.Name[j])
          names(pair)<-paste0(pair,"<>",i,"<>",j)
          
          mt_out<-MT.Out[B.Code==b_code & F.Level.Name %in% pair]
          mt_out<-mt_out[!grepl("[.][.][.]",T.Name)]
          
          trts<-mt_out[,T.Name]
          
          dat_out<-Data.Out[B.Code==b_code & T.Name %in% trts & is.na(IN.Level.Name) & is.na(R.Level.Name)]  
          
          # Skip if a structuring other practice is present
          if(mt_out[,any(!is.na(O.Structure))]|nrow(mt_out)<2|nrow(dat_out)<2){
            NULL
          }else{
            
            names(trts)<-paste0(trts,"<>",i,"<>",j)
            
            # Subset Fert.Out Rows
            fo_cont<-Fert.Out[B.Code==b_code & F.Level.Name==pair[1]]
            fo_trt<-Fert.Out[B.Code==b_code & F.Level.Name==pair[2]]
            
            # Subset Fert.Method Rows
            fm_cont<-Fert.Method[B.Code==b_code & F.Level.Name==pair[1]]
            fm_trt<-Fert.Method[B.Code==b_code & F.Level.Name==pair[2]]
            
            # Subset mt_out Rows
            mt_cont<-mt_out[F.Level.Name==pair[1]]
            mt_trt<-mt_out[F.Level.Name==pair[2]]
            
            # Recode practices
            # b23|b17
            if(compare_set[j,n_red]){
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b23|b17","b17.1")]
              fm_trt[grepl("b23|b17",F.Codes),F.Amount:=NA]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b23|b17","b17.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b23|b17","b17.1"),by=.I]
              mt_trt[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b23|b17","b17.1"),by=.I]
              
              fo_cont[,F.Codes:=split_gsub(F.Codes,"b23|b17","")]
              fm_cont[grepl("b23|b17",F.Codes),F.Amount:=NA]
              mt_cont[,F.Codes:=split_gsub(F.Codes,"b23|b17",""),by=.I]
              mt_cont[,T.Codes:=split_gsub(T.Codes,"b23|b17",""),by=.I]
              mt_cont[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b23|b17",""),by=.I]
              
              # Remove codes from Base.Codes
              mt_trt[,Base.Codes:=split_gsub(Base.Codes,"b23|b17",""),by=.I]
              mt_cont[,Base.Codes:=split_gsub(Base.Codes,"b23|b17",""),by=.I]
              
              }
            
            # b21
            if(compare_set[j,p_red]){
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b21","b21.1")]
              fm_trt[grepl("b21",F.Codes),F.Amount:=NA]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b21","b21.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b21","b21.1"),by=.I]
              mt_trt[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b21","b21.1"),by=.I]
              
              fo_cont[,F.Codes:=split_gsub(F.Codes,"b21","")]
              fm_cont[grepl("b21",F.Codes),F.Amount:=NA]
              mt_cont[,F.Codes:=split_gsub(F.Codes,"b21",""),by=.I]
              mt_cont[,T.Codes:=split_gsub(T.Codes,"b21",""),by=.I]
              mt_cont[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b21",""),by=.I]
              
              # Remove codes from Base.Codes
              mt_trt[,Base.Codes:=split_gsub(Base.Codes,"b21",""),by=.I]
              mt_cont[,Base.Codes:=split_gsub(Base.Codes,"b21",""),by=.I]
            }
            
            # b16
            if(compare_set[j,k_red]){
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b16","b16.1",F.Codes)]
              fm_trt[grepl("b16",F.Codes),F.Amount:=NA]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b16","b16.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b16","b16.1"),by=.I]
              mt_trt[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b16","b16.1"),by=.I]
              
              fo_cont[,F.Codes:=split_gsub(F.Codes,"b16","",F.Codes)]
              fm_cont[grepl("b16",F.Codes),F.Amount:=NA]
              mt_cont[,F.Codes:=split_gsub(F.Codes,"b16",""),by=.I]
              mt_cont[,T.Codes:=split_gsub(T.Codes,"b16",""),by=.I]
              mt_cont[,T.Codes.No.Agg:=split_gsub(T.Codes.No.Agg,"b16",""),by=.I]
              
              # Remove codes from Base.Codes
              mt_trt[,Base.Codes:=split_gsub(Base.Codes,"b16",""),by=.I]
              mt_cont[,Base.Codes:=split_gsub(Base.Codes,"16",""),by=.I]
            }
            
            # b29|b30|b73|b75|b67
            if(compare_set[j,org_sub]){
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b29","b29.1",F.Codes)]
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b30","b30.1",F.Codes)]
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b73","b73.1",F.Codes)]
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b75","b75.1",F.Codes)]
              fo_trt[,F.Codes:=split_gsub(F.Codes,"b67","b67.1",F.Codes)]
              
              fm_trt<-fm_trt[!grepl("b29|b30|b73|b75|b67",F.Codes)]
              
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b29","b29.1"),by=.I]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b30","b30.1"),by=.I]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b73","b73.1"),by=.I]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b75","b75.1"),by=.I]
              mt_trt[,F.Codes:=split_gsub(F.Codes,"b67","b67.1"),by=.I]
              
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b29","b29.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b30","b30.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b73","b73.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b75","b75.1"),by=.I]
              mt_trt[,T.Codes:=split_gsub(T.Codes,"b67","b67.1"),by=.I]
              
              mt_trt[,Base.Codes:=split_gsub(Base.Codes,"b29|b30|b73|b75|b67",""),by=.I]
              mt_cont[,Base.Codes:=split_gsub(Base.Codes,"b29|b30|b73|b75|b67",""),by=.I]
            }
            
            # Update F.Level.Names
            fo_comb<-rbind(fo_cont,fo_trt)
            fo_comb$F.Level.Name<-names(pair)
            fo_comb[,F.Codes:=unlist(F.Codes)]
            
            fm_comb<-rbind(fm_cont,fm_trt)
            fm_comb[,F.Level.Name:=names(pair)[match(F.Level.Name,pair)]]
            
            # Add modified names to Other.Out and set allow comparisons to F, if other practices already exist we should probably skip
            other<-Other.Out[B.Code==b_code][0][1:2]
            other$O.Level.Name<-names(pair)[1]
            other$O.Structure<-"No" 
            
            # Duplicate treatments using these practices, rename F.Level.Name add O.Level.Name (which is the same as F.Level.Name)
            # Do not include aggregated treatments - will get too complicated
            mt_out<-rbind(mt_cont,mt_trt)
            mt_out$O.Level.Name<-names(pair)[1]
            mt_out$O.Structure<-"No"
            mt_out[is.na(Structure.Comb),Structure.Comb:=O.Structure]
            mt_out[!is.na(Structure.Comb),Structure.Comb:=paste0(Structure.Comb,":::",O.Structure)]
            mt_out[,F.Level.Name:=names(pair)[match(F.Level.Name,pair)]]
            mt_out[,T.Name:=names(trts)[match(mt_out$T.Name,trts)]]
            
            # Data.Out - do not include intercrops or rotations
            dat_out[,F.Level.Name:=names(pair)[match(F.Level.Name,pair)]]
            dat_out[,T.Name:=names(trts)[match(dat_out$T.Name,trts)]]
            dat_out[,T.Codes:=mt_out[match(dat_out$T.Name,mt_out$T.Name),T.Codes]]
            dat_out[,T.Codes.No.Agg:=mt_out[match(dat_out$T.Name,mt_out$T.Name),T.Codes.No.Agg]]
            dat_out[,F.Codes:=mt_out[match(dat_out$T.Name,mt_out$T.Name),F.Codes]]
            dat_out[,Base.Codes:=mt_out[match(dat_out$T.Name,mt_out$T.Name),Base.Codes]]
            dat_out[,Structure.Comb:=mt_out[match(dat_out$T.Name,mt_out$T.Name),Structure.Comb]]
            dat_out$O.Level.Name<-names(pair)[1]
            dat_out$O.Structure<-"No"
            
            list(Fert.Out=fo_comb,Fert.Method=fm_comb,Other.Out=other,MT.Out=mt_out,Data.Out=dat_out)
          }
        })
        
        levels<-names(results[[1]])
        results<-lapply(levels,function(level){
          rbindlist(lapply(results,"[[",level))
        })
        names(results)<-levels
        
        results
        
      }else{
        NULL
      }
      
    })
    
    results<-results[lapply(results,length)>0]
    
    if(length(results)>0){
      levels<-names(results[[1]])
      results<-lapply(levels,function(level){
        rbindlist(lapply(results,"[[",level))
      })
      names(results)<-levels
      
      results
    }else{
      NULL
    }
    
  })
  
  fert_sub_dat<-fert_sub_dat[unlist(lapply(fert_sub_dat,length))>0]
  
  levels<-names(fert_sub_dat[[1]])
  fert_sub_dat<-lapply(levels,function(level){
    rbindlist(lapply(fert_sub_dat,"[[",level))
  })
  names(fert_sub_dat)<-levels
  
  # Reset N (duplicates are not allowed in N)
  x<-fert_sub_dat$Data.Out
  x[,N_original:=N][,N:=.I][,N:=N+n_start-1]
  fert_sub_dat$Data.Out<-x
  
  return(fert_sub_dat)
  
}

split_gsub<- function(x,pattern, replacement, delim="-", ...) {
  x<-unlist(strsplit(x,delim))
  x<-base::gsub(pattern, replacement,x)
  x<-unique(x[x!="" & !is.na(x)])
  return(paste(sort(x),collapse="-"))
}


