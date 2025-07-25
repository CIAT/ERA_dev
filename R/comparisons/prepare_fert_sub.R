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
prepare_fert_sub<-function(Fert.Out,Fert.Method,Other.Out,MT.Out,Data.Out,min_reduction=0.05,reduction_type="p"){
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
        
        compare_set[F.NI<=focus$F.NI ,n_red:=T
        ][F.P<=focus$F.P,p_red:=T
        ][F.K<=focus$F.K,k_red:=T
        ][focus$organic==F & organic==T,org_sub:=T]
        
        results<-lapply(1:nrow(compare_set),function(j){
          # cat(b,"/",length(b_codes),b_code,"| i=",i,"| j =",j,"     \n")
          
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
            
            # Copy Fert.Out Rows
            fo_cont<-Fert.Out[B.Code==b_code & F.Level.Name==pair[1]]
            fo_trt<-Fert.Out[B.Code==b_code & F.Level.Name==pair[2]]
            
            # Copy Fert.Method Rows
            fm_cont<-Fert.Method[B.Code==b_code & F.Level.Name==pair[1]]
            fm_trt<-Fert.Method[B.Code==b_code & F.Level.Name==pair[2]]
            
            fo_trt[,F.Codes:=strsplit(F.Codes,"-")]
            fo_cont[,F.Codes:=strsplit(F.Codes,"-")]
            
            # Copy MT.Out rows
            mt_out[,F.Codes:=strsplit(F.Codes,"-")]
            mt_out[,T.Codes:=strsplit(T.Codes,"-")]
            
            mt_cont<-mt_out[F.Level.Name==pair[1]]
            mt_trt<-mt_out[F.Level.Name==pair[2]]
            
            # Recode practices
            # b23|b17
            if(compare_set[j,n_red]){
              fo_trt[,F.Codes:=gsub_list("b23|b17","b17.1",F.Codes)]
              fo_cont[,F.Codes:=gsub_list("b23|b17","",F.Codes)]
              
              mt_trt[,F.Codes:=gsub_list("b23|b17","b17.1",F.Codes)]
              mt_trt[,T.Codes:=gsub_list("b23|b17","b17.1",T.Codes)]
              
              mt_cont[,F.Codes:=gsub_list("b23|b17","",F.Codes)]
              mt_cont[,T.Codes:=gsub_list("b23|b17","",T.Codes)]
              
              fm_cont[grepl("b23|b17",F.Codes),F.Amount:=NA]
              fm_trt[grepl("b23|b17",F.Codes),F.Amount:=NA]
            }
            
            # b21
            if(compare_set[j,p_red]){
              fo_trt[,F.Codes:=gsub_list("b21","b21.1",F.Codes)]
              fo_cont[,F.Codes:=gsub_list("b21","",F.Codes)]
              
              mt_trt[,F.Codes:=gsub_list("b21","b21.1",F.Codes)]
              mt_trt[,T.Codes:=gsub_list("b21","b21.1",T.Codes)]
              
              mt_cont[,F.Codes:=gsub_list("b21","",F.Codes)]
              mt_cont[,T.Codes:=gsub_list("b21","",T.Codes)]
              
              fm_cont[grepl("b21",F.Codes),F.Amount:=NA]
              fm_trt[grepl("b21",F.Codes),F.Amount:=NA]
            }
            
            # b16
            if(compare_set[j,k_red]){
              fo_trt[,F.Codes:=gsub_list("b16","b16.1",F.Codes)]
              fo_cont[,F.Codes:=gsub_list("b16","",F.Codes)]
              
              mt_trt[,F.Codes:=gsub_list("b16","b16.1",F.Codes)]
              mt_trt[,T.Codes:=gsub_list("b16","b16.1",T.Codes)]
              
              mt_cont[,F.Codes:=gsub_list("b16","",F.Codes)]
              mt_cont[,T.Codes:=gsub_list("b16","",T.Codes)]
              
              
              fm_cont[grepl("b16",F.Codes),F.Amount:=NA]
              fm_trt[grepl("b16",F.Codes),F.Amount:=NA]
            }
            
            # b29|b30|b73|b75|b67
            if(compare_set[j,org_sub]){
              fo_trt[,F.Codes:=gsub_list("b29","b29.1",F.Codes)]
              fo_trt[,F.Codes:=gsub_list("b30","b30.1",F.Codes)]
              fo_trt[,F.Codes:=gsub_list("b73","b73.1",F.Codes)]
              fo_trt[,F.Codes:=gsub_list("b75","b75.1",F.Codes)]
              fo_trt[,F.Codes:=gsub_list("b67","b67.1",F.Codes)]
              
              fm_trt<-fm_trt[!grepl("b29|b30|b73|b75|b67",F.Codes)]
              
              mt_trt[,F.Codes:=gsub_list("b29","b29.1",F.Codes)]
              mt_trt[,F.Codes:=gsub_list("b30","b30.1",F.Codes)]
              mt_trt[,F.Codes:=gsub_list("b73","b73.1",F.Codes)]
              mt_trt[,F.Codes:=gsub_list("b75","b75.1",F.Codes)]
              mt_trt[,F.Codes:=gsub_list("b67","b67.1",F.Codes)]
              
              mt_trt[,T.Codes:=gsub_list("b29","b29.1",T.Codes)]
              mt_trt[,T.Codes:=gsub_list("b30","b30.1",T.Codes)]
              mt_trt[,T.Codes:=gsub_list("b73","b73.1",T.Codes)]
              mt_trt[,T.Codes:=gsub_list("b75","b75.1",T.Codes)]
              mt_trt[,T.Codes:=gsub_list("b67","b67.1",T.Codes)]
            }
            
            # Update F.Level.Names
            fo_comb<-rbind(fo_cont,fo_trt)
            fo_comb$F.Level.Name<-names(pair)
            fo_comb[,F.Codes:=code_comb(F.Codes),by=.I
            ][,F.Codes:=unlist(F.Codes)]
            
            
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
            mt_out[,T.Name:=names(trts)]
            
            mt_out[,F.Codes:=code_comb(F.Codes),by=.I][,F.Codes:=unlist(F.Codes)]
            mt_out[,T.Codes:=code_comb(T.Codes),by=.I][,T.Codes:=unlist(T.Codes)]
            
            # Data.Out - do not include intercrops or rotations
            dat_out[,F.Level.Name:=names(pair)[match(F.Level.Name,pair)]]
            dat_out[,T.Name:=names(trts)[match(dat_out$T.Name,trts)]]
            dat_out[,T.Codes:=mt_out[match(dat_out$T.Name,mt_out$T.Name),T.Codes]]
            dat_out[,F.Codes:=mt_out[match(dat_out$T.Name,mt_out$T.Name),F.Codes]]
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
  x[,N_original:=N][,N:=.I]
  fert_sub_dat$Data.Out<-x
  
  return(fert_sub_dat)
  
}

#' gsub_list: gsub on Atomic or List-Columns
#'
#' A drop-in wrapper for base::gsub() that also recurses into list-columns of character vectors.
#'
#' @param pattern character. Regular expression to search for (see base::gsub).
#' @param replacement character. Replacement string (see base::gsub).
#' @param x character vector or list of character vectors. The input values.
#' @param ... additional arguments passed to base::gsub (e.g., fixed=TRUE, ignore.case=TRUE).
#'
#' @return If `x` is atomic character, returns the same as base::gsub.
#' If `x` is a list, returns a list of the same structure with gsub applied to every character element.
#'
#' @examples
#' # atomic character
#' gsub_list("a", "A", c("apple", "banana"))
#'
#' # list-column of codes
#' L <- list(c("b17","b29"), c("b16","b21"))
#' gsub_list("b2[0-9]", "b2x", L)
#'
#' @export
gsub_list <- function(pattern, replacement, x, ...) {
  if (is.list(x)) {
    # Recurse: apply to each character element in the list
    return(
      rapply(
        x,
        function(el) base::gsub(pattern, replacement, el, ...),
        classes = "character",
        how     = "replace"
      )
    )
  }
  # Fallback to standard gsub for atomic character
  base::gsub(pattern, replacement, x, ...)
}

#' code_comb: Collapse a List of Codes Into a Sorted String
#'
#' Takes a list or character vector of codes, removes empty entries,
#' sorts them, and pastes together with a dash delimiter.
#'
#' @param codes character vector or list of character vectors.
#'
#' @return single character string: sorted, non-empty codes joined by "-".
#'
#' @examples
#' code_comb(c("b17","","b16.1"))        # "b16.1-b17"
#' code_comb(list(c("b21","b17"), c("b29"))) # "b17-b21-b29"
#'
#' @export
code_comb <- function(codes) {
  # Flatten into a simple character vector
  vec <- unlist(codes, use.names = FALSE)
  # Drop empty strings (but keep NA if present)
  vec <- unique(vec[vec != "" | is.na(vec)])
  # Sort and collapse
  paste(sort(vec), collapse = "-")
}


