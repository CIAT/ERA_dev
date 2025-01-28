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
  
7} # Setting Debug to T prints comparison table rather than row numbers
