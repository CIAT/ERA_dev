#' create_fert_name: Generate a Summary String for Fertilizer Levels
#'
#' This function processes fertilizer data for a specific treatment and site,
#' combining nutrient information and fertilizer application details into a
#' formatted string. It handles cases where nutrient data or fertilizer levels
#' are missing, aggregates amounts by type, and applies rounding adjustments
#' to ensure consistent representation of fertilizer amounts.
#'
#' ## Key Features:
#' 1. Extracts fertilizer levels and nutrient information from the dataset.
#' 2. Aggregates fertilizer amounts by type.
#' 3. Adjusts for rounding issues in fertilizer amounts.
#' 4. Combines fertilizer and nutrient information into a single human-readable string.
#' 5. Handles cases where no fertilizer data is available, labeling them as "No Fert Control."
#'
#' ## Parameters:
#' @param X The name of the fertilizer level to process (e.g., treatment or control group).
#' @param Y The site or block code to filter the fertilizer data (`B.Code`).
#' @param F.NO Numeric; nitrogen content (organic).
#' @param F.PO Numeric; phosphorus content (organic).
#' @param F.KO Numeric; potassium content (organic).
#' @param F.NI Numeric; nitrogen content (inorganic).
#' @param F.PI Numeric; phosphorus content (inorganic).
#' @param F.P2O5 Numeric; phosphorus pentoxide content (inorganic).
#' @param F.KI Numeric; potassium content (inorganic, expressed as potassium ion).
#' @param F.K2O Numeric; potassium oxide content (inorganic).
#' @param Fert.Method A data table containing information about fertilizer types, amounts,
#' and metadata such as `Time`, `Site.ID`, `F.Level.Name`, and `B.Code`.
#'
#' ## Returns:
#' A string summarizing:
#' 1. Nutrient content (if available), formatted as `F.NO-10 F.KO-5`, etc.
#' 2. Fertilizer type and amount, formatted as `Type1 100|Type2 200`.
#' The two components are separated by `||`. If no data is available, the function
#' returns `"No Fert Control"`.
#'
#' ## Example:
#' Suppose the following inputs:
#' - `X = "Control"`, `Y = "SiteA"`.
#' - Nutrient data: `F.NO = 10`, `F.KO = 5`, `F.NI = 20`, `F.PI = 15`, others are `NA`.
#' - `Fert.Method` contains fertilizer types and amounts.
#'
#' Calling:
#' ```R
#' ReName.Fun(X = "Control", Y = "SiteA", F.NO = 10, F.PO = NA, F.KO = 5, 
#'            F.NI = 20, F.PI = 15, F.P2O5 = NA, F.KI = NA, F.K2O = 12, 
#'            Fert.Method = FertilizerData)
#' ```
#' Returns:
#' `"F.NO-10 F.KO-5 F.NI-20 F.PI-15||Type1 100|Type2 200"`
#'
#' ## Notes:
#' - Ensure `Fert.Method` is preprocessed and contains relevant fields for the function to work.
#' - Handles missing values gracefully, but logs meaningful fertilizer information where available.
#'
create_fert_name<-function(X,Y,F.NO,F.PO,F.KO,F.NI,F.PI,F.P2O5,F.KI,F.K2O,Fert.Method){
  NPK<-data.table(F.NO=F.NO,F.PO=F.NO,F.KO=F.KO,F.NI=F.NI,F.PI=F.PI,F.P2O5=F.P2O5,F.KI=F.KI,F.K2O=F.K2O)
  N<-colnames(NPK)[which(!apply(NPK,2,is.na))]
  
  F.Level<-Fert.Method[F.Level.Name==X & B.Code ==Y,list(F.Type,F.Amount)]
  
  F.Level<-unique(rbind(F.Level[is.na(F.Amount)],F.Level[!is.na(F.Amount),list(F.Amount=sum(F.Amount)),by=F.Type]))
  
  # Deal with rounding issues 
  # This code is supposed to deal with import issues in excel that create values like 1.9999999999999999, the solution however returns NaN when the rounded log amount is 0
  F.Level[,F.Amount2:=round(F.Amount*10*round(log(F.Amount,base=10)),0)/(10*round(log(F.Amount,base=10))),by=F.Type]
  F.Level[is.na(F.Amount2)|is.nan(F.Amount2),F.Amount2:=F.Amount]
  F.Level<-F.Level[,paste(F.Type,F.Amount2)]
  
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

#' process_string: Extract Common and Unique Elements from a Delimited String
#'
#' This helper function processes a delimited string, extracting common elements shared across 
#' subparts and isolating unique elements. It is particularly useful for analyzing strings 
#' with structured data that share patterns or attributes across multiple sections.
#'
#' ## Key Features:
#' 1. Splits the input string into components based on specified delimiters.
#' 2. Identifies common elements shared across all components.
#' 3. Separates and reassembles unique elements for each component.
#' 4. Returns a two-part output: shared/common elements and unique/different elements.
#'
#' @param input_string Character; the string to process. Expected to contain delimited subparts.
#' @param between_delim Character; delimiter separating sections of the string. Default is `".."`.
#' @param within_delim Character; delimiter separating elements within each section. Default is `"|"`.
#' @return A named character vector with two components:
#'   - `shared`: A string of elements common to all subparts, joined by `between_delim`.
#'   - `different`: A string of unique elements for each subpart, reassembled and joined by `between_delim`.
#' @examples
#' # Example Input
#' input <- "F.Type|F.Amount..F.Type|F.Amount|F.Date..F.Type|F.Date"
#' 
#' # Process the string
#' result <- process_string(input)
#' 
#' print(result)
#' # Output:
#' # shared   => "F.Type"
#' # different => "F.Amount..F.Amount|F.Date..F.Date"
#' 
#' @export
process_string <- function(input_string, between_delim = "..", within_delim = "|") {
  # Step 1: Split the string on `between_delim`
  split_string <- strsplit(input_string, between_delim, fixed = TRUE)[[1]]
  
  # Step 2: Split the first element on `within_delim`
  first_element_split <- strsplit(split_string[1], within_delim, fixed = TRUE)[[1]]
  
  # Step 3: Find common strings across all elements
  common_strings <- Reduce(intersect, lapply(split_string, function(x) strsplit(x, within_delim, fixed = TRUE)[[1]]))
  
  # Step 4: Remove common strings from each element
  removed_shared <- lapply(split_string, function(x) {
    setdiff(strsplit(x, within_delim, fixed = TRUE)[[1]], common_strings)
  })
  
  # Step 5: Rejoin each element with `within_delim`
  rejoined_elements <- sapply(removed_shared, function(x) paste(x, collapse = within_delim))
  
  # Replace empty strings with "NA"
  rejoined_elements[rejoined_elements == ""] <- "NA"
  
  # Step 6: Rejoin all elements with `between_delim`
  final_result <- paste(sort(rejoined_elements), collapse = between_delim)
  
  # Sort and join common strings
  common_strings <- paste(sort(unique(common_strings[!is.na(common_strings) & common_strings != ""])), 
                          collapse = between_delim)
  
  # Handle case where no common strings exist
  if (length(common_strings) == 0) {
    common_strings <- NA
  }
  
  # Return both shared and different elements
  return(c(shared = common_strings, different = final_result))
}
#' Infer Fertilizer Amounts
#'
#' This function calculates fertilizer amounts for various fertilizers based on provided master codes,
#' fertilizer methods, and fertilizer outputs. It handles remapping fertilizer names, calculating
#' unknown values, and performing checks on elemental completeness.
#'
#' @param fert_codes A `data.table` from the ERA master vocabularly containing fertilizer information.
#' @param Fert.Method A `data.table` containing fertilizer method details.
#' @param Fert.Out A `data.table` with fertilizer output information. 
#' @param remappings A `data.table` with columns `from` and `to` used to remap fertilizer names in the `F.Type` field. This is useful
#' for instance where a name is vague and we are making an assumption as to the exact nature of the fertilizer (e.g., we may
#' remap `Superphosphate` to `Single Superphosphate`).
#' @param max_diff A `numeric` value between 0-1 used to flag records for inclusion in the output `errors` table. For example,
#' a value of 0.2 means values with more than 20% difference between reported vs calculated total elemental N,P, or K are
#' returned in the `errors` output table.  
#' @return A list with two elements:
#' \item{data}{A `data.table` containing inferred fertilizer amounts and detailed calculations.}
#' \item{errors}{A `data.table` containing records with errors or discrepancies for further inspection. Records are indentified where
#' calculated NPK is }
#'
#' @details
#' This function performs the following operations:
#' - Extracts inorganic fertilizers and processes their NPK ratios.
#' - Maps fertilizer names to standardized formats using a predefined mapping table.
#' - Merges fertilizer data with methods and outputs to calculate missing values for fertilizer amounts.
#' - Converts P2O5 to P and K2O to K where applicable.
#' - Checks for completeness of NPK sums and flags significant discrepancies for review.
#'
#' @examples
#' # Example usage
#' fert_codes <- data.table(fert = ..., F.Category_New = ...)
#' Fert.Method <- data.table(F.Name = ..., F.Codes = ..., F.Category = ...)
#' Fert.Out <- data.table(F.NI = ..., F.PI = ..., F.KI = ...)
#' remappings<-data.table(from="Superphosphate",to="Single Superphosphate")
#' result <- infer_fert_amounts(master_codes, Fert.Method, Fert.Out,remappings,max_diff=0.2)
#'
#' # Access the results
#' data <- result$data
#' errors <- result$errors
#'
#' @export
#' 
infer_fert_amounts<-function(fert_codes,Fert.Method,Fert.Out,remappings=NULL,max_diff=0.2){

# Extract only inorganic fertilizers and their NPK content
inorganic_ferts<-fert_codes[F.Category_New=="Inorganic",.(F.Type_New,F.N,F.P,F.K)]
setnames(inorganic_ferts,"F.Type_New","F.Type")

# Function to process fertilizer ratios (e.g., "10-20-30") into numeric values
process_fert_ratios<-function(x){
  x<-strsplit(x,"-") # Split the string by hyphen
  x<-unlist(lapply(x,FUN=function(x){mean(as.numeric(x),na.rm=T)})) # Calculate the mean of numeric values
  return(x)
}

# Apply `process_fert_ratios` to NPK columns in inorganic fertilizers
cols_to_process <- c("F.N", "F.P", "F.K") # Columns to apply the function to
inorganic_ferts[, (cols_to_process) := lapply(.SD, process_fert_ratios), .SDcols = cols_to_process]

# Copy Fert.Method data and process it for presence of N, P, and K
fert_subset<-copy(Fert.Method)
setnames(fert_subset,"F.Name","F.Level.Name",skip_absent = T)

# Identify treatments that include phosphorus (P), nitrogen (N), or potassium (K)
fert_subset[,p_present:=F][grepl("b21",F.Codes),p_present:=T]
fert_subset[,n_present:=F][grepl("b17|b23",F.Codes),n_present:=T]
fert_subset[,k_present:=F][grepl("b16",F.Codes),k_present:=T]

# Count unique fertilizer types for N, P, and K presence
fert_subset[p_present==T,p_no:=length(unique(F.Type)),by=.(B.Code,F.Level.Name)]
fert_subset[n_present==T,n_no:=length(unique(F.Type)),by=.(B.Code,F.Level.Name)]
fert_subset[k_present==T,k_no:=length(unique(F.Type)),by=.(B.Code,F.Level.Name)]

# Filter only inorganic fertilizers and select relevant columns
fert_subset<-fert_subset[F.Category=="Inorganic",.(B.Code,F.Level.Name,F.Type,F.NPK,F.Amount,F.Unit,p_no,n_no,k_no)]

# Merge Fert.Out data to add N, P, and K total amounts for each treatment
fert_subset<-merge(fert_subset,Fert.Out[,.(B.Code,F.Level.Name,F.NI,F.PI,F.P2O5,F.KI,F.K2O,F.I.Unit)],by=c("B.Code","F.Level.Name"),all.x=T,sort=F)

# Convert P2O5 to P and K2O to K
fert_subset[is.na(F.PI)|(F.PI==0 & !is.na(F.P2O5)),F.PI:=F.P2O5*0.436]
fert_subset[is.na(F.KI)|(F.KI==0 & !is.na(F.K2O)),F.KI:=F.K2O*0.83]

# Optional: Remap fertilizer names based on the provided remappings table
fert_subset[,F.Type2:=F.Type]
if(!is.null(remappings)){
  for(i in 1:nrow(fert_remap)){
    fert_subset[F.Type2==fert_remap$from[i],F.Type2:=fert_remap$to[i]]
  }
}

# Merge with inorganic_ferts to add elemental compositions for fertilizers
fert_subset<-merge(fert_subset,inorganic_ferts,by.x="F.Type2",by.y="F.Type",all.x=T,sort=F)
fert_subset[,F.N:=F.N/100][,F.P:=F.P/100][,F.K:=F.K/100]

# Extract fertilizer composition directly from NPK strings
fert_subset[!is.na(F.NPK),F.P:=0.436*as.numeric(tstrsplit(F.NPK[1],"-",keep=2)[[1]])/100,by=F.NPK
][!is.na(F.NPK),F.N:=as.numeric(tstrsplit(F.NPK[1],"-",keep=1)[[1]])/100,by=F.NPK
][!is.na(F.NPK),F.K:=0.83*as.numeric(tstrsplit(F.NPK[1],"-",keep=3)[[1]])/100,by=F.NPK]

# Save original amount and unit
fert_subset[,n_row:=.N,by=.(B.Code,F.Level.Name,F.Type)
            ][,F.Amount_raw:=F.Amount
              ][,F.Unit_raw:=F.Unit]

# Infer missing amounts when the total elemental content is known
fert_subset[p_no==1 & is.na(F.Unit) & is.na(F.Amount),F.Amount:=round(F.PI/F.P/n_row,1)]
fert_subset[n_no==1 & is.na(F.Unit) & is.na(F.Amount),F.Amount:=round(F.NI/F.N/n_row,1)]
fert_subset[k_no==1 & is.na(F.Unit) & is.na(F.Amount),F.Amount:=round(F.KI/F.K/n_row,1)]

# Handle percentages for fertilizer amounts
fert_subset[grep("%",F.Unit,fixed=T),F.Amount_perc:=F.Amount/100]
fert_subset[grep("%",F.Unit,fixed=T),F.Amount:=NA]

fert_subset[p_no==1 & grepl("%",F.Unit),F.Amount:=round(F.PI/F.P*F.Amount_perc,1)]
fert_subset[n_no==1 & grepl("%",F.Unit),F.Amount:=round(F.NI/F.N*F.Amount_perc,1)]
fert_subset[k_no==1 & grepl("%",F.Unit),F.Amount:=round(F.KI/F.K*F.Amount_perc,1)]

# Add amounts for unspecified N, P, K fertilizers (developer notes included)
# Dev Note: Ensure logic to handle unspecified NPK is implemented when NPK totals are given but missing in the table
# Dev Note: it may also be possible to included unspecified N,P, and K after n_no==1 records are dealt with where n_no==2
fert_subset[n_no==1 & is.na(F.Amount) & F.Type=="N (Unspecified)",F.Amount:=F.NI/n_row]
fert_subset[n_po==1 & is.na(F.Amount) & F.Type=="P (Unspecified)",F.Amount:=F.PI/n_row]
fert_subset[n_ko==1 & is.na(F.Amount) & F.Type=="K (Unspecified)",F.Amount:=F.KI/n_row]

# Reality check for completeness of elemental sums
fert_subset[is.nan(F.N) & !is.na(n_no),N_sum_complete:=F
            ][is.nan(F.P) & !is.na(p_no),P_sum_complete:=F
              ][is.nan(F.K) & !is.na(k_no),K_sum_complete:=F]

# Ensure completeness checks for N, P, and K sums
fert_subset[,N_sum_complete:=!any(N_sum_complete==F,na.rm = T),by=.(B.Code,F.Level.Name)]
fert_subset[,P_sum_complete:=!any(P_sum_complete==F,na.rm = T),by=.(B.Code,F.Level.Name)]
fert_subset[,K_sum_complete:=!any(K_sum_complete==F,na.rm = T),by=.(B.Code,F.Level.Name)]

# Calculate amounts for P, N, and K based on the fertilizer amounts and their fractions
fert_subset[,P:=F.Amount*F.P
            ][,N:=F.Amount*F.N
              ][,K:=F.Amount*F.K]

# Compute total sums of P, N, and K by treatment (grouped by B.Code, F.Level.Name, and nutrient inputs)
fert_subset[P_sum_complete==T,P_sum:=sum(P,na.rm=T),by=.(B.Code,F.Level.Name,F.PI,F.NI,F.KI)
            ][N_sum_complete==T,N_sum:=sum(N,na.rm=T),by=.(B.Code,F.Level.Name,F.PI,F.NI,F.KI)
              ][K_sum_complete==T,K_sum:=sum(K,na.rm=T),by=.(B.Code,F.Level.Name,F.PI,F.NI,F.KI)]

# Calculate discrepancies (differences) between expected and observed sums for P, N, and K
fert_subset[,P_diff:=round(abs(1-P_sum/F.PI),2)
            ][,N_diff:=round(abs(1-N_sum/F.NI),2)
              ][,K_diff:=round(abs(1-K_sum/F.KI),2)]

# Infer amounts for missing fertilizers when discrepancies are within acceptable bounds
fert_subset[is.na(F.Amount) & n_no==2 & (p_no!=2|is.na(p_no)) & (k_no!=2|is.na(k_no)) & N_diff<0.95 & N_diff!=0,F.NI_2:=F.NI*N_diff]
fert_subset[is.na(F.Amount) & (n_no!=2|is.na(n_no)) & p_no==2 & (k_no!=2|is.na(k_no))  & P_diff<0.95 & P_diff!=0,F.PI_2:=F.PI*P_diff]
fert_subset[is.na(F.Amount) & (n_no!=2|is.na(n_no)) & (p_no!=2|is.na(p_no)) & k_no==2 & K_diff<0.95 & K_diff!=0,F.KI_2:=F.KI*K_diff]

# Recalculate fertilizer amounts based on inferred nutrient inputs
fert_subset[!is.na(F.PI_2) & is.na(F.Unit),F.Amount:=round(F.PI_2/F.P/n_row,1)]
fert_subset[!is.na(F.NI_2) & is.na(F.Unit),F.Amount:=round(F.NI_2/F.N/n_row,1)]
fert_subset[!is.na(F.KI_2) & is.na(F.Unit),F.Amount:=round(F.KI_2/F.K/n_row,1)]

# Handle percentage-based units for recalculated amounts
fert_subset[!is.na(F.PI_2) & grepl("%",F.Unit),F.Amount:=round(F.PI_2/F.P*F.Amount_perc,1)]
fert_subset[!is.na(F.NI_2) & grepl("%",F.Unit),F.Amount:=round(F.NI_2/F.N*F.Amount_perc,1)]
fert_subset[!is.na(F.KI_2) & grepl("%",F.Unit),F.Amount:=round(F.KI_2/F.K*F.Amount_perc,1)]

# Final reality checks for completeness and totals
fert_subset[,P:=F.Amount*F.P][,N:=F.Amount*F.N][,K:=F.Amount*F.K]
fert_subset[,P_sum:=sum(P,na.rm=T),by=.(B.Code,F.Level.Name)
            ][,N_sum:=sum(N,na.rm=T),by=.(B.Code,F.Level.Name)
              ][,K_sum:=sum(K,na.rm=T),by=.(B.Code,F.Level.Name)]

fert_subset[P_sum_complete==T,P_diff:=round(abs(1-P_sum/F.PI),2)
            ][N_sum_complete==T,N_diff:=round(abs(1-N_sum/F.NI),2)
              ][K_sum_complete==T,K_diff:=round(abs(1-K_sum/F.KI),2)]

# Identify rows with discrepancies exceeding the threshold
check<-fert_subset[(P_diff>max_diff & P_sum!=0)|(N_diff>max_diff & N_diff!=0)|(K_diff>max_diff & K_diff!=0)]

# Developer notes section for debugging specific discrepancies
if(F){
check[,unique(B.Code)]

i<-50 # Example index for manual investigation

# Filter rows for the selected code and level name
(check1<-check[B.Code==check[,unique(B.Code)][i]])
(check2<-check1[F.Level.Name==check1$F.Level.Name[1]])

# Inspect the subset of fertilizer data for identified discrepancies
fert_subset[B.Code==check2$B.Code[1] & F.Level.Name==check2$F.Level.Name[1]]
fert_subset[B.Code==check2$B.Code[1] & F.Level.Name==check2$F.Level.Name[1],
            .(B.Code,F.Level.Name,F.Type,F.Type2,F.Amount,F.Amount_raw,F.Unit,F.NPK,F.N,F.P,F.K,
              F.NI,N_sum,
              F.PI,F.P2O5,P_sum,
              F.KI,F.K2O,K_sum)]
}

errors<-fert_subset[B.Code==check2$B.Code[1] & F.Level.Name==check2$F.Level.Name[1],
                    .(B.Code,F.Level.Name,F.Type,F.Type2,F.Amount,F.Amount_raw,F.Unit,F.NPK,F.N,F.P,F.K,
                      F.NI,N_sum,
                      F.PI,F.P2O5,P_sum,
                      F.KI,F.K2O,K_sum)]

return(list(data=fert_subset,errors=errors))

}