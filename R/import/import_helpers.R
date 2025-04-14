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
#' Note units in Fert.Method (`F.Unit` and `F.I.Unit`) should be `% of Total`,`kg/ha`, or `NA`. Rows
#' with different values are removed.
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
inorganic_ferts<-unique(fert_codes[F.Category_New=="Inorganic",.(F.Type_New,F.N,F.P,F.K)])
setnames(inorganic_ferts,"F.Type_New","F.Type")

# Function to process fertilizer ratios (e.g., "10-20-30") into numeric values
process_fert_ratios<-function(x){
  x<-strsplit(x,"-") # Split the string by hyphen
  x<-suppressWarnings(unlist(lapply(x,FUN=function(x){mean(as.numeric(x),na.rm=T)}))) # Calculate the mean of numeric values
  return(x)
}

# Apply `process_fert_ratios` to NPK columns in inorganic fertilizers
cols_to_process <- c("F.N", "F.P", "F.K") # Columns to apply the function to
inorganic_ferts[, (cols_to_process) := lapply(.SD, process_fert_ratios), .SDcols = cols_to_process]

# Copy Fert.Method data and process it for presence of N, P, and K
fert_subset<-copy(Fert.Method)

# If F.NP2O5K2O colum present replace F.NPK with this
if("F.NP2O5K2O" %in% colnames(Fert.Method)){
  fert_subset[is.na(F.NPK),F.NPK:=F.NP2O5K2O]
}

setnames(fert_subset,"F.Name","F.Level.Name",skip_absent = T)

# Remove units when value is NA
fert_subset[is.na(F.Amount),F.Unit:=NA]

# Identify treatments that include phosphorus (P), nitrogen (N), or potassium (K)
fert_subset[,p_present:=F][grepl("b21",F.Codes),p_present:=T]
fert_subset[,n_present:=F][grepl("b17|b23",F.Codes),n_present:=T]
fert_subset[,k_present:=F][grepl("b16",F.Codes),k_present:=T]

# Count unique fertilizer types for N, P, and K presence
fert_subset[,p_no:=length(unique(F.Type[p_present==T])),by=.(B.Code,F.Level.Name)]
fert_subset[,n_no:=length(unique(F.Type[n_present==T])),by=.(B.Code,F.Level.Name)]
fert_subset[,k_no:=length(unique(F.Type[k_present==T])),by=.(B.Code,F.Level.Name)]

# Filter only inorganic fertilizers and select relevant columns
fert_subset<-fert_subset[F.Category=="Inorganic",.(index,B.Code,F.Level.Name,F.Type,F.NPK,F.Amount,F.Unit,p_no,n_no,k_no)]

# Merge Fert.Out data to add N, P, and K total amounts for each treatment
n_rows<-nrow(fert_subset)
fert_subset<-merge(fert_subset,
                   Fert.Out[,.(B.Code,F.Level.Name,F.NI,F.PI,F.P2O5,F.KI,F.K2O,F.I.Unit)],by=c("B.Code","F.Level.Name"),
                   all.x=T,
                   sort=F)

if(nrow(fert_subset)!=n_rows){
  stop(paste("Merging Fert.Method and Fert.Out results in changed table length (rows)."))
}

# Remove units that are not kg/ha
unit_issues<-list(F.I.Unit_issue=fert_subset[F.I.Unit!="kg/ha" & !is.na(F.I.Unit),unique(F.I.Unit)],
                  F.Unit_issue=fert_subset[!is.na(F.Unit) & !grepl("%|kg/ha",F.Unit),unique(F.Unit)])

if(any(sapply(unit_issues,length)>0)){
  warning(paste("Rows with these units will be excluded from the analysis: \n F.I.Unit = ",paste0(unit_issues$F.I.Unit_issue,collapse=","),
                "\n F.Unit = ",paste(unit_issues$F.Unit_issue,collapse = ",")))
}

fert_subset<-fert_subset[F.I.Unit=="kg/ha"|is.na(F.I.Unit)][is.na(F.Unit)|grepl("%|kg/ha",F.Unit)]

# Convert P2O5 to P and K2O to K
fert_subset[is.na(F.PI)|(F.PI==0 & !is.na(F.P2O5)),F.PI:=F.P2O5*0.436]
fert_subset[is.na(F.KI)|(F.KI==0 & !is.na(F.K2O)),F.KI:=F.K2O*0.83]

# Optional: Remap fertilizer names based on the provided remappings table
fert_subset[,F.Type2:=F.Type]
if(!is.null(remappings)){
  for(i in 1:nrow(remappings)){
    fert_subset[F.Type2==remappings$from[i],F.Type2:=remappings$to[i]]
  }
}

# Merge with inorganic_ferts to add elemental compositions for fertilizers
n_rows<-nrow(fert_subset)
fert_subset<-merge(fert_subset,inorganic_ferts,by.x="F.Type2",by.y="F.Type",all.x=T,sort=F)

if(nrow(fert_subset)!=n_rows){
  stop(paste("Merging Fert.Method and master_codes$fert results in changed table length (rows)."))
}

fert_subset[,F.N:=F.N/100][,F.P:=F.P/100][,F.K:=F.K/100]

# Extract fertilizer composition directly from NPK strings
fert_subset[!is.na(F.NPK),F.P:=0.436*as.numeric(tstrsplit(F.NPK[1],"-",keep=2)[[1]])/100,by=F.NPK
][!is.na(F.NPK),F.N:=as.numeric(tstrsplit(F.NPK[1],"-",keep=1)[[1]])/100,by=F.NPK
][!is.na(F.NPK),F.K:=0.83*as.numeric(tstrsplit(F.NPK[1],"-",keep=3)[[1]])/100,by=F.NPK]

# Save original amount and unit
fert_subset[,n_row:=.N,by=.(B.Code,F.Level.Name,F.Type)
            ][,F.Amount_raw:=F.Amount
              ][,F.Unit_raw:=F.Unit]

# Add Unspecified F.Type if elemental NPK is present with no corresponding entry in the F.Method table
n_add<-unique(fert_subset[n_no==0 & !(is.na(F.NI)|F.NI=="")][,`:=`(n_no=1,n_row=1,F.Type2="N (Unspecified)",index=NA,F.Type="N (Unspecified)",F.Amount=F.NI,F.N=1,F.P=NA,F.K=NA,F.Amount_raw=NA,F.Unit_raw=NA,F.Unit="kg/ha")])
p_add<-unique(fert_subset[p_no==0 & !(is.na(F.PI)|F.PI=="")][,`:=`(p_no=1,n_row=1,F.Type2="P (Unspecified)",index=NA,F.Type="P (Unspecified)",F.Amount=F.PI,F.N=NA,F.P=1,F.K=NA,F.Amount_raw=NA,F.Unit_raw=NA,F.Unit="kg/ha")])
k_add<-unique(fert_subset[k_no==0 & !(is.na(F.KI)|F.KI=="")][,`:=`(k_no=1,n_row=1,F.Type2="K (Unspecified)",index=NA,F.Type="K (Unspecified)",F.Amount=F.KI,F.N=NA,F.P=NA,F.K=1,F.Amount_raw=NA,F.Unit_raw=NA,F.Unit="kg/ha")])

add1<-rbind(n_add,p_add,k_add)

fert_subset<-rbind(fert_subset,add1)

# Update n_no.p_no,k_no now a row has been introduced
fert_subset[,n_no:=max(n_no,na.rm = T),by=.(B.Code,F.Level.Name)
            ][,p_no:=max(p_no,na.rm = T),by=.(B.Code,F.Level.Name)
              ][,k_no:=max(k_no,na.rm = T),by=.(B.Code,F.Level.Name)]

# Add in completely missing NPK (i.e. nothing in the Fert.Methods tab for a F.Level.Name)
fert_out<-Fert.Out[F.I.Unit=="kg/ha"]
fert_out[is.na(F.PI)|(F.PI==0 & !is.na(F.P2O5)),F.PI:=F.P2O5*0.436]
fert_out[is.na(F.KI)|(F.KI==0 & !is.na(F.K2O)),F.KI:=F.K2O*0.83]

fert_missing<-fert_out[!(is.na(F.NI) & is.na(F.PI) & is.na(F.KI)) & !F.Level.Name %in% fert_subset$F.Level.Name,.(B.Code,F.Level.Name,F.NI,F.PI,F.P2O5,F.KI,F.K2O,F.I.Unit)]
fm_n<-fert_missing[F.NI!=0 & !is.na(F.NI)]

n_add2<-data.table(F.Type2="N (Unspecified)",
                   B.Code=fm_n$B.Code,
                   F.Level.Name=fm_n$F.Level.Name,
                   index=NA,
                   F.Type="N (Unspecified)",
                   F.NPK=NA,
                   F.Amount=fm_n$F.NI,
                   F.Unit="kg/ha",
                   p_no=0,
                   n_no=1,
                   k_no=0,
                   F.NI=fm_n$F.NI,
                   F.PI=fm_n$F.PI,
                   F.P2O5=fm_n$F.P2O5,
                   F.KI=fm_n$F.KI,
                   F.K2O=fm_n$F.K2O,
                   F.I.Unit=fm_n$F.I.Unit,
                   F.N=1,
                   F.P=0,
                   F.K=0,
                   n_row=1,
                   F.Amount_raw=NA,
                   F.Unit_raw=NA)

p_add2<-data.table(F.Type2="P (Unspecified)",
                   B.Code=fm_n$B.Code,
                   F.Level.Name=fm_n$F.Level.Name,
                   index=NA,
                   F.Type="P (Unspecified)",
                   F.NPK=NA,
                   F.Amount=fm_n$F.PI,
                   F.Unit="kg/ha",
                   p_no=1,
                   n_no=0,
                   k_no=0,
                   F.NI=fm_n$F.NI,
                   F.PI=fm_n$F.PI,
                   F.P2O5=fm_n$F.P2O5,
                   F.KI=fm_n$F.KI,
                   F.K2O=fm_n$F.K2O,
                   F.I.Unit=fm_n$F.I.Unit,
                   F.N=0,
                   F.P=1,
                   F.K=0,
                   n_row=1,
                   F.Amount_raw=NA,
                   F.Unit_raw=NA)

k_add2<-data.table(F.Type2="K (Unspecified)",
                   B.Code=fm_n$B.Code,
                   F.Level.Name=fm_n$F.Level.Name,
                   index=NA,
                   F.Type="K (Unspecified)",
                   F.NPK=NA,
                   F.Amount=fm_n$F.KI,
                   F.Unit="kg/ha",
                   p_no=0,
                   n_no=0,
                   k_no=1,
                   F.NI=fm_n$F.NI,
                   F.PI=fm_n$F.PI,
                   F.P2O5=fm_n$F.P2O5,
                   F.KI=fm_n$F.KI,
                   F.K2O=fm_n$F.K2O,
                   F.I.Unit=fm_n$F.I.Unit,
                   F.N=0,
                   F.P=0,
                   F.K=1,
                   n_row=1,
                   F.Amount_raw=NA,
                   F.Unit_raw=NA)

add2<-rbind(n_add2,p_add2,k_add2)

add2<-add2[,n_no:=max(n_no,na.rm = T),by=.(B.Code,F.Level.Name)
][,p_no:=max(p_no,na.rm = T),by=.(B.Code,F.Level.Name)
][,k_no:=max(k_no,na.rm = T),by=.(B.Code,F.Level.Name)]

fert_subset<-rbind(fert_subset,add2)

# Infer missing amounts when the total elemental content is known
fert_subset[p_no==1 & is.na(F.Unit) & is.na(F.Amount) & !is.na(F.PI) & F.P!=0,`:=`(F.Amount=round(F.PI/F.P/n_row,1),F.Unit=F.I.Unit)]
fert_subset[n_no==1 & is.na(F.Unit) & is.na(F.Amount) & !is.na(F.NI) & F.N!=0,`:=`(F.Amount=round(F.NI/F.N/n_row,1),F.Unit=F.I.Unit)]
fert_subset[k_no==1 & is.na(F.Unit) & is.na(F.Amount) & !is.na(F.KI) & F.K!=0,`:=`(F.Amount=round(F.KI/F.K/n_row,1),F.Unit=F.I.Unit)]

# Handle percentages for fertilizer amounts
fert_subset[grep("%",F.Unit,fixed=T),F.Amount_perc:=F.Amount/100]
fert_subset[grep("%",F.Unit,fixed=T),F.Amount:=NA]

fert_subset[p_no==1 & grepl("%",F.Unit) & F.Type!="P (Unspecified)" & !is.na(F.PI) & F.P!=0,`:=`(F.Amount=round(F.PI/F.P*F.Amount_perc,1),F.Unit=F.I.Unit)]
fert_subset[n_no==1 & grepl("%",F.Unit) & F.Type!="N (Unspecified)" & !is.na(F.NI) & F.N!=0,`:=`(F.Amount=round(F.NI/F.N*F.Amount_perc,1),F.Unit=F.I.Unit)]
fert_subset[k_no==1 & grepl("%",F.Unit) & F.Type!="K (Unspecified)" & !is.na(F.KI) & F.K!=0,`:=`(F.Amount=round(F.KI/F.K*F.Amount_perc,1),F.Unit=F.I.Unit)]

# Add amounts for unspecified N, P, K fertilizers (developer notes included)
# Dev Note: Ensure logic to handle unspecified NPK is implemented when NPK totals are given but missing in the table
# Dev Note: it may also be possible to included unspecified N,P, and K after n_no==1 records are dealt with where n_no==2
fert_subset[n_no==1 & is.na(F.Amount) & F.Type=="N (Unspecified)" & !is.na(F.NI),`:=`(F.Amount = F.NI / n_row, F.Unit = F.I.Unit, F.N=1)]
fert_subset[p_no==1 & is.na(F.Amount) & F.Type=="P (Unspecified)" & !is.na(F.PI),`:=`(F.Amount = F.PI / n_row, F.Unit = F.I.Unit, F.P=1)]
fert_subset[k_no==1 & is.na(F.Amount) & F.Type=="K (Unspecified)" & !is.na(F.KI),`:=`(F.Amount = F.KI / n_row, F.Unit = F.I.Unit, F.K=1)]

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
fert_subset[!is.na(F.PI_2) & is.na(F.Unit) & !is.na(F.P) & F.P!=0,`:=`(F.Amount=round(F.PI_2/F.P/n_row,1),F.Unit=F.I.Unit)]
fert_subset[!is.na(F.NI_2) & is.na(F.Unit) & !is.na(F.N) & F.N!=0,`:=`(F.Amount=round(F.NI_2/F.N/n_row,1),F.Unit=F.I.Unit)]
fert_subset[!is.na(F.KI_2) & is.na(F.Unit) & !is.na(F.K) & F.K!=0,`:=`(F.Amount=round(F.KI_2/F.K/n_row,1),F.Unit=F.I.Unit)]

# Handle percentage-based units for recalculated amounts
fert_subset[!is.na(F.PI_2) & grepl("%",F.Unit) & F.Type!="P (Unspecified)" & !is.na(F.P) & F.P!=0,`:=`(F.Amount=round(F.PI_2/F.P*F.Amount_perc,1),F.Unit=F.I.Unit)]
fert_subset[!is.na(F.NI_2) & grepl("%",F.Unit) & F.Type!="N (Unspecified)" & !is.na(F.N) & F.N!=0,`:=`(F.Amount=round(F.NI_2/F.N*F.Amount_perc,1),F.Unit=F.I.Unit)]
fert_subset[!is.na(F.KI_2) & grepl("%",F.Unit) & F.Type!="K (Unspecified)" & !is.na(F.K) & F.K!=0,`:=`(F.Amount=round(F.KI_2/F.K*F.Amount_perc,1),F.Unit=F.I.Unit)]

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
            .(B.Code,F.Level.Name,F.Type,F.Type2,F.Amount,F.Amount_raw,F.I.Unit,F.Unit,F.Unit_raw,F.NPK,F.N,F.P,F.K,
              F.NI,N_sum,
              F.PI,F.P2O5,P_sum,
              F.KI,F.K2O,K_sum)]
}

errors<-check[,.(B.Code,F.Level.Name,F.Type,F.Type2,F.Amount,F.Amount_raw,F.I.Unit,F.Unit,F.Unit_raw,F.NPK,F.N,F.P,F.K,
                 F.NI,N_sum,F.PI,F.P2O5,P_sum,F.KI,F.K2O,K_sum)]


# Combine new data (Unspecified rows) and convert to Fert.Method format
add12<-rbind(add1,add2)
add12[F.Type=="N (Unspecified)",F.Codes:="b17"
      ][F.Type=="P (Unspecified)",F.Codes:="b21"
        ][F.Type=="K (Unspecified)",F.Codes:="b16"]

fm_new<-data.table(F.Level.Name=add12$F.Level.Name,
           F.Category="Inorganic",
           F.Type=add12$F.Type,
           F.NPK=add12$F.NPK,
           F.Amount=add12$F.Amount,
           F.Unit=add12$F.Unit,
           F.Method=NA,
           F.Physical=NA,
           F.Mechanization=NA,
           F.Source=NA,
           F.Fate=NA,
           Site.ID="All Sites",
           Time="All Times",
           F.Date.Start=NA,
           F.Date.End=NA,
           F.Date=NA,
           F.Date.Stage=NA,
           F.Date.DAP=NA,
           F.Date.DAE=NA,
           F.Date.Text=NA,
           B.Code=add12$B.Code,
           F.Codes=add12$F.Codes)

if("F.NP2O5K2O" %in% colnames(Fert.Method)){
  fm_new[,F.NP2O5K2O:=NA]
}

return(list(data=fert_subset,data_new=fm_new,errors=errors))

}

#' Replace Zeros with NA
#'
#' This function replaces all zero values in a given dataset with `NA`.
#'
#' @param data A numeric vector, matrix, or data frame in which zeros should be replaced with `NA`.
#' @return The input data with all zero values replaced by `NA`.
#' @examples
#' x <- c(1, 0, 3, 0, 5)
#' replace_zero_with_NA(x)
#'
#' df <- data.frame(a = c(0, 2, 3), b = c(4, 0, 6))
#' replace_zero_with_NA(df)
#' @export
replace_zero_with_NA <- function(data) {
  data[data == 0] <- NA
  return(data)
}
#' Error Tracker
#'
#' This function tracks errors, saves them to a CSV file, and merges new errors with existing ones.
#'
#' @param errors A data frame containing error information.
#' @param filename A string representing the name of the file to save the errors to (without extension).
#' @param error_dir A string representing the directory where the error file should be saved.
#' @param error_list A list of existing error data frames. Defaults to `NULL`.
#' @param character_cols A vector of column names that will be enforced to be class `character`. Defaults to `B.Code`.
#'
#' @return A list of error data frames, updated with the new errors.
#'
#' @examples
#' \dontrun{
#' errors <- data.frame(error_message = c("Error 1", "Error 2"), stringsAsFactors = FALSE)
#' error_tracker(errors, "errors_file", "/path/to/error_dir")
#' }
#' @importFrom data.table fread fwrite
error_tracker <- function(errors, filename, error_dir, error_list = NULL,character_cols="B.Code") {
  if (is.null(error_list)) {
    error_list <- list()
  }
  
  error_file <- file.path(error_dir, paste0(filename, ".csv"))
  
  if (nrow(errors) > 0) {
    if (file.exists(error_file)) {
      error_tracking <- unique(fread(error_file))
      error_tracking[, addressed_by_whom := as.character(addressed_by_whom)]
      
      if(!is.null(character_cols)){
        character_cols<-character_cols[character_cols %in% colnames(error_tracking)]
        if(length(character_cols)>0){
          error_tracking[, (character_cols) := lapply(.SD, function(x) as.character(x)), .SDcols = character_cols]
        }
      }
      if ("value" %in% colnames(error_tracking)) {
        error_tracking[, value := as.character(value)]
      }
      
      if ("notes" %in% colnames(error_tracking)) {
        error_tracking[, notes := as.character(notes)]
      } else {
        error_tracking[, notes := ""]
      }
      
      errors <- merge(errors, error_tracking, all.x = TRUE, by = colnames(errors))
      errors[is.na(issue_addressed), issue_addressed := FALSE
      ][is.na(addressed_by_whom), addressed_by_whom := ""
      ][is.na(notes), notes := ""]
    } else {
      errors[, issue_addressed := FALSE
      ][, addressed_by_whom := ""
      ][, notes := ""]
    }
    error_list[[filename]] <- errors
    fwrite(errors, error_file)
  } else {
    unlink(error_file)
  }
  
  return(error_list)
}
#' Convert "NA" Strings to NA Values
#'
#' This function converts character strings "NA" to actual NA values in a data.table for all character columns using .SD for efficiency.
#'
#' @param dt A data.table in which "NA" strings should be converted to NA values.
#'
#' @return A data.table with "NA" strings converted to NA values in all character columns.
#'
#' @examples
#' \dontrun{
#' library(data.table)
#' dt <- data.table(a = c("NA", "1", "2"), b = c("3", "NA", "4"))
#' convert_NA_strings_SD(dt)
#' }
#' @import data.table
convert_NA_strings_SD <- function(dt) {
  # Ensure the input is a data.table
  if (!is.data.table(dt)) {
    stop("Input must be a data.table")
  }
  
  # Identify which columns are character type
  char_cols <- names(dt)[sapply(dt, is.character)]
  
  # Perform the operation on all character columns at once
  dt[, (char_cols) := lapply(.SD, function(x) fifelse(x == "NA", NA_character_, x)), .SDcols = char_cols]
  
  # Return the modified data.table
  return(dt)
}
# 'harmonizer' function:
# This function harmonizes data fields by replacing old values with new values based on master codes.
# It supports both primary and alternate harmonization rules if provided.
#
# Args:
#   data: DataFrame - The data.table to be harmonized.
#   master_codes: List - A list containing the 'lookup_levels' DataFrame with old to new value mappings.
#   h_table: String - The name of the table in 'lookup_levels' used for the primary harmonization.
#   h_field: String - The name of the field in 'lookup_levels' used for the primary harmonization.
#   h_table_alt: String - Optional. The name of the alternate table for harmonization if different from primary.
#   h_field_alt: String - Optional. The name of the alternate field for harmonization if different from primary.
#
# Returns:
#   A list containing:
#   - 'data': DataFrame with the harmonized data.
#   - 'h_tasks': DataFrame listing any non-matched harmonization tasks by B.Code and value.
harmonizer <- function(data, master_codes, master_tab="lookup_levels",h_table, h_field, h_table_alt=NA, h_field_alt=NA,ignore_vals=NULL) {
  data<-data.table(data)
  
  # subset master_codes to relevant tab
  m_codes<-master_codes[[master_tab]]
  
  # Selecting relevant columns for output
  h_cols <- c("B.Code", h_field)
  
  if(master_tab!="lookup_levels") {
    if(is.na(h_field_alt)) {
      stop("h_field_alt must be provided when master_table is not lookup_levels")
    }else{
      c_names<-c(h_field,h_field_alt)
      h_tab<-unique(m_codes[,..c_names])
      colnames(h_tab)<-c("Values_Old","Values_New")
      h_tab<-h_tab[!is.na(Values_New)]
      h_table_alt<-as.character(h_table_alt)
    }
  }else{
    
    if (is.na(h_field_alt)) {
      # Retrieve mappings for primary fields if no alternate field is provided
      h_tab <- m_codes[Table == h_table & Field == h_field, list(Values_New, Values_Old)]
    } else {
      if (is.na(h_table_alt)) {
        # Warning if alternate field is provided without an alternate table
        warning("If h_field_alt is provided, h_table_alt should also be provided, currently using h_table which may result in non-matches.")
        h_table_alt <- h_table
      }
      
      # Retrieve mappings for alternate fields
      h_tab <- m_codes[Table == h_table_alt & Field == h_field_alt, list(Values_New, Values_Old)]
    }
  }
  
  # Split Values_Old by ";" and unnest the list into separate rows
  h_tab<-rbindlist(lapply(1:nrow(h_tab),FUN=function(i){
    data.table(Values_New=h_tab$Values_New[i],Values_Old=unlist(strsplit(h_tab$Values_Old[i],";")))
  }))
  
  # Matching old values to new values and updating data
  class<-unlist(data[, .(class=lapply(.SD,base::class)), .SDcols = h_field])
  
  if(class=="list"){
    data_focal<-data[,..h_field]
    data_focal<-lapply(1:nrow(data_focal),FUN=function(i){
      old<-unlist(data_focal[i])
      new<-h_tab[match(old,h_tab$Values_Old),Values_New]
      new[is.na(old)]<-NA
      old[!is.na(new)]<-new[!is.na(new)]
      return(old)
    })
    
    data[,(h_field):=data_focal]
    
    data_focal<-data[,list(vals=unique(unlist(.SD))),by=B.Code,.SDcols = h_field]
    setnames(data_focal,"vals",h_field)
    
  }else{
    N <- match(unlist(data[, ..h_field]), h_tab[, Values_Old])
    N[is.na(unlist(data[, ..h_field]))]<-NA
    data <- data[!is.na(N), (h_field) := h_tab[N[!is.na(N)], Values_New]]
    data_focal<-data[,..h_cols]
  }
  
  # Check for any non-matches after harmonization
  N <- match(unlist(data_focal[, ..h_field]), h_tab[, Values_New])
  if(!is.null(ignore_vals)){
    if(!is.na(ignore_vals)){
      h_tasks <- unique(data_focal[is.na(N) & !grepl(paste(ignore_vals,collapse="|"),unlist(data[, ..h_field]),ignore.case = T)])
    }else{
      h_tasks <- unique(data_focal[is.na(N)])
    }
  }else{
    h_tasks <- unique(data_focal[is.na(N)])
  }
  colnames(h_tasks)[2] <- "value"
  h_tasks <- h_tasks[, .(B.Code = paste(B.Code, collapse = "/")), by = list(value)][!is.na(value)]
  
  # Adding metadata columns to the tasks output for further tracking
  h_tasks[, table := h_table][, field := h_field][,table_alt:=h_table_alt][,field_alt:=h_field_alt][,master_tab:=master_tab]
  
  return(list(data = data, h_tasks = h_tasks))
}
#' Harmonizer Wrapper
#'
#' This function wraps the harmonization process, applying the harmonizer function to the provided data according to the specified parameters and master codes.
#'
#' @param data A data frame or data table to be harmonized.
#' @param h_params A data frame containing harmonization parameters, including `h_table`, `h_field`, `h_table_alt`, `h_field_alt`, and optionally `master_tab` and `ignore_vals`.
#' @param master_codes A data frame or data table containing master codes used for harmonization.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{data}{The harmonized data.}
#'   \item{h_tasks}{A data table of harmonization tasks.}
#' }
#'
#' @examples
#' \dontrun{
#' data <- data.frame(id = 1:3, value = c("A", "B", "C"))
#' h_params <- data.frame(h_table = "table1", h_field = "field1", 
#'                        h_table_alt = "table2", h_field_alt = "field2")
#' master_codes <- data.frame(code = c("A", "B", "C"), value = c(1, 2, 3))
#' harmonizer_wrap(data, h_params, master_codes)
#' }
harmonizer_wrap <- function(data, h_params, master_codes) {
  h_tasks <- list()
  
  if (!any(grepl("master_tab", colnames(h_params)))) {
    h_params$master_tab <- "lookup_levels"
  }
  
  if (!any(grepl("ignore_vals", colnames(h_params)))) {
    h_params$ignore_vals <- NA
  }
  
  for (i in 1:nrow(h_params)) {
    results <- harmonizer(
      data = data,
      master_codes = master_codes,
      master_tab = h_params$master_tab[i],
      h_table = h_params$h_table[i], 
      h_field = h_params$h_field[i],
      h_table_alt = h_params$h_table_alt[i],
      h_field_alt = h_params$h_field_alt[i],
      ignore_vals = h_params$ignore_vals[i]
    )
    
    data <- results$data
    h_tasks[[i]] <- results$h_tasks
  }
  
  return(list(data = data, h_tasks = rbindlist(h_tasks)))
}
#' Value Checker
#'
#' This function checks if values in a specified field of the data exist in the master codes table. It can handle both exact and approximate matching.
#'
#' @param data A data frame or data table to be checked.
#' @param tabname A string representing the name of the table in the data.
#' @param master_codes A list of data frames or data tables containing master codes for different tabs.
#' @param master_tab A string representing the tab name in the master codes list.
#' @param h_field A string representing the name of the field to be checked.
#' @param h_field_alt A string representing an alternate field to be used if `h_field` is not found. Defaults to `NA`.
#' @param exact A logical value indicating whether to perform exact matching (`TRUE`) or approximate matching (`FALSE`). Defaults to `TRUE`.
#' @param ignore_vals A vector of values to be ignored during the check. Defaults to `NULL`.
#'
#' @return If `exact` is `TRUE`, returns a data table of unmatched values. If `exact` is `FALSE`, returns a list containing a data table of unmatched values and the modified data with matched values.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), h_field = c("val1", "val2", "val3"))
#' master_codes <- list(tab1 = data.frame(h_field = c("val1", "val4"), h_field_alt = c("alt1", "alt4")))
#' val_checker(data, "example_table", master_codes, "tab1", "h_field")
#' }
#' @import data.table
val_checker <- function(data, tabname, master_codes, master_tab, h_field, h_field_alt = NA, exact = TRUE, ignore_vals = NULL) {
  
  n_col <- c("B.Code", h_field)
  h_tasks <- data.table(data)[, ..n_col]
  setnames(h_tasks, h_field, "value")
  h_tasks <- h_tasks[, .(B.Code = paste(B.Code, collapse = "/")), by = list(value)][!is.na(value)]
  h_tasks[, field := h_field][, field_alt := h_field_alt][, table := tabname][, master_tab := master_tab]
  
  # Subset master_codes to relevant tab
  m_codes <- master_codes[[master_tab]]
  
  # Harmonize old names to new names
  if (is.na(h_field_alt)) {
    # Retrieve mappings for primary fields if no alternate field is provided
    mc_vals <- unlist(m_codes[, ..h_field])
  } else {
    # Retrieve mappings for alternate fields
    mc_vals <- unlist(m_codes[, ..h_field_alt])
  }
  
  # Check if values are in master codes
  if (exact) {
    h_tasks <- h_tasks[, check := FALSE][value %in% mc_vals, check := TRUE][check == FALSE][, check := NULL]
    return(h_tasks)
  } else {
    matched <- as.character(mc_vals)[match(tolower(h_tasks$value), tolower(mc_vals))]
    h_tasks <- h_tasks[is.na(matched)]
    setnames(data, h_field, "value")
    matched <- as.character(mc_vals)[match(tolower(data$value), tolower(mc_vals))]
    data[!is.na(matched), value := matched[!is.na(matched)]]
    setnames(data, "value", h_field)
    if (!is.null(ignore_vals)) {
      h_tasks <- h_tasks[!grepl(paste0(ignore_vals, collapse = "|"), value, ignore.case = TRUE)]
    }
    return(list(h_task = h_tasks, data = data))
  }
}
#' Find and Report Non-Numeric Values
#'
#' This function identifies non-numeric values in specified numeric columns of a data frame or data table.
#'
#' @param data A data frame or data table to be checked.
#' @param numeric_cols A vector of column names that are expected to contain numeric values.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of non-numeric values, including the table name and field where they were found.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), num_col1 = c("1", "two", "3"), num_col2 = c("4", "five", "6"))
#' find_non_numeric(data, c("num_col1", "num_col2"), "example_table")
#' }
#' @import data.table
find_non_numeric <- function(data, numeric_cols, tabname) {
  results <- rbindlist(lapply(1:length(numeric_cols), FUN = function(i) {
    n_col <- numeric_cols[i]
    vals <- unlist(data[, ..n_col])
    vals_u <- unique(vals)
    vals_u <- vals_u[!is.na(vals_u)]
    NAs <- vals_u[is.na(as.numeric(vals_u))]
    n_col2 <- c("B.Code", n_col)
    result <- unique(data[vals %in% NAs, ..n_col2])[, table := tabname][, field := n_col]
    setnames(result, n_col, "value")
    result
  }))
  
  return(results)
}
#' Check Site and Time Fields
#'
#' This function checks if the `Site.ID` and `Time` fields in the data match the corresponding entries in the site and time tables.
#'
#' @param data A data frame or data table to be checked.
#' @param tabname A string representing the name of the table in the data.
#' @param ignore_values A vector of values to be ignored during the check.
#' @param site_data A data frame or data table containing valid `Site.ID` entries.
#' @param time_data A data frame or data table containing valid `Time` entries.
#' @param do_site A logical value indicating whether to check `Site.ID`. Defaults to `TRUE`.
#' @param do_time A logical value indicating whether to check `Time`. Defaults to `TRUE`.
#'
#' @return A data table of errors found, including unmatched `Site.ID` and `Time` values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), Site.ID = c("S1", "S2", "S3"), Time = c("T1", "T2", "T3"))
#' site_data <- data.frame(Site.ID = c("S1", "S2"), B.Code = c("A1", "B2"))
#' time_data <- data.frame(Time = c("T1", "T2"), B.Code = c("A1", "B2"))
#' check_site_time(data, "example_table", c("ignore1", "ignore2"), site_data, time_data)
#' }
#' @import data.table
check_site_time <- function(data, tabname, ignore_values, site_data, time_data, do_site = TRUE, do_time = TRUE) {
  errors <- list()
  
  if ("Site.ID" %in% colnames(data) & do_site) {
    site_data <- site_data[, check := TRUE][, list(Site.ID, B.Code, check)]
    # Non-match in Site.ID
    errors1 <- unique(data[!grepl(paste0(ignore_values, collapse = "|"), Site.ID, ignore.case = TRUE)
    ][!is.na(Site.ID), list(B.Code, Site.ID)])
    errors1 <- merge(errors1, site_data, all.x = TRUE)[is.na(check)
    ][, check := NULL
    ][, table := tabname
    ][, field := "Site.ID"
    ][, issue := "A Site.ID used does not match the Site tab."]
    setnames(errors1, "Site.ID", "value")
    errors$errors1 <- errors1
  }
  
  if ("Time" %in% colnames(data) & do_time) {
    # Non-match in Time
    time_data <- time_data[, check := TRUE][, list(Time, B.Code, check)]
    errors2 <- unique(data[!grepl(paste0(ignore_values, collapse = "|"), Time, ignore.case = TRUE)
    ][!is.na(Time), list(B.Code, Time)])
    errors2 <- merge(errors2, time_data, all.x = TRUE)[is.na(check)
    ][, check := NULL
    ][, table := tabname
    ][, field := "Time"
    ][, issue := "A Time used does not match the Time tab."]
    setnames(errors2, "Time", "value")
    errors$errors2 <- errors2
  }
  
  if (length(errors) > 0) {
    errors <- rbindlist(errors)
  } else {
    NULL
  }
  
  return(errors)
}
#' Check Dates
#'
#' This function checks if the dates in the specified date columns are within the valid date range.
#'
#' @param data A data frame or data table containing the data to be checked.
#' @param date_cols A vector of column names that are expected to contain date values.
#' @param valid_start A Date object representing the start of the valid date range.
#' @param valid_end A Date object representing the end of the valid date range.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including dates not within the valid date range, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(B.Code = c("A1", "B2", "C3"), date1 = as.Date(c("2021-01-01", "2022-02-02", "2023-03-03")))
#' check_dates(data, c("date1"), as.Date("2020-01-01"), as.Date("2022-12-31"), "example_table")
#' }
#' @import data.table
check_dates <- function(data, date_cols, valid_start, valid_end, tabname) {
  
  results <- rbindlist(lapply(1:length(date_cols), FUN = function(i) {
    n_col <- c("B.Code", date_cols[i])
    data_ss <- data[, ..n_col]
    colnames(data_ss)[2] <- "value"
    
    data_ss <- data_ss[!is.na(value)]
    
    data_ss <- data_ss[, problem := TRUE
    ][value >= valid_start & value <= valid_end, problem := FALSE
    ][, list(value = paste(value, collapse = "/")), by = list(B.Code, problem)
    ][, field := date_cols[i]]
    data_ss
  }))
  
  results <- results[problem == TRUE
  ][, table := tabname
  ][, problem := NULL
  ][, issue := paste("Date not between", valid_start, "&", valid_end)]
  
  return(results)
}
#' Detect Extreme Values
#'
#' This function detects extreme values in a specified field of the data that are outside a given range.
#'
#' @param data A data frame or data table containing the data to be checked.
#' @param field A string representing the name of the field to be checked for extreme values.
#' @param lower_bound A numeric value representing the lower bound of the acceptable range.
#' @param upper_bound A numeric value representing the upper bound of the acceptable range.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of extreme values found, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), value_field = c(1, 100, 50))
#' detect_extremes(data, "value_field", 0, 60, "example_table")
#' }
#' @import data.table
detect_extremes <- function(data, field, lower_bound, upper_bound, tabname) {
  # Dynamically construct the filtering condition
  condition <- paste0(field, " > ", upper_bound, " | ", field, " < ", lower_bound)
  
  # Evaluate the condition within the data.table context
  errors <- data[eval(parse(text = condition))
  ][, list(value = paste0(unique(get(field)), collapse = "/")), by = list(B.Code)
  ][, table := tabname
  ][, field := field
  ][, issue := paste0("Extreme values detected in field (outside range: ", lower_bound, "-", upper_bound, ")")]
  
  return(errors)
}
#' Check Units
#'
#' This function checks if units are missing for fields where amounts are present in the data.
#'
#' @param data A data frame or data table to be checked.
#' @param unit_pairs A data frame containing pairs of unit and variable fields along with their name fields.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including instances where amounts are present but units are missing, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), amount = c(10, NA, 30), unit = c(NA, "kg", NA), name_field = c("item1", "item2", "item3"))
#' unit_pairs <- data.frame(unit = c("unit"), var = c("amount"), name_field = c("name_field"))
#' check_units(data, unit_pairs, "example_table")
#' }
#' @import data.table
check_units <- function(data, unit_pairs, tabname) {
  n_cols<-table(colnames(data))
  if(any(n_cols)>1){
    stop(paste("Duplicate column names present:",paste0(names(n_cols)[n_cols>1],collapse = ", ")))
  }
  
  results <- rbindlist(lapply(1:nrow(unit_pairs), FUN = function(i) {
    Unit <- unit_pairs$unit[i]
    Var <- unit_pairs$var[i]
    Name_Field <<- unit_pairs$name_field[i]
    
    # Dynamically construct the filtering condition
    condition <- paste0("is.na(", Unit, ") & !is.na(", Var, ")")
    
    # Use 'mget(Name_Field)' to dynamically refer to the column within .SD
    errors <- data[eval(parse(text = condition)), 
                   .(value = paste(unique(na.omit(.SD[[1]])), collapse = "/")), 
                   by = B.Code, 
                   .SDcols = Name_Field
    ][, `:=`(table = tabname, 
             field = Name_Field, 
             issue = paste0("Value is present for ",Var," but associated item, value, or unit is missing in ", Unit, "."))
    ][order(B.Code)]
    
    errors
  }))
  
  return(results)
}
#' Check High and Low Values
#'
#' This function checks if the low values are greater than the high values for specified columns in the data.
#'
#' @param data A data frame or data table to be checked.
#' @param hilo_pairs A data frame containing pairs of high and low columns along with their name fields.
#' @param tabname A string representing the name of the table in the data.
#'
#' @return A data table of errors found, including instances where low values are greater than high values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), low_value = c(10, 30, 50), high_value = c(20, 25, 45), name_field = c("item1", "item2", "item3"))
#' hilo_pairs <- data.frame(low_col = c("low_value"), high_col = c("high_value"), name_field = c("name_field"))
#' check_hilow(data, hilo_pairs, "example_table")
#' }
#' @import data.table
check_hilow <- function(data, hilo_pairs, tabname) {
  results <- rbindlist(lapply(1:nrow(hilo_pairs), FUN = function(i) {
    low_col <- hilo_pairs$low_col[i]
    high_col <- hilo_pairs$high_col[i]
    Name_Field <- hilo_pairs$name_field[i]
    
    # Dynamically construct the filtering condition
    condition <- paste0(low_col, " > ", high_col)
    condition2 <- paste0(Name_Field, "[!is.na(", Name_Field, ")]")
    
    # Evaluate the condition within the data.table context
    errors <- data[eval(parse(text = condition))
    ][, list(value = paste(eval(parse(text = condition2)), collapse = "/")), by = B.Code
    ][, table := tabname
    ][, field := Name_Field
    ][, issue := paste0(low_col, " is greater than ", high_col, ".")
    ][order(B.Code)]
    errors
  }))
  
  return(results)
}
#' Validate Data
#'
#' This function performs various validation checks on the data, including checking for unique values, compulsory fields, non-numeric values in numeric fields, date ranges, extreme values, missing units, and high/low pair issues.
#'
#' @param data A data frame or data table to be checked.
#' @param numeric_cols A vector of column names that will be enforced to be class `numeric`. Defaults to `NULL`.
#' @param character_cols A vector of column names that will be enforced to be class `character`. Defaults to `NULL`.
#' @param numeric_ignore_vals A vector of values to be ignored in numeric columns. Defaults to `NULL`.
#' @param date_cols A vector of column names that are expected to contain date values. Defaults to `NULL`.
#' @param zero_cols A vector of column names where zeros should be replaced with `NA`. Defaults to `NULL`.
#' @param unique_cols A vector of column names that should contain unique values. Defaults to `NULL`.
#' @param compulsory_cols A named vector where names are columns and values are associated compulsory fields. Defaults to `NULL`.
#' @param extreme_cols A named list where names are columns and values are vectors with lower and upper bounds. Defaults to `NULL`.
#' @param unit_pairs A data frame containing pairs of unit and variable fields along with their name fields. Defaults to `NULL`.
#' @param hilo_pairs A data frame containing pairs of high and low columns along with their name fields. Defaults to `NULL`.
#' @param tabname A string representing the name of the table in the data.
#' @param valid_start A Date object representing the start of the valid date range.
#' @param valid_end A Date object representing the end of the valid date range.
#' @param site_data A data frame or data table containing valid `Site.ID` entries. Defaults to `NULL`.
#' @param time_data A data frame or data table containing valid `Time` entries. Defaults to `NULL`.
#' @param ignore_values A vector of values to be ignored during the checks. Defaults to `NULL`.
#' @param allowed_values A data frame or data table containing the fields `allowed_values` (this should be a list containing a vector of the allowed values), `parent_tab_name` (a character value of where the allowed values come from), and `field` (the name of the field in `data` to be checked).
#' @param trim_ws A logical value indicating whether to trim whitespace from character columns. Defaults to `FALSE`.
#' @param do_site A logical value indicating whether to check `Site.ID`. Defaults to `TRUE`.
#' @param do_time A logical value indicating whether to check `Time`. Defaults to `TRUE`.
#' @param convert_NA_strings A logical value indicating whether to convert "NA" strings to actual `NA` values. Defaults to `FALSE`.
#' @param duplicate_field A character value indicating the name of the field that should be used to label duplicate rows (e.g. `V.Level.Name`). Defaults to `NULL`.
#' @param duplicate_ignore_fields A vector of column names indicating any columns that should be excluded when checking for duplicate rows. Defaults to `NULL`.
#' @param find_duplicates A logical value indicating whether to check the data for exact duplicate rows. Defaults to `TRUE`. Only applies if `duplicate_field` is not set to `NULL.`
#' @param rm_duplicates A logical value indicating whether to remove exact duplicate rows/ Defaults to `TRUE`. Only applies if `duplicate_field` is not set to `NULL.`
#' @param template_cols A character vector of columns against which the columns names in data are checked again. Defaults to `NULL`.
#'
#' @return A list with two elements:
#' \describe{
#'   \item{data}{The validated data.}
#'   \item{errors}{A data table of validation errors found.}
#' }
#'
#' @examples
#' \dontrun{
#' data <- data.table(B.Code = c("A1", "B2", "C3"), numeric_col = c("10", "20", "non-numeric"))
#' validator(data, numeric_cols = c("numeric_col"), tabname = "example_table", valid_start = as.Date("2020-01-01"), valid_end = as.Date("2022-12-31"))
#' }
#' @import data.table
validator <- function(data,
                      numeric_cols = NULL,
                      character_cols = NULL,
                      numeric_ignore_vals = NULL,
                      date_cols = NULL,
                      zero_cols = NULL,
                      unique_cols = NULL,
                      compulsory_cols = NULL,
                      extreme_cols = NULL,
                      unit_pairs = NULL,
                      hilo_pairs = NULL,
                      tabname,
                      valid_start,
                      valid_end,
                      site_data = NULL,
                      time_data = NULL,
                      ignore_values = NULL,
                      trim_ws = FALSE,
                      do_site = TRUE,
                      do_time = TRUE,
                      convert_NA_strings = FALSE,
                      duplicate_field=NULL,
                      duplicate_ignore_fields=NULL,
                      find_duplicates=TRUE,
                      template_cols=NULL,
                      allowed_values=NULL,
                      check_keyfields=NULL,
                      rm_duplicates=TRUE) {
  
  errors <- list()
  n <- 1
  
  if(!is.null(template_cols)){
    a<-colnames(data) %in% template_cols
    a<-colnames(data)[!a]
    
    if(length(a)>0){
      errors1 <- data.table(value = paste(a, collapse = "/"), 
                            B.Code = NA,
                            table = tabname,
                            field = NA,
                            issue = "Column names in excel that do not match the master template.")
      
      errors[[n]] <- errors1
      n <- n + 1
    }
    
    a<-template_cols %in% colnames(data)
    a<-template_cols[!a]
    
    if(length(a)>0){
      errors1 <- data.table(value = paste(a, collapse = "/"), 
                            B.Code = NA,
                            table = tabname,
                            field = NA,
                            issue = "Column names in template that do not match the excel")
      
      errors[[n]] <- errors1
      n <- n + 1
    }
  }
  
  if(convert_NA_strings){
    data <- convert_NA_strings_SD(data)
  }
  
  if(!is.null(character_cols)){
    character_cols<-character_cols[character_cols %in% colnames(data)]
    if(length(character_cols)>0){
      data[, (character_cols) := lapply(.SD, function(x) as.character(x)), .SDcols = character_cols]
    }
  }
  
  if (!is.null(zero_cols)) {
    zero_cols<-zero_cols[zero_cols %in% colnames(data)]
    data <- data[, (zero_cols) := lapply(.SD, replace_zero_with_NA), .SDcols = zero_cols]
  }
  
  if (!is.null(unique_cols)) {
    errors1 <- rbindlist(lapply(1:length(unique_cols), FUN = function(i) {
      field <- unique_cols[i]
      n_col <- c("B.Code", field)
      dat <- data[, ..n_col]
      colnames(dat)[2] <- "value"
      errors <- dat[, list(N = .N), by = list(B.Code, value)
      ][N > 1
      ][, N := NULL
      ][, list(value = paste(value, collapse = "/")), by = B.Code
      ][, table := tabname
      ][, field := field
      ][, issue := "Duplicate value in unique field."
      ][order(B.Code)]
      errors
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  if (!is.null(compulsory_cols)) {
    errors1 <- rbindlist(lapply(1:length(compulsory_cols), FUN = function(i) {
      field <- compulsory_cols[i]
      assoc_field <- names(compulsory_cols)[i]
      n_col <- c("B.Code", field, assoc_field)
      dat <- data[, ..n_col]
      colnames(dat)[2:3] <- c("focus", "value")
      errors <- dat[is.na(focus),
      ][, list(value = paste(unique(value), collapse = "/")), by = B.Code
      ][, table := tabname
      ][, field := assoc_field
      ][, issue := paste0("Missing value in compulsory field ", compulsory_cols[i], ".")
      ][order(B.Code)]
      errors
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  if (!is.null(date_cols)) {
    numeric_cols <- unique(c(numeric_cols, date_cols))
  }
  
  
  # Substitute , for . in numeric columns
  if (!is.null(numeric_cols)) {
    numeric_cols<-numeric_cols[numeric_cols %in% colnames(data)]
    
    data[, (numeric_cols) := lapply(.SD, function(x) gsub(",|·", ".", x)), .SDcols = numeric_cols]
    # Remove spaces in numeric columns
    data[, (numeric_cols) := lapply(.SD, function(x) gsub(" ", "", x)), .SDcols = numeric_cols]
    # Replace − with -
    data[, (numeric_cols) := lapply(.SD, function(x) gsub("−", "-", x)), .SDcols = numeric_cols]
    
    # Look for instances where a non-numeric value is present in a numeric field
    errors1 <- find_non_numeric(data = data, numeric_cols = numeric_cols, tabname = tabname)
    
    if(!is.null(ignore_values)[1]){
      errors1<-errors1[!value %in% ignore_values]
    }
    
    errors1 <- errors1[, list(value = paste(value, collapse = "/")), by = list(B.Code, table, field)
    ][, issue := "Non-numeric value in numeric field."]
    errors[[n]] <- errors1
    n <- n + 1
    # Convert numeric fields to being numeric
    data[, (numeric_cols) := lapply(.SD, function(x) as.numeric(x)), .SDcols = numeric_cols]
  }
  
  # Detect extremes
  if (!is.null(extreme_cols)) {
    errors1 <- rbindlist(lapply(1:length(extreme_cols), FUN = function(i) {
      detect_extremes(data = data,
                      field = names(extreme_cols)[i],
                      lower_bound = extreme_cols[[i]][1],
                      upper_bound = extreme_cols[[i]][2],
                      tabname = tabname)
    }))
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Trim white space
  if (trim_ws) {
    char_cols <- sapply(data, is.character)
    data[, (names(data)[char_cols]) := lapply(.SD, trimws), .SDcols = char_cols]
  }
  
  # Check keyfields
  if(!is.null(check_keyfields)){
    errors1<-rbindlist(lapply(1:nrow(check_keyfields),FUN=function(i){
      result<-check_key(parent=check_keyfields$parent_tab[[i]],
                        child=data,
                        tabname=tabname,
                        tabname_parent=check_keyfields$parent_tab_name[i],
                        keyfield=check_keyfields$keyfield[i],
                        collapse_on_code=T)[,table:=paste0(parent_table,"/",table)][,parent_table:=NULL]
      return(result)
    }))
    errors[[n]]<-errors1
    n<-n+1
  }
  
  # Check if any non-allowed values are present
  # We may want to consider allowing trim_ws to act within this function
  if(!is.null(allowed_values)){
    errors1<-rbindlist(lapply(1:nrow(allowed_values), function(i){
      focal_cols<-c("B.Code",allowed_values[i,field])
      a_vals<-unlist(allowed_values[i,allowed_values])
      dat<-data[,..focal_cols]
      setnames(dat,focal_cols[2],"values")
      dat<-unique(dat[!is.na(values)])
      
      if(nrow(dat)>0){
        dat<-dat[,.(value=unique(unlist(strsplit(values,";")))),by=B.Code][!value %in% a_vals]
        errors1<-dat[,.(value=paste(unique(value),collapse = "/")),by=B.Code
        ][, table := paste0(tabname," & ", allowed_values[i,parent_tab_name])
        ][, field := allowed_values[i,field]
        ][, issue := "Values found in field that are not present in lookup values."]
        return(errors1)
      }else{
        return(NULL)
      }
    }))
    
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Convert date cols to date format
  if (!is.null(date_cols)) {
    # Convert Excel date numbers to R dates
    data[, (date_cols) := lapply(.SD, function(x) as.Date(x, origin = "1899-12-30")), .SDcols = date_cols]
    
    # Look for dates outside of a specified range
    errors[[n]] <- check_dates(data = data,
                               date_cols = date_cols,
                               valid_start = valid_start,
                               valid_end = valid_end,
                               tabname = tabname)[, value := as.character(value)]
    n <- n + 1
  }
  
  # Check for missing units
  if (!is.null(unit_pairs)) {
    errors1 <- check_units(data, unit_pairs = unit_pairs, tabname = tabname)
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Check for high/low pair issues
  if (!is.null(hilo_pairs)) {
    errors1 <- check_hilow(data, hilo_pairs = hilo_pairs, tabname = tabname)
    errors[[n]] <- errors1
    n <- n + 1
  }
  
  # Check for non-matches between site.id and time fields and their parent tables
  if (any(c("Site.ID", "Time") %in% colnames(data)) & tabname != "Site.Out" & (do_site|do_time)) {
    if (is.null(ignore_values)) {
      ignore_values <- c("All Times", "Unspecified", "Not specified", "All Sites")
    }
    errors1 <- check_site_time(data = data,
                               ignore_values = ignore_values,
                               tabname = tabname,
                               site_data = site_data,
                               time_data = time_data,
                               do_site = do_site,
                               do_time = do_time)
    
    if (length(errors1) > 0) {
      errors1 <- errors1[, list(value = paste0(value, collapse = "/")), by = list(B.Code, table, field, issue)]
      errors[[n]] <- errors1
      n<-n+1
    }
  }
  
  if(find_duplicates & !is.null(duplicate_field)){
    if(!is.null(duplicate_ignore_fields)){
      keep_cols<-colnames(data)[!colnames(data) %in% duplicate_ignore_fields]
      errors1<-data[duplicated(data[,..keep_cols])]
    }else{
      errors1<-data[duplicated(data)]
    }
    
    if(nrow(errors1)>0){
      setnames(errors1,duplicate_field,"value")
      errors1<-errors1[,list(value=paste0(unique(value),collapse = "/")),by=B.Code
      ][, table := tabname
      ][, field := duplicate_field
      ][, issue := "Exact Duplicate rows in table"]
      errors[[n]] <- errors1
      n<-n+1
    }
    
    if(rm_duplicates){
      data<-data[!duplicated(data)]
    }
  }
  
  errors <- rbindlist(errors, use.names = TRUE)
  
  return(list(data = data, errors = errors))
}
#' Check Key Field between Parent and Child Tables
#'
#' This function checks for mismatches in a key field between parent and child tables.
#'
#' @param parent A data frame or data table representing the parent table.
#' @param child A data frame or data table representing the child table.
#' @param tabname A string representing the name of the child table.
#' @param keyfield A string representing the name of the key field to be checked.
#' @param collapse_on_code A logical value indicating whether to collapse mismatched values on the B.Code. Defaults to `TRUE`.
#' @param tabname_parent A string representing the name of the parent table. Defaults to `NULL`.
#'
#' @return A data table of mismatched key field values, with the table name, field, and issue described.
#'
#' @examples
#' \dontrun{
#' parent <- data.table(B.Code = c("A1", "A2"), key_field = c("key1", "key2"))
#' child <- data.table(B.Code = c("A1", "A3"), key_field = c("key1", "key3"))
#' check_key(parent, child, "child_table", "key_field", TRUE, "parent_table")
#' }
#' @import data.table
check_key <- function(parent, child, tabname, keyfield, collapse_on_code = TRUE, tabname_parent = NULL,na_omit=T,delim=";") {
  keyfield<-unlist(strsplit(keyfield,"/"))
  n_col <- c("B.Code", keyfield)
  
  parent<-parent[, ..n_col][,check:= TRUE]
  parent<-unique(data.table(parent))
  
  child<-data.table(child)
  child<-child[, ..n_col]
  child<-split_syn(data=child,split_col = tail(keyfield,1),delim=delim)
  
  child <- child[!is.na(child[[ncol(child)]])]
  
  mergetab<-data.table(merge(child, parent,by=n_col, all.x = TRUE))
  mergetab <- unique(mergetab[is.na(check)][,check := NULL])
  
  if(length(keyfield)>1){
    x<-mergetab[,..keyfield]
    mergetab<-mergetab[,value:=apply(x,1,paste,collapse="-")][,.(B.Code,value)]
  }else{
    setnames(mergetab, keyfield, "value")
  }
  
  if(na_omit){
    mergetab<-mergetab[!is.na(value)]
  }
  
  if (collapse_on_code) {
    mergetab <- mergetab[, list(value = paste(unique(value), collapse = "/")), by = list(B.Code)]
  } else {
    mergetab <- mergetab[, list(B.Code, value)]
  }
  
  mergetab[, table := tabname
  ][, field := paste0(keyfield,collapse = "/")
  ][, issue := "Mismatch in field value between parent and child tables."]
  
  if (!is.null(tabname_parent)) {
    mergetab[, parent_table := tabname_parent]
  }
  return(mergetab)
}
#' Check Coordinates
#'
#' This function checks if the coordinates (longitude and latitude) in the data are within the boundaries of the specified countries.
#'
#' @param data A data frame or data table containing the coordinates and country ISO codes. The data should have columns `Site.LonD` for longitude, `Site.LatD` for latitude, and `ISO.3166.1.alpha.3` for country ISO codes.
#'
#' @return A logical vector indicating whether each coordinate is within the boundaries of the specified country. `NA` is returned if the country code is not found.
#'
#' @examples
#' \dontrun{
#' data <- data.frame(Site.LonD = c(10, 20, 30), Site.LatD = c(50, 60, 70), ISO.3166.1.alpha.3 = c("USA", "CAN", "MEX"))
#' check_coordinates(data)
#' }
#' @importFrom rnaturalearth ne_countries
#' @importFrom sf st_transform st_make_valid st_as_sf st_within
#' @importFrom dplyr filter
check_coordinates <- function(data) {
  # Load country boundaries
  countries <- rnaturalearth::ne_countries(scale = "large", returnclass = "sf")
  
  # Ensure CRS (Coordinate Reference System) is set to WGS84 (EPSG:4326)
  countries <- st_transform(countries, crs = 4326)
  
  # Make geometries valid
  countries <- sf::st_make_valid(countries)  
  
  # Convert data to sf object
  data_sf <- sf::st_as_sf(data, coords = c("Site.LonD", "Site.LatD"), crs = 4326, remove = FALSE)
  
  # Initialize result vector
  is_within_country <- logical(nrow(data))
  
  # Iterate over unique ISO codes in the data
  for (iso3 in unique(data$ISO.3166.1.alpha.3)) {
    # Filter country boundaries for the current ISO code
    country_boundary <- countries %>% filter(iso_a3 == iso3)
    
    if (nrow(country_boundary) > 0) {
      # Check if points are within the country boundary
      points_in_country <- st_within(data_sf, country_boundary, sparse = FALSE)
      
      # Store results
      is_within_country[data$ISO.3166.1.alpha.3 == iso3] <- apply(points_in_country, 1, any)[data$ISO.3166.1.alpha.3 == iso3]
    } else {
      # Assign NA if country code not found
      is_within_country[data$ISO.3166.1.alpha.3 == iso3] <- NA
    }
  }
  
  return(is_within_country)
}
#' Split and Expand Delimited Values into Multiple Rows
#'
#' This function splits the entries of a specified column in a data frame based 
#' on a given delimiter, creating a new row for each split value, while retaining 
#' the values in the other columns.
#'
#' @param data A data frame containing the data to be split.
#' @param split_col A character string specifying the name of the column to be split.
#' @param delim A character string specifying the delimiter by which to split the column. 
#' Default is `";"`.
#'
#' @return A data frame where each value in the `split_col` is split into multiple rows, 
#' with other columns duplicated accordingly.
#'
#' @examples
#' merge_dat <- data.frame(
#'   C.Name = c("2,4-D;2-4D;2,4-Dichlorophenoxyacetic acid", "Aatrex", "Acetamiprid"),
#'   C.Name.New = c("2,4-D", "aatrex", "acetamiprid"),
#'   stringsAsFactors = FALSE
#' )
#' 
#' split_syn(merge_dat, "C.Name", ";")
#'
#' @importFrom dplyr mutate
#' @importFrom tidyr unnest
#' @importFrom data.table data.table
#' @export
split_syn <- function(data, split_col, delim = ";") {
  
  # Split the values in split_col and repeat the corresponding C.Name.New for each split value
  new_data <- data %>%
    dplyr::mutate(!!split_col := strsplit(as.character(.data[[split_col]]), delim)) %>%
    tidyr::unnest(cols = c(!!split_col)) %>%
    data.table
  
  return(new_data)
}
#' Convert NPK Ratio from Tons to Percentage
#'
#' This function converts an NPK fertilizer ratio from a bulk representation (e.g., expressed in tons per unit)
#' to a percentage-based format. If the sum of N, P, and K exceeds 100, the function assumes that the values 
#' are expressed per ton and scales them down by a factor of 10. If the sum remains above 100 after scaling, 
#' the function issues a warning and returns `NA`.
#'
#' @param y A character string representing the NPK ratio, formatted as `"N-P-K"`, where `N`, `P`, and `K` 
#' are numerical values.
#' @param delim A character string specifying the delimiter used in `y`. Defaults to `"-"`. 
#'               Ensure this matches the actual delimiter used in `y`.
#' 
#' @return A character string representing the adjusted NPK ratio in percentage format, or `NA` if the 
#' adjusted sum still exceeds 100 or if the input is invalid.
#'
#' @examples
#' convert_npk("300-150-300")  # Returns "30-15-30"
#' convert_npk("35-6-35")      # Returns "35-6-35" (unchanged)
#' convert_npk("350-60-350")   # Returns "35-6-35"
#' convert_npk("120-50-50")    # Returns "12-5-5"
#' convert_npk("100-abc-300")  # Returns NA with a warning
#' convert_npk("50-")          # Returns NA with a warning
#' convert_npk("100--300")     # Returns NA with a warning
#' convert_npk("100:50:50", delim = ":")  # Returns "10-5-5"
#' 
convert_npk <- function(y, delim = "-") {
  # Ensure the delimiter is a single character and not empty
  if (!is.character(delim) || nchar(delim) != 1) {
    stop("Invalid delimiter: delim must be a single character string.")
  }
  
  # Check if input contains at least two delimiters
  split_values <- unlist(strsplit(y, delim, fixed = TRUE))
  if (length(split_values) < 3) {
    warning(y, " is not a valid NPK format. Expected format: 'N", delim, "P", delim, "K'.")
    return(NA)
  }
  
  # Convert input string to numeric vector
  x <- suppressWarnings(as.numeric(split_values))
  
  # Check if conversion to numeric was successful (i.e., no non-numeric values)
  if (any(is.na(x))) {
    warning(y, " contains non-numeric values or missing values.")
    return(NA)
  }
  
  # Check for negative values
  if (any(x < 0)) {
    warning(y, " contains negative values, which are invalid.")
    return(NA)
  }
  
  # If the sum of N, P, and K exceeds 100, assume it's in tons and scale down
  if (sum(x[1:3]) > 100) {
    x <- x / 10
    
    # If sum is still greater than 100 after scaling, issue a warning and return NA
    if (sum(x[1:3]) > 100) {
      warning(y, " values sum to greater than 100 even when divided by 10.")
      return(NA)
    }
  }
  
  # Return formatted NPK ratio using the default "-" delimiter for output
  return(paste(x, collapse = "-"))
}
