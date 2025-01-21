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