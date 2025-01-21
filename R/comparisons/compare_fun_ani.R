#' compare_fun_ani: Compare Treatments and Controls for Animal Experiments
#'
#' This function compares control and treatment groups in animal-related experiments. It identifies 
#' which treatments can act as controls for other treatments based on specified practices and metadata. 
#' The function accounts for linked tables, specific exceptions like feed additions, and improved breeds.
#'
#' ## Key Features:
#' 1. Identifies valid treatment-control pairs based on shared and unique practices.
#' 2. Handles special cases such as feed additions and improved breeds.
#' 3. Validates comparisons using linked metadata and custom logic.
#' 4. Returns a list of valid comparisons or detailed debugging information.
#'
#' @param Data A data.table containing experimental data. Expected columns include:
#'   - `B.Code`: Experimental grouping identifier.
#'   - `N`: Unique identifiers for treatments.
#'   - `Final.Codes`: Codes representing practices.
#'   - `N.Prac`: Number of practices per treatment.
#' @param PracticeCodes A data.table mapping practice codes to linked tables and columns. Expected columns:
#'   - `Code`: Unique code for each practice.
#'   - `Linked.Tab`: Table linked to the practice.
#'   - `Linked.Col`: Column linked to the practice.
#' @param Verbose Logical; if TRUE, outputs detailed progress and debugging information.
#' @param Debug Logical; if TRUE, returns raw intermediate tables for inspection.
#' @return A list of valid treatment-control pairs, with detailed debugging information if `Debug = TRUE`.
#' @note Requires the `data.table` package.
#' @examples
#' # Example Usage
#' comparisons <- compare_fun_ani(
#'   Data = experimental_data,
#'   PracticeCodes = practice_codes,
#'   PracticeCodes1 = practice_codes1,
#'   Verbose = TRUE,
#'   Debug = FALSE
#' )
#'
#' print(comparisons)
#'
#' @export
compare_fun_ani <- function(Data, PracticeCodes, Verbose = FALSE, Debug = FALSE) {
  
  PracticeCodes1<-copy(PracticeCodes)
  
  # Helper function to identify elements in A that are not in B
  Match.Fun <- function(A, B) {
    A <- unlist(A)
    B <- unlist(B)
    list(A[!A %in% B])
  }
  
  # Helper function to map matched elements from A and B to C
  Match.Fun2 <- function(A, B, C) {
    A <- unlist(A)
    B <- unlist(B)
    C <- unlist(C)
    list(unique(C[match(A, B)]))
  }
  
  # Initialize variables
  BC <- Data$B.Code[1]
  N <- Data[, N]
  Final.Codes <- Data[, Final.Codes]
  Y <- Data[, .(Final.Codes, N, N.Prac)][, Y.N := 1:.N]
  
  # Iterate over all treatments
  lapply(seq_along(N), FUN = function(j) {
    if (Verbose) print(paste("N =", j))
    
    X <- unlist(Data$Final.Codes[j])
    i <- N[j]
    
    if (is.na(X[1])) {
      # Case where treatment has no practices
      Z <- Y[N != i & !is.na(Final.Codes)]
      Z[, Match := sum(X %in% unlist(Final.Codes)), by = N]
      Z[, NoMatch := sum(!unlist(Final.Codes) %in% X), by = N]
      Z <- Z[Match >= 0 & NoMatch > 0]
      Z[, Control.Code := rep(list(X), nrow(Z))]
      Z[, Prac.Code := list(Match.Fun(Final.Codes, Control.Code)), by = N]
      Z[, Linked.Tab := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Tab)), by = N]
      Z[, Linked.Col := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Col)), by = N]
      
    } else {
      # Case where treatment has practices
      Z <- Y[N != i]
      Z[, Match := sum(X %in% unlist(Final.Codes)), by = N]
      Z[, NoMatch := sum(!unlist(Final.Codes) %in% X), by = N]
      Z <- Z[Match >= length(X) & NoMatch > 0]
      Z[, Control.Code := rep(list(X), nrow(Z))]
      Z[, Prac.Code := list(Match.Fun(Final.Codes, Control.Code)), by = N]
      Z[, Linked.Tab := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Tab)), by = N]
      Z[, Linked.Col := list(Match.Fun2(Control.Code, PracticeCodes$Code, PracticeCodes$Linked.Col)), by = N]
    }
    
    # Validate comparisons
    if (nrow(Z) > 0) {
      Z$Level.Check <- lapply(seq_len(nrow(Z)), FUN = function(ii) {
        unlist(lapply(seq_along(unlist(Z[ii, Linked.Tab])), FUN = function(jj) {
          if (is.na(Z[ii, Linked.Tab])) {
            TRUE
          } else {
            if (unlist(Z[ii, Linked.Tab])[jj] == "Animal.Out" && 
                any(Z$Prac.Code[ii] %in% PracticeCodes1[Practice == "Feed Addition", Code])) {
              if (Verbose) print(paste0("Feed Add: ii = ", ii, " | jj = ", jj))
              Data.Sub[j, A.Feed.Add.C] == "Yes"
            } else if (unlist(Z[ii, Linked.Tab])[jj] == "Var.Out") {
              if (Verbose) print(paste0("Improved Breed: ii = ", ii, " | jj = ", jj))
              COL <- unlist(Z[ii, Linked.Col])[jj]
              if (!is.na(COL)) {
                Data[N == i, ..COL] == Data[N == Z[ii, N], ..COL]
              } else {
                !is.na(unlist(Z$Prac.Code)[ii])
              }
            } else {
              if (Verbose) print(paste0("Simple: ii = ", ii, " | jj = ", jj))
              TRUE
            }
          }
        }))
      })
      
      # Check all validation results
      Z[, Level.Check := all(unlist(Level.Check)), by = "Y.N"]
      
      if (Debug) {
        return(Z)
      } else {
        return(Z[Level.Check == TRUE, N])
      }
    } else {
      return(NA)
    }
  })
}