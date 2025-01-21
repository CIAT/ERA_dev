#' compare_wrap: Parallel Wrapper for Treatment-Control Comparisons
#'
#' This wrapper function automates and optionally parallelizes the comparison of control and treatment groups 
#' across multiple experimental datasets (`B.Code` groups). It leverages `compare_fun` for the 
#' group-wise analysis, manages progress tracking, and handles edge cases such as groups without 
#' valid comparisons. For `worker_n = 1`, the function executes sequentially, allowing `Verbose` output.
#'
#' ## Key Features:
#' 1. Supports parallel processing using the `future` and `future.apply` packages.
#' 2. Tracks and reports progress for debugging and monitoring.
#' 3. Filters out experimental groups with no valid comparisons.
#' 4. Automatically switches to a sequential mode when `worker_n = 1`.
#' 5. Returns comparison results, non-comparable groups, and any errors encountered.
#'
#' @import data.table
#' @import future
#' @import future.apply
#' @import progressr
#' 
#' @param DATA A data.table containing experimental data. Must include columns like `B.Code` 
#'   (grouping identifier) and others required by `compare_fun`.
#' @param CompareWithin A character vector of column names to define subgroups within `B.Code`. 
#'   Comparisons are performed within these subgroups.
#' @param worker_n Integer; number of parallel workers to use. If set to 1, runs sequentially.
#' @param Verbose Logical; if TRUE, outputs detailed progress and debugging information.
#' @param Debug Logical; if TRUE, outputs raw intermediate data for inspection.
#' @param Return.Lists Logical; if TRUE, outputs comparisons as lists (useful for programmatic analysis). 
#'   If FALSE, results are returned as flat tables.
#' @param Fert.Method Data table detailing fertilizer applications.
#' @param Plant.Method Data table detailing planting methods and spacings.
#' @param Irrig.Method Data table detailing irrigation methods.
#' @param Res.Method Data table detailing residue management methods.
#' @param p_density_similarity_threshold Numeric; minimum similarity needed in planting density comparisons 
#'   (0–1, where 1 means 100% similar). Default = 0.95.
#' @return A list containing:
#'   - `data`: A data.table summarizing valid comparisons for each group.
#'   - `groups_n1`: Groups with only one entry, where no comparisons were possible.
#'   - `all_groups_n1`: Groups excluded entirely due to lack of valid comparisons.
#'   - `no_comparison`: Groups where comparisons were attempted but yielded no results.
#' @note Requires the `future`, `future.apply`, `progressr`, and `data.table` packages.
#' @examples
#' result <- compare_wrap(
#'   DATA = experimental_data,
#'   CompareWithin = c("Region", "Crop"),
#'   worker_n = 4,
#'   Verbose = TRUE,
#'   Debug = FALSE,
#'   Return.Lists = FALSE,
#'   Fert.Method = fert_method_data,
#'   Plant.Method = plant_method_data,
#'   Irrig.Method = irrig_method_data,
#'   Res.Method = res_method_data,
#'   p_density_similarity_threshold = 0.95
#' )
#'
#' print(result)
#'
compare_wrap <- function(DATA,
                         CompareWithin,
                         worker_n,
                         Verbose,
                         Debug = FALSE,
                         Return.Lists = FALSE,
                         Fert.Method,
                         Plant.Method,
                         Irrig.Method,
                         Res.Method,
                         p_density_similarity_threshold = 0.95) {
  
  # Function to process a single B.Code
  process_b_code <- function(ii, b_codes, DATA, CompareWithin, Verbose, Debug, Return.Lists,
                             Fert.Method, Plant.Method, Irrig.Method, Res.Method, p_density_similarity_threshold) {
    BC <- b_codes[ii]
    Data.Sub <- DATA[B.Code == BC]
    CW <- unique(Data.Sub[, ..CompareWithin]) 
    CW <- match(
      apply(Data.Sub[, ..CompareWithin], 1, paste, collapse = "-"),
      apply(CW, 1, paste, collapse = "-")
    )
    Data.Sub[, Group := CW]
    
    group_n <- Data.Sub[, .N, by = Group]
    no_comparison <- Data.Sub[Group %in% group_n[N == 1, Group], ..CompareWithin]
    Data.Sub <- Data.Sub[Group %in% group_n[N > 1, Group]]
    
    if (nrow(Data.Sub) > 0) {
      comp_dat <- rbindlist(lapply(unique(Data.Sub$Group), function(i) {
        if (Verbose) {
          cat(BC, " Subgroup = ", i, "\n")
        }
        Data <- Data.Sub[Group == i]
        compare_fun(
          Data = Data,
          Debug = Debug,
          Verbose = Verbose,
          PracticeCodes = PracticeCodes,
          Fert.Method = Fert.Method,
          Plant.Method = Plant.Method,
          Irrig.Method = Irrig.Method,
          Res.Method = Res.Method,
          p_density_similarity_threshold = p_density_similarity_threshold,
          Return.Lists = Return.Lists
        )
      }))
    } else {
      comp_dat <- NULL
    }
    
    list(
      data = comp_dat,
      groups_n1 = no_comparison,
      all_groups_n1 = if (is.null(comp_dat)) BC else NULL,
      no_comparison = if (!is.null(comp_dat) && nrow(comp_dat) == 0) BC else NULL
    )
  }
  
  # Determine execution mode: sequential or parallel
  b_codes <- unique(DATA[, B.Code])
  if (worker_n == 1) {
    # Sequential execution
    Comparisons <- lapply(seq_along(b_codes), function(ii) {
      process_b_code(ii, b_codes, DATA, CompareWithin, Verbose, Debug, Return.Lists,
                     Fert.Method, Plant.Method, Irrig.Method, Res.Method, p_density_similarity_threshold)
    })
  } else {
    # Parallel execution
    plan(multisession, workers = worker_n)
    handlers(global = TRUE)
    handlers("progress")
    Comparisons <- with_progress({
      b_codes <- unique(DATA[, B.Code])
      p <- progressor(steps = length(b_codes))
      future_lapply(seq_along(b_codes), function(ii) {
        # Only pass the required subset of DATA to workers
        Data.Sub <- DATA[B.Code == b_codes[ii]]
        process_b_code(ii, b_codes, DATA=Data.Sub, CompareWithin, Verbose, Debug, Return.Lists,
                       Fert.Method, Plant.Method, Irrig.Method, Res.Method, p_density_similarity_threshold)
      }, future.seed = TRUE)
    })
    plan(sequential)
  }
  
  groups_n1<-rbindlist(lapply(Comparisons,"[[","groups_n1")) # These unique groupings have only 1 row
  all_groups_n1<-unlist(lapply(Comparisons,"[[","all_groups_n1")) # There no unique groupings with >1 row in these publications
  no_comparison<-unlist(lapply(Comparisons,"[[","no_comparison")) # There no comparisons in these publications and there are groupings with >1 row
  Comparisons<-rbindlist(lapply(Comparisons,"[[","data"))
  
  # Remove NA values and no comparisons
  Comparisons<-Comparisons[!is.na(Control.For)][,Len:=length(unlist(Control.For)),by=N]
  Comparisons<-Comparisons[unlist(lapply(Comparisons$Control.For, length))>0]
  
  # Restructure results
  Cols<-c("T.Name","IN.Level.Name","R.Level.Name")
  Cols1<-c(CompareWithin,Cols)
  
  Comparisons1<-DATA[match(Comparisons[,N],N),..Cols1]
  Comparisons1[,Control.For:=Comparisons[,Control.For]]
  Comparisons1[,Control.N:=Comparisons[,N]]
  Comparisons1[,Mulch.Code:=Comparisons[,Mulch.Code]]
  setnames(Comparisons1,"T.Name","Control.Trt")
  setnames(Comparisons1,"IN.Level.Name","Control.Int")
  setnames(Comparisons1,"R.Level.Name","Control.Rot")
  
  Comparisons1[,Compare.Trt:=DATA[match(Control.For,N),T.Name]]
  Comparisons1[,Compare.Int:=DATA[match(Control.For,N),IN.Level.Name]]
  Comparisons1[,Compare.Rot:=DATA[match(Control.For,N),R.Level.Name]]
  
  results<-list(Comparisons_raw=Comparisons,Comparisons_processed=Comparisons1,groups_n1=groups_n1,all_groups_n1=all_groups_n1,no_comparison=no_comparison)
  
  return(results)
}
