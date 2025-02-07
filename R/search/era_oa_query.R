#' @title Run OpenAlex Query for a Single Search
#'
#' @description This function runs a single query against the OpenAlex API for scientific works. It accepts either a preformatted boolean-encoded search string or a list of term vectors (to be combined into a boolean query).
#'
#' @param search_terms Either a list of vectors with terms or a preformatted boolean-encoded string.
#' @param from_date The start date for the publication date filter "YYYY-MM-DD".
#' @param to_date The end year for the publication date filter "YYYY-MM-DD".
#' @param continent The continent for filtering works by authorship institutions. Default is `NULL` (no continent filter).
#' @param download A logical value indicating whether to download the search results or only return the number of search hits. Default is `TRUE`.
#' @param full A logical value indicating whether to download the full set of data fields (including abstract) or just title and DOI. Default is `FALSE`.
#' @param max_char_limit The maximum number of characters allowed in the API request URL. Default is `4000`.
#'
#' @return A `data.table` containing the downloaded search results.
#' 
#' @import data.table
#' @importFrom openalexR oa_query oa_request oa2df
#' 
#' @details This function can either generate a boolean-encoded search string from a list of vectors (where each level is combined with `AND` and terms within each vector are combined with `OR`), or it can take a preformatted search string. The function queries the OpenAlex API and returns the result.
#'
#' @examples
#' \dontrun{
#' # Example of running the function with search terms provided as a list of vectors
#' search_terms <- list(
#'   c("animal performance", "ADG", "average daily gain"),
#'   c("goats", "sheep", "cattle"),
#'   c("rations", "feeds", "diets")
#' )
#' 
#' This represents the boolean logic:
#' (animal performance OR ADG OR average daily gain) AND (goats OR sheep OR cattle") AND (rations OR feeds OR diets)
#' 
#' results <- run_openalex_query_single(search_terms = search_terms, from_year = 2020, to_year = 2023)
#'
#' # Example with a preformatted boolean-encoded string
#' search_string <- "(\"animal performance\" OR ADG OR \"average daily gain\") AND (goats OR sheep OR cattle)"
#' results <- run_openalex_query(search_terms = search_string, from_year = 2020, to_year = 2023)
#' }
#' 
#' @export
era_oa_query <- function(search_terms, 
                         from_date = NULL, 
                         to_date = NULL, 
                         continent = NULL, 
                         download = TRUE,
                         full = FALSE, 
                         max_char_limit = 4000) {
  
  # Helper function: returns TRUE if the query contains the boolean operators AND or OR
  is_boolean_query <- function(query) {
    grepl("\\b(AND|OR)\\b", query)
  }
  
  if (is.null(from_date) | is.null(to_date)) {
    stop("from_date and/or to_date not provided")
  }
  
  if (from_date > to_date) {
    stop("from_date > to_date")
  }
  
  # If search_terms is a list, generate a boolean search string
  if (is.list(search_terms)) {
    search_strings <- lapply(search_terms, function(terms) {
      terms <- add_quotes(terms)
      paste0("(", paste(terms, collapse = " OR "), ")")
    })
    search_string <- paste0(search_strings, collapse = " AND ")
  } else {
    # If a character vector (but not a list) is provided, collapse to a single string if necessary.
    if (is.character(search_terms) && length(search_terms) > 1) {
      search_terms <- paste(search_terms, collapse = " ")
    }
    
    # If the provided string is not a boolean query, convert it
    if (!is_boolean_query(search_terms)) {
      # Assume it is a plain string of terms separated by whitespace
      terms <- unlist(strsplit(search_terms, "\\s+"))
      terms <- add_quotes(terms)
      search_string <- paste0("(", paste(terms, collapse = " OR "), ")")
    } else {
      search_string <- search_terms
    }
  }
  
  # The remainder of your function remains unchanged...
  
  if (is.null(continent)) {
    api_endpoint <- oa_query(
      entity = "works",
      title_and_abstract.search = search_string,
      from_publication_date = from_date,
      to_publication_date = to_date
    )
  } else {
    api_endpoint <- oa_query(
      entity = "works",
      title_and_abstract.search = search_string,
      authorships.institutions.continent = continent,
      from_publication_date = from_date,
      to_publication_date = to_date
    )
  }
  
  if (!full) {
    api_endpoint <- paste0(api_endpoint, "&select=title,doi")
  }
  
  if (nchar(api_endpoint) > max_char_limit) {
    stop(paste0("Encoded search string has ", nchar(api_endpoint), 
                " characters. Max allowed is ", max_char_limit))
  }
  
  search_hits <- oa_request(query_url = api_endpoint, count_only = TRUE)$count
  cat("search hits =", search_hits, "\n")
  
  meta_data <- data.table(string = search_string,
                          api_endpoint = api_endpoint,
                          date_search_ran = Sys.Date(),
                          from_date = from_date,
                          to_date = to_date)
  
  if (download) {
    if (full) { 
      cat("Running query - full")
    } else {
      cat("Running query - title & doi only")
    }
    hits <- oa_request(query_url = api_endpoint)
    hits_tab <- data.table(oa2df(hits, entity = "works", verbose = FALSE))
    
    if (full) {
      hits_tab <- hits_tab[, .(id, display_name, author, ab, doi, url, relevance_score, 
                               is_oa, language, type, publication_date)]
      
      hits_tab[, authors := unlist(lapply(1:nrow(hits_tab), FUN = function(i) {
        authors <- hits_tab[i, author][[1]]
        if (length(authors) > 1) {
          paste0(authors[, "au_display_name"], collapse = ";")
        } else {
          NA
        }
      }))][, author := NULL]
    }
    
    return(list(results = hits_tab, meta_data = meta_data))
  } else {
    meta_data$search_hits <- search_hits
    return(meta_data)
  }
}
#' @title Add Quotes to Multi-word Terms
#'
#' @description This function adds quotes around terms that contain spaces, 
#' which is often necessary when constructing boolean search queries. 
#' If a term contains one or more spaces, it will be wrapped in quotes. 
#' Single-word terms will be returned without modification.
#'
#' @param vector A character vector containing search terms.
#'
#' @return A character vector where multi-word terms are quoted using the appropriate syntax for the system.
#'
#' @details This function is useful when preparing terms for search queries, where terms with spaces must be enclosed in quotes to ensure correct interpretation.
#'
#' @importFrom utils shQuote
#'
#' @examples
#' terms <- c("animal performance", "ADG", "average daily gain", "milk yield")
#' quoted_terms <- add_quotes(terms)
#' # quoted_terms will return:
#' # c("\"animal performance\"", "ADG", "\"average daily gain\"", "\"milk yield\"")
#'
#' @export
add_quotes <- function(vector) {
  # Apply to each term in the vector
  sapply(vector, function(term) {
    # If term contains a space, quote it
    if (grepl("\\s", term)) {
      return(shQuote(term, type = "cmd"))  # shQuote ensures proper quoting
    } else {
      return(term)  # If no spaces, return the term as is
    }
  }, USE.NAMES = FALSE)
}
