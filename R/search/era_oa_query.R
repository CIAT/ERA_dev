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
                         max_char_limit = 4000,
                         max_term = 100) {  # maximum number of terms per query
  is_boolean_query <- function(query) {
    any(grepl("\\b(AND|OR)\\b", query))
  }
  
  
  # Validate dates
  if (is.null(from_date) || is.null(to_date)) {
    stop("from_date and/or to_date not provided")
  }
  if (from_date > to_date) {
    stop("from_date > to_date")
  }
  
  #### Build the Boolean Search String ####
  if (is.list(search_terms)) {
    # Each element of the list is assumed to be a vector of terms;
    # each block is built by joining terms with OR,
    # and blocks are combined with AND.
    search_strings <- lapply(search_terms, function(terms) {
      terms <- add_quotes(terms)
      paste0("(", paste(terms, collapse = " OR "), ")")
    })
    search_string <- paste0(search_strings, collapse = " AND ")
    
  } else if (is.character(search_terms)) {
    if (length(search_terms) == 1) {
      # A single string was provided.
      if (!is_boolean_query(search_terms)) {
        # Instead of splitting on whitespace, treat the entire string as one phrase.
        term <- add_quotes(search_terms)
        search_string <- paste0("(", term, ")")
      } else {
        # Already a boolean query.
        search_string <- search_terms
      }
      
    } else {
      # A character vector of length > 1 is assumed to contain full terms/phrases.
      # Each element is treated as one term.
      terms <- add_quotes(search_terms)
      search_string <- paste0("(", paste(terms, collapse = " OR "), ")")
    }
    
  } else {
    stop("search_terms must be either a list or a character vector.")
  }
  
  #### Count the Terms and Decide if Splitting is Needed ####
  # Remove any parentheses and split on " OR " (assuming a flat OR query)
  all_terms <- unlist(strsplit(gsub("[()]", "", search_string), "\\s+OR\\s+"))
  
  if (length(all_terms) > max_term) {
    num_chunks <- ceiling(length(all_terms) / max_term)
    
    # In interactive sessions, prompt the user.
    do_download <- TRUE
    if (interactive()) {
      user_ans <- readline(paste0("There are ", length(all_terms), 
                                  " search terms, which will be split into ", 
                                  num_chunks, " chunks. Downloading all references may take a long time. ",
                                  "Do you want to download all references? [y/n]: "))
      if (tolower(trimws(user_ans)) == "n") {
        do_download <- FALSE
      }
    }
    
    message("More than ", max_term, " search terms detected; splitting into ", num_chunks, " chunks.")
    
    # Split the terms into chunks (each with at most max_term terms)
    term_chunks <- split(all_terms, ceiling(seq_along(all_terms) / max_term))
    results_list <- list()
    meta_data_list <- list()
    
    for (i in seq_along(term_chunks)) {
      chunk_terms <- term_chunks[[i]]
      # Rebuild a query string for this chunk (using OR between terms)
      chunk_query <- paste0("(", paste(chunk_terms, collapse = " OR "), ")")
      
      # Build the API endpoint for the current chunk.
      if (is.null(continent)) {
        api_endpoint_chunk <- oa_query(
          entity = "works",
          title_and_abstract.search = chunk_query,
          from_publication_date = from_date,
          to_publication_date = to_date
        )
      } else {
        api_endpoint_chunk <- oa_query(
          entity = "works",
          title_and_abstract.search = chunk_query,
          authorships.institutions.continent = continent,
          from_publication_date = from_date,
          to_publication_date = to_date
        )
      }
      
      if (!full) {
        api_endpoint_chunk <- paste0(api_endpoint_chunk, "&select=title,doi")
      }
      
      if (nchar(api_endpoint_chunk) > max_char_limit) {
        stop("Encoded search string chunk has ", nchar(api_endpoint_chunk),
             " characters. Max allowed is ", max_char_limit)
      }
      
      # Get the number of hits for this chunk (informational)
      search_hits_chunk <- oa_request(query_url = api_endpoint_chunk, count_only = TRUE)$count
      cat("Chunk", i, "search hits =", search_hits_chunk, "\n")
      
      if (do_download) {
        cat("Downloading chunk", i, "\n")
        hits_chunk <- oa_request(query_url = api_endpoint_chunk)
        hits_tab_chunk <- data.table(oa2df(hits_chunk, entity = "works", verbose = FALSE))
        
        if (full) {
          hits_tab_chunk <- hits_tab_chunk[, .(id, display_name, author, ab, doi, url, 
                                               relevance_score, is_oa, language, type, 
                                               publication_date)]
          # Convert author field from list to semicolon-separated string
          hits_tab_chunk[, authors := unlist(lapply(1:nrow(hits_tab_chunk), function(j) {
            authors <- hits_tab_chunk[j, author][[1]]
            if (length(authors) > 1) {
              paste0(authors[, "au_display_name"], collapse = ";")
            } else {
              NA
            }
          }))]
          hits_tab_chunk[, author := NULL]
        }
        results_list[[i]] <- hits_tab_chunk
      }
      
      meta_data_list[[i]] <- data.table(
        chunk = i,
        query = chunk_query,
        api_endpoint = api_endpoint_chunk,
        search_hits = search_hits_chunk,
        date_search_ran = Sys.Date()
      )
    }
    
    if (do_download) {
      # Combine results from all chunks, deduplicate, and return.
      combined_results <- unique(rbindlist(results_list))
      combined_meta <- rbindlist(meta_data_list)
      overall_meta <- data.table(
        query = search_string,
        date_search_ran = Sys.Date(),
        total_chunks = length(term_chunks),
        total_search_hits = sum(combined_meta$search_hits)
      )
      return(list(results = combined_results, meta_data = overall_meta))
    } else {
      # User declined download; return the per-chunk metadata.
      combined_meta <- rbindlist(meta_data_list)
      return(combined_meta)
    }
    
  } else {
    #### No Splitting Required: Proceed with a Single Query ####
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
      stop("Encoded search string has ", nchar(api_endpoint), 
           " characters. Max allowed is ", max_char_limit)
    }
    
    search_hits <- oa_request(query_url = api_endpoint, count_only = TRUE)$count
    cat("Search hits =", search_hits, "\n")
    
    if (download) {
      cat("Downloading query results\n")
      hits <- oa_request(query_url = api_endpoint)
      hits_tab <- data.table(oa2df(hits, entity = "works", verbose = FALSE))
      
      if (full) {
        hits_tab <- hits_tab[, .(id, display_name, author, ab, doi, url, 
                                 relevance_score, is_oa, language, type, publication_date)]
        hits_tab[, authors := unlist(lapply(1:nrow(hits_tab), function(j) {
          authors <- hits_tab[j, author][[1]]
          if (length(authors) > 1) {
            paste0(authors[, "au_display_name"], collapse = ";")
          } else {
            NA
          }
        }))]
        hits_tab[, author := NULL]
      }
      
      meta_data <- data.table(
        query = search_string,
        api_endpoint = api_endpoint,
        search_hits = search_hits,
        date_search_ran = Sys.Date()
      )
      return(list(results = hits_tab, meta_data = meta_data))
      
    } else {
      meta_data <- data.table(
        query = search_string,
        api_endpoint = api_endpoint,
        search_hits = search_hits,
        date_search_ran = Sys.Date()
      )
      return(meta_data)
    }
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
