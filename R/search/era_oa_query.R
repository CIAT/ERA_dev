#' @title Run OpenAlex Query for a Single Search
#'
#' @description This function runs a query against the OpenAlex API for works.
#' It uses the helper function \code{build_search_string} to construct a boolean search string
#' from various input formats and then queries the API.
#'
#' @param search_terms Either:
#'   - A list of character vectors (blocks), or
#'   - A single character string (without boolean operators), or
#'   - A character vector of length > 1 (treated as one block).
#' @param from_date The start publication date in "YYYY-MM-DD" format.
#' @param to_date The end publication date in "YYYY-MM-DD" format.
#' @param continent (Optional) A continent filter for authorship institutions.
#' @param download Logical. If \code{TRUE} (default), the function downloads the search results.
#' @param full Logical. If \code{TRUE}, a full set of fields is downloaded (including abstracts);
#'   if \code{FALSE} (default) only title and DOI are returned.
#' @param max_char_limit Maximum allowed characters in the constructed API URL (default: 4000).
#' @param max_term Maximum number of tokens (as estimated by advanced token splitting) allowed in the query;
#'   if exceeded, a warning is issued (default: 100).
#' @param within_block_operators A character vector (or single value) specifying how to combine
#'   terms within each block. Default is \code{"OR"}.
#' @param between_block_operators A character vector (or single value) specifying how to combine
#'   the blocks. Default is \code{"AND"}.
#' @param quote_terms Logical. If \code{TRUE}, then terms are quoted for use with the OpenAlex API.
#'   This affects both multi-term blocks and single-term blocks that contain boolean operators.
#'   Default is \code{TRUE}.
#'
#' @return If \code{download = TRUE}, a list with two components:
#'   \item{results}{A \code{data.table} of downloaded results.}
#'   \item{meta_data}{A \code{data.table} containing metadata about the query.}
#' If \code{download = FALSE}, only the metadata is returned.
#'
#' @details The function first validates the provided dates.
#' It then calls \code{build_search_string} to convert the \code{search_terms} into a single boolean query string.
#' This query string is used to build an API endpoint via \code{oa_query}.
#' If \code{full = FALSE}, only minimal fields (title and DOI) are requested.
#' The function then checks the length of the API endpoint and finally retrieves either the full results
#' or just the metadata (including search hit count) via \code{oa_request}.
#'
#' @examples
#' \dontrun{
#' # Define two search configurations:
#' search1 <- list(
#'   term1 = c("crop production", "animal production", "dairy production"),
#'   term2 = c("mexico", "brazil", "colombia"),
#'   term3 = c("usa", "canada")
#' )
#'
#' search2 <- list(
#'   term1 = "crop production NOT (animal production OR dairy production)",
#'   term2 = c("mexico", "brazil", "colombia"),
#'   term3 = c("usa", "canada")
#' )
#'
#' result1 <- era_oa_query(
#'   search_terms = search1,
#'   from_date = "2020-01-01",
#'   to_date   = "2025-02-18",
#'   within_block_operators  = c("OR", "OR", "AND"),
#'   between_block_operators = c("AND", "NOT"),
#'   max_term       = 100,
#'   quote_terms    = TRUE,
#'   max_char_limit = 1000
#' )
#'
#' result2 <- era_oa_query(
#'   search_terms = search2,
#'   from_date = "2020-01-01",
#'   to_date   = "2025-02-18",
#'   within_block_operators  = c("OR", "OR", "AND"),
#'   between_block_operators = c("AND", "NOT"),
#'   max_term       = 100,
#'   quote_terms    = TRUE,
#'   max_char_limit = 1000
#' )
#'
#' # Using result1 to build an API endpoint:
#' string <- result1$meta_data$query
#' api_endpoint <- oa_query(
#'   entity = "works",
#'   title_and_abstract.search = string,
#'   from_publication_date = "2020-01-01",
#'   to_publication_date   = "2025-02-18"
#' )
#' api_endpoint <- paste0(api_endpoint, "&select=title,doi")
#' search_hits <- oa_request(query_url = api_endpoint, count_only = TRUE)$count
#' cat("Search hits =", search_hits, "\n")
#' }
#'
#' @import data.table
#' @importFrom openalexR oa_query oa_request oa2df
#' @export
era_oa_query <- function(search_terms,
                         from_date = NULL,
                         to_date = NULL,
                         continent = NULL,
                         download = TRUE,
                         full = FALSE,
                         max_char_limit = 4000,
                         max_term = 100,
                         within_block_operators = "OR",
                         between_block_operators = "AND",
                         quote_terms = TRUE,
                         verbose=T) {
  
  # Validate dates
  if (is.null(from_date) || is.null(to_date)) {
    stop("from_date and/or to_date not provided")
  }
  if (from_date > to_date) {
    stop("from_date is greater than to_date")
  }
  
  # Build the boolean search string using the helper function.
  search_string <- build_search_string2(
    search_terms = search_terms,
    within_block_operators = within_block_operators,
    between_block_operators = between_block_operators,
    max_term = max_term,
    quote_terms = quote_terms,
    max_char_limit = max_char_limit
  )
  
  tokens <- unlist(strsplit(gsub("[()]", "", search_string), "\\s+(?i:AND|OR|NOT)\\s+", perl = TRUE))
  if(verbose){cat("Token count =", length(tokens), "\n")}
  if (length(tokens) > max_term) {
    stop(
      "Number of terms (", length(tokens),
      ") exceeds the maximum allowed (", max_term, ")."
    )
  }
  
  
  # Construct the API endpoint using openalexR's oa_query.
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
  
  # If only minimal fields are needed, append the select parameter.
  if (!full) {
    api_endpoint <- paste0(api_endpoint, "&select=title,doi")
  }
  
  # Check the length of the API endpoint.
  if (nchar(api_endpoint) > max_char_limit) {
    stop("Encoded search string has ", nchar(api_endpoint),
         " characters, which exceeds the maximum allowed (", max_char_limit, ").")
  }
  
  # Get the number of search hits.
  search_hits <- oa_request(query_url = api_endpoint, count_only = TRUE)$count
  if(verbose){cat("Search hits =", search_hits, "\n")}
  
  if (download) {
    if(verbose){cat("Downloading query results...\n")}
    hits <- oa_request(query_url = api_endpoint)
    hits_tab <- data.table(oa2df(hits, entity = "works", verbose = FALSE))
    
    if (full) {
      hits_tab <- hits_tab[, .(id, display_name, author, ab, doi, url,
                               relevance_score, is_oa, language, type, publication_date)]
      hits_tab[, authors := sapply(seq_len(nrow(hits_tab)), function(i) {
        auth <- hits_tab[i, author][[1]]
        if (length(auth) > 1) {
          paste0(auth[, "au_display_name"], collapse = ";")
        } else {
          NA
        }
      })]
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
#' @title Add Quotes to Multi-word Terms
#'
#' @description This function adds quotes around terms that contain spaces.
#'
#' @param vector A character vector containing search terms.
#'
#' @return A character vector where terms that have spaces are wrapped in quotes.
#'
#' @importFrom utils shQuote
#' @export
add_quotes <- function(vector) {
  sapply(vector, function(term) {
    if (grepl("\\s", term)) {
      shQuote(term, type = "cmd")
    } else {
      term
    }
  }, USE.NAMES = FALSE)
}

#' @title Build a Boolean Search String (with Advanced Term Splitting and Optional Quoting)
#'
#' @description Constructs a single boolean-encoded search string from either:
#'   - A list of term vectors (blocks) with optional within-block and between-block operators,
#'   - A single character string (without boolean operators),
#'   - A character vector of length > 1 (treated as a single block).
#'
#' This version uses advanced token splitting to estimate term count and, when requested,
#' will parse blocks that already contain boolean operators so that individual terms are quoted.
#'
#' @param search_terms 
#'   - A list of character vectors (blocks), \strong{or}
#'   - A single character string (without boolean operators), \strong{or}
#'   - A character vector of length > 1 (treated as a single block).
#' @param within_block_operators A character vector or a single string specifying how to combine 
#'   \emph{terms within each block}. If you provide n blocks, you can supply a single operator
#'   (applied to all blocks) or a vector of length n.
#'   \strong{Default}: `"OR"`.
#' @param between_block_operators A character vector specifying how to combine the blocks.
#'   If there are n blocks, you need n-1 operators (or a single string will be repeated).
#'   \strong{Default}: `"AND"`.
#' @param quote_terms Logical. If \code{TRUE}, then terms are quoted for use with the OpenAlex API.
#'   In particular, if a block is given as a single string that contains boolean operators,
#'   its individual tokens will be parsed and quoted.
#'   \strong{Default}: \code{FALSE}.
#' @param max_term Maximum number of terms allowed (across all blocks). If exceeded, a warning is issued.
#' @param max_char_limit Maximum length of the final search string. If exceeded, a warning is issued.
#'
#' @return A single character string containing the combined boolean query.
#'
#' @details 
#' \strong{Behavior}:
#' \enumerate{
#'   \item For a list input, each element (block) can have multiple terms or a single term.
#'     \itemize{
#'       \item For blocks with multiple terms, the terms are optionally quoted (if \code{quote_terms} is \code{TRUE})
#'         and then joined by the within-block operator.
#'       \item For a block with a single term:
#'         \itemize{
#'           \item If that term contains boolean operators (a "simple boolean" expression):
#'             \itemize{
#'               \item If \code{quote_terms} is \code{TRUE}, the expression is parsed so that each non-operator token is quoted.
#'               \item Otherwise, the expression is used as is.
#'             }
#'           \item Otherwise, the term is quoted only if \code{quote_terms} is \code{TRUE}.
#'         }
#'       \item Blocks are concatenated using the between-block operators.
#'     }
#'   \item If \code{search_terms} is a single string (not in a list) that contains boolean operators,
#'     an error is thrown (forcing the user to wrap it in a list for parsing).
#'   \item If \code{search_terms} is a character vector of length > 1, it is treated as one block,
#'     with terms joined by the first within-block operator.
#' }
#'
#' The function uses advanced token splitting (splitting on any instance, case-insensitive, of AND, OR, or NOT)
#' to approximate the number of terms.
#'
#' @examples
#' \dontrun{
#' # Define two search configurations:
#'
#' # Example 1: A list with three blocks.
#' # Block 1: Three terms that will be quoted and joined by OR.
#' # Block 2: Three country names.
#' # Block 3: Two country names that will be joined by AND.
#' search1 <- list(
#'   term1 = c("crop production", "animal production", "dairy production"),
#'   term2 = c("mexico", "brazil", "colombia"),
#'   term3 = c("usa", "canada")
#' )
#'
#' # Example 2: A list with three blocks, but the first block is a single string
#' # that already contains a boolean expression.
#' search2 <- list(
#'   term1 = "crop production NOT (animal production OR dairy production)",
#'   term2 = c("mexico", "brazil", "colombia"),
#'   term3 = c("usa", "canada")
#' )
#'
#' # Build the search strings with quote_terms = TRUE.
#' result1 <- build_search_string(
#'   search_terms = search1,
#'   within_block_operators  = c("OR", "OR", "AND"),
#'   between_block_operators = c("AND", "NOT"),
#'   max_term       = 100,
#'   quote_terms    = TRUE,
#'   max_char_limit = 1000
#' )
#'
#' result2 <- build_search_string(
#'   search_terms = search2,
#'   within_block_operators  = c("OR", "OR", "AND"),
#'   between_block_operators = c("AND", "NOT"),
#'   max_term       = 100,
#'   quote_terms    = TRUE,
#'   max_char_limit = 1000
#' )
#'
#' # Expected outputs:
#' # result1:
#' #   ("crop production" OR "animal production" OR "dairy production") AND 
#' #   ("mexico" OR "brazil" OR "colombia") NOT ("usa" AND "canada")
#'
#' # result2:
#' #   ("crop production" NOT ("animal production" OR "dairy production")) AND 
#' #   ("mexico" OR "brazil" OR "colombia") NOT ("usa" AND "canada")
#'
#' # Example of using the resulting string in an API query:
#' string <- result1
#' api_endpoint <- oa_query(
#'   entity = "works",
#'   title_and_abstract.search = string,
#'   from_publication_date = "2020-01-01",
#'   to_publication_date   = "2025-02-18"
#' )
#'
#' api_endpoint <- paste0(api_endpoint, "&select=title,doi")
#' search_hits <- oa_request(query_url = api_endpoint, count_only = TRUE)$count
#' cat("Chunk 1 search hits =", search_hits, "\n")
#' }
#'
#' @export
build_search_string<- function(search_terms,
                                within_block_operators = "OR",
                                between_block_operators = "AND",
                                max_term = 100,
                                max_char_limit = 4000,
                                quote_terms = TRUE) {
  # Process when search_terms is a list:
  if (is.list(search_terms)) {
    n_blocks <- length(search_terms)
    block_strings <- vector("character", n_blocks)
    
    # Ensure within_block_operators is a vector of length n_blocks
    if (length(within_block_operators) == 1) {
      within_block_operators <- rep(within_block_operators, n_blocks)
    } else if (length(within_block_operators) != n_blocks) {
      stop("Length of within_block_operators must be 1 or equal to the number of blocks")
    }
    
    # Process each block individually
    for (i in seq_len(n_blocks)) {
      block <- search_terms[[i]]
      op <- within_block_operators[i]
      if (length(block) > 1) {
        # If block is a vector of terms, simply wrap each term in quotes (if requested) and join them.
        if (quote_terms) {
          block_strings[i] <- paste0("(", paste0('"', block, '"', collapse = paste0(" ", op, " ")), ")")
        } else {
          block_strings[i] <- paste0("(", paste(block, collapse = paste0(" ", op, " ")), ")")
        }
      } else if (length(block) == 1) {
        # For a single-element block:
        if (grepl("(?i)\\b(AND|OR|NOT)\\b", block[1])) {
          # The block appears preformatted; apply a simple split on " OR " to quote individual tokens.
          tokens <- strsplit(block[1], "\\s+OR\\s+", perl = TRUE)[[1]]
          if (quote_terms) {
            block_strings[i] <- paste0("(", paste0('"', tokens, '"', collapse = " OR "), ")")
          } else {
            block_strings[i] <- paste0("(", block[1], ")")
          }
        } else {
          # Single plain phrase:
          if (quote_terms) {
            block_strings[i] <- paste0("(", '"', block[1], '"', ")")
          } else {
            block_strings[i] <- paste0("(", block[1], ")")
          }
        }
      } else {
        stop("Empty block encountered.")
      }
    }
    
    # Combine blocks with between_block_operators
    if (n_blocks == 1) {
      search_string <- block_strings[1]
    } else {
      if (length(between_block_operators) == 1) {
        between_block_operators <- rep(between_block_operators, n_blocks - 1)
      } else if (length(between_block_operators) != (n_blocks - 1)) {
        stop("Length of between_block_operators must be 1 or equal to n_blocks - 1")
      }
      search_string <- block_strings[1]
      for (j in seq_len(n_blocks - 1)) {
        search_string <- paste(search_string, between_block_operators[j], block_strings[j + 1])
      }
    }
  } else if (is.character(search_terms)) {
    # If search_terms is a character vector (not a list)
    if (length(search_terms) > 1) {
      # Treat it as one block
      if (quote_terms) {
        search_string <- paste0("(", paste0('"', search_terms, '"', collapse = " OR "), ")")
      } else {
        search_string <- paste0("(", paste(search_terms, collapse = " OR "), ")")
      }
    } else {
      # Single string provided
      if (quote_terms && grepl("(?i)\\b(AND|OR|NOT)\\b", search_terms[1])) {
        stop("This function cannot yet quote a boolean search string provided as a string.")
      } else {
        if (quote_terms) {
          search_string <- paste0("(", '"', search_terms[1], '"', ")")
        } else {
          search_string <- paste0("(", search_terms[1], ")")
        }
      }
    }
  } else {
    stop("search_terms must be either a list or a character vector.")
  }
  
  # (Optional) Check for max_term and max_char_limit could be added here.
  
  return(search_string)
}
