#############################################################################
# Load necessary libraries
library(dplyr)
library(stringr)
library(stringdist)
library(readxl)

# Function to clean and deduplicate OpenAlex results
deduplicate_openalex <- function(data, key_columns = c("display_name", "doi"), abstract_column = "ab", similarity_threshold = 0.85) {
  
  # Ensure column names are clean
  colnames(data) <- trimws(colnames(data))  # Remove leading/trailing spaces
  colnames(data) <- iconv(colnames(data), to = "ASCII//TRANSLIT")  # Fix encoding
  
  # Check for missing columns
  required_columns <- c(key_columns, abstract_column)
  missing_cols <- setdiff(required_columns, colnames(data))
  if (length(missing_cols) > 0) {
    stop("Missing columns in dataset: ", paste(missing_cols, collapse = ", "))
  }
  
  # Convert relevant columns to lowercase and trim spaces
  clean_data <- data %>%
    mutate(across(all_of(key_columns), ~ str_to_lower(str_trim(.)), .names = "clean_{.col}"),
           across(all_of(abstract_column), ~ iconv(., from = "", to = "UTF-8", sub = ""), .names = "clean_{.col}"))  # Ensure UTF-8 encoding
  
  # Create a unique key based only on title & DOI
  clean_data <- clean_data %>%
    mutate(unique_key = paste(clean_display_name, clean_doi, sep = "||")) 
  
  # Remove exact duplicates
  deduplicated_data <- clean_data %>%
    distinct(unique_key, .keep_all = TRUE) %>%
    select(-starts_with("clean_"), -unique_key)  # Drop helper columns
  
  # Handle missing abstracts separately
  has_abstract <- deduplicated_data %>%
    filter(!is.na(!!sym(abstract_column)) & nchar(!!sym(abstract_column)) > 50)  # Ignore very short abstracts
  
  no_abstract <- deduplicated_data %>%
    filter(is.na(!!sym(abstract_column)) | nchar(!!sym(abstract_column)) <= 50)  # Keep NA abstracts separately
  
  # Print the number of abstracts before processing
  cat("Original dataset size:", nrow(data), "\n")
  cat("After removing exact duplicates:", nrow(deduplicated_data), "\n")
  cat("Rows with abstracts:", nrow(has_abstract), "\n")
  cat("Rows without abstracts:", nrow(no_abstract), "\n")
  
  # Remove rows with highly similar abstracts (≥85% similarity)
  if (nrow(has_abstract) > 1) {
    n <- nrow(has_abstract)
    to_remove <- logical(n)  # Track rows to remove
    
    for (i in 1:(n-1)) {
      for (j in (i+1):n) {
        if (!to_remove[i] & !to_remove[j]) {  # Skip already removed rows
          abs1 <- has_abstract[[abstract_column]][i]
          abs2 <- has_abstract[[abstract_column]][j]
          
          # Ensure UTF-8 encoding for abstracts before comparison
          abs1 <- iconv(abs1, from = "", to = "UTF-8", sub = "")
          abs2 <- iconv(abs2, from = "", to = "UTF-8", sub = "")
          
          if (!is.na(abs1) & !is.na(abs2) & nchar(abs1) > 50 & nchar(abs2) > 50) {  
            sim_score <- 1 - stringdist::stringdist(abs1, abs2, method = "cosine")  
            
            # Print comparison info before removing
            if (sim_score >= similarity_threshold) {
              cat("\nRemoving row", j, "due to high similarity with row", i, "\n")
              cat("Abstract 1:", substr(abs1, 1, 200), "\n")  # Print first 200 chars
              cat("Abstract 2:", substr(abs2, 1, 200), "\n")
              cat("Similarity Score:", sim_score, "\n")
              
              to_remove[j] <- TRUE  # Mark row for removal
            }
          }
        }
      }
    }
    
    # Keep only non-duplicate abstracts and add back NA abstracts
    has_abstract <- has_abstract[!to_remove, ]
  }
  
  # Combine cleaned abstracts with NA abstracts
  final_data <- bind_rows(has_abstract, no_abstract)
  
  # Print final dataset size
  cat("\nFinal dataset size after abstract deduplication:", nrow(final_data), "\n")
  
  return(final_data)
}


# Example Usage:
# df <- read.csv("openalex_results.csv")  # Load OpenAlex dataset
# clean_df <- deduplicate_openalex(df)
# write.csv(clean_df, "cleaned_openalex_results.csv", row.names = FALSE)

# Example usage
df <- read_excel("C:/Users/JNamita/OneDrive - CGIAR/ERA/Bibliography/2024 Livestock/livestock_papers_2024_tagging - Copy.xlsx") # Load your OpenAlex dataset


# Convert all character columns to UTF-8
df <- df %>%
  mutate(across(where(is.character), ~ iconv(., from = "", to = "UTF-8", sub = "")))

# Save a UTF-8 version to check if encoding issues persist
write.csv(df, "cleaned_utf8_dataset.csv", row.names = FALSE)
df <- df %>%
  mutate(across(where(is.character), ~ iconv(., from = "", to = "UTF-8", sub = " ")))
df$ab <- gsub("[^\x20-\x7E]", "", df$ab)  # Removes all non-ASCII characters

clean_df <- deduplicate_openalex(df, abstract_column = "ab")
