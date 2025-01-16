# Code copied from elsewhere needs modification to work, currently not functional
pacman::p_load(readxl, tm, textstem, caret, xgboost, e1071,ggplot2,scales,tokenizers,Matrix,data.table,openalexR,pbapply)
  
  # Simple ####
  ERA_papers<-ERA.Compiled[Product.Type=="Animal" & grepl("Feed",PrName),unique(Code)]
  data<-ERA_Bibliography[ERACODE %in% ERA_papers]
  
  keywords<-tolower(trimws(unlist(strsplit(data$KEYWORDS,":;"))))
  keywords<-trimws(unlist(strsplit(keywords,",")))
  keywords<-keywords[!is.na(keywords)]
  
  keywords<-sort(table(keywords),decreasing = T)
  keywords[1:20]
  
  # ML basic ####
  # Combine abstract and keywords into one text column
  data[, combined_text := paste(ABSTRACT, KEYWORDS, sep = " ")]
  
  # Clean and preprocess text
  corpus <- Corpus(VectorSource(data$combined_text))
  corpus <- tm_map(corpus, content_transformer(tolower))
  corpus <- tm_map(corpus, removePunctuation)
  corpus <- tm_map(corpus, removeNumbers)
  corpus <- tm_map(corpus, removeWords, stopwords("en"))
  corpus <- tm_map(corpus, stripWhitespace)
  corpus <- tm_map(corpus, lemmatize_strings)
  
  # Identify the most frequent terms:
  dtm <- TermDocumentMatrix(corpus)
  m <- as.matrix(dtm)
  word_freqs <- sort(rowSums(m), decreasing = TRUE)
  
  # Get top N keywords; you can adjust N based on your needs
  top_keywords <- names(word_freqs)[1:20]
  
  # Example of constructing a simple Boolean query
  boolean_query <- paste(top_keywords, collapse = " OR ")
  
  # ML: Build terms with training data ####
  # Load 2013-2018 search history
  
  # Remove any papers we did not have FT access to
  data<-ERA_13_18_bib_cleaned[NoAccess==F,list(TITLE,ABSTRACT,KEYWORDS,DOI,ERACODE,Passed.FT,Passed.TA)]
  
  # Set papers not about animal feeding to excluded
  data[is.na(ERACODE) | !ERACODE %in% ERA_papers,c("Passed.FT","Passed.TA"):=list(NA,NA)]
  setnames(data,c("TITLE","ABSTRACT","KEYWORDS"),c("title","abstract","keywords"))
  
  data<-data[,screened:=0][Passed.FT==T,screened:=1][,list(title,abstract,keywords,screened)]
  
  
  # Add in additional papers that can be included
  data<-rbind(data,search_manual[!is.na(doi),list(title,abstract,keywords)][,screened:=1])
  
  # Downsample the rejected papers to the number of accepted papers
  accepted_papers <- data[data$screened == 1]
  rejected_papers <- data[data$screened == 0]
  
  # Sample rejected papers
  sampled_rejected <- rejected_papers[sample(.N, size = 10*nrow(accepted_papers))]
  
  # Combine accepted and sampled rejected papers
  balanced_data <- rbind(accepted_papers, sampled_rejected)
  
  # Shuffle the combined dataset
  set.seed(123)
  data <- balanced_data[sample(nrow(balanced_data)), ]
  
  # Clean and preprocess text
  prepare_text <- function(text) {
    corpus <- Corpus(VectorSource(text))
    corpus <- tm_map(corpus, content_transformer(tolower))
    corpus <- tm_map(corpus, removePunctuation)
    corpus <- tm_map(corpus, removeNumbers)
    corpus <- tm_map(corpus, removeWords, stopwords("en"))
    corpus <- tm_map(corpus, stripWhitespace)
    corpus <- tm_map(corpus, lemmatize_strings)
    return(corpus)
  }
  
  # All fields
  data$processed_text <- sapply(prepare_text(paste(data$title, data$abstract, data$keywords, sep=" ")), paste, collapse=" ")
  
  # Keywords only
  data<-data[!is.na(keywords)]
  data$processed_text <- sapply(prepare_text(data$keywords), paste, collapse=" ")
  
  # Create a document-term matrix with TF-IDF weighting
  #dtm <- DocumentTermMatrix(data$processed_text, control=list(weighting=weightTfIdf))
  
  # Function to tokenize into unigrams and bigrams
  
  # Define an enhanced list of stop words
  enhanced_stop_words <- c(stopwords::stopwords("en"), "na", "x", "p", "c", "etc")
  
  tokenize_bigrams <- function(text,enhanced_stop_words) {
    unigrams <- unlist(tokenizers::tokenize_words(text))
    # automatically filter out stop words
    unigrams <- unigrams[!unigrams %in% enhanced_stop_words & nchar(unigrams) > 1]
    # Remove single characters and specific patterns
    unigrams <- unigrams[grepl("^[a-z]{2,}$", unigrams, ignore.case = TRUE)]
    
    bigrams <- unlist(lapply(seq_len(length(unigrams) - 1), function(i) {
      paste(unigrams[i], unigrams[i + 1])
    }))
    return(c(unigrams, bigrams))
  }
  
  # Apply the tokenizer
  tokens <- lapply(data$processed_text, tokenize_bigrams,enhanced_stop_words=enhanced_stop_words)
  
  # Flatten and count occurrences for vocabulary
  flat_tokens <- unlist(tokens)
  vocab <- table(flat_tokens)
  vocab <- vocab[vocab >= 5]  # Prune vocabulary to only include terms appearing at least 5 times
  
  # Filter tokens based on vocabulary
  filtered_tokens <- lapply(tokens, function(ts) ts[ts %in% names(vocab)])
  
  # Create a mapping of words to column indices
  vocab_indices <- match(names(vocab), unique(flat_tokens))
  
  # Initialize an empty list to collect rows
  rows_list <- vector("list", length(filtered_tokens))
  
  # Preparing indices and values for the sparse matrix
  rows <- integer(0)
  cols <- integer(0)
  values <- numeric(0)
  
  # Populate the indices and values
  for (i in seq_along(filtered_tokens)) {
    token_indices <- match(filtered_tokens[[i]], names(vocab), nomatch = 0)
    valid_indices <- token_indices[token_indices > 0]
    rows <- c(rows, rep(i, length(valid_indices)))
    cols <- c(cols, valid_indices)
    values <- c(values, rep(1, length(valid_indices)))
  }
  
  # Combine all rows into a single sparse matrix
  dtm <- sparseMatrix(i = rows, j = cols, x = values, dims = c(length(filtered_tokens), length(vocab)), dimnames = list(NULL, names(vocab)))
  
  # Convert to matrix format for machine learning
  dtm_matrix<-dtm
  #dtm_matrix <- as.matrix(dtm)
  #colnames(dtm_matrix) <- make.names(colnames(dtm_matrix))
  
  # Prepare training data
  training_index <- createDataPartition(data$screened, p=0.8, list=FALSE)
  train_data <- dtm_matrix[training_index,]
  test_data <- dtm_matrix[-training_index,]
  train_labels <- data$screened[training_index]
  test_labels <- data$screened[-training_index]
  
  train_labels <- as.factor(train_labels)  # Convert to factor
  test_labels <- as.factor(test_labels)  # Convert to factor
  
  
  # Train model
  model <- train(train_data, train_labels, method="xgbTree",
                 trControl=trainControl(method="cv", number=10))
  
  # Evaluate model
  predictions <- predict(model, test_data)
  confusionMatrix(predictions, test_labels)
  
  # Feature importance
  importance <- xgb.importance(feature_names = colnames(train_data), model = model$finalModel)
  print(xgb.plot.importance(importance))
  
  # Feature importance 2 
  
  # Extract the raw xgboost model from the caret model
  xgb_model <- model$finalModel
  
  # Make sure your feature data is in a matrix form as expected by xgboost
  data_matrix <- as.matrix(train_data)
  
  # Create an xgb.DMatrix from your feature data
  dtest <- xgb.DMatrix(data = data_matrix)
  shap_values <- predict(xgb_model, dtest, predcontrib = TRUE)
  
  # Calculate average SHAP values for each feature
  average_shap_values <- colMeans(shap_values[, -ncol(shap_values)], na.rm = TRUE)
  
  # Combine with feature names
  feature_importance <- data.frame(Feature = colnames(data_matrix), SHAP_Value = average_shap_values)
  
  
  # Order by SHAP value to see the most influential features
  feature_importance <- feature_importance[order(-abs(feature_importance$SHAP_Value)), ]
  
  # Sorting the features by absolute SHAP values and selecting the top 20
  top_features <- feature_importance[order(-abs(feature_importance$SHAP_Value)), ][1:20, ]
  
  # Plotting the top 20 features using ggplot2
  ggplot(top_features, aes(x = reorder(Feature, SHAP_Value), y = SHAP_Value, fill = SHAP_Value)) +
    geom_bar(stat = "identity") +
    coord_flip() +  # Flip coordinates to make feature names readable
    labs(title = "Top 20 Features by SHAP Value",
         x = "SHAP Value",
         y = "Feature") +
    scale_fill_gradient2(low = "blue", high = "red", mid = "white", midpoint = 0,
                         name = "SHAP Value",
                         breaks = pretty_breaks(n = 5)) +
    theme_minimal()
  
