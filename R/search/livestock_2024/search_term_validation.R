# First run R/0_set_env.R
# 0) Load packages ####
if(!require(ERAg)){
  devtools::install_github("EiA2030/ERAg")
  require(ERAg)
}

if(!require(ERAgON)){
  devtools::install_github("EiA2030/ERAgON")
  require(ERAgON)
}

if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(readxl, tm, textstem, caret, xgboost, e1071,ggplot2,scales,tokenizers,Matrix,data.table,openalexR,pbapply)


# 1) Set-up workspace ####
# 1.1) Set directories #####
search_data_dir<-paste0(era_dir,"/data_entry/data_entry_2024/search_history/livestock_2024")
if(!dir.exists(search_data_dir)){
  dir.create(search_data_dir,recursive = T)
}

# 2) Download era data from s3 ####
update<-F

s3_file<-"https://digital-atlas.s3.amazonaws.com/era/data_entry/data_entry_2024/search_history/livestock_2024/livestock_2024.zip"
local_file<-file.path(search_data_dir,basename(s3_file))

if(length(list.files(search_data_dir))<1|update==T){
  options(timeout = 300) 
  download.file(s3_file, destfile = local_file)
  unzip(local_file, exdir = search_data_dir,overwrite=T,junkpaths=T)
  unlink(local_file)
}

# Initial searches ####
search_history<-fread(file.path(project_dir,"data/search_history/livestock_2024/search_history.csv"))[,-1]
search_history[,search_date:=as.Date(trimws(search_date), format = "%d-%b-%y")]

search_history_oa<-fread(file.path(search_data_dir,"searches.csv"))[,list(terms,search_name,search_date)]
setnames(search_history_oa,c("terms","search_name"),c("search_name","filename"))
search_history_oa[,citation_db:="openalex"
                  ][,timeframe:="all"
                    ][,filename:=paste0("openalex_ta-only_",filename,".csv")
                      ][,search_date:=as.Date(search_date)]

search_history<-rbindlist(list(search_history,search_history_oa),use.names = T)

# Load titles that should be included
search_manual<-fread(file.path(project_dir,"data/search_history/livestock_2024/search_manual.csv"))
search_manual<-search_manual[,year:=as.numeric(year)
                             ][year>=2018
                               ][,doi:=trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi))
                                 ][,title:=trimws(tolower(title))]

# Check which dois are 
oa_dois<-data.table(oa_fetch(
  entity = "works",
  doi = search_manual[!is.na(doi),doi],
  verbose = TRUE
))[,indexed_oa:="yes"
   ][,list(doi,indexed_oa)
     ][,doi:=trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi))]


search_manual<-merge(search_manual,oa_dois,all.x=T)
search_manual[is.na(indexed_oa),indexed_oa:="no"]

search_manual[,list(total_dois=sum(!is.na(doi)),
                                   wos=sum(indexed_wos=="yes"),
                                   wos_unique=sum(indexed_wos=="yes" & indexed_scopus=="no" & indexed_oa=="no"),
                                   scopus=sum(indexed_scopus=="yes"),
                                   scopus_unique=sum(indexed_wos=="no" & indexed_scopus=="yes" & indexed_oa=="no"),
                                   openalex=sum(indexed_oa=="yes"),
                                   openalex_unique=sum(indexed_wos=="no" & indexed_scopus=="no" & indexed_oa=="yes"),
                                   any=sum(indexed_wos=="yes"|indexed_scopus=="yes"|indexed_oa=="yes"),
                                   scopus_openalex=sum(indexed_scopus=="yes"|indexed_oa=="yes"))]


# Load search results ####
search_files<-list.files(search_data_dir,full.names = T)

# wos #####
search_files_wos<-grep("wos_",search_files,value = T)

wos_searches<-rbindlist(pblapply(1:length(search_files_wos),FUN=function(i){
  file<-search_files_wos[i]
  data<-data.table(readxl::read_xls(file))
  data[,search:=basename(file)]
  data
}))[,citation_db:="wos"
    ][,search:=gsub("_a.xls|_b.xls|_c.xls|_d.xls|_e.xls|_f.xls|_g.xls|_h.xls|_i.xls","",search)
      ][,search:=gsub(".xls","",search)
        ][grepl("Article",`Document Type`)# Keep only articles
          ][,relevance_score:=NA] 

# scopus #####
search_files_scopus<-grep("scopus_",search_files,value = T)

scopus_searches<-rbindlist(pblapply(1:length(search_files_scopus),FUN=function(i){
  file<-search_files_scopus[i]
  data<-fread(file)
  data[,search:=basename(file)]
  data
  }))[,citation_db:="scopus"
      ][,search:=gsub(".csv","",search)
        ][grepl("Article",`Document Type`)
          ][,relevance_score:=NA]

# openalex #####
search_files_oa<-grep("openalex_ta-only",search_files,value = T)

oa_searches<-rbindlist(pblapply(1:length(search_files_oa),FUN=function(i){
  file<-search_files_oa[i]
  data<-fread(file)
  data[,search:=basename(file)]
  data
}))[,citation_db:="openalex"
    ][,search:=gsub(".csv","",search)
      ][,ab:=NA
        ][,keywords:=NA
          ][,authors:=NA
            ][,id:=NA
              ][,relevance_score:=NA
                ]
setnames(oa_searches,"publication_year","year")

# Harmonization fields between scopus and wos
terms<-c("authors","title","year","doi","abstract","keywords","search","citation_db","citation_db_id","relevance_score")
wos<-c("Authors","Article Title","Publication Year","DOI","Abstract","Author Keywords","search","citation_db","UT (Unique WOS ID)","relevance_score")
scopus<-c("Authors","Title","Year","DOI","Abstract","Author Keywords","search","citation_db","EID","relevance_score")
oa<-c("authors","title","year","doi","ab","keywords","search","citation_db","id","relevance_score")

# Merge data
wos_searches<-wos_searches[,..wos]
setnames(wos_searches,wos,terms)

scopus_searches<-scopus_searches[,..scopus]
setnames(scopus_searches,scopus,terms)

oa_searches<-oa_searches[,..oa]
setnames(oa_searches,oa,terms)

searches<-rbind(wos_searches,scopus_searches,oa_searches)
searches[,search:=gsub("wos_|scopus_|openalex_","",search)]

searches[,list(doi_n=length(unique(doi[!is.na(doi)])),doi_na=sum(is.na(doi))),by=search]

searches[,doi:=gsub("https://doi.org/","",doi)]

searches[,search:=gsub("ta-only_","",search)]

# Remove corrections, corrigendum and erratum
remove_phrases<-c("correction to:","corrigendum to:","erratum")
searches<-searches[!grepl(paste0(remove_phrases,collapse = "|"),title)]

# Cross reference manual search
# remove manual targets that did not appear to be in citation dbs?
rm_no_citation_db<-F
search_manual[,doi:=tolower(trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi)))]
if(rm_no_citation_db){
  no_citation_db<-search_manual[indexed_wos=="no" & indexed_scopus=="no" & indexed_oa=="no" & !is.na(doi),doi]
  doi_targets<-search_manual[!is.na(doi) | !doi %in% no_citation_db,doi]
}else{
  doi_targets<-search_manual[!is.na(doi),doi]
}

# Match dois
#searches[,target_doi:=F][grep(paste0(doi_targets,collapse="|"),doi),target_doi:=T]
searches[,target_doi:=doi[1] %in% doi_targets,by=doi]
searches[,list(performance=round(sum(target_doi)/nrow(search_manual),2)),by=list(search,citation_db)][order(performance,decreasing=T)]

# Match titles
title_targets<-search_manual[,trimws(tolower(title))]
searches[,target_title:=grepl(paste0(title_targets,collapse="|"),title)]
searches[,list(performance=round(sum(target_title)/nrow(search_manual),2)),by=list(search,citation_db)][order(performance,decreasing=T)]

# Combine doi and title matches
searches[,target_any:=F][target_title|target_doi,target_any:=T]

# Different approach to matching that gives different results, this will need investigation
search_perf<-searches[,list(search,citation_db,doi,title,target_doi,target_title,target_any)]

search_perf[,list(performance=round(sum(target_any)/nrow(search_manual),2)),by=list(search,citation_db)][order(performance,decreasing = T)]

search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2)),by=list(search,citation_db)][order(performance,decreasing = T)]
search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2)),by=list(search)][order(performance,decreasing = T)]
search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2))]

# What papers are not in openalex, but in other databases?
search_perf[citation_db=="openalex" & target_any==T,sort(unique(title))]
search_perf[citation_db=="scopus" & target_any==T,sort(unique(title))]
search_perf[citation_db=="wos" & target_any==T,sort(unique(title))]

hits_other<-dcast(unique(search_perf[citation_db!="openalex" & target_any==T,list(citation_db,title)]),title~citation_db, fun.aggregate=length)
hits_other[,openalex:=0][title %in% hits_oa,openalex:=1]

search_manual[,ID:=1:.N]
hits<-search_manual[,list(ID,title,doi,indexed_wos,indexed_scopus,indexed_oa)
                    ][,title:=trimws(tolower(title))
                      ][,doi:=trimws(tolower(doi))
                        ][,indexed_wos:=indexed_wos=="yes"
                          ][,indexed_scopus :=indexed_scopus =="yes"
                            ][,indexed_oa :=indexed_oa =="yes"
                              ][,indexed_any:=any(indexed_oa,indexed_scopus,indexed_wos),by=ID
                                ][,hit_oa:=F
                                  ][,hit_wos:=F
                                    ][,hit_scopus:=F]

hits[title %in% search_perf[citation_db=="openalex" & target_any==T,unique(title)]|
       doi %in% search_perf[citation_db=="openalex" & target_any==T,unique(doi)],hit_oa:=T]
     
hits[title %in% search_perf[citation_db=="wos" & target_any==T,unique(title)]|
       doi %in% search_perf[citation_db=="wos" & target_any==T,unique(doi)],hit_wos:=T]     

hits[title %in% search_perf[citation_db=="scopus" & target_any==T,unique(title)]|
       doi %in% search_perf[citation_db=="scopus" & target_any==T,unique(doi)],hit_scopus:=T]     

hits[,hit_any:=any(hit_oa,hit_wos,hit_scopus),by=ID]

# Indexed OA result not found
search_manual[ID %in% hits[indexed_oa & !hit_oa,ID],list(title,keywords,abstract,doi)]

# Wos result not in OA
search_manual[ID %in% hits[hit_wos & ! hit_oa,ID]]

# Scopus result not in OA
search_manual[ID %in% hits[hit_scopus & ! hit_oa,ID]]


# Missing papers from search
missing<-doi_targets[!doi_targets %in% search_perf[,unique(target)]]
(missing<-search_manual[tolower(trimws(doi)) %in% missing,list(title,abstract,keywords,indexed_oa)])

write.table(missing,"clipboard-256000",row.names = F,sep="\t")

# Explore ERA data for keywords to include ####
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

