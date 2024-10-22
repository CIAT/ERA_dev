# Powered by https://github.com/ropensci/openalexR

if(!require(openalexR)){
  devtools::install_github("https://github.com/ropensci/openalexR")
  library(openalexR)
}

# Info on the open alex r package
?openalexR

if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

p_load(data.table)

source("https://raw.githubusercontent.com/CIAT/ERA_dev/main/R/search/era_oa_query.R")

terms<-list(practice = c("fertilizer"),
            geo=c("kenya","tanzania","malawi","africa"),
            outcome = c("yield","profit margin","usd","efficiency"))

results<-era_oa_query(search_terms=terms, 
             from_date = "1900-01-01", 
             to_date = "2024-01-01", 
             continent = NULL, 
             download = FALSE,
             full = FALSE, 
             max_char_limit = 4000
             ) 

results$meta_data
results$results
str(results$results)

# Save to clipboard on windows
write.table(results$results,"clipboard-256000",row.names = F,sep="\t")

# Search if a doi is in openalex, this can be used to validate if a list of key literature is present or not
# You may also want to search titles to get a complete picture
dois<-c("10.3390/su12229465","https://doi.org/10.1007/978-3-319-93336-8_80","https://doi.org/10.1007/978-3-3-93336-8_80")

doi_check<-data.table(oa_fetch(
  entity = "works",
  doi = dois,
  verbose = TRUE
))[,indexed_oa:="yes"
][,list(doi,indexed_oa)
][,doi:=trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi))]

titles<-c("Opportunities to strengthen Africa’s efforts to track national-level climate adaptation","Status of global coastal adaptation")

oa_titles<-rbindlist(lapply(1:length(titles),FUN=function(i){
  data<-data.table(oa_fetch(
    entity = "works",
    display_name.search =titles[i],
    verbose = TRUE
  ))[,indexed_oa:="yes"
  ][,list(title,indexed_oa)]
  data
}))

# If you need to strip out any weird text encoding issues in the title you can also try this
oa_titles<-rbindlist(lapply(1:length(titles),FUN=function(i){
  data<-data.table(oa_fetch(
    entity = "works",
    display_name.search = gsub("[^a-zA-Z0-9 ]", "",titles),
    verbose = TRUE
  ))[,indexed_oa:="yes"
  ][,list(title,indexed_oa)]
  data
}))
