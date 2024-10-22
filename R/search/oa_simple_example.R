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

terms<-list(practice=c("climate information services","drought tolerant crop"),
            outcome = c("adaptation","resilience","resistance"))


results<-era_oa_query(search_terms=terms, 
             from_date = "1980-01-01", 
             to_date = Sys.Date(), 
             continent = "africa", 
             download = TRUE,
             full = TRUE, 
             max_char_limit = 4000
             ) 

results$meta_data
results$results
str(results$results)

# Save to clipboard on windows
write.table(results$results,"clipboard-256000",row.names = F,sep="\t")

oa_fetch(
  entity = "works",
  doi = c("https://doi.org/10.3390/su12229465","https://doi.org/10.1007/978-3-319-93336-8_80","https://doi.org/10.1007/978-3-3-93336-8_80"),
  verbose = TRUE
)

