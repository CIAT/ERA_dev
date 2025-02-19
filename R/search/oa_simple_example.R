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

outcomes_terms <- c(
  "Benefit-cost analysis", "Break-even period", "Economic valuation",
  "Economic impact", "Net present value", "Payback period", "Willingness to pay",
  "Gross margin","Kilograms per hectare","Cost",
  "Climate change", "Risk assessment", "Vulnerability assessment",
  "Adaptive management", "Community awareness", "Drought resistance","Indigenous knowledge",
  "Changing climate","Flood resistance", "Soil fertility", "Soil degradation", 
  "Biodiversity","Soil degradation","Yield loss cost","Revenue","Yield",
  "Food security", "Water availability", "Water stress"
)

practices_terms <- c(
  "Agroforestry", "Evergreen agriculture", "Farmer managed natural regeneration",
  "Silvopastoral systems", "Alley cropping", "Crop rotation", "Cover cropping",
  "Integrated pest management", "No tillage","Minimum tillage", "Soil conservation",
  "Adapted cultivar","Crop diversification","Crop rotation","Green manure", 
  "Water harvesting", "Managed grazing", "Irrigation","Residue retention",
  "Rotational grazing", "Intensive grazing", "Biological pest control", "Managed grazing",
  "Organic fertilizers","Short duration grazing","Strip grazing"
)

product_terms <- c(
  "Maize", "Zea mays", "Common beans", "Phaseolus vulgaris", "Cattle", "Bos taurus",
  "Bos indicus", "Bull", "Heifer", "Bovine", "Coffee", "Coffea arabica", "Coffea robusta")

region_terms <- c(
  "Belize", "Bolivia", "Brazil", "Colombia", "Costa Rica", "Cuba", "Dominica",
  "Dominican Republic", "Ecuador", "El Salvador", "Grenada", "Guatemala",
  "Guyana", "Haiti", "Honduras", "Jamaica", "Mexico", "Nicaragua", "Paraguay",
  "Peru", "Saint Lucia", "Saint Vincent and the Grenadines", "Suriname",
  "Venezuela", "Argentina", "Bahamas", "Barbados", "Chile", "Panama",
  "Trinidad and Tobago", "Uruguay"
)

terms<-list(outcomes_terms,practices_terms,product_terms,region_terms)


b<-build_search_string2 (search_terms=terms, 
                      within_block_operators  = "OR",
                      between_block_operators = "AND",
                      quote_terms             = T,
                      max_term                = 100,
                      max_char_limit          = 4000)

cat(b)



results <- era_oa_query(
  search_terms = terms,
  from_date    = "1900-01-01",
  to_date      = Sys.Date(),
  download     = FALSE,
  full         = FALSE,
  quote_terms  = TRUE,
  max_char_limit = 4000,
  max_term = 129
)

cat(results$api_endpoint)

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
