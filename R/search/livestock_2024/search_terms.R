# ERA livestock update search terms
# 0) Set up workspace ####
# 0.1) Load packages #####
if (!require("pacman")) {
  install.packages("pacman")
  require(pacman)
}

# Use p_load to install if not present and load the packages
p_load(data.table)

if(!require(openalexR)){
  devtools::install_github("https://github.com/ropensci/openalexR")
  library(openalexR)
}

# 0.2) Create functions #####
add_quotes<-function(vector){
  sapply(vector, function(term) {
    if (lengths(strsplit(term, "\\s+")) > 1) {
      return(paste0('"', term, '"'))
    } else {
      return(term)
    }
  })
}

# 0.3) Set directories ######
search_data_dir<-paste0(era_dir,"/data_entry/data_entry_2024/search_history/livestock_2024")
if(!dir.exists(search_data_dir)){
  dir.create(search_data_dir,recursive = T)
}


# 1) Create terms ####
outcome_terms <- c("animal performance", 
                   "ADG",
                   "average daily gain", 
                   "beef yield", 
                   "body weight gain",
                   "calving rate", 
                   "carcase weight", 
                   "carcass weight", 
                   "dairy production", 
                   "dry matter intake",
                   "feed consumption",
                   "feed conversion", 
                   "feed efficiency", 
                   "feed intake", 
                   "feeding efficiency", 
                   "growth performance",
                   "lactation performance", 
                   "lactation yield", 
                   "liveweight gain",
                   "meat yield", 
                   "milk output", 
                   "milk production",
                   "milk yield", 
                   "production efficiency", 
                   "weight gain", 
                   "DMI", 
                   "digestibility", 
                   "digestible acid",
                   "digestible fibre", 
                   "digestible cellulose",
                   "crude fat", 
                   "crude protein",
                   "crude lipid", 
                   "digestible energy",
                   "digestible fat", 
                   "digestible lipid",
                   "detergent fibre", 
                   "detergent lignin", 
                   "digestible nitrogen",
                   "ether extract")

outcome_terms_extra<-c("yield","performance","production")

animal_terms <- c("goat*", 
                  "sheep", 
                  "cattle", 
                  "livestock",
                  "cow*", 
                  "bull*", 
                  "steer*", 
                  "heifer*",
                  "ewe*",
                  "lamb*", 
                  "grower*", 
                  "finisher*", 
                  "calf", 
                  "yearling*",
                  "kid*", 
                  "bullock*",
                  "backgrounder*", 
                  "buck*", 
                  "doe*", 
                  "zebu*", 
                  "Bos taurus",
                  "Bos indicus", 
                  "Ovis aries", 
                  "Capra hircus", 
                  "ruminant*", 
                  "calves")


animal_terms_update<-c("ram","rams")

region_terms <- c(
  "Africa",
  "Algeria",
  "Angola",
  "Benin",
  "Botswana",
  "Burkina Faso",
  "Burundi",
  "Cabo Verde",
  "Cameroon",
  "Central African Republic",
  "CAR",
  "Chad",
  "Comoros",
  "Congo",
  "Cote d'Ivoire",
  "Ivory Coast",
  "Djibouti",
  "Egypt",
  "Equatorial Guinea",
  "Eritrea",
  "Ethiopia",
  "Eswatini",
  "Gabon",
  "Gambia",
  "Ghana",
  "Guinea",
  "Kenya",
  "Lesotho",
  "Liberia",
  "Libya",
  "Madagascar",
  "Malawi",
  "Mali",
  "Mauritania",
  "Mauritius",
  "Morocco",
  "Mozambique",
  "Namibia",
  "Niger",
  "Nigeria",
  "Rwanda",
  "Sao Tome",
  "Principe",
  "Senegal",
  "Seychelles",
  "Sierra Leone",
  "Somalia",
  "South Africa",
  "South Sudan",
  "Sudan",
  "Swaziland",
  "Tanzania",
  "Togo",
  "Tunisia",
  "Uganda",
  "Zambia",
  "Zimbabwe",
  "Sahara",
  "Sub-Sahara*",
  "Sahel",
  "DRC"
)


# Create a vector called feed_terms with each term as an element
feed_terms <- c(
  "ration",
  "feed",
  "diet",
  "byproduct",
  "supplement",
  "ingredient",
  "nutrient",
  "fodder",
  "forage",
  "silage",
  "hay",
  "grazing",
  "feedstuff",
  "pasture",
  "cut-and-carry",
  "grass",
  "concentrate",
  "meal",
  "bran",
  "cake",
  "straw",
  "haulms",
  "hulls",
  "pellet",
  "mash",
  "block"
)

# Create a vector called experiment_terms with each term as an element
experiment_terms <- c(
  "experiment*",
  "trial",
  "station",
  "facility",
  "in-vivo",
  "feedlot",
  "controlled study",
  "observational study"
)

# 1.1) Convert to boolean ####

outcome_terms2 <-add_quotes(outcome_terms)
outcome_boolean<-paste0("(",paste0(outcome_terms2,collapse = " OR "),")")

outcome_terms_ex2 <-add_quotes(c(outcome_terms,outcome_terms_extra))
outcome_ex_boolean<-paste0("(",paste0(outcome_terms_ex2,collapse = " OR "),")")

animal_terms2 <-add_quotes(animal_terms)
animal_boolean<-paste0("(",paste0(animal_terms2,collapse = " OR "),")")

animal_terms3 <-add_quotes(c(animal_terms,animal_terms_update))
animal_2_boolean<-paste0("(",paste0(animal_terms3,collapse = " OR "),")")

region_terms2 <-add_quotes(region_terms)
region_boolean<-paste0("(",paste0(region_terms2,collapse = " OR "),")")

feed_terms2 <-add_quotes(feed_terms)
feed_boolean<-paste0("(",paste0(feed_terms2,collapse = " OR "),")")

experiment_terms2 <-add_quotes(experiment_terms)
experiment_boolean<-paste0("(",paste0(experiment_terms2,collapse = " OR "),")")

terms<-list(l1=outcome_boolean,
            l1ex=outcome_ex_boolean,
            l2=animal_boolean,
            l3=region_boolean,
            l4=feed_boolean,
            l5=experiment_boolean,
            l2.2=animal_2_boolean)

# Create searches
searches<-list(l12345=paste0(unlist(terms[c(1,3:6)]),collapse=" AND "),
              l1234=paste0(unlist(terms[c(1,3:5)]),collapse=" AND "),
              l123=paste0(unlist(terms[c(1,3:4)]),collapse=" AND "),
              lex_12345=paste0(unlist(terms[c(2,3:6)]),collapse=" AND "),
              lex_1234=paste0(unlist(terms[c(2,3:5)]),collapse=" AND "),
             # lex_123=paste0(unlist(terms[c(2,3:4)]),collapse=" AND "),  # Removed as results are too large
              l12.2345=paste0(unlist(terms[c(1,4:7)]),collapse=" AND "),
              l12.234=paste0(unlist(terms[c(1,4:5,7)]),collapse=" AND "),
              l12.23=paste0(unlist(terms[c(1,4,7)]),collapse=" AND "),
              lex_12.2345=paste0(unlist(terms[c(2,4:7)]),collapse=" AND "),
              lex_12.234=paste0(unlist(terms[c(2,4:5,7)]),collapse=" AND ")
             # lex_12.23=paste0(unlist(terms[c(2,4,7)]),collapse=" AND ")
             )

# 2) OpenAlex ####

run_searches<-c(1:length(searches))
overwrite<-F

for(i in run_searches){
  
  search_code<-gsub("l","",names(searches)[i])

  save_file<-file.path(search_data_dir,paste0("openalex_",search_code,".csv"))
  cat(save_file,"\n")
  
  
  if(!file.exists(save_file)|overwrite==T){

    
  base_search_query <-searches[[i]]

  # URL-encode the query
  encoded_query <- URLencode(base_search_query)
  
  # Filters
  # https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists
  # https://docs.openalex.org/api-entities/works/filter-works
  
  # Filter title and abstract
  api_endpoint <- paste0("https://api.openalex.org/works?filter=title_and_abstract.search:", encoded_query)
  
  # How many hits do we have?
  cat("search =", search_code,"| search hits =",oa_request(query_url=api_endpoint,count_only=T)$count,"\n")
  
  hits<-oa_request(
    query_url=api_endpoint
  )
  
  hits_tab<-data.table(oa2df(hits,entity = "works"))
  hits_tab<-hits_tab[,list(id,display_name,author,ab,doi,url,relevance_score,is_oa,language,type,publication_date)]
  
  # Convert author to non-list form
  hits_tab[,authors:=unlist(lapply(1:nrow(hits_tab),FUN=function(i){
    authors<-hits_tab[i,author][[1]]
    if(length(authors)>1){
    paste0(authors[,"au_display_name"],collapse=";")
    }else{
      NA
    }
    
  }))][,author:=NULL]
  
  
  colnames(hits_tab)
  
  fwrite(hits_tab,file=save_file)
  
  }

}

# Topic search
# https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/search-entities
api_endpoint <- paste0("https://api.openalex.org/works?search=",encoded_query)

oa_request(
  query_url=api_endpoint,
  count_only=T
        )

# Figure out:
# 1: how to add date ranges
# 2: how to add types
# 3: how to include keywords in filter search
# 4: topic search returns too many hits - investigate if exact matching is happening

# Look at the help for ?oa_request to see how we might convert the boolean search string into the oa filter (e.g Example 3)

