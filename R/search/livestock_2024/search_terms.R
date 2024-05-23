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
add_quotes <- function(vector) {
  sapply(vector, function(term) {
    if (grepl("\\s", term)) {
      return(shQuote(term, type = "cmd"))
    } else {
      return(term)
    }
  }, USE.NAMES = FALSE)
}
# 0.3) Set directories ######
search_data_dir<-paste0(era_dir,"/data_entry/data_entry_2024/search_history/livestock_2024")
if(!dir.exists(search_data_dir)){
  dir.create(search_data_dir,recursive = T)
}


# 1) Create terms ####
# Read in additional animal breed terms provided by Claudia Arndt's team
outcome_terms <- c("animal performance", 
                   "ADG",
                   "average daily gain", 
                   "beef yield", 
                   "body weight gain",
                   "calving rate", 
                   "carcase weight", 
                   "carcass weight", 
                   "dairy production", 
                   "matter intake",
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
outcome_terms_extra2<-c("energy utilization",
                        "nitrogen utilization",
                        "energy utilisation",
                        "nitrogen utilisation",
                        "energy output",
                        "energy intake",
                        "metabolizable energy",
                        "metabolizable nitrogen",
                        "production performance")

# https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/search-entities
# For most text search we remove stop words and use stemming (specifically, the Kstem token filter) to improve results.
# So words like "the" and "an" are transparently removed, and a search for "possums" will also return records using the word 
# possum." 

animal_terms <- c("goats",
                  "sheep", 
                  "cattle", 
                  "livestock",
                  "cows", 
                  "bulls", 
                  "steers", 
                  "heifers",
                  "ewes",
                  "lambs", 
                  "growers", 
                  "finishers", 
                  "calf", 
                  "yearlings",
                  "kids", 
                  "bullocks",
                  "backgrounders", 
                  "bucks", 
                  "does", 
                  "zebus", 
                  "Bos taurus",
                  "Bos indicus", 
                  "Ovis aries", 
                  "Capra hircus", 
                  "ruminants", 
                  "calves")

animal_terms_update<-c("rams")

region_terms <- c(
  "African",
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
  "Sub-Saharan",
  "Sahel",
  "DRC"
)

# load africa specific breeds
process_breeds <- function(breeds) {
  # Load necessary library
  library(stringr)
  
  # Initialize an empty vector to store processed breeds
  processed_breeds <- c()
  
  # Iterate over each breed in the input vector
  for (breed in breeds) {
    # Remove punctuation and convert to lowercase
    breed <- tolower(breed)
    breed<-gsub("/"," ",breed)
    
    # Split on parentheses to extract words within them
    parts <- str_split(breed, "\\s*\\(\\s*|\\s*\\)\\s*", simplify = TRUE)
    
    # Add each part to the processed breeds vector
    processed_breeds <- c(processed_breeds, parts[parts != ""])
  }
  
  return(processed_breeds)
}
africa_breeds<-fread("https://raw.githubusercontent.com/CIAT/ERA_dev/main/data/search_history/livestock_2024/african_livestock_breeds.csv")
africa_breeds<-process_breeds(breeds=africa_breeds$breed)
# Remove breeds that are commonly used words
africa_breeds<-africa_breeds[!africa_breeds %in% c("delta","dwarf","somali","taitataveta","pare","hammer","british alpine",
                                                   "alpine goat","forest goat","corriedale","hampshire down","meatmaster",
                                                   "bovines of tete","grassland dwarf","chèvre naine","chèvre naine des savanes",
                                                   "pygmy",'toggenburger',"angora","damascus","blackhead persian","romney marsh",
                                                   "barbary","ram ewe","anglo-nubian goat","afar bhs","angora","caprivi sanga",
                                                   "maure black maure","mere","merino","morcia granada"
                                                   )]
africa_breeds<-africa_breeds[!grepl(paste0(tolower(region_terms),collapse = "|"),africa_breeds)]
africa_breeds<-gsub(" goat| zebu","",africa_breeds)
africa_breeds[grep("boran",africa_breeds)]<-"boran"
africa_breeds[grep("fulani",africa_breeds)]<-"fulani"
africa_breeds<-c(africa_breeds,"sahelian","majorera")


africa_breeds<-sort(unique(africa_breeds))

# Create a vector called feed_terms with each term as an element
feed_terms <- c(
  "rations",
  "feeds",
  "diets",
  "byproducts",
  "supplements",
  "ingredients",
  "nutrients",
  "fodders",
  "forages",
  "silages",
  "hay",
  "grazing",
  "feedstuffs",
  "pastures",
  "cut-and-carry",
  "grasses",
  "concentrates",
  "meals",
  "brans",
  "cakes",
  "straw",
  "haulms",
  "hulls",
  "pellets",
  "mash",
  "blocks"
)

# Create a vector called experiment_terms with each term as an element
experiment_terms <- c(
  "experiments",
  "trials",
  "station",
  "facility",
  "in-vivo",
  "feedlots",
  "controlled study",
  "observational study",
  "grazing management",
  "ruminant nutrition",
  "feedlot management"
)

# 1.1) Convert to boolean ####

terms <-add_quotes(outcome_terms)
outcome_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(c(outcome_terms,outcome_terms_extra))
outcome_2_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(c(outcome_terms,outcome_terms_extra2))
outcome_3_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(animal_terms)
animal_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(c(animal_terms,animal_terms_update))
animal_2_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(region_terms)
region_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(c(africa_breeds[1:71]))
breeds_1_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(c(africa_breeds[72:length(africa_breeds)]))
breeds_2_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(feed_terms)
feed_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms <-add_quotes(experiment_terms)
experiment_boolean<-paste0("(",paste0(terms,collapse = " OR "),")")

terms<-list(l1=outcome_boolean,
            l1.1=outcome_2_boolean,
            l1.2=outcome_3_boolean,
            l2=animal_boolean,
            l2.2=animal_2_boolean,
            l3=region_boolean,
            l4=feed_boolean,
            l5=experiment_boolean,
            l6.1=breeds_1_boolean,
            l6.2=breeds_2_boolean)

fwrite(terms,file.path(search_data_dir,"terms.csv"))

# Create searches

# Note another approach can be to just download the search elements and then combine these,
# Although I suspect it would return a lot of hits

searches<-list(
  paste0("l",1:5),
  paste0("l",1:4),
  paste0("l",1:3),
  #paste0("l",c(1.1,2:5)),
  #paste0("l",c(1.1,2:4)),
  #paste0("l",c(1.1,2:3)),
  paste0("l",c(1.2,2:5)),
  paste0("l",c(1.2,2:4)),
  paste0("l",c(1.2,2:3)),
  
  paste0("l",c(1,2.2,3:5)),
  paste0("l",c(1,2.2,3:4)),
  paste0("l",c(1,2.2,3)),
  #paste0("l",c(1.1,2.2,3:5)),
  #paste0("l",c(1.1,2.2,3:4)),
  #paste0("l",c(1.1,2.2,3)),
  paste0("l",c(1.2,2.2,3:5)),
  paste0("l",c(1.2,2.2,3:4)),
  paste0("l",c(1.2,2.2,3)),
  
  paste0("l",c(1,2.2,6.1,4:5)), 
  paste0("l",c(1,2.2,6.1,4)),
  paste0("l",c(1,2.2,6.1)),
  #paste0("l",c(1.1,2.2,6.1,4:5)), 
  #paste0("l",c(1.1,2.2,6.1,4)),
  #paste0("l",c(1.1,2.2,6.1)),
  paste0("l",c(1.2,2.2,6.1,4:5)), 
  paste0("l",c(1.2,2.2,6.1,4)),
  paste0("l",c(1.2,2.2,6.1)),
  
  paste0("l",c(1,2.2,6.2,4:5)),
  paste0("l",c(1,2.2,6.2,4)),
  paste0("l",c(1,2.2,6.2)), 
  #paste0("l",c(1.1,2.2,6.2,4:5)),
  #paste0("l",c(1.1,2.2,6.2,4)),
  #paste0("l",c(1.1,2.2,6.2)),
  paste0("l",c(1.2,2.2,6.2,4:5)),
  paste0("l",c(1.2,2.2,6.2,4)),
  paste0("l",c(1.2,2.2,6.2))
)

s_names<-sapply(searches,FUN=function(x){paste0(gsub("l","",unlist(x)),collapse="")})

search_strings<-lapply(searches,FUN=function(x){
  paste0(unlist(terms[x]),collapse=" AND ")
})

searches<-data.table(terms=sapply(searches,paste,collapse="|"),search_name=s_names,string=unlist(search_strings))

searches[,encoded_string:=URLencode(string)
         ][,nchar:=nchar(encoded_string)]

searches[,list(search_name,nchar)]

# 2) OpenAlex ####
openalex_dates_file<-file.path(search_data_dir,"openalex_search_dates.csv")

if(file.exists(openalex_dates_file)){
  search_dates<-fread(openalex_dates_file)
}else{
  search_dates<-data.table(search_name=as.character(NULL),full=as.logical(NULL),search_date=as.Date(NULL))
}

overwrite<-T
full<-F

for(i in 1:nrow(searches)){
  search_code<-searches$search_name[i]
  
  if(full){
    prefix<-"openalex_"
  }else{
    prefix<-"openalex_ta-only_"
  }

  save_file<-file.path(search_data_dir,paste0(prefix,search_code,".csv"))

  cat(i,"-",save_file,"\n")
  
  if(!file.exists(save_file)|overwrite==T){

  if(searches$nchar[i]>4000){
    stop(paste0("String has ",searches$nchar[i]," characters. Max allowed is 4000?"))
  }
    
    
  # Filters
  # https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists
  # https://docs.openalex.org/api-entities/works/filter-works
  
    
  if(full){
    api_endpoint <- paste0("https://api.openalex.org/works?filter=title_and_abstract.search:", searches$encoded_string[i])
  }else{
    # Filter title and abstract
    api_endpoint <- paste0("https://api.openalex.org/works?filter=title_and_abstract.search:", searches$encoded_string[i], "&select=title,doi,publication_year")
  }
  
  # How many hits do we have?
  cat("search =", search_code,"| search hits =",oa_request(query_url=api_endpoint,count_only=T)$count,"\n")
  
  hits<-oa_request(
    query_url=api_endpoint
  )
  
  hits_tab<-data.table(oa2df(hits,entity = "works"))
  
  if(full){
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
  }

  fwrite(hits_tab,file=save_file)
  
  # Update dates
  search_dates<-unique(rbind(search_dates,data.table(search_name=search_code,full=full,search_date=Sys.Date())))
  }
}

search_dates<-data.table(search_name=searches$search_name,full=full,prefix=prefix,search_date=Sys.Date()-1)
searches<-merge(searches,search_dates[,list(search_name,search_date)],all.x=T)

fwrite(search_dates,file=openalex_dates_file)

# 3) Merge searches ####

merge_searches<-function(combine,search_data_dir,full){

files<-list.files(search_data_dir,".csv$",full.names = T)

if(full){
  files<-files[!grepl("openalex_ta-only_",files)]
  prefix<-"openalex"
}else{
  files<-files[grepl("openalex_ta-only_",files)]
  prefix<-"openalex_ta-only"
}

search_dat<-lapply(files,fread)
names(search_dat)<-gsub(".csv","",unlist(tail(tstrsplit(files,"_"),1)))

# Combine searches

# Order is important here the function will conduct a merge of search names that contain the first element with the second two elements
# that match on the other search elements. So if we have abcd, abcde, abe, abf and combine = c("e","f") the function will pick out
# elements "abcde" and  "abe". For each of these selected elements and the search names the values from combine are removed the 
# list giving  abcd and ab selected and  abd, abcd, ab, ab in the list. Searches that search the selected name are then combined, in
# this case abe and abf would be combined, but there is nothing to combine abcde with.
sterms<-searches$terms
terms_sub<-gsub(paste(combine,collapse = "|"),"",sterms)
# Remove combined searches that contain sub elements that are listed in the combine argument
rm_terms<-grepl("[|][+]|[+][+]",terms_sub)
terms_sub<-terms_sub[!rm_terms]

# Remove trailing | and ||
terms_sub<-gsub("||","|",terms_sub,fixed=T)
terms_sub <- sub("\\|$", "", terms_sub)

comb_n<-which(grepl(combine[1],sterms) & !rm_terms)

search_new<-rbindlist(lapply(comb_n,FUN=function(i){
  term<-terms_sub[i]
  lists<-terms_sub==term
  
  if(sum(lists)!=length(combine)){
    stop(paste0("Issue with combination of lists. Not enough elements are being combined. Check all searches have downloaded to ",search_data_dir))
  }
  
  if(!all(searches$search_name[lists] %in% names(search_dat))){
    stop(paste0("Issue with combination of lists. Names in searches object not matching files in ",search_data_dir,".  Check all searches have downloaded."))
  }
  
  search_comb<-unique(rbindlist(search_dat[searches$search_name[lists]]))
  
  search_new<-data.table(terms=paste(sort(c(paste(sort(combine),collapse="+"),unlist(strsplit(term,"[|]")))),collapse="|"))
  search_new[,search_name:=gsub("l|[|]","",terms)
             ][,string:=paste0("Searches merged:",paste0(searches$search_name[lists],collapse = "+"))
               ][,encoded_string:=NA
                 ][,nchar:=NA
                   ][,search_date:=as.Date(NA)]
  
  fwrite(search_comb,file.path(search_data_dir,paste0(prefix,"_",search_new$search_name,".csv")))
  
  search_new
}))

return(search_new)
}

searches_merged<-merge_searches(combine=paste0("l",c(6.1, 6.2,3)),
                         search_data_dir=search_data_dir,
                         full=F)

searches_all<-unique(rbindlist(list(searches,searches_merged),use.names = T))

fwrite(searches_all,file.path(search_data_dir,"searches.csv"))

# 4) Alternative approach using author location to geographical filter research 
searches<-list(
  paste0("l",c(1:2,4:5)),
  paste0("l",c(1:2,4)),
  #paste0("l",1:2),
  #paste0("l",c(1.1,2,4:5)),
  #paste0("l",c(1.1,2,4)),
 # paste0("l",c(1.1,2)),
  paste0("l",c(1.2,2,4:5)),
  paste0("l",c(1.2,2,4)),
 # paste0("l",c(1.2,2)),
  
  paste0("l",c(1,2.2,4:5)),
  paste0("l",c(1,2.2,4)),
  #paste0("l",c(1,2.2)),
  #paste0("l",c(1.1,2.2,4:5)),
  #paste0("l",c(1.1,2.2,4)),
 # paste0("l",c(1.1,2.2)),
  paste0("l",c(1.2,2.2,4:5)),
  paste0("l",c(1.2,2.2,4))
  #paste0("l",c(1.2,2.2))
)

s_names<-sapply(searches,FUN=function(x){paste0(gsub("l","",unlist(x)),collapse="")})

search_strings<-lapply(searches,FUN=function(x){
  paste0(unlist(terms[x]),collapse=" AND ")
})

searches<-data.table(terms=sapply(searches,paste,collapse="|"),search_name=s_names,string=unlist(search_strings))

searches[,encoded_string:=URLencode(string)
][,nchar:=nchar(encoded_string)]

openalex_dates_file<-file.path(search_data_dir,"openalex_search_dates.csv")

if(file.exists(openalex_dates_file)){
  search_dates<-fread(openalex_dates_file)
}else{
  search_dates<-data.table(search_name=as.character(NULL),full=as.logical(NULL),search_date=as.Date(NULL))
}

overwrite<-T
full<-F

if(full){
  prefix<-"openalex"
}else{
  prefix<-"openalex_ta-only"
}

download<-T

# Define the year constraints
from_year <- "2018-01-01"
to_year <- "2024-05-23"

for(i in 1:nrow(searches)){
  search_code<-searches$search_name[i]

  save_file<-file.path(search_data_dir,paste0(prefix,"_",search_code,".csv"))
  
  cat(i,"-",save_file,"\n")
  
  if(!file.exists(save_file)|overwrite==T){
    
    if(searches$nchar[i]>4000){
      stop(paste0("String has ",searches$nchar[i]," characters. Max allowed is 4000?"))
    }
    
    
    # Filters
    # https://docs.openalex.org/how-to-use-the-api/get-lists-of-entities/filter-entity-lists
    # https://docs.openalex.org/api-entities/works/filter-works
    
    api_endpoint<-oa_query(entity="works",
                           title_and_abstract.search=searches$string[i],
                           from_publication_date=from_year,
                           to_publication_date=to_year)
    
    if(!full){
      api_endpoint<-paste0(api_endpoint,"&select=title,doi")
      }
    
    # How many hits do we have?
    n_hits<-oa_request(query_url=api_endpoint,count_only=T)$count
    cat("search =", search_code,"| search hits =",n_hits,"\n")
    n_hits<-data.table(search_name=search_code,hits=n_hits)
    
    if(i==1){
     n_hits_tab<-n_hits
    }else{
      n_hits_tab<-rbind(n_hits_tab,n_hits)
    }
    
    if(download){
      hits<-oa_request(
        query_url=api_endpoint
      )
      
      hits_tab<-data.table(oa2df(hits,entity = "works"))
      
      if(full){
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
      }
      
      fwrite(hits_tab,file=save_file)
      
      # Update dates
      search_dates<-unique(rbind(search_dates,data.table(search_name=search_code,full=full,search_date=Sys.Date())))
    }else{
      n_hits_tab
    }
  }
}

search_dates<-data.table(search_name=searches$search_name,full=full,prefix=prefix,search_date=Sys.Date()-1)
searches<-merge(searches,search_dates[,list(search_name,search_date)],all.x=T)

fwrite(search_dates,file=openalex_dates_file)


