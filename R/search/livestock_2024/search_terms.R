# ERA livestock update search terms

# Create functions ####
add_quotes<-function(vector){
  sapply(vector, function(term) {
    if (lengths(strsplit(term, "\\s+")) > 1) {
      return(paste0('"', term, '"'))
    } else {
      return(term)
    }
  })
}
# Create terms ####
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

# Convert to boolean ####

outcome_terms2 <-add_quotes(outcome_terms)
outcome_boolean<-paste0("(",paste0(outcome_terms2,collapse = " OR "),")")

outcome_terms_ex2 <-add_quotes(c(outcome_terms,outcome_terms_extra))
outcome_ex_boolean<-paste0("(",paste0(outcome_terms_ex2,collapse = " OR "),")")

animal_terms2 <-add_quotes(animal_terms)
animal_boolean<-paste0("(",paste0(animal_terms2,collapse = " OR "),")")

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
            l5=experiment_boolean)

# Create searches
searchs<-list(l12345=paste0(unlist(terms[c(1,3:6)]),collapse=" AND "),
              l1234=paste0(unlist(terms[c(1,3:5)]),collapse=" AND "),
              l123=paste0(unlist(terms[c(1,3:4)]),collapse=" AND "),
              lex_12345=paste0(unlist(terms[c(2,3:6)]),collapse=" AND "),
              lex_1234=paste0(unlist(terms[c(2,3:6)]),collapse=" AND "),
              lex_123=paste0(unlist(terms[c(2,3:6)]),collapse=" AND "))


