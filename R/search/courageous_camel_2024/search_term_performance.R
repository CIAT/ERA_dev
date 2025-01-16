# First run R/0_set_env.R & R/create_search_query_oa.R
# 0) Load packages ####
pacman::p_load(data.table,openalexR,pbapply,s3fs)

# 1) Set-up workspace ####
  # 1.1) Set directories #####
project<-era_projects$courageous_camel_2024
search_data_dir<-file.path(era_dirs$era_search_dir,project)
s3_bucket<-file.path(era_dirs$era_search_s3,era_projects$courageous_camel_2024)


# 2) Download any files needed from the s3 bucket ####
update<-F
s3_files<-s3$dir_ls(s3_bucket)
local_files<-file.path(search_data_dir,basename(s3_files))
s3_files<-s3_files[!basename(s3_files) %in% basename(local_files)]

if(length(s3_files)>0|update==T){
  for(i in 1:length(s3_files)){
    cat("downloading file",s3_files[i],"\n")
    options(timeout = 300) 
    s3$file_download(s3_files[i], file.path(search_data_dir,basename(s3_files[i])))
  }
}

# 3) Load search history ####
search_history<-fread(file.path(search_data_dir,"search_history.csv"))[,-1]
search_history[,search_date:=NULL]

search_history_oa<-fread(file.path(search_data_dir,"searches.csv"))[,list(terms,search_name)]
setnames(search_history_oa,c("terms","search_name"),c("search_name","filename"))
search_history_oa[,citation_db:="openalex"
                  ][,timeframe:="all"
                    ][,filename:=paste0("openalex_ta-only_",filename,".csv")
                      ]

search_history<-rbindlist(list(search_history,search_history_oa),use.names = T)

# 4) Load validation data ####
  # 4.1) Load titles that should be included #####
  search_manual<-fread(file.path(search_data_dir,"search_manual.csv"))
  search_manual<-search_manual[,year:=as.numeric(year)
                               ][year>=2018
                                 ][,doi:=trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi))
                                   ][,title:=trimws(tolower(title))]
  
  # 4.2) Check which dois & titles are in openalex #####
  # dois
  oa_dois<-data.table(oa_fetch(
    entity = "works",
    doi = search_manual[!is.na(doi),doi],
    verbose = TRUE
  ))[,indexed_oa:="yes"
     ][,list(doi,indexed_oa)
       ][,doi:=trimws(gsub("https://doi.org/|http://dx.doi.org/","",doi))]
  
  # titles
  oa_titles<-lapply(1:nrow(search_manual),FUN=function(i){
      oa_fetch(
        entity = "works",
        display_name.search = gsub("[^a-zA-Z0-9 ]", "",search_manual[i,title]),
       verbose = TRUE
    )
  })
  
  oa_titles<-rbindlist(oa_titles,use.names = T,fill = T)
  
  oa_titles<-oa_titles[,indexed_oa:="yes"][,list(title,indexed_oa)]
  
  # Merge oa results with the the validation data
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
  
# 5) Load search results ####
search_files<-list.files(search_data_dir,full.names = T)

  # 5.1) wos #####
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
  
  # 5.2) scopus #####
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
  
  # 5.3) openalex #####
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
                  ][,year:=NA]
  
  # 5.4) Harmonize fields between citation databases #####
  terms<-c("authors","title","year","doi","abstract","keywords","search","citation_db","citation_db_id","relevance_score")
  wos<-c("Authors","Article Title","Publication Year","DOI","Abstract","Author Keywords","search","citation_db","UT (Unique WOS ID)","relevance_score")
  scopus<-c("Authors","Title","Year","DOI","Abstract","Author Keywords","search","citation_db","EID","relevance_score")
  oa<-c("authors","title","year","doi","ab","keywords","search","citation_db","id","relevance_score")
  
  # 5.5) Merge data #####
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
  remove_phrases<-c("correction to:","corrigendum to:","erratum","editor's evaluation")
  searches<-searches[!grepl(paste0(remove_phrases,collapse = "|"),title,ignore.case = T)]
  
  # 5.6) Cross reference to manual search #####
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
  
  # 5.7) Assess search performance #####
  search_perf<-searches[,list(search,citation_db,doi,title,target_doi,target_title,target_any)][,hits:=.N,by=search]
  
  search_perf[,list(performance=round(sum(target_any)/nrow(search_manual),2)),by=list(search,citation_db,hits)][order(performance,decreasing = T)]
  
  (perf_search_db<-search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2)),by=list(search,citation_db,hits)][order(performance,decreasing = T)])
  (perf_search<-search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2)),by=list(search)][order(performance,decreasing = T)])
  (perf_all<-search_perf[target_any==T,list(performance=round(length(unique(title))/nrow(search_manual),2))])
  
  best_search<-perf_search_db[performance==max(performance)][hits==min(hits),search]
  
    # 5.7.1) What papers are in what databases? ######
    search_perf[citation_db=="openalex" & target_any==T,sort(unique(title))]
    search_perf[citation_db=="scopus" & target_any==T,sort(unique(title))]
    search_perf[citation_db=="wos" & target_any==T,sort(unique(title))]
    
    # 5.7.2) Append search results to validation table #####
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
    
    # 5.7.3) What hits are in other openalex searches but not in the top search? ######
    oa_best<-search_perf[citation_db=="openalex" & search==best_search & target_any,list(doi,title)][,best:=T]
    oa_other<-unique(search_perf[citation_db=="openalex" & search!=best_search & target_any,list(search,doi,title)])
    oa_other<-merge(oa_other,oa_best[,list(doi,title,best)],all.x=T)
    oa_other[is.na(best),unique(title)]
    n<-nrow(unique(oa_other[is.na(best),list(doi,title)]))
    oa_second<-perf_search_db[search %in% oa_other[is.na(best),list(N=.N),by=search][N==n,search]][hits==min(hits),search]
    
    # Final search
    nrow(oa_best) # Papers found by best search
    nrow(oa_best)== hits[,sum(hit_any)] # Does it account for all search hits?
    
    # Papers found by top 2 searchs
    final_search<-unique(searches[search %in% c(best_search,oa_second) & citation_db=="openalex",list(doi,title,target_any)])
    final_search[,sum(target_any)]
    # Do these search contain all the hits from all searches?
    final_search[,sum(target_any)] == hits[,sum(hit_any)]
    
    # 5.7.4) Summarize final search ######
    oa_searches[,search2:=gsub(".csv","",tail(unlist(tstrsplit(search[1],"_")),1)),by=search]
    final_search<-unique(oa_searches[,search:=tail(unlist(tstrsplit(search[1],"_")),1),by=search][search %in% c(best_search,oa_second)][,search:=NULL])
    
    # 6) Run full retrival for final search ####
    # Source era helper function for running oa search
    source("https://raw.githubusercontent.com/CIAT/ERA_dev/main/R/search/era_oa_query.R")
    
    # This is a bit of legacy issue, the .csv saving of the search terms does not appear to retain the formatting needing to run the searches.
    # We need to get the terms then get the search from RData files recording the searches made.
    search_history_oa<-fread(file.path(search_data_dir,"searches.csv"))
    search_1<-search_history_oa[search_name==best_search,string]
    search_2<-search_history_oa[search_name==oa_second,string]
    
    # Check if the search include geo term (T) or not (F)
    grepl("Cabo Verde",search_1)
    grepl("Cabo Verde",search_2)
    
    # Load Rdata versions of the searches
    load.Rdata(file.path(search_data_dir,"oa_searches_auth.RData"),"auth_searches")
    load.Rdata(file.path(search_data_dir,"oa_searches_geo.RData"),"geo_searches")

    # Get the relevant search strings for best and second best searches (which together found all the target pdfs)
    search_1<-auth_searches[terms==search_history_oa[search_name==best_search,terms],string]
    search_2<-geo_searches[terms==search_history_oa[search_name==oa_second,terms],string]
    
    # Set search dates
    from_date <- "2018-01-01"
    to_date <- Sys.Date()
    
    # Run searches
    results_auth<-run_openalex_query(search_terms=search_1, 
                                     from_date = from_date, 
                                     to_date = to_date, 
                                     continent = "africa", 
                                     download = TRUE,
                                     full = TRUE, 
                                     max_char_limit = 4000)
    
    results_geo<-run_openalex_query(search_terms=search_2, 
                                     from_date = from_date, 
                                     to_date = to_date, 
                                     continent = NULL, 
                                     download = TRUE,
                                     full = TRUE, 
                                     max_char_limit = 4000)
    
    results_merged<-unique(rbind(results_auth$results,results_geo$results))
    results_merged[,publication_date:=as.Date(publication_date)]
    results_merged<-results_merged[,.(relevance_min=min(relevance_score),relevance_max=max(relevance_score),relevance_mean=mean(relevance_score),publication_date=publication_date[1]),by=.(id,display_name,ab,doi,url,is_oa,language,type,authors)][order(doi)]
    
    # check for duplicates
    results_merged[!is.na(doi),which(duplicated(doi))]
    results_merged[!is.na(id),which(duplicated(id))]
    results_merged[!is.na(url),which(duplicated(url))]
    dim(results_merged)
    
    # List results and save
    final_results<-list(search1=results_auth,search2=results_geo,combined_results=results_merged)
    save(final_results,file=file.path(search_data_dir,"final_search.RData"))
    