# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

p_load(rvest,tidyverse,data.table,future,future.apply,progressr,parallel,httr,arrow)

check_node_exists <- function(node_number,timeout_seconds) {
  url <- paste0("https://www.feedipedia.org/node/", node_number)
  response <- GET(url,timeout(timeout_seconds))
  if (status_code(response) == 200) {
    return(url)
  } else {
    return(NULL)
  }
}

# 1) Set save directory ####
save_dir<-file.path(era_dirs$ancillary_dir,"feedipedia")
if(!dir.exists(save_dir)){
  dir.create(save_dir)
}

# 2) Set parameters ####
nodes<-sample(3:30000,replace=F)
timeout_seconds<-60
worker_n<-parallel::detectCores()-1

# 3) Loop through downloading nodes ####
future::plan("multisession", workers = worker_n)

# Enable progressr
progressr::handlers(global = TRUE)
progressr::handlers("progress")

p <- progressr::with_progress({
  # Define the progress bar
  progress <- progressr::progressor(along = nodes)
  
f_tabs<-future_lapply(nodes,FUN=function(j){
  progress(sprintf("Node %d/%d", j, max(nodes)))
  #       f_tabs<-lapply(nodes,function(j){
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("Node: ", j, "/", max(nodes))
    flush.console()
    
    save_file<-file.path(save_dir,paste0(j,".csv"))
  
  if(!file.exists(save_file)){
  u<-check_node_exists(j,timeout_seconds)
  
  if(!is.null(u)){
  content <- read_html(u) 
  
  #Extract the title of the page
  page_title <- content %>%
    html_node("title") %>%  # Select the <title> element
    html_text()             # Extract the text inside the <title> tag
  page_title<-trimws(unlist(tstrsplit(page_title,"[|]",keep=1)))
  
  # Extract table titles
  titles <- content %>% html_nodes("h3") %>% html_text()
  titles<-titles[-(1:6)]
  titles <- gsub("\u00A0", " ", titles)
  titles<-titles[!titles %in%  c("Project leaders","Associate organizations","Beef cattle","Project governance",
                                 "Editing and administration","Scientific authors and data providers",
                                 "Scientific advisors","Other contributors","Explore Feedipedia","Sponsors",
                                 "Other supporting organizations","Get involved with Feedipedia","Pigs",
                                 "Poultry","Fish ","Pesticides and contaminants","Spines", 
                                 "Abstract","Introduction","Feed additive strategies","References","Résumé",
                                 "Direct effects of climate change on livestock","Indirect effects of climate change on livestock",
                                 "Concept of multiple stressor impacts on livestock","Impact of climate change on livestock production",
                                 "Impact of climate change on livestock reproduction","Impact of climate change on livestock adaptation" , 
                                 "Impact of climate change on livestock diseases","Conclusion",                                
                                 "Future perspectives","Forage","Seeds","Enterprise driven","Collective investment driven",
                                 "Farm organization","Personnel management","Management system","Production management","Profit",
                                 "Dividend mode","Programme","Speakers","2.1 Feed Technology Advancements in Tunisia",
                                 "2.2 A case of pellet feed use in Tunisia","Ease of collecting raw data", "Signs of ergot on plants",
                                 "Ergot alkaloids","Ergotism symptoms in animals","Treatments","Legislation","Further reading",
                                 "Description","Different categories of organic trace minerals","Process of preparing of amino acid chelates of trace minerals",
                                 "Benefits of using organic trace minerals for livestock production","Testing of chelated minerals",
                                 "Molecular Size determination of chelated minerals","Test for solubility and structural integrity of complexed and chelated trace minerals",
                                 "Conclusions and applications")]
  
  titles<-titles[!grepl("\n\t",titles,fixed = T)]
  
  if(length(titles)!=0){
  # Extract the table titles (inside h3) and their links (inside a)
  f_nodes <- unlist(tail(tstrsplit(content %>%  html_nodes("h3 a") %>% html_attr(., "href"),"/"),1))
  
  tbls <- content %>% html_table(fill = TRUE)
  
  # Is data from FAO or Bo Gohl?
  is_fao<-any(sapply(tbls,function(x){if(sum(dim(x))!=0){grepl("IMPORTANT INFORMATION: ",x[1,1])}else{F}}))

  # Subset to nutritional composition tables
  keep_tabs<- sapply(tbls,function(x){if(sum(dim(x))!=0){x[1,1] %in% c("Main analysis","Amino acids","Ruminant nutritive values","Pig nutritive values")}else{T}})
  keep_tabs[is.na(keep_tabs)]<-F
  tbls<-tbls[keep_tabs]
  
  # Remove any 0 x 0 dimension tables
  zero_dim<-sapply(tbls,function(x){sum(dim(x))!=0})
  tbls<-tbls[zero_dim]
  titles<-titles[zero_dim]
  f_nodes<-f_nodes[zero_dim]
  
  dat<-rbindlist(lapply(1:length(titles),FUN=function(i){
    # Display progress
    cat('\r', strrep(' ', 150), '\r')
    cat("Node: ", j, "/", max(nodes),"| i = ",i)
    flush.console()
    
    tb <- tbls[[i]]
    colnames(tb) <- tb[1,]
    tb <- tb[-1,]
    tb<-data.table(tb)
    tb<-tb[Unit!="" & !Unit=="Unit"][,-8]
    num_cols<-c("Avg","SD","Min","Max","Nb")
    
    tb <- tb[, (num_cols) := lapply(.SD,as.numeric), .SDcols = num_cols]
    tb[,Diet.Item:=titles[i]
       ][,feedipedia_node:=f_nodes[i]
         ][,page_title:=page_title
           ][,page_node:=j]
    
    colnames(tb)[1]<-"Variable"
    tb
  }))
  
  dat[,is_fao_or_gohl:=is_fao]
  
  fwrite(dat,save_file)
  dat
  }else{
    fwrite(data.table(status=paste(page_title,"- no nutritional table")),save_file)
    NULL
  }
  }else{
    fwrite(data.table(status="does not exist"),save_file)
    NULL
  }
  }else{
    dat<-suppressWarnings(fread(save_file))[,page_node:=j]
    if("status" %in% colnames(dat)[1:2]){
      NULL
    }else{
      dat
    }
  }
})

})

plan("sequential")

# 4) Merge downloaded data ####
f_data<-rbindlist(f_tabs)

# 5) Save results ####
arrow::write_parquet(f_data,file.path(save_dir,"feedipedia.parquet"))

