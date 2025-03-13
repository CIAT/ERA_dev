# First run R/0_set_env.R

# 1) Set-up workspace ####
  ## 1.1) Load packages and create functions #####
  pacman::p_load(data.table,pbapply,future,future.apply,elevatr,RCurl)
  curlSetOpt(timeout = 190) # increase timeout if experiencing issues with slow connection
  source("https://raw.githubusercontent.com/CIAT/ERA_dev/refs/heads/main/R/add_geodata/functions/get_elevation.R")
  
  ## 1.2) Increase globals size ####
  options(future.globals.maxSize = 2 * 1024^3)
# 2) Download data ####
  
  results<-get_elevation(df=era_locations[order(Buffer)],
                      out_dir=era_dirs$dem_dir,
                      z = 12,
                      overwrite = FALSE,
                      max_tries = 3,
                      workers = 15,
                      use_progressr = TRUE,
                      calc_topo=T,
                      lat_col = "Latitude",
                      lon_col = "Longitude",
                      id_col = "Site.Key",
                      buffer_col = "Buffer")
  
  fwrite(results$stats,file=file.path(era_dirs$era_geodata_dir,"elevation.csv"))

  