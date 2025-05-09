# Debugging missing sites

missing_lat<--19.7833
missing_lon<-47.1

# chirps ####
(file<-tail(list.files(era_dirs$era_geodata_dir,"chirps_2.*parquet",full.names = T),1))
chirps<-data.table(arrow::read_parquet(file))

chirps_sites<-chirps[,unique(Site.Key)]

grep(missing_lat,chirps_sites,value=T)

chirps[Site.Key==grep(missing_lat,chirps_sites,value=T)[1]]

# power ####
(file<-tail(list.files(era_dirs$era_geodata_dir,"POWER_2.*parquet",full.names = T),1))
power<-data.table(arrow::read_parquet(file))

power_sites<-power[,unique(Site.Key)]

grep(missing_lat,power_sites,value=T)

power[Site.Key==grep(missing_lat,power_sites,value=T)[1]]

# power x chirps ####
setnames(chirps,c("Rain","day_count"),c("Rain_chirps","DayCount"),skip_absent = T)

power_chirps<-merge(power,chirps[,.(Site.Key,DayCount,Rain_chirps)],by=c("Site.Key","DayCount"),all.x=T,sort=F)  

power_chirps_sites<-power_chirps[,unique(Site.Key)]

grep(missing_lat,power_chirps_sites,value=T)
grep(missing_lon,power_chirps_sites,value=T)

power_chirps[Site.Key==grep(missing_lat,isda_sites,value=T)[1]]

# Does the site have incomplete climate data?
(missing<-power_chirps[is.na(ETMAX) & Date<"2024-01-01",.(from=min(Date),to=max(Date)),by=Site.Key])
(missing<-unique(era_data[Site.Key %in% missing$Site.Key & Buffer<50000,.(Code,Site.Key,Site.ID,Country,M.Year)]))

missing[grepl(missing_lat,Site.Key)]

# elevation ####
(file<-tail(list.files(era_dirs$era_geodata_dir,"elevation.*parquet",full.names = T),1))
elevation<-data.table(arrow::read_parquet(file))

elevation_sites<-elevation[,unique(Site.Key)]

grep(missing_lat,elevation_sites,value=T)

elevation[Site.Key==grep(missing_lat,elevation_sites,value=T)[1]]

# isda ####

# Site is not in Africa it will not be in isda
era_locations[grepl(missing_lon,era_locations_vect_g$Site.Key) & grepl(missing_lat,era_locations_vect_g$Site.Key)]

(file<-tail(list.files(era_dirs$era_geodata_dir,"^isda_2.*parquet",full.names = T),1))

isda<-data.table(arrow::read_parquet(file))

isda_sites<-isda[,unique(Site.Key)]

grep(missing_lat,isda_sites,value=T)
grep(missing_lon,isda_sites,value=T)

isda[Site.Key==grep(missing_lat,isda_sites,value=T)[1]]

# isda_watbal ####

# Site is not in Africa it will not be in isda
era_locations[grepl(missing_lon,era_locations_vect_g$Site.Key) & grepl(missing_lat,era_locations_vect_g$Site.Key)]

(file<-tail(list.files(era_dirs$era_geodata_dir,"watbal-isda_2.*parquet",full.names = T),1))

wbisda<-data.table(arrow::read_parquet(file))

wbisda_sites<-wbisda[,unique(Site.Key)]

grep(missing_lat,wbisda_sites,value=T)
grep(missing_lon,wbisda_sites,value=T)

wbisda[Site.Key==grep(missing_lat,wbisda_sites,value=T)[1]]


# soilgrids2.0 (not Africa) ####

# Site is in Africa it will not be in soilgrid2.0
era_locations[grepl(missing_lon,era_locations_vect_g$Site.Key) & grepl(missing_lat,era_locations_vect_g$Site.Key)]

(file<-tail(list.files(era_dirs$era_geodata_dir,"soilgrids2.0_2.*parquet",full.names = T),1))

sg2<-data.table(arrow::read_parquet(file))

sg2_sites<-sg2[,unique(Site.Key)]

grep(missing_lat,sg2_sites,value=T)
grep(missing_lon,sg2_sites,value=T)

sg2[Site.Key==grep(missing_lat,sg2_sites,value=T)[1]]

# sos ####

(file<-tail(list.files(era_dirs$era_geodata_dir,"sos_2.*RData",full.names = T),1))

sos<-miceadds::load.Rdata2(basename(file),dirname(file))
sos<-sos$Seasonal_SOS2

sos_sites<-sos[,unique(Site.Key)]

grep(missing_lat,sos_sites,value=T)
grep(missing_lon,sos_sites,value=T)

sos[Site.Key==grep(missing_lat,sos_sites,value=T)[1]]

# ERA.compiled ####
(file<-tail(list.files(era_dirs$era_masterdata_dir,"compiled.*parquet",full.names = T),1))
era<-data.table(arrow::read_parquet(file))

era_sites<-era[,unique(Site.Key)]

grep(missing_lat,era_sites,value=T)
grep(missing_lon,era_sites,value=T)

# Subset data to crop yields of annual products with no temporal aggregation and no combined products
perennial_subtypes<-c("Fibre & Wood","Tree","Fodders","Nuts") 
perennial_crops<-c("Sugar Cane (Cane)","Sugar Cane (Sugar)","Coffee","Gum arabic","Cassava or Yuca","Pepper","Vanilla","Tea",
                   "Plantains and Cooking Bananas","Jatropha (oil)","Palm Oil","Olives (fruits)" , "Olives (oil)",
                   "Taro or Cocoyam or Arrowroot","Palm Fruits (Other)","Turmeric","Ginger", "Cardamom",
                   "Apples","Oranges","Grapes (Wine)","Peaches and Nectarines","Jujube","Jatropha (seed)","Vegetables (Other)",
                   "Herbs","Pulses (Other)","Beans (Other)","Leafy Greens (Other)","Bananas (Ripe Sweet)","Jatropha") 

era_yields<-era[Outcode == 101 &  # Subset to crop yield outcomes
                            !nchar(M.Year)>6 &  # Remove observations aggregated over time
                            !Product.Subtype %in% perennial_subtypes & # Remove observations where yields are from perennial crops
                            !Product %in% perennial_crops # Remove observations where yields are from perennial crops
][!grepl(" x |-",Product),] # Remove observations where yields are from multiple products are combined

# Remove sites spatially aggregated over a large area
era_yields<-era_yields[!Buffer>50000]

era_yields_sites<-era_yields[,unique(Site.Key)]

grep(missing_lat,era_yields_sites,value=T)
grep(missing_lon,era_yields_sites,value=T)

era_yields[Site.Key==grep(missing_lat,era_yields_sites,value=T)[1]]


# Explore data from script 3 - run script 3 first ####
SS[Site.Key==grep(missing_lat,era_yields_sites,value=T)[1]]

# Need to refine getting planting date from nearby site as we have planting date for rice at the 
# site for one year x season, but it is not being used for others.

