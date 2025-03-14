library(soilDB)
library(terra)

p_load(soilDB,terra,data.table)

site_vect<-era_locations_vect_g

# Create a data.table with the requested structure
meta_data <- data.table(
  Name = c("bdod", "cec", "cfvo", "clay", "nitrogen", "phh2o", "sand", "silt", "soc", "ocd", "ocs", "wv0010", "wv0033", "wv1500"),
  Description = c(
    "Bulk density of the fine earth fraction",
    "Cation Exchange Capacity of the soil",
    "Volumetric fraction of coarse fragments (> 2 mm)",
    "Proportion of clay particles (< 0.002 mm) in the fine earth fraction",
    "Total nitrogen (N)",
    "Soil pH",
    "Proportion of sand particles (> 0.05 mm) in the fine earth fraction",
    "Proportion of silt particles (>= 0.002 mm and <= 0.05 mm) in the fine earth fraction",
    "Soil organic carbon content in the fine earth fraction",
    "Organic carbon density",
    "Organic carbon stocks (0-30cm depth interval only)",
    "Volumetric Water Content at 10kPa",
    "Volumetric Water Content at 33kPa",
    "Volumetric Water Content at 1500kPa"
  ),
  `Mapped units` = c(
    "cg/cm^3", "mmol(c)/kg", "cm^3/dm^3 (vol per mil)", "g/kg", "cg/kg", "pH*10", "g/kg", "g/kg", "dg/kg", "hg/m^3",
    "t/ha", "0.1 v% or 1 mm/m", "0.1 v% or 1 mm/m", "0.1 v% or 1 mm/m"
  ),
  `Conversion factor` = c(100, 10, 10, 10, 100, 10, 10, 10, 10, 10, 10, 10, 10, 10),
  `Conventional units` = c(
    "kg/dm^3", "cmol(c)/kg", "cm^3/100cm^3 (vol%)", "g/100g (%)", "g/kg", "pH", "g/100g (%)", "g/100g (%)",
    "g/kg", "kg/m^3", "kg/m^2", "volume (%)", "volume (%)", "volume (%)"
  )
)

meta_data$source<-"SoilGrids 2.0"
meta_data$resolution<-"250m"

variables<-meta_data[Name!="ocs",Name]
depths<-c("0-5", "5-15", "15-30", "30-60", "60-100") # also avilable "100-200"
stats<-c("mean") # Also available is "Q0.05", "Q0.5", "Q0.95"


dl_dir<-file.path(era_dirs$soilgrid_dir,"soilgrids2.0_download")
if(!dir.exists(dl_dir)){
  dir.create(dl_dir)
}

site_vect<-site_vect[!site_vect$Country %in% african_countries][!grepl("[.][.]",site_vect$Country)]
temp_site_vect<-file.path(era_dirs$soilgrid_dir,"site_vect_temp.shp")
terra::writeVector(site_vect,temp_site_vect)

sg2_data <- download_soilgrids_data(site_vect=temp_site_vect,
                                    variables=variables, 
                                    depth=depths, 
                                    stats=stats,
                                    dl_dir=dl_dir,
                                    worker_n = 1)




