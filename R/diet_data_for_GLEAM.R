packages<-c("miceadds","data.table","openxlsx")
p_load(char=packages)

# 1) Read in ERA livestock data ####
data<-miceadds::load.Rdata2(path=era_dirs$era_masterdata_dir,filename= list.files(era_dirs$era_masterdata_dir,"skinny_cow"))

# 2) Load era vocab ####

# Get names of all sheets in the workbook
sheet_names <- readxl::excel_sheets(era_vocab_local)
sheet_names<-sheet_names[!grepl("sheet|Sheet",sheet_names)]

# Read each sheet into a list
master_codes <- sapply(sheet_names, FUN=function(x){data.table(readxl::read_excel(era_vocab_local, sheet = x))},USE.NAMES=T)


# 3) Subset to cattle, sheep and goats ####
livestock.papers<-data$Prod.Out[grepl("Cattle|Sheep|Goat",P.Product),unique(B.Code)]

Animals.Diet.Comp_ss<-data$Animals.Diet.Comp[B.Code %in% livestock.papers,]
Animals.Out_ss<-data$Animals.Out[B.Code %in% Animals.Diet.Comp_ss[,unique(B.Code)]]
Animals.Diet.Digest_ss<-data$Animals.Diet.Digest[B.Code %in% livestock.papers,list(B.Code,D.Item)]
Site.Out_ss<-data$Site.Out[B.Code %in% Animals.Diet.Comp_ss[,unique(B.Code)]]
Pub.Out_ss<-data$Pub.Out[B.Code %in% Animals.Diet.Comp_ss[,unique(B.Code)]]


# 4) ***TO DO!!!*** Update animal diet item harmonization ####


# 5) Subset meta.data ####
era_fields<-master_codes$era_fields[Table %in% c("Pub.Out","Site.Out","Animals.Diet.Comp","Animals.Out", "Animals.Diet","Animals.Diet.Digest") & Display_Name!="FIELD NOT REQUIRED"]
era_fields<-era_fields[,list(Field,Calculated,Display_Name,Data_Type,Field_Description,Values,Ref_Source,Ref_Name,Ref_Link,Ref_Match,Ref_Notes)]

data_colnames<-unique(unlist(lapply(list(Animals.Diet.Comp_ss,Animals.Out_ss,Animals.Diet.Digest_ss,Site.Out_ss,Pub.Out_ss),colnames)))
era_fields<-era_fields[Field %in% data_colnames]

# 6) Combine datasets into excel ####
dt_list<-list(field_descriptions=era_fields,
              diet_names=Animals.Out_ss,
             diet_composition=Animals.Diet.Comp_ss,
             diet_digestibility=Animals.Diet.Digest_ss,
             location=Site.Out_ss,
             source=Pub.Out_ss)

# Create a new workbook
wb <- createWorkbook()

# Add each data.table as a sheet
for (sheet_name in names(dt_list)) {
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet = sheet_name, dt_list[[sheet_name]])
}

save_loc<-choose.dir()
saveWorkbook(wb, file.path(save_loc,"era_diet_data.xlsx"), overwrite = TRUE)
