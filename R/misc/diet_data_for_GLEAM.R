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


# 3) ***TO DO!!!*** Update animal diet item harmonization ####

# 4) Subset Intake and Weight data ####
weight_gain<-unique(data$Out.Out[,list(B.Code,Out.WG.Start,Out.WG.Unit,Out.WG.Days,Out.Code.Joined)])
data_ss<-unique(data$Data.Out[ED.Outcome %in% weight_gain$Out.Code.Joined,list(B.Code, ED.Treatment,ED.Outcome,ED.Product.Simple)])
data_ss<-merge(data_ss,unique(data$MT.Out[,list(B.Code,T.Name,T.Comp,V.Level.Name,A.Level.Name)]),
      by.x=c("B.Code","ED.Treatment","ED.Product.Simple"),
      by.y=c("B.Code","T.Name","T.Comp"),all.x=T)
weight_gain<-merge(data_ss,weight_gain,by.x=c("B.Code","ED.Outcome"),by.y=c("B.Code","Out.Code.Joined"))
weight_gain<-unique(weight_gain[!is.na(Out.WG.Start)][,ED.Outcome:=NULL])

feed_intake<-data$Data.Out[grepl("Feed Intake",ED.Outcome),
                           list(B.Code,ED.Site.ID,ED.Treatment,ED.Product.Simple,ED.Product.Comp,ED.M.Year,ED.Outcome,ED.Intake.Item,ED.Mean.T,ED.Error,ED.Error.Type)]
feed_intake<-merge(feed_intake,unique(data$MT.Out[,list(B.Code,T.Name,T.Comp,V.Level.Name,A.Level.Name)]),
               by.x=c("B.Code","ED.Treatment","ED.Product.Simple"),
               by.y=c("B.Code","T.Name","T.Comp"),all.x=T)

# 5) Combine datasets into excel ####
dt_list<-list(field_descriptions=master_codes$era_fields,
              source=data$Pub.Out,
              location=data$Site.Out,
              exp_design=data$ExpD.Out,
              focal_species=data$Prod.Out,
              breed=data$Var.Out,
              veterinary=data$Chems.Out,
              diet_names=data$Animals.Out,
              diet_ingredients=data$Animals.Diet,
              diet_nutrition=data$Animals.Diet.Comp,
              diet_digestibility=data$Animals.Diet.Digest,
              treatments=data$MT.Out,
              outcomes=data$Out.Out,
              values=data$Data.Out,
              era_vocab=master_codes$AOM
         )

# Create a new workbook
wb <- createWorkbook()

# Add each data.table as a sheet
for (sheet_name in names(dt_list)) {
  addWorksheet(wb, sheet_name)
  writeData(wb, sheet = sheet_name, dt_list[[sheet_name]])
}

save_dir<-file.path(getwd(),"era2gleam")
if(!file.exists(save_dir)){
  dir.create(save_dir)
}

saveWorkbook(wb, file.path(save_dir,"era_diet_data.xlsx"), overwrite = TRUE)

# 6) Wrangle to GLEAM format ####
# 6.1) Merge diet_composition, diet_digestibility and study location
diet_names<-data$Animals.Out[,code:=paste0(A.Level.Name,"-",B.Code)]
diet_composition<-data$Animals.Diet.Comp[,code:=paste0(D.Item,"-",B.Code)]
diet_digestibility<-data$Animals.Diet.Digest[,code:=paste0(D.Item,"-",B.Code)]
# Remove "compound" diets from composition and digestibility
diet_composition[!code %in% diet_names$code]

