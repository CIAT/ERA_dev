

if (!requireNamespace("pacman", quietly = TRUE)) {
  install.packages("pacman")
}

pacman::p_load(dm,data.table,DiagrammeR,miceadds)


# Set a directory for downloaded data
dl_dir <- "downloaded_data"

# Create the directory if it doesn't already exist
if(!dir.exists(dl_dir)){
  dir.create(dl_dir, showWarnings = FALSE)
}

# Download the data tables ####

# Create an S3 filesystem handle
s3 <- s3fs::S3FileSystem$new(anonymous = TRUE)
era_s3 <- "s3://digital-atlas/era"

# List files in the s3 bucket
all_files <- s3$dir_ls(file.path(era_s3, "data"))

# Filter for the "agronomic" RData file, selecting the most recent version
(target_file <- tail(
  grep(".RData", grep("agronomic", all_files, value = TRUE), value = TRUE),
  1
))

# Define a local path for saving the downloaded file
save_path <- file.path(getwd(), dl_dir, basename(target_file))

# Download the file if it does not exist already
if (!file.exists(save_path)) {
  s3$file_download(target_file, save_path, overwrite = TRUE)
}

# Load the agronomy data using the miceadds package
agronomic_data <- miceadds::load.Rdata2(
  file = basename(save_path),
  path = dirname(save_path)
)

# Download the era master vocab #####
era_vocab_url <- "https://github.com/peetmate/era_codes/raw/main/era_master_sheet.xlsx"
era_vocab_local <- file.path(getwd(), basename(era_vocab_url))
download.file(era_vocab_url, era_vocab_local, mode = "wb")  # Download and write in binary mode

# Import the vocab
sheet_names <- readxl::excel_sheets(era_vocab_local)
sheet_names <- sheet_names[!grepl("sheet|Sheet", sheet_names)]

# Read each sheet into a list named downloaded_data
era_master_codes <- sapply(
  sheet_names,
  FUN = function(x) {
    data.table::data.table(readxl::read_excel(era_vocab_local, sheet = x))
  },
  USE.NAMES = TRUE
)


# Prepare data tables and master vocab #### 

tab<-"Prod.Out"
data<-agronomic_data[[tab]]  
setnames(data,c("P.Product.Subtype","P.Product"),c("Product.Subtype","Product.Simple"),skip_absent = T)
agronomic_data[[tab]] <-data

tab<-"MT.Out"
data<-agronomic_data[[tab]]  
setnames(data,c("P.Product"),c("Product.Simple"),skip_absent = T)
data<-merge(data,agronomic_data$Prod.Out[,.(B.Code,Product.Simple,Product.Subtype)],by=c("B.Code","Product.Simple"))
agronomic_data[[tab]] <-data

tab<-"Var.Out"
data<-agronomic_data[[tab]]  
setnames(data,c("V.Product"),c("Product.Simple"),skip_absent = T)
data<-merge(data,agronomic_data$Prod.Out[,.(B.Code,Product.Simple,Product.Subtype)],by=c("B.Code","Product.Simple"))
agronomic_data[[tab]] <-data

tab<-"Chems.Out"
data<-agronomic_data[[tab]]  
setnames(data,c("Times"),c("Time"),skip_absent = T)
agronomic_data[[tab]] <-data

data<-era_master_codes$vars
setnames(data,c("V.Product"),c("Product.Simple"),skip_absent = T)
data[,V.Var:=V.Var1][,V.Species:=V.Species1][,c("V.Var1","V.Species1","V.Subspecies1"):=NULL]
era_master_codes$vars<-data

data<-era_master_codes$chem
chem<-data[,.(AOM,C.Type...3,C.Type.AI,C.Name,C.Is.Name.Commercial,C.AI)]
setnames(chem,"C.Type...3","C.Type",skip_absent = T)
chem<-chem[!is.na(C.Type)]

chem.comm<-data[,.(C.Name.Commercial,C.Name.AI...10,C.Animal,C.Crop,C.Is.Chem,C.Type...14)]
setnames(chem.comm,c("C.Name.AI...10","C.Type...14"),c("C.Name.AI","C.Type"),skip_absent = T)
chem.comm<-chem.comm[!is.na(C.Name.Commercial)]

chem.ai<-data[,.(C.Name.AI...16,C.Name.AI.CHEBI,C.Tname.AI.Type_gpt)]
setnames(chem.ai,c("C.Name.AI...16"),c("C.Name.AI"))
chem.ai<-chem.ai[!is.na(C.Name.AI)]

era_master_codes$chem<-chem
era_master_codes$chem.ai<-chem.ai
era_master_codes$chem.comm<-chem.comm

tab<-"Out.Out"
data<-agronomic_data[[tab]]
setnames(data,"Out.Subind","Subindicator")
agronomic_data[[tab]] <-data

tab<-"Res.Method"
data<-agronomic_data[[tab]]
setnames(data,"M.Tree","Tree.Latin.Name",skip_absent = T)
agronomic_data[[tab]] <-data

tab<-"AF.Trees"
data<-agronomic_data[[tab]]
setnames(data,"AF.Tree","Tree.Latin.Name",skip_absent = T)
agronomic_data[[tab]] <-data

# Dev.Note: Probably needs converting from wide to long format
tab<-"Int.Out"
data<-agronomic_data[[tab]]
data[,T.Name:=IN.Level.Name] # Note this multiple T.Names is concatenated with a "..."
agronomic_data[[tab]] <-data

tab<-"Rot.Seq"
data<-agronomic_data[[tab]]
data[!is.na(R.Prod2)|!is.na(IN.Code),IN.Level.Name:=R.Treatment]
data[is.na(R.Prod2) & is.na(IN.Code),T.Name:=R.Treatment]
agronomic_data[[tab]] <-data

# Create data model ####

era_agronomy.model <- dm(
  Pub.Out = agronomic_data$Pub.Out,
  Site.Out = agronomic_data$Site.Out,
  Times.Out = agronomic_data$Times.Out,
  Times.Clim = agronomic_data$Times.Clim,
  Prod.Out = agronomic_data$Prod.Out,
  ExpD.Out = agronomic_data$ExpD.Out,
  Var.Out = agronomic_data$Var.Out,
  Till.Out = agronomic_data$Till.Out,
  Plant.Out = agronomic_data$Plant.Out,
  Plant.Method = agronomic_data$Plant.Method,
  PD.Codes = agronomic_data$PD.Codes,
  PD.Out = agronomic_data$PD.Out,
  Chems.Code = agronomic_data$Chems.Code,
  Chems.Out = agronomic_data$Chems.Out,
  # Chems.AI = agronomic_data$Chems.AI,
  Res.Out = agronomic_data$Res.Out,
  Res.Method = agronomic_data$Res.Method,
  # Res.Comp = agronomic_data$Res.Comp,
  Fert.Out = agronomic_data$Fert.Out,
  Fert.Method = agronomic_data$Fert.Method,
  # Fert.Comp =  agronomic_data$Fert.Comp,
  Har.Out = agronomic_data$Har.Out,
  pH.Out = agronomic_data$pH.Out,
  pH.Method = agronomic_data$pH.Method,
  WH.Out = agronomic_data$WH.Out,
  AF.Out = agronomic_data$AF.Out,
  AF.Trees=agronomic_data$AF.Trees,
  Weed.Out=agronomic_data$Weed.Out,
  Other.Out=agronomic_data$Other.Out,
  Irrig.Codes=agronomic_data$Irrig.Codes,
  Irrig.Method=agronomic_data$Irrig.Method,
  MT.Out = agronomic_data$MT.Out,
  Int.Out = agronomic_data$Int.Out,
  Rot.Out = agronomic_data$Rot.Out,
  Rot.Seq = agronomic_data$Rot.Seq,
  Rot.Seq.Summ = agronomic_data$Rot.Seq.Summ,
  Out.Out = agronomic_data$Out.Out,
  Data.Out=agronomic_data$Data.Out,
  era_master_codes.fert = era_master_codes$fert,
  era_master_codes.prod  = era_master_codes$prod,
  era_master_codes.vars = era_master_codes$vars,
  era_master_codes.chem = era_master_codes$chem,
  era_master_codes.residues = era_master_codes$residues,
  era_master_codes.trees = era_master_codes$trees,
  era_master_codes.out = era_master_codes$out,
  era_master_codes.prac = era_master_codes$prac) |>
  
  # PRIMARY KEYS
  dm_add_pk(Pub.Out, B.Code) |>
  dm_add_pk(Site.Out, c(B.Code,Site.ID))|>
  dm_add_pk(Times.Out, c(B.Code,Time)) |>
  dm_add_pk(Times.Clim, c(B.Code,Site.ID,Time)) |>
  dm_add_pk(Prod.Out, c(B.Code,Product.Subtype,Product.Simple)) |>
  dm_add_pk(ExpD.Out, B.Code) |>
  dm_add_pk(Var.Out, c(B.Code,V.Level.Name)) |>
  dm_add_pk(Till.Out, c(B.Code,Till.Level.Name)) |>
  dm_add_pk(Plant.Out, c(B.Code,P.Level.Name)) |>
  dm_add_pk(Plant.Method, c(B.Code,P.Level.Name)) |>
  dm_add_pk(PD.Codes, c(B.Code,PD.Level.Name)) |>
  dm_add_pk(PD.Out, c(B.Code,PD.Level.Name,Site.ID ,Time)) |>
  dm_add_pk(Chems.Code, c(B.Code,C.Level.Name)) |>
  dm_add_pk(Chems.Out, c(B.Code,C.Level.Name,C.Type,C.Name)) |>
  #dm_add_pk(Chems.AI, c(B.Code,C.Name)) |>
  dm_add_pk(Fert.Out, c(B.Code,F.Level.Name)) |>
  dm_add_pk(Fert.Method, c(B.Code,F.Level.Name,F.Type)) |>
  #dm_add_pk(Fert.Comp, c(B.Code,F.Type)) |>
  dm_add_pk(Res.Out, c(B.Code,M.Level.Name)) |>
  dm_add_pk(Res.Method, c(B.Code,M.Level.Name)) |>
  #dm_add_pk(Res.Comp, c(B.Code,M.Tree,M.Material)) |>
  dm_add_pk(Har.Out, c(B.Code,H.Level.Name)) |>
  dm_add_pk(pH.Out, c(B.Code,pH.Level.Name)) |>
  dm_add_pk(pH.Method, c(B.Code,pH.Level.Name)) |>
  dm_add_pk(WH.Out, c(B.Code,WH.Level.Name)) |>
  dm_add_pk(AF.Out, c(B.Code,AF.Level.Name)) |>
  dm_add_pk(AF.Trees, c(B.Code,AF.Level.Name)) |>
  dm_add_pk(Weed.Out, c(B.Code,W.Level.Name)) |>
  dm_add_pk(Other.Out, c(B.Code,O.Level.Name)) |>
  dm_add_pk(Irrig.Codes, c(B.Code,I.Level.Name)) |>
  dm_add_pk(Irrig.Method, c(B.Code,I.Level.Name)) |>
  dm_add_pk(MT.Out, c(T.Name,B.Code)) |>
  dm_add_pk(Int.Out, c(IN.Level.Name,B.Code)) |>
  dm_add_pk(Rot.Out, c(R.Level.Name,B.Code)) |>
  dm_add_pk(Rot.Seq, c(R.Level.Name,T.Name,IN.Level.Name,B.Code)) |>
  dm_add_pk(Rot.Seq.Summ, c(R.Level.Name,B.Code)) |>
  dm_add_pk(Out.Out, c(Out.Code.Joined,B.Code)) |>
  dm_add_pk(Data.Out, c(Site.ID,Time,T.Name,IN.Level.Name,R.Level.Name,Out.Code.Joined,B.Code)) |>
  dm_add_pk(era_master_codes.fert , F.Type) |>
  dm_add_pk(era_master_codes.prod, c(Product.Subtype,Product.Simple)) |>
  dm_add_pk(era_master_codes.vars, c(Product.Simple,V.Var)) |>
  dm_add_pk(era_master_codes.chem, c(C.Type,C.Name)) |>
  dm_add_pk(era_master_codes.residues, M.Material) |>
  dm_add_pk(era_master_codes.trees, Tree.Latin.Name) |>
  dm_add_pk(era_master_codes.out, Subindicator) |>
  dm_add_pk(era_master_codes.prac, Subpractice) |>
  
  # FOREIGN KEYS
  dm_add_fk(Site.Out, B.Code, Pub.Out) |>
  dm_add_fk(Times.Out, B.Code, Pub.Out) |>
  dm_add_fk(Times.Clim, c(B.Code,Time), Times.Out) |>
  dm_add_fk(Times.Clim, c(B.Code,Site.ID), Site.Out) |>
  dm_add_fk(Prod.Out, B.Code, Pub.Out) |>
  dm_add_fk(Prod.Out, c(Product.Subtype,Product.Simple), era_master_codes.prod) |>
  dm_add_fk(ExpD.Out, B.Code, Pub.Out) |>
  dm_add_fk(Plant.Out, B.Code,Pub.Out) |>
  dm_add_fk(Plant.Method, c(B.Code,P.Level.Name),Plant.Out) |>
  dm_add_fk(Var.Out, c(Product.Simple,V.Var),era_master_codes.vars) |>
  dm_add_fk(Var.Out, c(B.Code,Product.Subtype,Product.Simple), Prod.Out) |>
  dm_add_fk(Var.Out, B.Code, Pub.Out) |>
  dm_add_fk(Till.Out, B.Code, Pub.Out) |>
  dm_add_fk(Till.Out, c(B.Code,Site.ID), Site.Out) |>
  dm_add_fk(Till.Out, c(B.Code,Time), Times.Out) |>
  dm_add_fk(PD.Codes, B.Code, Pub.Out) |>
  dm_add_fk(PD.Out, c(B.Code,PD.Level.Name), PD.Codes) |>
  dm_add_fk(PD.Out, c(B.Code,Site.ID), Site.Out) |>
  dm_add_fk(PD.Out, c(B.Code,Time), Times.Out) |>
  dm_add_fk(Chems.Code, B.Code, Pub.Out) |>
  dm_add_fk(Chems.Out, c(B.Code,C.Level.Name), Chems.Code) |>
  dm_add_fk(Chems.Out, c(B.Code,Site.ID), Site.Out) |>
  dm_add_fk(Chems.Out, c(B.Code,Time), Times.Out) |>
  dm_add_fk(Chems.Out, c(C.Type,C.Name), era_master_codes.chem) |>
  dm_add_fk(Fert.Out, B.Code, Pub.Out) |>
  dm_add_fk(Fert.Method, c(B.Code,F.Level.Name), Fert.Out) |>           
  dm_add_fk(Fert.Method, F.Type, era_master_codes.fert ) |>
  dm_add_fk(Fert.Method, c(B.Code,Time), Times.Out) |>           
  dm_add_fk(Fert.Method, c(B.Code,Site.ID), Site.Out ) |>
  dm_add_fk(Res.Out, B.Code, Pub.Out) |>
  dm_add_fk(Res.Method, c(B.Code,M.Level.Name), Res.Out) |>           
  dm_add_fk(Res.Method, M.Material, era_master_codes.residues) |>
  dm_add_fk(Res.Method, Tree.Latin.Name, era_master_codes.trees) |>
  dm_add_fk(Res.Method, c(B.Code,Time), Times.Out) |>           
  dm_add_fk(Res.Method, c(B.Code,Site.ID), Site.Out ) |>
  dm_add_fk(pH.Out, B.Code, Pub.Out) |>
  dm_add_fk(pH.Method, c(B.Code,pH.Level.Name), pH.Out) |>           
  dm_add_fk(WH.Out, B.Code, Pub.Out) |>
  dm_add_fk(AF.Out, B.Code, Pub.Out) |>
  dm_add_fk(AF.Trees, c(B.Code,AF.Level.Name), AF.Out) |>           
  dm_add_fk(AF.Trees, Tree.Latin.Name, era_master_codes.trees) |>
  dm_add_fk(Weed.Out, B.Code, Pub.Out) |>
  dm_add_fk(Other.Out, B.Code, Pub.Out) |>
  dm_add_fk(Irrig.Codes, B.Code, Pub.Out) |>
  dm_add_fk(Irrig.Method, c(B.Code,I.Level.Name), Irrig.Codes) |>           
  dm_add_fk(Irrig.Method, c(B.Code,Time), Times.Out) |>           
  dm_add_fk(Irrig.Method, c(B.Code,Site.ID), Site.Out ) |>
  dm_add_fk(MT.Out, c(B.Code,Product.Subtype,Product.Simple), Prod.Out) |>
  dm_add_fk(MT.Out, c(B.Code,F.Level.Name), Fert.Out) |> 
  dm_add_fk(MT.Out, c(B.Code,P.Level.Name), Plant.Out) |>
  dm_add_fk(MT.Out, c(B.Code,V.Level.Name), Var.Out) |>
  dm_add_fk(MT.Out, c(B.Code,Till.Level.Name), Till.Out) |>
  dm_add_fk(MT.Out, c(B.Code,PD.Level.Name), PD.Codes) |>
  dm_add_fk(MT.Out, c(B.Code,C.Level.Name), Chems.Code) |>
  dm_add_fk(MT.Out, c(B.Code,M.Level.Name), Res.Out) |>
  dm_add_fk(MT.Out, c(B.Code,H.Level.Name), Har.Out) |>
  dm_add_fk(MT.Out, c(B.Code,pH.Level.Name), pH.Out) |>
  dm_add_fk(MT.Out, c(B.Code,WH.Level.Name), WH.Out) |>
  dm_add_fk(MT.Out, c(B.Code,AF.Level.Name), AF.Out) |>
  dm_add_fk(MT.Out, c(B.Code,W.Level.Name), Weed.Out) |>
  dm_add_fk(MT.Out, c(B.Code,O.Level.Name), Other.Out) |>
  dm_add_fk(MT.Out, c(B.Code,I.Level.Name), Irrig.Codes) |>
  dm_add_fk(Int.Out, c(B.Code,T.Name),MT.Out) |>
  dm_add_fk(Rot.Seq, c(B.Code,T.Name),MT.Out) |>
  dm_add_fk(Rot.Seq, c(B.Code,IN.Level.Name),Int.Out) |>
  dm_add_fk(Rot.Seq, c(B.Code,R.Level.Name),Rot.Out) |>
  dm_add_fk(Rot.Seq, c(B.Code,Time),Times.Out) |>
  dm_add_fk(Rot.Seq.Summ, c(B.Code,R.Level.Name),Rot.Out) |>
  dm_add_fk(Out.Out, Subindicator,era_master_codes.out) |>
  dm_add_fk(Data.Out, c(B.Code,Out.Code.Joined),Out.Out) |>
  dm_add_fk(Data.Out, c(B.Code,Site.ID),Site.Out) |>
  dm_add_fk(Data.Out, c(B.Code,Time),Times.Out) |>
  dm_add_fk(Data.Out, c(B.Code,T.Name),MT.Out) |>
  dm_add_fk(Data.Out, c(B.Code,IN.Level.Name),Int.Out) |>
  dm_add_fk(Data.Out, c(B.Code,R.Level.Name),Rot.Out) 

# Save data model ####
save_name<-gsub(".RData","_model.rds",basename(target_file))
save_file<-file.path(dl_dir,save_name)
saveRDS(era_agronomy.model, file = save_file)
