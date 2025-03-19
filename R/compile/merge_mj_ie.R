# First run R/0_set_env.R
# Super users, to compile the data from scratch:
  # R/import/import_majestic_hippo_2020.R
  # R/import/import_industrious_elephant_2023.R

## 0.0) Install and load packages ####
pacman::p_load(data.table, 
               miceadds)


# 1) Load data ####
## 1.1) Majestic hippo ####
files<-list.files("Data",era_projects$majestic_hippo_2020,full.names = T)
(files<-grep(".RData",files,value=T))
(files<-tail(files,1))

era_mh<-miceadds::load.Rdata2(file=basename(files),path=dirname(files))

# 1.1.1) Remove unneeded tabs ####
rm_pubs<-c(era_mh$Animals.Out[,unique(B.Code)],era_mh$E.Out[,unique(B.Code)])
era_mh$Animals.Diet<-NULL
era_mh$Animals.Diet.Comp<-NULL
era_mh$E.Out<-NULL
era_mh$Animals.Out<-NULL

# Remove data from animal or energy publications
tabs<-c("Pub.Out","Site.Out","Soil.Out","Prod.Out","ExpD.Out","Var.Out","Other.Out","Base.Out","MT.Out","MT.Out2","Int.Out","Out.Out","Data.Out","Times")

for(i in 1:length(tabs)){
  tab<-tabs[i]
  era_mh[[tab]]<-era_mh[[tab]][!B.Code %in% rm_pubs]
}

names(era_mh)[names(era_mh) == "Res.Composition"] <- "Res.Comp"
names(era_mh)[names(era_mh) == "Times"] <- "Times.Out"
names(era_mh)[names(era_mh) == "Irrig.Out"] <- "Irrig.Method"


# Replace MT.Out with MT.Out2, remove MT.Out2
era_mh$MT.Out<-era_mh$MT.Out2
era_mh$MT.Out2<-NULL

# 1.1.2) Restructure Chems.Out ####
chems_mh<-era_mh$Chems.Out
chems_mh$C.Notes<-field_absent
Chems.Code<-chems_mh[,.(B.Code,C.Level.Name,C.Structure,C.Notes,C.Code)]
setnames(Chems.Code,"C.Code","C.Codes")

Chems.Out<-chems_mh[,.(C.Level.Name,C.Type,C.Name,C.Amount,C.Unit,C.Applications,C.Date,C.Date.DAP,C.Date.Text,B.Code,C.Code)
                    ][,C.Date.Start:=as.Date(C.Date,"%d/%m/%Y")
                      ][!is.na(C.Date) & is.na(C.Date.Text) & is.na(C.Date.Start),C.Date.Text:=C.Date
                        ][,C.Date:=NULL]

Chems.AI<-chems_mh[,.(C.Name,C.Type,C.AI.Amount,C.AI.Unit,B.Code)]

era_mh$Chems.Code<-unique(Chems.Code)
era_mh$Chems.Out<-unique(Chems.Out)
era_mh$Chems.AI<-unique(Chems.AI)

# 1.1.3) Restructure Plant.Out ####
plant_mh<-era_mh$Plant.Out
setnames(plant_mh,"P.Method","Plant.Method")
Plant.Out<-plant_mh[,.(P.Product,P.Level.Name,P.Structure,B.Code)]
Plant.Method<-plant_mh[,.(P.Level.Name,Plant.Method,Plant.Density,Plant.Density.Unit,Plant.Row,Plant.Station,Plant.Seeds,Plant.Thin,Plant.Tram.Row,Plant.Tram.N,Plant.Intercrop,Plant.Block.Rows,Plant.Block.Perc,Plant.Block.Width,B.Code)]

era_mh$Plant.Out<-Plant.Out
era_mh$Plant.Method<-Plant.Method

# 1.1.4) Restructure AF.Out ####

af_mh<-era_mh$AF.Out
AF.Out<-af_mh[,.(AF.Level.Name,AF.Notes,AF.Codes,B.Code)]
AF.Out[AF.Codes=="a99",AF.Other:="Yes"]
AF.Out[AF.Codes=="a11",AF.Silvopasture:="Silvopasture"]
AF.Out[AF.Codes=="a11",AF.Silvopasture:="Agrosilvopastoral System"]
AF.Out[,AF.Parklands:=as.character(NA)]
AF.Out[,AF.Multistrata:=as.character(NA)]
AF.Out[,AF.Boundary:=as.character(NA)]

AF.Trees<-af_mh[,.(AF.Level.Name,AF.Tree,B.Code)]

era_mh$AF.Out<-AF.Out
era_mh$AF.Trees<-AF.Trees

## Edit Pub.Out ####
era_mh$Pub.Out$B.New<-NULL

## Edit Site.Out ####
tab<-era_mh$Site.Out
setname(tab,"Site.Buffer.Manual","Buffer.Manual")
era_mh$Site.Out$Site.Loc.Type<-NULL
era_mh$Site.Out$Site.Loc.Fixed<-NULL

## 1.2) Industrious elephant
files<-list.files("Data",era_projects$industrious_elephant_2023,full.names = T)
(files<-grep(".RData",files,value=T))
(files<-tail(files,1))

era_cc<-miceadds::load.Rdata2(file=basename(files),path=dirname(files))

# Site.Out 
era_cc$Site.Out$check<-NULL

# 2) Compare data ####
# 2.1) Compare tables names ####
names_mh<-names(era_mh)
names_cc<-names(era_cc)

names_mh[!names_mh %in% names_cc]
(not_shared<-names_cc[!names_cc %in% names_mh])
shared<-names_cc[names_cc %in% names_mh]

# 2.2) Compare table fields ####
tab_comparisons<-lapply(1:length(shared),FUN=function(i){
  tab<-shared[i]
  fields_mh<-colnames(era_mh[[tab]])
  fields_cc<-colnames(era_cc[[tab]])
  mh_not_cc<-fields_mh[!fields_mh %in% fields_cc]
  cc_not_mh<-fields_cc[!fields_cc %in% fields_mh]
  fields_shared<-fields_cc[fields_cc %in% fields_mh]
  
  class_mh<-sapply(era_mh[[tab]][,fields_shared,with=F],class)
  class_cc<-sapply(era_mh[[tab]][,fields_shared,with=F],class)
  class_mismatch<-class_mh[class_mh != class_cc]
  
  list(fields_mh=fields_mh,fields_cc=fields_cc,mh_not_cc=mh_not_cc,cc_not_mh=cc_not_mh,fields_shared=fields_shared,class_mh=class_mh,class_cc=class_cc,class_mismatch=class_mismatch)
})

names(tab_comparisons)<-shared

tab_comparisons$ExpD.Out







