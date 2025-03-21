# Author: Pete Steward, p.steward@cgiar.org, ORCID 0000-0003-3985-4911
# Organization: Alliance of Bioversity International & CIAT
# Project: Evidence for Resilient Agriculture (ERA)
#
# Description:
# This script prepares two agronomic datasets: "Majestic Hippo 2020" and "Industrious Elephant 2023". 
# It involves cleaning, standardizing, and merging them into one harmonized dataset.
# Key steps:
# 1. Load Majestic Hippo data, remove unnecessary tabs, restructure key tables.
# 2. Load Industrious Elephant data, clean as needed.
# 3. Compare both datasets, handle structural differences.
# 4. Merge datasets and save final output.
#
# Note: First run R/0_set_env.R
# Super users, to compile the data from scratch:
#   - R/import/import_majestic_hippo_2020.R
#   - R/import/import_industrious_elephant_2023.R

## 0.0) Install and load packages ####
pacman::p_load(data.table, 
               miceadds)


# 1) Load data ####
  ## 1.1) Majestic hippo ####
  files<-list.files("Data",era_projects$majestic_hippo_2020,full.names = T)
  (files<-grep(".RData",files,value=T))
  (file_mh<-tail(files,1))
  
  era_mh<-miceadds::load.Rdata2(file=basename(file_mh),path=dirname(file_mh))
  
    ### 1.1.1) Remove unneeded tabs ####
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
    
    ### 1.1.2) Restructure Chems.Out ####
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
    
    ### 1.1.3) Restructure Plant.Out ####
    plant_mh<-era_mh$Plant.Out
    setnames(plant_mh,"P.Method","Plant.Method")
    Plant.Out<-plant_mh[,.(P.Product,P.Level.Name,P.Structure,B.Code)]
    Plant.Method<-plant_mh[,.(P.Level.Name,Plant.Method,Plant.Density,Plant.Density.Unit,Plant.Row,Plant.Station,Plant.Seeds,Plant.Thin,Plant.Tram.Row,Plant.Tram.N,Plant.Intercrop,Plant.Block.Rows,Plant.Block.Perc,Plant.Block.Width,B.Code)]
    
    era_mh$Plant.Out<-Plant.Out
    era_mh$Plant.Method<-Plant.Method
    
    ### 1.1.4) Restructure AF.Out ####
    
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
     
    ### 1.1.5) Edit Pub.Out ####
  era_mh$Pub.Out$B.New<-NULL
    ### 1.1.6) Edit Site.Out ####
  tab<-copy(era_mh$Site.Out)
  setnames(tab,"Site.Buffer.Manual","Buffer.Manual")
  tab$Site.Loc.Type<-NULL
  tab$Site.Loc.Fixed<-NULL
  
  # In industrious elephant texture comes from USDA triangle calculated from reported textures in the soils tab
  most_common <- function(x) {
    # Remove NA values if present
    x <- na.omit(x)
    
    # Calculate frequencies
    freq <- table(x)
    
    # Find max frequency
    max_freq <- max(freq)
    
    # Get all values with max frequency
    common_vals <- names(freq[freq == max_freq])
    
    # Return the first one (convert back to original type if possible)
    return(type.convert(common_vals[1], as.is = TRUE))
  }
  
   usda<-era_mh$Soil.Out[!is.na(USDA.Texture),.(Site.ID,Soil.Upper,Soil.Lower,B.Code,USDA.Texture)
                         ][,.(USDA.Texture=most_common(USDA.Texture)),by=.(Site.ID,B.Code)]
   
   tab<-merge(tab,usda,by=c("Site.ID","B.Code"),all.x = T,sort=F)
   tab[,Site.Soil.Texture:=USDA.Texture][,USDA.Texture:=NULL]
   
   era_mh$Site.Out<-tab
  
    ### 1.1.7) Edit Soils.Out ####
  ## Soils needs to go from wide to long
  #[1] "Site.ID"    "Soil.Upper" "Soil.Lower" "B.Code"     "variable"   "value"      "Unit"       "Method"    
  (tab<-copy(era_mh$Soil.Out))
  tab[,USDA.Texture:=NULL][,N:=NULL][,Texture.Total:=NULL][,ED.Site.ID:=NULL]
  
  unit_cols<-grep("Unit",colnames(tab),value=T)
  value_cols<-gsub(".Unit","",unit_cols)
  method_cols<-grep("Method",colnames(tab),value=T)
  id_cols<- c("Site.ID","Soil.Upper","Soil.Lower","B.Code")
  
  
  tab_vals<-melt(tab[,c(id_cols,value_cols),with=F],id.vars = c("B.Code","Site.ID","Soil.Upper","Soil.Lower"))
  tab_units<-unique(melt(tab[,c(id_cols,unit_cols),with=F],id.vars = c("B.Code","Site.ID","Soil.Upper","Soil.Lower"),value.name = "Unit"))
  tab_units<-tab_units[,variable:=gsub("[.]Unit","",variable)][!is.na(Unit)]
  tab_method<-unique(melt(tab[,c(id_cols,method_cols),with=F],id.vars = c("B.Code","Site.ID","Soil.Upper","Soil.Lower"),value.name = "Method"))
  tab_method<-tab_method[,variable:=gsub("[.]Method","",variable)][!is.na(Method)]
  
  tab_vals<-merge(tab_vals,tab_units,by=c(id_cols,"variable"),all.x=T,sort=F)
  tab_vals<-merge(tab_vals,tab_method,by=c(id_cols,"variable"),all.x=T,sort=F)
  
  era_mh$Soil.Out<-tab_vals



    ### 1.1.8) Edit Times ####
  
    # Time.Out
    tab<-copy(era_mh$Data.Out)
    
    Time.Out<-unique(tab[,.(ED.M.Year,B.Code)])
    Time.Out<-rbindlist(lapply(1:nrow(Time.Out),FUN=function(i){
      dat<-Time.Out[i]
      times<-dat$ED.M.Year
      times<-data.table(Time=unlist(strsplit(times,"[.][.]")))
      times$Time.Season<-as.character(NA)
      
      times[grepl("-5|[.]5",Time),Time.Season:="Dry or off season"
            ][,Time2:=Time
              ][grepl("-5|[.]5",Time),Time2:=gsub("-5|.5","",Time,fixed = T)]
      
      if(any(grepl("[.]",times$Time2))){
        times[grepl("[.]",Time2),Time.Season:=unlist(tstrsplit(Time2,"[.]",keep=2))]
      }
      
      times[,Time2:=NULL]
      
      times$B.Code<-dat$B.Code
      times
    }))
    
    # Time.Clim
    tab<-copy(era_mh$Times.Out)[,.(Site.ID,Time,TSP,TAP,B.Code)]
    setnames(tab,c("TSP","TAP"),c("Time.Clim.SP","Time.Clim.TAP"))
    Time.Clim<-tab[,Time.Clim.SP:=round(as.numeric(Time.Clim.SP),0)][,Time.Clim.TAP:=round(as.numeric(Time.Clim.TAP),0)]
    
    
    # PD.Out
    tab<-copy(era_mh$Data.Out)
    plant<-tab[!(is.na(ED.Plant.Start) & is.na(ED.Plant.End)),.(T.Name,ED.Int,ED.Rot,Site.ID,ED.Product.Simple,ED.M.Year,ED.Plant.Start,ED.Plant.End,B.Code)]
    
    plant<-unique(plant[,.(Site.ID=unlist(strsplit(Site.ID,"[.][.]"))),by=.(T.Name,ED.Int,ED.Rot,ED.M.Year,ED.Product.Simple,ED.Plant.Start,ED.Plant.End,B.Code)])
    plant<-plant[grepl("/",ED.Plant.Start),PD.Date.Start:=as.Date(ED.Plant.Start,format="%d/%m/%Y")
          ][!grepl("/",ED.Plant.Start) & nchar(ED.Plant.Start),PD.Date.Start:=as.Date(as.numeric(ED.Plant.Start),origin = "1899-12-30")
            ][grepl("/",ED.Plant.End),PD.Date.End:=as.Date(ED.Plant.End,format="%d/%m/%Y")
              ][!grepl("/",ED.Plant.End) & nchar(ED.Plant.End),PD.Date.End:=as.Date(as.numeric(ED.Plant.End),origin = "1899-12-30")
                  ][,PD.Variable:="Planting"
                    ][,PD.Level.Name:=T.Name
                    ][is.na(PD.Level.Name),PD.Level.Name:=ED.Int
                      ][,c("ED.Plant.Start","ED.Plant.End","ED.Product.Simple"):=NULL
                        ][,PD.Date.DAS:=NA]

    harvest<-tab[!(is.na(ED.Harvest.Start) & is.na(ED.Harvest.End) & is.na(ED.Harvest.DAS)),.(T.Name,ED.Int,ED.Rot,Site.ID,ED.Product.Simple,ED.M.Year,ED.Harvest.Start,ED.Harvest.End,ED.Harvest.DAS,B.Code)]
    harvest<-harvest[grepl("/",ED.Harvest.Start),PD.Date.Start:=as.Date(ED.Harvest.Start,format="%d/%m/%Y")
                     ][!grepl("/",ED.Harvest.Start) & nchar(ED.Harvest.Start),PD.Date.Start:=as.Date(as.numeric(ED.Harvest.Start),origin = "1899-12-30")
                       ][grepl("/",ED.Harvest.End),PD.Date.End:=as.Date(ED.Harvest.End,format="%d/%m/%Y")
                         ][!grepl("/",ED.Harvest.End) & nchar(ED.Harvest.End),PD.Date.End:=as.Date(as.numeric(ED.Harvest.End),origin = "1899-12-30")
                           ][,PD.Variable:="Harvest"
                             ][,PD.Level.Name:=T.Name
                             ][is.na(PD.Level.Name),PD.Level.Name:=ED.Int
                               ][,c("ED.Harvest.Start","ED.Harvest.End","ED.Product.Simple"):=NULL]
    setnames(harvest,"ED.Harvest.DAS","PD.Date.DAS")
    
    PD.Out<-rbind(plant,harvest)
    setnames(PD.Out,"ED.M.Year","Time")
    
    # Merge with MT.Out and Data.Out
    # MT.Out
    mergedat<-unique(PD.Out[,.(B.Code,T.Name,Time,ED.Int,ED.Rot,PD.Level.Name)])
    mergedat[is.na(T.Name),T.Name:=ED.Int]
    # split_intercrops
    mergedat<-mergedat[,.(T.Name=unlist(strsplit(T.Name,"..",fixed = T))),by=.(B.Code,Time,PD.Level.Name,ED.Int,ED.Rot)]
    # mergedat<-mergedat[,.(PD.Level.Name=unlist(strsplit(PD.Level.Name,"[.][.]"))),by=.(B.Code,T.Name)]
    
    tab<-merge(era_mh$MT.Out,unique(mergedat[,.(B.Code,T.Name,PD.Level.Name)]),by=c("T.Name","B.Code"),all.x=T,sort=F)
    # Non-matches - these all appear to be related to crop rotations, this is being addressed in the raw data
    unique(mergedat[!PD.Level.Name %in% tab$PD.Level.Name])
    
    # Data.Out
    tab<-copy(era_mh$Data.Out)
    tab[,Code:=T.Name][is.na(Code),Code:=ED.Int]
    mergedat[,T.Name:=PD.Level.Name]
    tab<-merge(tab,unique(mergedat[,.(B.Code,Time,T.Name,PD.Level.Name)]),by.x=c("Code","B.Code","ED.M.Year"),by.y=c("T.Name","B.Code","Time"),all.x=T,sort=F)
    unique(mergedat[!PD.Level.Name %in% tab$PD.Level.Name])
    
    # Remove int and rot cols from PD.Out
    PD.Out<-unique(PD.Out[,c("ED.Int","ED.Rot","T.Name"):=NULL])
    
    # PD.Codes
    PD.Codes<-PD.Out[,.(PD.Level.Name,B.Code)]
    
    era_mh$Times.Out<-Time.Out
    era_mh$Times.Clim<-Time.Clim
    era_mh$PD.Out<-PD.Out
    era_mh$PD.Codes<-PD.Codes
    
    ### 1.1.9) Edit Prod.Out ####
    era_mh$Prod.Out[,c("P.New","P.New.Name","P.New.Nfix","P.New.Tree","P.Aggregated"):=NULL]
    ### 1.1.10) Edit Var.Out ####
    era_mh$Var.Out[,V.Level.Name:=Join
        ][,c("V.Species","V.Subspecies","V.Animal.Practice","Join"):=NULL]
    ### 1.1.11) Till.Out ####
    tab<-copy(era_mh$Till.Out)
    tab[,Till.Method:=NULL]
    setnames(tab,c("T.Level.Name","T.Method.Other"),c("Till.Level.Name","Till.Other"))
    era_mh$Till.Out<-tab
    ### 1.2.12) Fert.Out ####
    tab<-copy(era_mh$Fert.Out)
    tab[,c("F.NI.Code","F.PI.Code","F.KI.Code","F.Urea","F.Compost","F.Manure","F.Biosolid","F.MicroN","F.Biochar","F.Codes2"):=NULL]
    era_mh$Fert.Out<-tab
    ### 1.2.13) Fert.Method ####
    era_mh$Fert.Method[,c("Fert.Code1","Fert.Code2","Fert.Code3"):=NULL]

    ### 1.2.14) Fert.Comp ####
    tab<-copy(era_mh$Fert.Comp)
    # Add is.F.Level.Name
    tab[,Is.F.Level.Name:=F]
    mergedat<-unique(era_mh$Fert.Out[,.(B.Code,F.Level.Name)])[,check:=T]
    tab<-merge(tab,mergedat,by.x=c("B.Code","F.Type"),by.y=c("B.Code","F.Level.Name"),all.x=T,sort=F)
    tab[check==T,Is.F.Level.Name:=T][,check:=NULL]
    era_mh$Fert.Comp<-tab
    
    ### 1.2.15) Weed.Out ####
    tab<-copy(era_mh$Weed.Out)
    tab$W.New.Method<-NULL
    setnames(tab,"W.Code","W.Codes")
    era_mh$Weed.Out<-tab
    
    ### 1.2.16) Res.Method ####
    era_mh$Res.Method[,c("M.New.Tree","M.New.Material","M.Material.Type.New","M.Material.Type","M.Material.Type,New"):=NULL]

    ### 1.2.17) Irrig.Method/Irrig.Codes ####
    
    era_mh$Irrig.Codes[!is.na(I.Method),I.Method:=unlist(tstrsplit(I.Method[1]," (",keep=1,fixed=T)),by=I.Method]
    era_mh$Irrig.Codes[!is.na(I.Strategy),I.Strategy:=unlist(tstrsplit(I.Strategy[1]," (",keep=1,fixed=T)),by=I.Strategy]
    
    era_mh$Irrig.Method[,c("I.New.Unit","I.New.Type"):=NULL]
    
    # What about data in Data.Out?
    tab<-era_mh$Data.Out[!is.na(ED.I.Amount),.(B.Code,ED.Treatment,ED.Site.ID,I.Level.Name,ED.M.Year,ED.I.Amount,ED.I.Unit)]
    setnames(tab,c("ED.Treatment","ED.Site.ID","ED.M.Year","ED.I.Amount","ED.I.Unit"),c("T.Name","Site.ID","Time","I.Amount","I.Unit"))
    
    # We have an issue if I.Level.Name is blank
    i_missing<-tab[is.na(I.Level.Name)][,I.Level.Name:=T.Name]
    tab<-tab[!is.na(I.Level.Name)]
    
    # If missing values are present these will be intercrops or rotations, this is being dealt with in the raw data
    i_missing[is.na(I.Level.Name),unique(B.Code)]
    i_missing<-unique(i_missing[!is.na(I.Level.Name)])

    
    # Levels now need to be pushed to MT.Out and Data.Out
    i_missing[,I.Level.NameX:=I.Level.Name]
    # MT.Out
    mergedat<-merge(era_mh$MT.Out,i_missing[,.(B.Code,T.Name,I.Level.NameX)],by=c("B.Code","T.Name"),all.x=T,sort=F)
    check<-mergedat[!is.na(I.Level.NameX),.(B.Code,T.Name,I.Level.NameX)]
    i_missing[!I.Level.NameX %in% check$I.Level.NameX] 
    
    mergedat[is.na(I.Level.Name),I.Level.Name:=I.Level.NameX][,I.Level.NameX:=NULL]
    era_mh$MT.Out<-mergedat
    
    # Data.Out
    mergedat<-merge(era_mh$Data.Out,i_missing[,.(B.Code,T.Name,Time,I.Level.NameX)],
                    by.x=c("B.Code","ED.Treatment","ED.M.Year"),
                    by.y=c("B.Code","T.Name","Time"),all.x=T,sort=F)
    
    check<-mergedat[!is.na(I.Level.NameX),.(B.Code,T.Name,I.Level.NameX)]
    i_missing[!I.Level.NameX %in% check$I.Level.NameX]
    
    mergedat[is.na(I.Level.Name),I.Level.Name:=I.Level.NameX][,I.Level.NameX:=NULL]
    era_mh$Data.Out<-mergedat
    
    # join i_missing and tab
    i_missing[,c("T.Name","I.Level.NameX"):=NULL]
    tab[,T.Name:=NULL]
    tab<-rbind(tab,i_missing)
    
    # Merge with Irrig.Method table
    era_mh$Irrig.Method<-rbindlist(list(era_mh$Irrig.Method,tab),fill = T,use.names=T)

    ### 1.2.18) Int.Out ####
    setnames(era_mh$Int.Out,"Practice","I.Practice")
    era_mh$Int.Out[,c("IN.T.Structure","IN.ID"):=NULL]
    
    era_mh$Int.Out[,IN.Level.Name:=IN.Level.Name2][,IN.Level.Name2:=NULL]
    
    ### 1.2.19) Rot.Out ####
    setnames(era_mh$Rot.Out,"R.User.Code","R.Practice")
    era_mh$Rot.Out[,c("N","R.ID","R.T.Level.Names.All2"):=NULL]
    
    # Update delimiters
    era_mh$Rot.Out[,R.All.Products:=gsub("[.][.]","-",R.All.Products)]
    
    ### 1.2.20) Rot.Seq ####
    
    # dev_note: Many fields missing from era_mh consider updating process in import script to ie method? ####
    
    setnames(era_mh$Rot.Seq,
             c("R.Prod.Prev.1","R.Prod.Prev.2","R.Prod.Prev.3","R.Prod.Prev.4",
               "R.Res.Prev1","R.Res.Prev2","R.Res.Prev3","R.Res.Prev4",
               "Rotation Treatment","Rotation Component"
               ),
             c("R.Prod.Prev1","R.Prod.Prev2","R.Prod.Prev3","R.Prod.Prev4",
               "R.Res.Code.1","R.Res.Code.2","R.Res.Code.3","R.Res.Code.4",
               "R.Level.Name","R.Treatment"))
    
    era_mh$Rot.Seq[,c("R.Res1","R.Res2","R.Res3","R.Res4","R.Intercrop","Seq.Ordered","N"):=NULL]
    
    ### 1.2.21) Rot.Seq.Summ ####
    setnames(era_mh$Rot.Seq.Summ,"Seq","R.Prod.Seq")

    ### 1.2.23) Out.Out ####
    era_mh$Out.Out[!is.na(Out.Partial.Outcome.Name),Out.Notes:=paste0(Out.Partial.Outcome.Name,"||",Out.Partial.Outcome.Code)]
    era_mh$Out.Out[,c("Out.WG.Start","Out.WG.Unit","Out.WG.Days","Out.Partial.Outcome.Name","Out.Partial.Outcome.Code"):=NULL]
    
    ### 1.2.23) pH.Out ###
    era_mh$pH.Out[!is.na(pH.Prac),pH.Prac:=unlist(tstrsplit(pH.Prac," (",keep=1,fixed=T))]
    
    ### 1.2.24) MT.Out####
    era_mh$MT.Out[,c("b16","b17","b21","b64","b23","b29","b30","b75","b73","b74","b67","b28",
                     "b16.Code","b17.Code","b21.Code","b64.Code","b23.Code","b29.Code","b30.Code","b75.Code","b73.Code","b74.Code","b67.Code","b28.Code",
                     "A.Level.Name","T.Animals","A.Codes","A.Notes","A.Grazing","A.Hay",
                     "E.Level.Name","E.Codes","E.Notes",
                     "PO.Level.Name","PO.Codes","PO.Notes","PO.Source",
                     "W.New.Method",
                     "V.Animal.Practice","V.Species","V.Subspecies","V.Product",
                     "P.Method","Plant.Density","Plant.Density.Unit","Plant.Row","Plant.Station","Plant.Seeds","Plant.Thin","Plant.Tram.Row","Plant.Tram.N","Plant.Intercrop",
                     "Plant.Block.Rows","Plant.Block.Perc","Plant.Block.Width",
                     "AF.Tree","AF.Subspecies","AF.New.Tree","AF.New.Subsp","AF.New.Var","AF.Var",
                     "N2","T.ID","T.Structure"
                     ):=NULL]
    
    setnames(era_mh$MT.Out,c("Weed.Code","H.Codes","Till.Codes","T.Comp"),c("W.Codes","H.Code","Till.Code","P.Product"),skip_absent = T)
    
    era_mh$MT.Out[,T.Name:=T.Name2][,T.Name2:=NULL]
    
    # Add C.Code
    era_mh$MT.Out<-merge(era_mh$MT.Out,era_mh$Chems.Code[,.(B.Code,C.Level.Name,C.Codes)],by=c("B.Code","C.Level.Name"),all.x=T,sort=F)

    # dev note: Base.Codes are missing ####
    
    #### 1.2.25) Data.Out ####
    # Update delimiters
    era_mh$Data.Out[,R.All.Products:=gsub("[.][.]","-",R.All.Products)]
    era_mh$Data.Out[,IN.Level.Name:=IN.Level.Name2][,IN.Level.Name2:=NULL]
    
    # Update treatment and intercrop
    era_mh$Data.Out[,ED.Treatment:=T.Name2][,ED.Int:=IN.Level.Name]
    
    # Delete unused fields
    era_mh$Data.Out[,c("b16","b17","b21","b64","b23","b29","b30","b75","b73","b74","b67","b28",
                     "b16.Code","b17.Code","b21.Code","b64.Code","b23.Code","b29.Code","b30.Code","b75.Code","b73.Code","b74.Code","b67.Code","b28.Code",
                     "A.Level.Name","T.Animals","A.Codes","A.Notes","A.Grazing","A.Hay",
                     "E.Level.Name","E.Codes","E.Notes",
                     "PO.Level.Name","PO.Codes","PO.Notes","PO.Source",
                     "W.New.Method",
                     "V.Animal.Practice","V.Species","V.Subspecies","V.Product",
                     "P.Method","Plant.Density","Plant.Density.Unit","Plant.Row","Plant.Station","Plant.Seeds","Plant.Thin","Plant.Tram.Row","Plant.Tram.N","Plant.Intercrop",
                     "Plant.Block.Rows","Plant.Block.Perc","Plant.Block.Width",
                     "AF.Tree","AF.Subspecies","AF.New.Tree","AF.New.Subsp","AF.New.Var","AF.Var",
                     "T.ID","T.Structure","B.Code.1","B.Code.2","B.Code.3","Base.Codes.No.Animals",
                     "Site.ID","T.Name","T.Name2","IN.Level.Name",
                     "R.Level.Name","R.T.Level.Names.All2",
                     "ED.I.Amount","ED.I.Unit","Site.Loc.Type","Site.Loc.Fixed",
                     "IN.T.Structure","IN.ID","N","R.ID","N.1","B.New","Temp.Code.Treat",
                     "Out.WG.Start","Out.WG.Unit","Out.WG.Days","Out.Partial.Outcome.Name","Out.Partial.Outcome.Code",
                     "ED.Start.Year","ED.Start.Season","EU.Component","ED.Product.Simple"         
    ):=NULL]
    
    A<-table(colnames(era_mh$Data.Out))
    A[A>1]
    
    # Rename fields
    setnames(era_mh$Data.Out,
             c("ED.Site.ID","ED.Treatment","ED.Int","ED.Rot","ED.M.Year","ED.Outcome",
               "EU.Product.Type","EU.Product.Subtype","EU.Product","EU.Product.Simple","EU.Latin.Name",
               "ED.Plant.Start","ED.Plant.End","ED.Harvest.Start","ED.Harvest.End","ED.Harvest.DAS",
               "Site.Buffer.Manual","ED.Comparison","H.Codes","Till.Codes","Weed.Code","T.Comp",
               "R.User.Code","Final.Start.Year.Code","Final.Start.Season.Code","Practice"
               ),
             c("Site.ID","T.Name","IN.Level.Name","R.Level.Name","Time","Out.Code.Joined",
               "Product.Type","Product.Subtype","Product","Product.Simple","Latin.Name",
               "PD.Plant.Start","PD.Plant.End","PD.Harvest.Start","PD.Harvest.End","PD.Harvest.DAS",
               "Buffer.Manual","ED.Comparison1","H.Code","Till.Code","W.Codes","P.Product",
               "R.Practice","Final.Start.Year","Final.Start.Season","I.Practice"),
             skip_absent = T)
    
    # Get codes for irrigation
    irrig_codes<-master_codes$prac[grepl("Irrigat",Subpractice) & !grepl("No Irrigation",Subpractice),.(Code,Subpractice)]
    # Add logical field to indicate presence of irrigation
    era_mh$Data.Out[,Irrig:=F][,Irrig:=any(unlist(strsplit(c(T.Codes[1],Base.Codes[1]),"-")) %in% irrig_codes$Code),by=.(T.Codes,Base.Codes)]
    # Add PD.Plant.Variable
    era_mh$Data.Out[!is.na(PD.Plant.Start),PD.Plant.Variable:="Planting"]
    
    # Add C.Code
    era_mh$Data.Out<-merge(era_mh$Data.Out,era_mh$Chems.Code[,.(B.Code,C.Level.Name,C.Codes)],by=c("B.Code","C.Level.Name"),all.x=T,sort=F)
    
    # Add Time.Clim
    era_mh$Data.Out<-merge(era_mh$Data.Out,era_mh$Times.Clim,by=c("B.Code","Site.ID","Time"),all.x=T,sort=F)

    # Dev Note: Consider adding Time.Agg.Duration see 8.4.12.1 in R/import/import_industrious_elephant_2023.R ####
    
    ## 1.2) Industrious elephant ####
    files<-list.files("Data",era_projects$industrious_elephant_2023,full.names = T)
    (files<-grep(".RData",files,value=T))
    (file_ie<-tail(files,1))
    
    era_ie<-miceadds::load.Rdata2(file=basename(file_ie),path=dirname(file_ie))
    
    era_ie$Rot.Levels<-NULL
    
      ### 1.2.1) Edit Site.Out ####
      era_ie$Site.Out$check<-NULL
      
      ### 1.2.2) Edit Times.Out ####
      era_ie$Times.Out$seq_n<-NULL
      era_ie$Times.Out$check<-NULL
      
      ### 1.2.3) Edit MT.Out ####
      era_ie$MT.Out[,c("V.Accession","AF.Boundary","AF.Silvopasture","AF.Parklands","AF.Multistrata","AF.Other","Till.Practice",
                       "pH.Prac"
                       ):=NULL]
      
      ### 1.2.4) Edit Int.Out ####
      era_ie$Int.Out[,IN.Level.Name:=IN.Level.Name2][,IN.Level.Name2:=NULL]
      
      ### 1.2.5) Edit Rot.Out ####
      era_ie$Rot.Out[,R.T.Level.Names.All:=R.T.Level.Names.All2][,R.T.Level.Names.All2:=NULL]
      
      ### 1.2.6) Edit Data.Out ####
      era_ie$Data.Out[,c("V.Accession","AF.Boundary","AF.Silvopasture","AF.Parklands","AF.Multistrata","AF.Other","Till.Practice",
                       "pH.Prac","N.x","N.y","N"
      ):=NULL]
    

# 2) Compare data ####
  ## 2.1) Compare tables names ####
  names_mh<-names(era_mh)
  names_ie<-names(era_ie)
  
  names_mh[!names_mh %in% names_ie]
  (not_shared<-names_ie[!names_ie %in% names_mh])
  shared<-names_ie[names_ie %in% names_mh]
  
  ## 2.2) Compare table fields ####
  tab_comparisons<-lapply(1:length(shared),FUN=function(i){
    tab<-shared[i]
    fields_mh<-colnames(era_mh[[tab]])
    fields_ie<-colnames(era_ie[[tab]])
    mh_not_ie<-fields_mh[!fields_mh %in% fields_ie]
    cc_not_mh<-fields_ie[!fields_ie %in% fields_mh]
    fields_shared<-fields_ie[fields_ie %in% fields_mh]
    
    class_mh<-sapply(era_mh[[tab]][,fields_shared,with=F],class)
    class_ie<-sapply(era_mh[[tab]][,fields_shared,with=F],class)
    class_mismatch<-class_mh[class_mh != class_ie]
    
    list(fields_mh=fields_mh,fields_ie=fields_ie,mh_not_ie=mh_not_ie,cc_not_mh=cc_not_mh,fields_shared=fields_shared,class_mh=class_mh,class_ie=class_ie,class_mismatch=class_mismatch)
  })
  
  names(tab_comparisons)<-shared
  
  tab_comparisons$Rot.Levels[3:4]
  
  ## 2.3) Merge tables ####
  era_merge<-lapply(1:length(shared),FUN=function(i){
    tab<-shared[i]
    cat("merging tab",i,tab,"\n")
    mh<-era_mh[[tab]]
    mh$Version<-era_projects$majestic_hippo_2020
    ie<-era_ie[[tab]]
    ie$Version<-era_projects$industrious_elephant_2023
    
    fields_mh<-colnames(mh)
    fields_ie<-colnames(ie)
    mh_not_ie<-fields_mh[!fields_mh %in% fields_ie]
    cc_not_mh<-fields_ie[!fields_ie %in% fields_mh]
    if(length(mh_not_ie)>0){
      stop(paste0("Fields in mh not present in ie. Tab = ",tab,", Field(s) = ",paste0(mh_not_ie,collapse = ",")))
    }
    
    ie_class<-sapply(ie[,cc_not_mh,with=F],class)
    ie_char<-names(ie_class[ie_class=="character"])
    ie_num<-names(ie_class[ie_class=="numeric"])
    ie_int<-names(ie_class[ie_class=="integer"])
    ie_date<-names(ie_class[ie_class=="Date"])
    ie_logical<-names(ie_class[ie_class=="logical"])
    
    if(length(ie_char)>0){
    mh[,(ie_char):=field_absent]
    }
    
    if(length(ie_int)>0){
      mh[,(ie_int):=as.integer(field_absent_num)]
    }
    
    if(length(ie_num)>0){
      mh[,(ie_num):=field_absent_num]
    }
    
    if(length(ie_date)>0){
      mh[,(ie_date):=as.Date(NA)]
    }
    
    if(length(ie_logical)>0){
      mh[,(ie_logical):=as.logical(NA)]
    }
    
    result<-rbindlist(list(ie,mh),use.names = T)
    
  })
  
  names(era_merge)<-shared
  
  ## 2.4) Save result ####
  save_file<-paste0(
    "agronomic_",
    gsub("_","-",gsub(".RData","",basename(file_mh))),
    "_",
    gsub("_","-",gsub(".RData","",basename(file_ie))),
    ".RData"
    )
  
  save_path<-file.path(era_dirs$era_masterdata_dir,save_file)

  save(era_merge,file=save_path)    


  era_merge$Data.Out[,length(unique(B.Code))]

