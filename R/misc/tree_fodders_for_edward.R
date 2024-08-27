# 0) Load libraries and functions ####
# Install and load pacman if not already installed
if (!require("pacman", character.only = TRUE)) {
  install.packages("pacman")
  library(pacman)
}

pacman::p_load(data.table)

if(!require(ERAg)){
  remotes::install_github(repo="https://github.com/EiA2030/ERAg")
  library(ERAg)
}

