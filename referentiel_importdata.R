library(jsonlite)
library(sqldf)
library(plyr)
library(DBI)
library(dtplyr)
library(data.table)
library(magrittr)
library(treemap)
library(lubridate)


# TODO : initialiser la base sqlite si elle est absente, et travailler en append si des données sont déja présentes.
#         Prévoir de nettoyer la base pour ne garder que les lignes uniques.


data_fromSQLITE_to_df <- function(SQLite_db, tbl_name) {
  con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db )
  df <- dbReadTable(con, tbl_name)
  dbDisconnect(con)
  return (df)
}

###############################################################################
# Comment convertire le json intitial et générer les fichiers SQLITE ... lent
setwd("~/Documents/dev/Cepe/ubiquitous-velib")
# Placer les données dans un répertoire data/ à côté du script
csv_file <- 'data/velib_a_paris_et_communes_limitrophes.csv'
SQLite_db <- "SQLiteData/Test.sqlite"

dat_full <- read.csv2(csv_file)
con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db)
dbWriteTable(con, name="ref_data", value = dat_full , row.names = FALSE)

###############################################################################
tbl_name <- "ref_data"
referentiel <- data_fromSQLITE_to_df(SQLite_db, tbl_name)

###############################################################################
ref_number_cp <- data.frame(referentiel$number, referentiel$cp)
colnames(ref_number_cp) <- c("number","code_postal")

###############################################################################
hist(referentiel$bike_stands)
