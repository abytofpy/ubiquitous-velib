library(sqldf)
library(plyr)
library(DBI)
library(dtplyr)
library(dplyr)
library(data.table)
library(magrittr)
library(lubridate)


data_fromSQLITE_to_df <- function(SQLite_db, tbl_name) {
  con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db )
  df <- dbReadTable(con, tbl_name)
  dbDisconnect(con)
  return (df)
}


# data_fromdf_to_SQLITE <- function(SQLite_db, tbl_name) {
#   con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db)
#   dbWriteTable(con, name=tbl_name, value = tbl_name , row.names = FALSE)
#}



###############################################################################
# Définition des variables pour l'ensemble du script de production
setwd("W:/SFS_tmp/Projet_Velib/velib_group/prod")
SQLite_db <- "data/SQLiteData/Test.sqlite"


###############################################################################
# Chargement des donnees traitees de sql vers le format df

tbl_name <- "raw_data"
enriched_data <- data_fromSQLITE_to_df(SQLite_db, tbl_name)


#### Enrichissement de la table avec les données temporelles

enriched_data$download_datetemps<-as.POSIXct(enriched_data$download_date, origin="1970-01-01", tz="Europe/Paris")
enriched_data$lastupdt_datetemps<-as.POSIXct(enriched_data$last_update/1000, origin="1970-01-01", tz="Europe/Paris")
enriched_data$date=as.Date(ymd_hms(enriched_data$download_datetemps), tz="Europe/Paris")

enriched_data$day=factor(weekdays(enriched_data$download_datetemps))

enriched_data$weekEnd <- 0
samedi<-which(enriched_data$day=='samedi')
dimanche<-which(enriched_data$day=='dimanche')
enriched_data$weekEnd[samedi]<-1
enriched_data$weekEnd[dimanche]<-1
enriched_data$weekEnd <- factor(enriched_data$weekEnd, levels = c(0,1))
enriched_data$downloadHour <- hour(enriched_data$download_datetemps)

#### Enrichissement de la table avec les données issues des capacités des stations
#enriched_data <- enriched_data %>% mutate(bike_stands_cat = ntile(bike_stands, 4))
#enriched_data$bike_stands_cat <- lapply(enriched_data$bike_stands_cat, as.factor) 
## TODO : à optimiser : prendre pour chaque station simplement le max correspondant à la date max 


#### Sauvegarde de la table enrichie dans la base sqlite

con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db)
dbWriteTable(con, name="enriched_data", value = enriched_data , row.names = FALSE, overwrite = TRUE)

###############################################################################
#### Préparation de la table sur le descriptif des communes

csv_file <- "data/correspondance-code-insee-code-postal.csv"

communes_data_raw <- read.csv2(csv_file, encoding = "UTF-8")
communes_data = communes_data_raw[c("Code.INSEE", "Code.Postal", "Commune", 
                                    "Altitude.Moyenne", "Superficie", "Population")]


colnames(communes_data)[1] <- "Code_INSEE"
colnames(communes_data)[2] <- "CP"
colnames(communes_data)[4] <- "Altitude_Moyenne"

communes_data$Code_INSEE <- as.numeric(communes_data$Code_INSEE)
communes_data$CP <- as.numeric(communes_data$CP)
communes_data$Altitude_Moyenne <- as.numeric(communes_data$Altitude_Moyenne)

#data_fromdf_to_SQLITE(SQLite_db, ref_data)

con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db)
dbWriteTable(con, name="communes_data", value = communes_data , overwrite = TRUE,
             row.names = FALSE)



###############################################################################
#### Préparation de la table sur le descriptif des stations

csv_file <- 'data/velib_a_paris_et_communes_limitrophes.csv'

ref_data <- read.csv2(csv_file, encoding = "UTF-8")
colnames(ref_data)[1] <- "number"

ref_data$CP <- as.numeric(ref_data$CP)


###############################################################################
# On intègre les 126 stations les plus en altitude : Velib+ à + de 60 m.
csv_file2 <- 'data/Vplus_stations.csv'
Vplus_stations <- read.csv(csv_file2, sep='\t')
ref_data <- merge(Vplus_stations, ref_data, by = 'number', all = TRUE)

CP_alt <- communes_data[c("CP", "Altitude_Moyenne")]
ref_data <- merge(CP_alt, ref_data, by = 'CP', all = TRUE)
ref_data<-ref_data[!(is.na(ref_data$number)),]
#### TODO : à corriger, ne fonctionne pas

ref_data$Latitude <- as.numeric(ref_data$Latitude)
ref_data$Longitude <- as.numeric(ref_data$Longitude)


#### Sauvegarde de la table de referentiel dans la base sqlite
con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db)
dbWriteTable(con, name="ref_data", value = ref_data , row.names = FALSE)



