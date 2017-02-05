library(jsonlite)
library(sqldf)
library(plyr)
library(DBI)
library(dtplyr)
library(dplyr)
library(data.table)
library(magrittr)
library(treemap)
library(lubridate)
library(ggplot2)

# TODO : initialiser la base sqlite si elle est absente, et travailler en append si des données sont déja présentes.
#         Prévoir de nettoyer la base pour ne garder que les lignes uniques.



# La fonction suivante prend en argument un nom de fichier json et une base sqlite de destination,
# et remplit la base à partir des données contenues dans le fichier json.

data_fromJSON_to_SQLite <- function(json_file,SQLite_db ) {
  dat_full <- fromJSON(sprintf("[%s]", paste(readLines(json_file), collapse=",")))
  # On obtient une liste contenant l'ensemble des df des données collectées sur 1 mois, le pas temporel est de 20 minutes.

  # single element example
  #data <- data.frame(dat_full[1])

  #example avec tous les éléments
  data_frame_full  <- rbind.fill(dat_full)

  # Conversion des informations temporelles
  data_frame_full$download_datetemps<-as.POSIXct(data_frame_full$download_date, origin="1970-01-01", tz="UTC")
  data_frame_full$lastupdt_datetemps<-as.POSIXct(data_frame_full$last_update/1000, origin="1970-01-01", tz="UTC")

  #export vers la base sqlite
  #file.remove("SQLiteData/Test.sqlite") # peut être nécessaire si l'on veut remplacer le contenu de la base
  con <- dbConnect(RSQLite::SQLite(), dbname="SQLiteData/Test.sqlite")
  dbWriteTable(con, name="raw_data", value = data_frame_full , row.names = FALSE) # Append à ajouter pour plus tard
  
  }


data_fromSQLITE_to_df <- function(SQLite_db, tbl_name) {
  con <- dbConnect(RSQLite::SQLite(), dbname=SQLite_db )
  df <- dbReadTable(con, tbl_name)
  dbDisconnect(con)
  return (df)
  }


###############################################################################
# Comment convertire le json intitial et générer les fichiers SQLITE ... lent
setwd("~/Documents/dev/Cepe/ubiquitous-velib")
# Placer les données dans un répertoire data/data_all_Paris/ à côté du script
json_file <- "data/data_all_Paris/data_all_Paris.jjson_2017-01-01-1483248351.gz" 
SQLite_db <- "SQLiteData/Test.sqlite"

data_fromJSON_to_SQLite(json_file,SQLite_db)


###############################################################################
# Comment charger les données traitées en df, beaucoup plus rapide !
SQLite_db <- "SQLiteData/Test.sqlite"
tbl_name <- "raw_data"
raw_data <- data_fromSQLITE_to_df(SQLite_db, tbl_name)
raw_data$download_datetemps<-as.POSIXct(raw_data$download_date, origin="1970-01-01", tz="UTC")
raw_data$lastupdt_datetemps<-as.POSIXct(raw_data$last_update/1000, origin="1970-01-01", tz="UTC")


###############################################################################
# Comment explorer les données : station 1013, évolution de la disponibilité
data <- tbl_df(raw_data)
filter(data, number == 1013) %>% select(download_datetemps, available_bikes) %>% plot(type = 'l', lty = 1)


###############################################################################
# Exploration des données par durée d'indisponibilité compléte : aucun Velib disponible

identifier_vides_continus <- function(x){ tmp<-cumsum(!x);tmp-cummax(x*tmp)} 
data <- tbl_df(raw_data)
data <- filter(data, number == 1013) 

data$unempty_station <- 1
data$unempty_station[data$available_bikes == 0]<-0

data$empty_station <- 0
data$empty_station[data$available_bikes == 0]<-1

data$vides_continus = cumsum(!data$unempty_station)-cummax(data$unempty_station*cumsum(!data$unempty_station ))

data$vides_continus_starts <- 0
data$vides_continus_starts[data$vides_continus==1] <- 1
data$no_evenement_vides_continus <- cumsum(data$vides_continus_starts)

durée <- 1/3 * filter(data, empty_station == 1) %>% select(no_evenement_vides_continus) %>% table %>% t()
hist(durée, main = 'Répartition des durées (en h) des périodes où la station 1013 est vide de tout Velib')

###############################################################################
# Visualisation globale des indisponibilités sur un mois via des treemaps

setwd("~/Documents/dev/Cepe/ubiquitous-velib")
SQLite_db <- "SQLiteData/Test.sqlite"
tbl_name <- "raw_data" 
raw_data <- data_fromSQLITE_to_df(SQLite_db, tbl_name)
raw_data$download_datetemps<-as.POSIXct(raw_data$download_date, origin="1970-01-01", tz="UTC")
raw_data$lastupdt_datetemps<-as.POSIXct(raw_data$last_update/1000, origin="1970-01-01", tz="UTC")
data <- tbl_df(raw_data)

data_treemap = filter(data, available_bikes == 0)
data_treemap$Nombre_indispo_totales = 1
data_treemap$Nom_Station = as.character(data_treemap$number)
#Enrichissement du df par les dates : jour de la semaine, etc.
data_treemap$date=ymd_hms(data_treemap$download_datetemps)
data_treemap$day=weekdays(data_treemap$download_datetemps)


# NPO possibilité de visualisation en D3.js : cf http://www.buildingwidgets.com/blog/2015/7/22/week-29-d3treer-v2

treemap(data_treemap, index = c("day","Nom_Station"), vSize ='Nombre_indispo_totales', type='value', title = "Nombre d'indisponibilités totales cumulées par station en fonction du jour")
# On pourrait aussi pondérer par le nombre de vélos manquants en employant en vsize bike_stands, ou colorer en fonction de bikes_stands

data_treemap <- merge(data_treemap, ref_number_cp, by = 'number')
treemap(data_treemap, index = c("code_postal","Nom_Station"), vColor = 'bike_stands', vSize ='Nombre_indispo_totales', type='value', title = "Nombre d'indisponibilités totales cumulées par code postal et par station")

###############################################################################
# Exploration globale des indisponibilités sur un mois enrichie avec la durée d'indispo

data <- tbl_df(raw_data)

#On ne travaille que sur les stations avec status OPEN
data <- data[which(data$status == 'OPEN'),]

data$unempty_station <- 1
data$unempty_station[data$available_bikes == 0]<-0

data$empty_station <- 0
data$empty_station[data$available_bikes == 0]<-1

data <- arrange(data, number, last_update)

data <- ddply(data,.(number),transform,vides_continus = cumsum(!unempty_station)-cummax(unempty_station*cumsum(!unempty_station )))

data$vides_continus_starts <- 0
data$vides_continus_starts[data$vides_continus==1] <- 1
data <- ddply(data,.(number),transform,no_evenement_vides_continus = cumsum(vides_continus_starts) +100000*number)

#### On ne travaille que sur le sous element contenant des indisponibilités totales
data_tmp <- data[which(data$empty_station == 1),]

a <- rle(sort(data_tmp$no_evenement_vides_continus))
b <- data.frame(no_evenement_vides_continus=a$values, durée_indispo_totale=a$lengths*20) # n est la durée en minutes de l'indispo totale associée à l'indispo evenement_num
hist(b$durée_indispo_totale)


data_tmp <- data_tmp[c("number", 'download_date','no_evenement_vides_continus', 'bike_stands')][!duplicated(data_tmp$'no_evenement_vides_continus'),]

data_ggplot <- merge(data_tmp, ref_number_cp, by = 'number')
data_ggplot <- merge (b, data_ggplot)
data_ggplot$download_datetemps<-as.POSIXct(data_ggplot$download_date, origin="1970-01-01", tz="UTC")
data_ggplot$date=ymd_hms(data_ggplot$download_datetemps)
data_ggplot$day=weekdays(data_ggplot$download_datetemps)

data_ggplot$CP <- as.character(data_ggplot$code_postal)

#### Let's GGplot !

ggplot(data_ggplot, aes(durée_indispo_totale , ..density.., colour = CP, weight = bike_stands)) + scale_y_sqrt() + 
  geom_freqpoly(binwidth = 60)

ggplot(data_ggplot, aes(durée_indispo_totale , ..density.., colour = day , weight = bike_stands)) + scale_y_sqrt() +
  geom_freqpoly(binwidth = 60) 

