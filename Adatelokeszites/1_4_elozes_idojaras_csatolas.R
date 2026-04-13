rm(list=ls())

#### csomagok ####

library(dplyr)
library(arrow)
library(duckdb)

#### remove az előző obejkteket ####

unique(ot$Season)
rm(ot, weather, ot_with_weather, season, con, query)

#### Szezononkénti OT parquetek beolvasása ####

ot <- read_parquet("2_all_overtakes_drs/sikeres/2018_drs_success.parquet")
season <- 2018L
weather <- read_parquet("2018/weather_2018_all.parquet")

# 2019
ot <- read_parquet("2_all_overtakes_drs/sikeres/2019_drs_success.parquet")
season <- 2019L
weather <- read_parquet("2019/weather_2019_all.parquet")

# 2020
ot <- read_parquet("2_all_overtakes_drs/sikeres/2020_drs_success.parquet")
season <- 2020L
weather <- read_parquet("2020/weather_2020_all.parquet")

# 2021
ot <- read_parquet("2_all_overtakes_drs/sikeres/2021_drs_success.parquet")
season <- 2021L
weather <- read_parquet("2021/weather_2021_all.parquet")

# 2022
ot <- read_parquet("2_all_overtakes_drs/sikeres/2022_drs_success.parquet")
season <- 2022L
weather <- read_parquet("2022/weather_2022_all.parquet")

# 2023
ot <- read_parquet("2_all_overtakes_drs/sikeres/2023_drs_success.parquet")
season <- 2023L
weather <- read_parquet("2023/weather_2023_all.parquet")

# 2024
ot <- read_parquet("2_all_overtakes_drs/sikeres/2024_drs_success.parquet")
season <- 2024L
weather <- read_feather("2024/weather_2024_all.feather")

# 2025
ot <- read_parquet("2_all_overtakes_drs/sikeres/2025_drs_success.parquet")
season <- 2025L
weather <- read_feather("2025/weather_2025_all.feather")

# Javasolt mindkét oldalon numerikussá alakítani az időt (másodpercekben) az egyszerűbb 
# és gyorsabb összehasonlítás érdekében az SQL-ben.
ot <- ot %>%
  mutate(OvertakeIntervall_Bottom_sec = as.numeric(OvertakeIntervall_Bottom))

weather <- weather %>%
  mutate(Time_sec = as.numeric(Time))

#### Kapcsolat létrehozása és táblák regisztrálása ####

con <- dbConnect(duckdb::duckdb())
duckdb_register(con, "ot_table", ot)
duckdb_register(con, "weather_table", weather)

# fennáll-e a kapcsolat
dbIsValid(con)

#### ASOF JOIN: a legközelebbi korábbi időpont párosítása ####

query <- "
  SELECT 
    o.*,
    w.Time_sec AS Weather_Time,
    w.AirTemp, 
    w.Humidity, 
    w.Pressure, 
    w.Rainfall, 
    w.TrackTemp, 
    w.WindDirection, 
    w.WindSpeed
  FROM ot_table o
  ASOF LEFT JOIN weather_table w
    ON o.Round = w.Race 
   AND o.OvertakeIntervall_Bottom_sec >= w.Time_sec
"

#### Eredmény letöltése ####

ot_with_weather <- dbGetQuery(con, query)

#### Kapcsolat lezárása ####

dbDisconnect(con, shutdown = TRUE)

# fennáll-e a kapcsolat
dbIsValid(con)

#### Mentés ####

#mappa <- "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/adatok/2_1_all_overtakes_weather/sikeres"

write_parquet(ot_with_weather, sink = file.path(mappa, paste0(season, "_wet_success.parquet")))
