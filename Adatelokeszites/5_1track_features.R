rm(list=ls())

library(arrow)
library(dplyr)
library(stringr)
library(readxl)
library(openxlsx)
library(readr)

schedules <- open_dataset("schedules/parquets")

locations <- schedules %>%
  distinct(Location) %>%
  collect()

events <- schedules %>%
  group_by(Location) %>%
  distinct(EventName) %>%
  ungroup() %>%
  collect()

events <- events %>%
  filter(str_detect(EventName, "Grand Prix"))

events <- events %>%
  mutate(BrakingLevel = case_when(
    Location %in% c("Silverstone", "Suzuka") ~ 1,
    Location %in% c("Barcelona", "São Paulo", "Zandvoort", "Hockenheim") ~ 2,
    Location %in% c("Yas Island", "Austin", "Lusail", "Budapest", "Imola",
                    "Las Vegas", "Melbourne", "Miami", "Monaco", "Shanghai",
                    "Marina Bay", "Miami Gardens", "Monte Carlo", "Singapore",
                    "Mugello", "Sochi", "Nürburgring", "Portimão", "Le Castellet") ~ 3,
    Location %in% c("Baku", "Mexico City", "Jeddah", "Montréal", "Spa-Francorchamps",
                    "Spielberg", "Istanbul") ~ 4,
    Location %in% c("Monza", "Sakhir") ~ 5,
  ))

#write.xlsx(events, file = "events.xlsx")

track_features <- read_excel("schedules/track_features.xlsx")

events <- events %>%
  left_join(track_features, by = "Location")

#write.xlsx(events, "eventsxy.xlsx")

events <- events %>%
  select(-ends_with(".x")) %>%
  rename_with(~ str_remove(.x, "\\.y$"),
              ends_with(".y"))

events <- events %>%
  mutate(
    Overtaking = str_remove(Overtaking, "^Overtaking\\s+"),
    Speed = str_remove(Speed, "^Speed\\s+"))

events <- events %>%
  rename(OvertakingDifficulty = Overtaking,
         SpeedDescription = Speed,
         S1Description = "Sector 1",
         S2Description = "Sector 2",
         S3Description = "Sector 3")

events <- events %>%
  mutate(Location = if_else(Location == 'Yas Island', 'Yas Marina', Location))

write_parquet(events, "schedules/track_features.parquet")
