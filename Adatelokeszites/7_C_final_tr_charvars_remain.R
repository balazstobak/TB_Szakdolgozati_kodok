
#Külön-külön hagyom a szektorok leírását, és meghagyom a Locationt is

rm(list=ls())

library(arrow)
library(dplyr)
library(stringr)
library(openxlsx)
library(tidytext)
library(tidyr)
library(tm)

final <- read_parquet("5_all_overtakes_final/final_dataset_flattered.parquet")

fin <- final %>%
  mutate(
    Rainfall = if_else(Rainfall == TRUE, 1L, 0L),
    FreshTyre_Overtaker = if_else(FreshTyre_Overtaker == TRUE, 1L, 0L),
    FreshTyre_Passed = if_else(FreshTyre_Passed == TRUE, 1L, 0L),
    Team_Overtaker = case_when(
      Team_Overtaker %in% c("Force India", "Racing Point") ~ "Aston Martin",
      Team_Overtaker %in% c("Alfa Romeo", "Alfa Romeo Racing", "Sauber") ~ "Kick Sauber",
      Team_Overtaker %in% c("AlphaTauri", "Toro Rosso", "RB") ~ "Racing Bulls",
      Team_Overtaker == "Renault" ~ "Alpine",
      TRUE ~ Team_Overtaker),
    Team_Passed = case_when(
      Team_Passed %in% c("Force India", "Racing Point") ~ "Aston Martin",
      Team_Passed %in% c("Alfa Romeo", "Alfa Romeo Racing", "Sauber") ~ "Kick Sauber",
      Team_Passed %in% c("AlphaTauri", "Toro Rosso", "RB") ~ "Racing Bulls",
      Team_Passed == "Renault" ~ "Alpine",
      TRUE ~ Team_Passed),
    Team_Overtaker = str_remove_all(Team_Overtaker, " "),
    Team_Passed = str_remove_all(Team_Passed, " "),
    OvertakingDifficulty = case_when(
      OvertakingDifficulty == "Medium" ~ 1L,
      OvertakingDifficulty == "Good" ~ 2L,
      OvertakingDifficulty == "Difficult" ~ 3L,
      OvertakingDifficulty == "Very Difficult" ~ 4L,
      TRUE ~ as.integer(mean(OvertakingDifficulty))),
    SpeedDescription = case_when(
      SpeedDescription == "Low" ~ 1L,
      SpeedDescription == "Medium" ~ 2L,
      SpeedDescription == "High" ~ 3L,
      SpeedDescription == "Very High" ~ 4L,
      TRUE ~ as.integer(mean(SpeedDescription))),
    Location = recode(Location, 
                      "Yas Island" = "Yas Marina", 
                      "Monte Carlo" = "Monaco",
                      "Miami Gardens" = "Miami"),) %>%
  select(-c(AttemptID))

"S1Description" %in% colnames(fin)
length(unique(fin$S1Description)) #26

sort(unique(fin$Location))

# atlagosan hány szó esik egy sorba

s1szavak_szama <- lengths(strsplit(fin$S1Description, "\\s+"))
s2szavak_szama <- lengths(strsplit(fin$S2Description, "\\s+"))
s3szavak_szama <- lengths(strsplit(fin$S3Description, "\\s+"))

mean(s1szavak_szama, na.rm = TRUE)
mean(s2szavak_szama, na.rm = TRUE)
mean(s3szavak_szama, na.rm = TRUE)

#felesleges szavak kivétele

stopwords("en")

stopwords_regex <- paste0("\\b(", paste(stopwords("en"), collapse = "|"), ")\\b")

my_stopwords <- stopwords("en")

megtalalt_kotoszavak <- fin %>%
  select(S1Description, S2Description, S3Description) %>%
  pivot_longer(cols = everything(), values_to = "text") %>%
  drop_na(text) %>%
  mutate(word = str_extract_all(str_to_lower(text), "\\b[[:alpha:]]+\\b")) %>%
  unnest(word) %>%
  distinct(word) %>%
  filter(word %in% my_stopwords) %>%
  pull(word)

# Eredmény megtekintése
print(megtalalt_kotoszavak)

fin <- fin %>%
  mutate(
    across(
      c(S1Description, S2Description, S3Description), 
      ~ .x %>%
        # Kisbetűssé alakítás
        str_to_lower() %>%
        # Kötőszavak és nyelvtani szavak eltávolítása
        str_remove_all(stopwords_regex) %>%
        # (Opcionális) Írásjelek eltávolítása, hogy ne maradjanak a szavak végén
        str_remove_all("[[:punct:]]") %>%
        # Felesleges szóközök (dupla szóközök, elején/végén lévők) eltávolítása
        str_squish() %>%
        # Szóközök cseréje alsóvonásra
        str_replace_all(" ", "_")
    )
  )

unique(fin$S1Description)

sum(is.na(fin))

cn <- as.list(colnames(fin))

write_parquet(fin, "6_all_overtakes_final_cleaned/final_allvars_character_2laps.parquet")
write_parquet(fin, "C:/Users/tobakbalazs/egyetem/ELTE/IV/01_Szakdolgozat/Python_R/models/final_allvars_character_2laps.parquet")

