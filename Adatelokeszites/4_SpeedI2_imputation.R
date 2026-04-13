rm(list=ls())

library(dplyr)
library(arrow)
library(mice)

#### Imputálás: 2025. R22 (LV) SpeedI2 változója csupa NA ####

full_attempts <- read_parquet("4_all_overtakes_merged_imputated/final_dataset.parquet")

#predikcióhoz szükséges változók
predictor_cols <- c("Season", "Round", "Sector1", "Sector2", "Sector3", 
                    "SpeedI1", "SpeedST", "LapNumber", "SpeedI2")

#segédtábla ezekből
df_temp <- full_attempts %>% select(all_of(predictor_cols))

#MICE futtatása 10 iterációval
imp_model <- mice(df_temp, m = 10, method = 'pmm', seed = 123)

#csekk
plot(imp_model)
densityplot(imp_model)

#a10 különböző imputálás átlagolása a SpeedI2-re
imp_matrix <- sapply(1:10, function(i) complete(imp_model, i)$SpeedI2)
final_speed_vector <- rowMeans(imp_matrix)

#imputálás
full_attempts <- full_attempts %>%
  mutate(SpeedI2 = if_else(Season == 2025 & Round == 22 & is.na(SpeedI2), 
                           final_speed_vector, 
                           SpeedI2))

#DeltaSpeedI2
full_attempts <- full_attempts %>%
  group_by(Season, Round, AttemptID) %>%
  mutate(
    DeltaSpeedI2 = case_when(
      Season == 2025 & Round == 22 & Role == "Overtaker" ~ 
        SpeedI2 - SpeedI2[Role == "Passed"],
      Season == 2025 & Round == 22 & Role == "Passed" ~ 
        SpeedI2 - SpeedI2[Role == "Overtaker"],
      TRUE ~ DeltaSpeedI2
    )
  ) %>%
  ungroup()

#ellenőrzés
cat("Maradék NA a SpeedI2-ben:", sum(is.na(full_attempts$SpeedI2)), "\n")
cat("Maradék NA a DeltaSpeedI2-ben:", sum(is.na(full_attempts$DeltaSpeedI2)), "\n")

sum(is.na(full_attempts))

table(full_attempts$Sikeres)

write_parquet(full_attempts, "4_all_overtakes_merged_imputated/final_dataset_imp.parquet")
