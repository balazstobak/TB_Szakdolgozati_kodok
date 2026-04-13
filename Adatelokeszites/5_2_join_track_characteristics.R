rm(list=ls())

library(arrow)
library(dplyr)
library(openxlsx)

tf <- read_parquet("schedules/track_features.parquet")

ot <- read_parquet("4_all_overtakes_merged_imputated/final_dataset_imp.parquet")

colnames(ot)

joined <- ot %>%
  left_join(tf, by = "EventName")

joined <- joined %>%
  arrange(Season, Round, AttemptID) %>%
  mutate(Era = if_else(Season < 2022, 0L, 1L),
         Stratify = paste(Sikeres, Era, sep = "_")) %>%
  rename(Location = Location.x) %>%
  select(-Location.y)

table(joined$Stratify)

colSums(is.na(joined))

#joined <- joined %>%
#  relocate(Sikeres, .after = last_col())

write_parquet(joined, "5_all_overtakes_final/final_dataset.parquet")
