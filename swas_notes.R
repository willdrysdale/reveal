library(tidyr)
library(dplyr)
library(ggplot2)

filepath = "data/faam_merge/swasMerge.RDS"


datswas = readRDS(filepath) |> 
  pivot_longer(-c(start_sample_date_time, stop_sample_date_time, comments, flightNumber, swas_case, swas_bottle, gc_date_time)) |> 
  mutate(
    type = ifelse(
      name %in% c("O3_TECO", "no_mr", "no2_mr", "co2_drymole", "ch4_drymole", "chl", "nh4", "no3", "org", "so4", "mass_bc_ugm3", "n_bc"),
      "other", 
      "voc")
  )

p1 = datswas |> 
  filter(type == "voc") |> 
  pivot_wider() |> 
  group_by(flightNumber) |> 
  mutate(swas_bottle = row_number()) |> 
  pivot_longer(-c(start_sample_date_time, stop_sample_date_time, comments, flightNumber, swas_case, swas_bottle, gc_date_time, type)) |> 
  ggplot()+
  geom_bar(aes(x = swas_bottle, y = value, fill = name), stat = "identity")+
  facet_wrap(~flightNumber)+
  guides(fill = "none")+
  theme_minimal()

plotly::ggplotly(p1)
