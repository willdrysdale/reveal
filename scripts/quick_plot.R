library(dplyr)
library(tidyr)
library(ggplot2)
library(lubridate)

dat = readRDS(here::here('data','faam_merge','merge.RDS'))

dat |> 
  select(date, flightNumber, no_mr, no2_mr, co2_drymole_ppm, ALT_GIN) |> 
  pivot_longer(-c(date, flightNumber)) |> 
  mutate(date = as.numeric(date)) |> 
  ggplot()+
  geom_line(aes(date, value))+
  facet_grid(name ~ flightNumber, scales = "free")





dat |> 
  mutate(nox = no_mr+no2_mr) |> 
  select(date, flightNumber, nox, LAT_GIN, LON_GIN) |> 
  arrange(nox) |> 
  ggplot()+
  geom_point(aes(LAT_GIN, LON_GIN, colour = log10(nox)), size = 2)+
  scale_colour_viridis_c()+
  facet_wrap(~flightNumber)
