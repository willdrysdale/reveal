library(faamr)
library(dplyr)
library(purrr)
library(tidyr)
library(stringr)
library(nanotime)
library(lubridate)

source(here::here('functions','flight_details.R'))

flightFiles = flight_files() |> 
  select(flightNumber, startDate, endDate) |> 
  distinct() |> 
  mutate(dateOfFlight = startDate |> 
           as_datetime(tz = "UTC") |> 
           floor_date("1 day"))

files = tibble(
  path = list.files(here::here('data', 'particles_non_ceda', 'raw'), full.names = T, recursive = T)
) |> 
  mutate(
    fileName = basename(path),
    type = case_when(
      str_detect(fileName, "AMS") ~ "AMS",
      str_detect(fileName, "SP2") ~ "SP2",
      TRUE ~ "SMPS"
    ),
    flightNumber = fileName |> 
      str_remove("AMS") |> 
      str_remove("SP2") |> 
      str_remove("export") |> 
      str_remove(".csv") |> 
      str_remove_all("_") |> 
      tolower() |> 
      str_trim()
  ) 


dirOut = here::here('data','particles_non_ceda','tidy')

if(!dir.exists(dirOut)){
  dir.create(dirOut)
}


# AMS ---------------------------------------------------------------------

datAMS = files |> 
  filter(type == "AMS") |> 
  rowwise() |> 
  mutate(
    data = path |> 
      read.csv() |> 
      tibble() |> 
      rename_with(\(x) str_remove(x, "_export")) |> 
      rename_with(\(x) ifelse(str_detect(x, "secs"), "seconds_since_midnight", x)) |> 
      janitor::clean_names() |> 
      pivot_longer(-seconds_since_midnight, names_to = c("spec", "flightNumber"), names_sep = "_") |>
      pivot_wider(names_from = "spec") |>
      mutate(flightNumber = tolower(flightNumber)) |>
      list()
  ) |> 
  pull(data) |> 
  bind_rows() |> 
  left_join(
    select(flightFiles, flightNumber, dateOfFlight),
    by = "flightNumber") |> 
  mutate(date = dateOfFlight+seconds_since_midnight,
         date = as.nanotime(date)) |> 
  select(-flightNumber, -dateOfFlight) |>  # the flight numbers in the AMS files don't account for doubles, so lets drop them and rejoin based on wow time
  left_join(
    select(flightFiles, -dateOfFlight),
    by = join_by(between(date, startDate, endDate))        
  ) |> 
  filter(!is.na(flightNumber)) |> 
  select(date, flightNumber, chl, nh4, no3, org, so4)

saveRDS(datAMS, here::here(dirOut, "AMS.RDS"))
  
# SP2 ---------------------------------------------------------------------


datSP2 = files |> 
  filter(type == "SP2") |> 
  rowwise() |> 
  mutate(
    data = path |> 
      read.csv() |> 
      tibble() |> 
      janitor::clean_names() |> 
      mutate(flightNumber = flightNumber) |> 
      rename(seconds_since_midnight = secs_bc) |> 
      list()
  ) |> 
  pull(data) |> 
  bind_rows() |> 
  left_join(
    select(flightFiles, flightNumber, dateOfFlight),
    by = "flightNumber") |> 
  mutate(date = dateOfFlight+seconds_since_midnight,
         date = as.nanotime(date)) |> 
  select(-flightNumber, -dateOfFlight) |>  # the flight numbers in the AMS files don't account for doubles, so lets drop them and rejoin based on wow time
  left_join(
    select(flightFiles, -dateOfFlight),
    by = join_by(between(date, startDate, endDate))        
  ) |> 
  filter(!is.na(flightNumber)) |> 
  select(date, flightNumber, mass_bc_ugm3, n_bc)

saveRDS(datSP2, here::here(dirOut, "SP2.RDS"))


# SMPS --------------------------------------------------------------------

files |> 
  filter(type == "SMPS") |> 
  rowwise() |> 
  mutate(data = path |> 
           read.csv() |> 
           tibble() |> 
           list()) |> 
  pull(data) |> 
  bind_rows()
