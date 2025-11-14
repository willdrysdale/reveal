flight_details = function(){
  dplyr::tribble(
    ~flightNumber,
           # "c391",
           "c409",
           "c410",
           "c412",
           "c413",
           "c415",
           "c416",
           "c417",
           "c418"
  )
}

flight_files = function(){
  flightFiles = faamr::list_flight_data_local(here::here('data','faam_raw'), skipFlightListCheck = T) |>
    mutate(r = faamr:::file_revision(basename(filePath))) |> 
    group_by(flightNumber, fileType) |> 
    filter(r == max(r)) |> 
    ungroup() |> 
    select(-r) |> 
    nest_by(flightNumber) |> 
    mutate(corePath = data |> 
             filter(fileType == "core.nc") |> 
             pull(filePath),
           wow = map2(corePath,flightNumber, 
                      \(x,y) faamr::get_weight_off_wheels(x) |> 
                        summarise(startDate = min(date),
                                  endDate = max(date))
           )) |> 
    unnest(wow) |> 
    unnest(data) |> 
    select(-corePath) |> 
    ungroup()
}
