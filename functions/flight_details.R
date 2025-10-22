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
  flightFiles = list_flight_data_local(here::here('data','faam_raw')) |> 
    nest_by(flightNumber) |> 
    mutate(corePath = data |> 
             filter(fileType == "core.nc") |> 
             pull(filePath),
           wow = map2(corePath,flightNumber, 
                      \(x,y) get_weight_off_wheels(x) |> 
                        summarise(startDate = min(date),
                                  endDate = max(date))
           )) |> 
    unnest(wow) |> 
    unnest(data) |> 
    select(-corePath) |> 
    ungroup()
}
