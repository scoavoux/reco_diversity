get_one_artist_related <- function(artist_id){
  url <- str_glue("https://api.deezer.com/artist/{artist_id}/related")
  page <- RETRY("GET", url) |> 
    content()
  if(!is.null(page$total) && page$total > 0){
    res <- page$data |> 
      bind_rows() |> 
      filter(type == "artist") |> 
      mutate(orig_artist_id = artist_id) %>% 
      select(orig_artist_id, dest_artist_id = "id", name, nb_album, nb_fan)
  } else {
    return(NULL)
  }
  return(res)
}


