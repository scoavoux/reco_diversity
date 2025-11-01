library(tidyverse)
library(targets)
library(httr)

# we need functions to scrape deezer api
tar_source()

# First round -----
##We start from unique artists and for everyone get their related artists network
tar_load(unique_artists)
dir.create("data/temp/related")
ua <- unique_artists %>% 
  mutate(scrapped = FALSE)

for(i in seq(1, nrow(ua), 50)){
  if(ua$scrapped[i+49]) next
  res <- vector("list", length = 50)
  date <- now()
  for(j in 1:50){
    if(ua$scrapped[i + j - 1]) next
    res[[j]] <- get_one_artist_related(ua$artist_id[i + j - 1])
    ua$scrapped[i + j - 1] <- TRUE
  }
  
  write_csv(bind_rows(res), str_glue("data/temp/related/{i}.csv"))
  if((i - 1) %% 1000 == 0){
    print(i)
  }
  # check that we don't do more than 50 API calls every 5 seconds
  while((now() - date) < duration(6)){
    Sys.sleep(.1)
  }
  
}

# Look at results ------

files <- dir("data/temp/related/", full.names = TRUE)
artist_network <- map(files, read_csv, col_types = "iicii") %>% 
  bind_rows()

artist_network <- artist_network %>% 
  select(orig_artist_id, dest_artist_id, name, nb_album, nb_fan)

# Second Wave: artists without a match in the first round ------
ua <- distinct(ua, artist_id) %>% 
  filter(!(artist_id %in% unique(artist_network$orig_artist_id))) %>% 
  mutate(scrapped = FALSE)

for(i in seq(1, nrow(ua), 50)){
  if(!is.na(ua$scrapped[i+49]) && ua$scrapped[i+49]) next
  res <- vector("list", length = 50)
  date <- now()
  for(j in 1:50){
    if(ua$scrapped[i + j - 1]) next
    res[[j]] <- get_one_artist_related(ua$artist_id[i + j - 1])
    ua$scrapped[i + j - 1] <- TRUE
  }
  
  write_csv(bind_rows(res), str_glue("data/temp/related/2nd_{i}.csv"))
  if((i - 1) %% 1000 == 0){
    print(i)
  }
  # check that we don't do more than 50 API calls every 5 seconds
  while((now() - date) < duration(6)){
    Sys.sleep(.1)
  }
  
}

# Look at results ------
files <- dir("data/temp/related/", full.names = TRUE)
artist_network <- map(files, read_csv, col_types = "iicii") %>% 
  bind_rows()

artist_network <- artist_network %>% 
  select(orig_artist_id, dest_artist_id, name, nb_album, nb_fan)

# Now we look at artists in the network but not scrapped, nor in unique_artists ------
ua <- distinct(artist_network, artist_id = dest_artist_id) %>% 
  filter(!(artist_id %in% unique(unique_artists$artist_id))) %>% 
  mutate(scrapped = FALSE)

for(i in seq(1, nrow(ua), 50)){
  if(!is.na(ua$scrapped[i+49]) && ua$scrapped[i+49]) next
  res <- vector("list", length = 50)
  date <- now()
  for(j in 1:50){
    if(ua$scrapped[i + j - 1]) next
    res[[j]] <- get_one_artist_related(ua$artist_id[i + j - 1])
    ua$scrapped[i + j - 1] <- TRUE
  }
  
  write_csv(bind_rows(res), str_glue("data/temp/related/3rd_{i}.csv"))
  if((i - 1) %% 1000 == 0){
    print(i)
  }
  # check that we don't do more than 50 API calls every 5 seconds
  while((now() - date) < duration(6)){
    Sys.sleep(.1)
  }
  
}


files <- dir("data/temp/related/", full.names = TRUE)
artist_network <- map(files, read_csv, col_types = "iicii") %>% 
  bind_rows()

artist_network <- artist_network %>% 
  select(orig_artist_id, dest_artist_id, name, nb_album, nb_fan)
write_csv(artist_network, "data/temp/artist_network.csv")

