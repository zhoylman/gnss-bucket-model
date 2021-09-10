library(dplyr)
library(reticulate)
library(raster)
library(sf)
library(devtools) # install_github("hunzikp/velox")
library(velox)
library(stringr)
library(doParallel)
library(foreach)

var = 'precipitation'

data.dir = paste0('/home/zh192885e/data/snodas/processed/', var, '/')

files = list.files(data.dir, full.names = T)
time = list.files(data.dir) %>% str_extract(., "(\\d)+")
  
sf = st_read('/home/zh192885e/gnss-bucket-model/data/SLBHUC8/SLBHUC8.shp') %>%
  st_union()

cl = makeCluster(40)
registerDoParallel(cl)

tictoc::tic()

extraction = foreach(i = 1:length(files), .packages = c('dplyr', 'velox', 'raster')) %dopar% {
#extraction = foreach(i = 1:100, .packages = c('dplyr', 'velox', 'raster')) %dopar% {
  gc()
  temp_rast = raster(files[[i]]) %>% velox
  
  extract = temp_rast$extract(sp=sf,  fun = function(x) sum(x, na.rm = TRUE), df = T) %>%
    mutate(time = time[[i]]) %>%
    dplyr::select(time, out) 
  
  extract
}

stopCluster(cl)

final = bind_rows(extraction)

write.csv(final, paste0('/home/zh192885e/local_data/', var, '_selway_summary.csv'), row.names = F)

tictoc::toc()
