#Load packages
library(xfun)
xfun::pkg_attach(c('googlePolylines','dplyr','tidyr','stringr','sf','lwgeom','swfscMisc','geosphere','units','smoothr','readr','leaflet','leafem','leaflet.extras','googlesheets4'), install = TRUE)
#-----------------------------------------------------------------------------
#Read data
sellerdata <- read_csv('C:\\Users\\gfloress\\Downloads\\Distritos2023\\Mapas\\data.csv') %>%
              as_tibble()
sellerpick <- sellerdata %>%
              filter(warehouse_name=='Main warehouse') %>%
              select(seller_id,seller_name,latitude,longitude) %>%
              distinct(seller_id, .keep_all=TRUE)
#-----------------------------------------------------------------------------
# City
city <- st_read("C:\\Users\\gfloress\\Downloads\\Distritos2023\\Mapas\\PEdistritos.kml") %>% st_transform(32718)
# Zonas peligrosas
zona <- st_read("C:\\Users\\gfloress\\Downloads\\Distritos2023\\Mapas\\ZonasPeligrosas.kml") %>% st_transform(32718)
# Pickup
sellerpu <- sellerpick %>%
            st_as_sf(coords = c("longitude","latitude"), crs=4326) %>%
            st_transform(32718)
#-----------------------------------------------------------------------------
# Distritos
sellersdist <- st_join(sellerpu, city) %>%
              select(-Description) %>%
              st_drop_geometry() %>%
              rename("district"="Name")
# Zonas poligonos
sellerszona <- st_join(sellerpu, zona) %>% 
              select(-Description) %>%
              st_drop_geometry() %>%
              rename("zona"="Name")
#-----------------------------------------------------------------------------
# Cruces
sellerdatafinal <- sellerdata %>% 
                   left_join(storesdist %>% select(seller_id,district), 
                             by=c("seller_id"="seller_id")
                             ) %>%
                   left_join(sellerszona %>% select(seller_id,zona), 
                             by=c("seller_id"="seller_id")
                             )
#-----------------------------------------------------------------------------
# Exportar
write_csv(sellerdatafinal,'sellerdatafinal.csv')
