#Load packages
library(xfun)
xfun::pkg_attach(c('sp','sf','dplyr','tidyr','lubridate','stringr','readr','tmap'), install = TRUE)
#Data processing
#-----------------------------------------------------------------------------
#Glovo
glovo <- st_read("PE.kml") %>% filter(Name == "LIM") %>% st_transform(32718) %>% fill_holes(threshold = set_units(0.5, km^2)) 
#Peru
peru <- st_read("PEdistritos.kml") %>% st_transform(32718) %>% filter(Description == "LIM")
glovoperu <- st_intersection(glovo,peru)
#Hexagons
hexagons <- st_make_grid(glovo, cellsize = 500, square = FALSE) %>% st_sf() %>% mutate(Description = row_number()) 
#-----------------------------------------------------------------------------
##Orders
ordersmkt <- read.table('datamkt.csv', header = TRUE, sep = ',', stringsAsFactors = FALSE, quote="\"", allowEscapes = FALSE) %>% filter(city == "LIM")
#-----------------------------------------------------------------------------
#Points
ordersap <- ordersmkt %>% st_as_sf(coords = c("acceptance_longitude", "acceptance_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
orderspu <- ordersmkt %>% st_as_sf(coords = c("pickup_longitude", "pickup_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
ordersdp <- ordersmkt %>% st_as_sf(coords = c("delivery_longitude", "delivery_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
##Districts
ordersapdist <- st_join(ordersap, peru, join = st_intersects)
orderspudist <- st_join(orderspu, peru, join = st_intersects)
ordersdpdist <- st_join(ordersdp, peru, join = st_intersects)
##Hexagons
ordersaphex <- st_join(ordersap, hexagons, join = st_intersects)
orderspuhex <- st_join(orderspu, hexagons, join = st_intersects)
ordersdphex <- st_join(ordersdp, hexagons, join = st_intersects)
#-----------------------------------------------------------------------------
##Final
ordersmktdf <- ordersmkt %>%
               mutate("acceptance_district" = ordersapdist$Name,
                      "pickup_district" = orderspudist$Name,
                      "delivery_district" = ordersdpdist$Name,
                      "acceptance_hexagon" = ordersaphex$Description,
                      "pickup_hexagon" = orderspuhex$Description,
                      "delivery_hexagon" = ordersdphex$Description
                      ) %>%
               distinct(order_id, .keep_all = TRUE)
##Export to CSV
write_csv(ordersmkt, "mktdata.csv")
#Analysis Pre Covid / Post Covid
orderscovid <- ordersmktdf %>%
               mutate(date = as.Date(mdy(date))) %>%
               arrange(date) %>%
               filter(day != "Sunday") %>%
               filter(day != "Saturday") %>%
               filter(date >= "2020-03-09" & date <= "2020-03-15") %>%
               filter(hour > 7 & hour < 20) %>%
               filter(category != "Quiero")
###CSV Covid
#write_csv(ordersmktdf, "mktdatadf.csv")
#-----------------------------------------------------------------------------
#Points
covidap <- orderscovid %>% st_as_sf(coords = c("acceptance_longitude", "acceptance_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
covidpu <- orderscovid %>% st_as_sf(coords = c("pickup_longitude", "pickup_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
coviddp <- orderscovid %>% st_as_sf(coords = c("delivery_longitude", "delivery_latitude"), crs = 4326, na.fail = FALSE) %>% st_transform(32718)
#-----------------------------------------------------------------------------
##Distritos
#Points
ordispupleaf <- st_join(peru, covidpu) %>%
                group_by(Name) %>%
                mutate(n = n()) %>%
                select(Name, n) %>% 
                st_drop_geometry() %>%
                distinct(Name, n) %>%
                left_join(city %>% select(Name), by = c("Name" = "Name")) %>%
                st_sf()
#Points
ordisdepleaf <- st_join(peru, coviddp) %>%
  group_by(Name) %>%
  mutate(n = n()) %>%
  select(Name, n) %>% 
  st_drop_geometry() %>%
  distinct(Name, n) %>%
  left_join(city %>% select(Name), by = c("Name" = "Name")) %>%
  st_sf()
##Hexagonos
#Points
orhexpupleaf <- st_join(hexagons, covidpu) %>%
                filter(pickup_hexagon != is.na(pickup_hexagon)) %>%
                group_by(Description) %>% 
                mutate(n = n()) %>%
                select(Description, n) %>%
                st_drop_geometry() %>%
                distinct(Description, n) %>%
                left_join(hexagons %>% select(Description), by = c("Description" = "Description")) %>%
                st_sf()
#-----------------------------------------------------------------------------
##Labels
###Glovo
labelglovo <- sprintf("<strong>%s</strong><br/>%g km<sup>2</sup>","LIM", st_area(glovo)/1000000) %>% lapply(htmltools::HTML)
###District
labelordispup <- sprintf("<strong>District: %s</strong><br/><strong>Orders: %s</strong><br/>", ordispupleaf$Name, ordispupleaf$n) %>% lapply(htmltools::HTML)
###Hexagons
labelorhexpup <- sprintf("<strong>Orders: %s</strong><br/>", orhexpupleaf$n) %>% lapply(htmltools::HTML)
##Palettes
###Districts
palordispup <- colorNumeric(palette = "RdYlBu", domain = ordispupleaf$n)
###Hexagons
palorhexpup <- colorNumeric(palette = "RdYlBu", domain = orhexpupleaf$n)
#-----------------------------------------------------------------------------
##Leafmap
###Base map
leafmap <- leaflet() %>% 
  addTiles() %>% 
  addScaleBar(position = "bottomright") %>% 
  #Glovo
  addPolygons(data = st_transform(glovo, 4326), 
              group = "Glovo", 
              weight = 2, 
              color = "yellow", 
              fillOpacity = 0.25, 
              opacity = 0.5, 
              label = labelglovo, 
              labelOptions = labelOptions(style = list("font-weight" = "normal", padding = "3px 8px")), 
              highlightOptions = highlightOptions(sendToBack = TRUE) 
  ) %>%
  #Districts
  addPolygons(data = st_transform(peru, 4326), 
              group = "Districts", 
              weight = 1, 
              color = "red", 
              fillOpacity = 0,
              opacity = 0.25, 
              highlightOptions = highlightOptions(sendToBack = TRUE)
  )
###Orders Districts / Hexagons
leaford <- leafmap %>%
  #Pickup Districts
  addPolygons(data = st_transform(ordispupleaf, 4326), 
              group = "Pickup Districts", 
              weight = 1, 
              color = ~palordispup(ordispupleaf$n), 
              fillOpacity = 0.5,
              opacity = 0.25, 
              label = labelordispup, 
              labelOptions = labelOptions(style = list("font-weight" = "normal", padding = "3px 8px"), textsize = "10px", direction = "auto"), 
              highlightOptions = highlightOptions(bringToFront = TRUE, weight = 3, fillOpacity = 0.3)
  ) %>%
  #Pickup Hexagons
  addPolygons(data = st_transform(orhexpupleaf, 4326), 
              group = "Pickup Hexagons", 
              weight = 1, 
              color = ~palorhexpup(orhexpupleaf$n), 
              fillOpacity = 0.5,
              opacity = 0.25, 
              label = labelorhexpup, 
              labelOptions = labelOptions(style = list("font-weight" = "normal", padding = "3px 8px"), textsize = "10px", direction = "auto"), 
              highlightOptions = highlightOptions(bringToFront = TRUE, weight = 3, fillOpacity = 0.3)
  ) %>%
  #Layers
  addLayersControl(overlayGroups = c("Glovo", "Districts", "Pickup Districts", "Pickup Hexagons"))
#sthexpupleaf %>% st_drop_geometry() %>% arrange(desc(n)) %>% rename("Hexagon"="Description","Orders"="n") %>% as.data.frame()
#orhexpupleaf %>% st_drop_geometry() %>% arrange(desc(n)) %>% rename("Hexagon"="Description","Orders"="n") %>% as.data.frame()
#ordispupleaf %>% st_drop_geometry() %>% arrange(desc(n)) %>% rename("District"="Name","Orders"="n") %>% as.data.frame()

tm_shape(ordispupleaf %>% filter(n>10)) + 
  tm_polygons("n", palette = "RdYlBu", title = "District Pickup Orders")  +
  tm_layout(legend.position = c("right","bottom"))

#ordispupleaf %>% filter(n>10) %>% arrange(desc(n)) %>% rename("District"="Name","Orders"="n") %>% st_drop_geometry() %>% as.data.frame()

tm_shape(ordisdepleaf %>% filter(n>10)) + 
  tm_polygons("n", palette = "RdYlBu", title = "District Delivery Orders")

#ordisdepleaf %>% filter(n>10) %>% arrange(desc(n)) %>% rename("District"="Name","Orders"="n") %>% st_drop_geometry() %>% as.data.frame()

#Rutas
#orderscovid %>% group_by(pickup_district,delivery_district) %>% summarise(Orders = n()) %>% arrange(desc(Orders)) %>% as.data.frame()

#Users y Active Users
orderscovid %>% 
  group_by(customer_id) %>% 
  transmute(n=n()) %>%
  ungroup() %>%
  group_by(customer_id) %>%
  filter(n()>2) %>%
  summarise(Heavyusers = n_distinct(customer_id)) %>% 
  arrange(desc(Heavyusers)) %>% 
  as.data.frame()

#Active Users
orderscovid %>% group_by(pickup_district) %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()
#Recurrent users
orderscovid %>% group_by(pickup_district) %>% filter(new_customer=="f") %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()
#New users
orderscovid %>% group_by(pickup_district) %>% filter(new_customer!="f") %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()

orderscovid %>% group_by(customer_id) %>% filter(n()>1) %>% ungroup() %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()

orderscovid %>% filter(new_customer=="f") %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()

orderscovid %>% filter(new_customer!="f") %>% summarise(n=n_distinct(customer_id)) %>% arrange(desc(n)) %>% as.data.frame()




tm_shape(glovoperu) + 
  tm_polygons(alpha = 0) +
tm_shape(orhexpupleaf %>% filter(Description %in% c(245,188,141,171,175,199,181,142,178,209,221,290))) + 
  #tm_polygons("n", breaks = c(400,450,500,1000,5000), title = "Hexagons") +
  tm_polygons("n", title = "Hexagons") +
  tm_layout(legend.position = c("right","bottom"))

sthexpupleaf %>% arrange(desc(n)) %>% filter(n>=22)

tm_shape(glovoperu) + 
  tm_polygons(alpha = 0) +
  tm_shape(sthexpupleaf %>% arrange(desc(n)) %>% filter(n>=22)) + 
  tm_polygons("n", palette = "RdYlBu", title = "Hexagons") +
  tm_layout(legend.position = c("right","bottom"))
