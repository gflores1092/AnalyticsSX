#DISTRITOS FINAL
library(sf)
library(dplyr)
#Read shapefile Peru
perulayer <- st_read('C:/Users/Gerardo Flores/Documents/MapasPeru/Shapefile/DISTRITOS.shp', quiet = TRUE, stringsAsFactors = FALSE)
distritos <- perulayer %>%
             select(region = DEPARTAMEN, provincia = PROVINCIA, distrito = DISTRITO) %>%
             mutate(distrito = as.character(str_replace_all(distrito,"Ã\u0091|Ñ","N"))) %>%
             mutate(pais = "PE",
                    codigo = case_when(provincia == "LIMA" ~ "LIM",
                                       provincia == "CALLAO" ~ "LIM",
                                       provincia == "AREQUIPA" ~ "AQP",
                                       provincia == "CAJAMARCA" ~ "CJA",
                                       provincia == "CHICLAYO" ~ "CHI",
                                       provincia == "CHINCHA" ~ "CNC",
                                       provincia == "CUSCO" ~ "CUZ",
                                       provincia == "HUANCAYO" ~ "HUA",
                                       provincia == "ICA" ~ "ICA",
                                       provincia == "PIURA" ~ "PIU",
                                       provincia == "SANTA" ~ "CHB",
                                       provincia == "TACNA" ~ "TCQ",
                                       provincia == "TRUJILLO" ~ "TRU"
                                       )
                    ) %>%
             group_by(pais, region, provincia, distrito, codigo) %>%           
             summarise(geometry = st_union(geometry)) %>%
             ungroup() %>%
             as.data.frame() %>%
             st_sf()
#KML
kml_distritos <- distritos %>% 
                 filter(pais == "PE") %>%
                 filter(codigo != is.na(codigo)) %>%
                 select(Name = distrito, Description = codigo)
st_write(kml_distritos, "PEdistritos.kml", driver = "kml", delete_dsn = TRUE)
#Plot
plot(kml_distritos %>% filter(Description == "AQP") %>% st_geometry())
