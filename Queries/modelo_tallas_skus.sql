CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.modelo_tallas_skus` AS (

SELECT
  country,
  sellerId,
  sellerName,
  sku_seller,
  product_description,
  product_name,
  brand_name,
  model,
  CASE WHEN dimension_quantity=0 THEN 1
       WHEN dimension_units='in' AND G1!='G19' THEN 1 
       WHEN dimension_quantity - CAST(dimension_quantity AS INT64) > 0 THEN 1
       ELSE dimension_quantity
       END AS dimension_quantity,
  CASE WHEN dimension_units='in' AND G1!='G19' THEN 'un'
       ELSE dimension_units
       END AS dimension_units,
  main_sku,
  shop_sku,
  primary_category,
  global_identifier,
  G1,
  N1,
  G2,
  N2,
  G3,
  N3,
  G4,
  N4,
  status,
  approval_status,
  created_at,
  updated_at,
  image_meta,
  url,
  color_basico,
  color_secundario,
  talla,
  condiciones_del_producto,
  condiciones_de_garantia_del_vendedor,
  detalle_de_la_condicion,
  condiciones_de_garantia,
  garantia_del_proveedor,
  contenido_del_paquete,
  numero_de_piezas,
  cantidad_de_paquetes,
  width,
  length,
  height,
  weight,
  size,
  --CASE 
  --  WHEN size IS NOT NULL THEN size
  --  WHEN weight>=0 AND weight<0.5 AND volume_m3>=0 AND volume_m3<0.005 AND length>=0 AND length<40 THEN 'XS3'
  --  WHEN weight>=0.5 AND weight<1 AND volume_m3>=0.005 AND volume_m3<0.009 AND length>=0 AND length<40 THEN 'XS2'
  --  WHEN weight>=1 AND weight<3 AND volume_m3>=0.009 AND volume_m3<0.032 AND length>=0 AND length<40 THEN 'XS'
  --  WHEN weight>=3 AND weight<10 AND volume_m3>=0.032 AND volume_m3<0.18 AND length>=0 AND length<40 THEN 'S'
  --  WHEN weight>=10 AND weight<20 AND volume_m3>=0.18 AND volume_m3<0.33 AND length>=40 AND length<100 THEN 'M'
  --  WHEN weight>=10 AND weight<20 AND volume_m3>=0.18 AND volume_m3<0.33 AND length>=100 AND length<650 THEN 'LO'
  --  WHEN weight>=20 AND weight<30 AND volume_m3>=0.33 AND volume_m3<0.66 AND length>=100 AND length<240 THEN 'L'
  --  WHEN weight>=30 AND weight<50 AND volume_m3>=0.33 AND volume_m3<0.66 AND length>=100 AND length<240 THEN 'XL'
  --  WHEN weight>=50 AND weight<100 AND volume_m3>=0.66 AND volume_m3<1 AND length>=240 AND length<650 THEN 'XXL'
  --  WHEN weight>=100 AND weight<300 AND volume_m3>=1 AND volume_m3<2.5 AND length>=240 AND length<650 THEN 'O'
  --  WHEN weight>=300 AND weight<99999 AND volume_m3>=2.5 AND volume_m3<99999 AND length>=650 AND length<99999 THEN 'EO'
  --  END AS calculated_size,
  previous_item_size,
  new_item_size,
  volume_m3,
  aspect_ratio,
  width_in_cm,
  length_in_cm,
  height_in_cm,
  weight_in_kg,
  dist_xs3, 
  dist_xs2, 
  dist_xs, 
  dist_s, 
  dist_m, 
  dist_lo, 
  dist_l, 
  dist_xl, 
  dist_xxl, 
  dist_o, 
  dist_eo,
  --price,
  variation,
  attributes
FROM (

  WITH 
  a AS (
    SELECT
      country,
      sellerId,
      sellerName,
      sku_seller,
      product_name,
      brand_name,
      model,
      product_description,
      SAFE_CAST(
        -- Por contenido
        -- Tamaño de pantalla
        COALESCE(
          CASE WHEN SAFE_CAST(
            COALESCE(
              JSON_EXTRACT_SCALAR(attributes, '$.258'),
              JSON_EXTRACT_SCALAR(attributes, '$.724'),
              JSON_EXTRACT_SCALAR(attributes, '$.844'),
              JSON_EXTRACT_SCALAR(attributes, '$.1138'),
              JSON_EXTRACT_SCALAR(attributes, '$.1379'),
              JSON_EXTRACT_SCALAR(attributes, '$.1434'),
              JSON_EXTRACT_SCALAR(attributes, '$.1703')
            )
          AS FLOAT64) >= 99 THEN NULL ELSE 
            COALESCE(
              JSON_EXTRACT_SCALAR(attributes, '$.258'),
              JSON_EXTRACT_SCALAR(attributes, '$.724'),
              JSON_EXTRACT_SCALAR(attributes, '$.844'),
              JSON_EXTRACT_SCALAR(attributes, '$.1138'),
              JSON_EXTRACT_SCALAR(attributes, '$.1379'),
              JSON_EXTRACT_SCALAR(attributes, '$.1434'),
              JSON_EXTRACT_SCALAR(attributes, '$.1703')
            ) END,
          -- Logica antigua
          IFNULL(
            REGEXP_REPLACE(
              REGEXP_EXTRACT(
                product_name,
                r'(?:\d{1,3}\.\d{1,3}|(?:\d{1,3})(?:cm|kg|lb|mg|ml|mm|oz|in|g|l|m)\b|\d{2}in)'
              ),
              r'([a-z]{1,2})',''
            ),
          '1')
        )
      AS FLOAT64) AS dimension_quantity,
      COALESCE(
        -- Televisores y portatiles
        CASE WHEN global_identifier IN ('G19020305','G19080602') THEN 'in' ELSE NULL END,
        IFNULL(
          REGEXP_EXTRACT(
            REGEXP_EXTRACT(
              product_name,
              r'(?:\d{1,3}\.\d{1,3}|(?:\d{1,3})(?:cm|kg|lb|mg|ml|mm|oz|in|g|l|m)\b|\d{2}in)'
            ),
            r'([a-z]{1,2})'
          ),
        'un')
      ) AS dimension_units,
      main_sku,
      COALESCE(shop_sku,sku_seller) AS shop_sku,
      primary_category,
      global_identifier,
      G1,
      N1,
      G2,
      N2,
      G3,
      N3,
      G4,
      N4,
      status,
      approval_status,
      created_at,
      updated_at,
      image_meta,
      url,
      LOWER(color_basico) AS color_basico,
      LOWER(color_secundario) AS color_secundario,
      UPPER(REGEXP_REPLACE(LOWER(talla),'talla ','')) AS talla,
      LOWER(condiciones_del_producto) AS condiciones_del_producto,
      LOWER(condiciones_de_garantia_del_vendedor) AS condiciones_de_garantia_del_vendedor,
      LOWER(detalle_de_la_condicion) AS detalle_de_la_condicion,
      LOWER(condiciones_de_garantia) AS condiciones_de_garantia,
      LOWER(garantia_del_proveedor) AS garantia_del_proveedor,
      LOWER(contenido_del_paquete) AS contenido_del_paquete,
      numero_de_piezas,
      cantidad_de_paquetes,
      width,
      length,
      height,
      weight,
      IFNULL(size,'S') AS size,
      previous_item_size,
      new_item_size,
      (width*height*length) / 1000000 AS volume_m3,
      SAFE_DIVIDE(height,length) / 1000000 AS aspect_ratio,
      width_in_cm,
      length_in_cm,
      height_in_cm,
      weight_in_kg,
      price,
      variation,
      attributes
    FROM (
      SELECT DISTINCT
        CASE WHEN country IS NULL THEN 'CL' ELSE country END AS country,
        sellerId,
        sellerName,
        sku_seller,
        product_description,
        REGEXP_REPLACE(
          --REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  TRIM(
                    CASE WHEN REGEXP_CONTAINS(product_name, r'tv') THEN REGEXP_REPLACE(product_name, r'(\b\d{2}\b|\b\d{2}|d{2})', r'\1in')
                        WHEN REGEXP_CONTAINS(product_name, r'laptop|monitor|tablet') THEN REGEXP_REPLACE(product_name, r'(\b\d{2}\b|\b\d{2}|d{2})', r'\1in')
                        WHEN REGEXP_CONTAINS(primary_category, r'portatiles|notebooks|desktop') THEN REGEXP_REPLACE(product_name, r'(\b\d{2}\b|\b\d{2}|d{2})', r'\1in')
                          WHEN a.G1 IN ('G07','G08','G18') THEN REGEXP_REPLACE(product_name, r'\d', '')
                          ELSE product_name END
                  ),r'\s{2,}', ' '
                ),r'inin','in'
              ),r'\b([a-z]+)as\b',r'\1a'
            ),r'\b([a-z]+)os\b',r'\1o'
          --),r'([bcdfghjklmnpqrstvwxyz])es\b',r'\1'
        ) AS product_name,
        REGEXP_REPLACE(
          LOWER(CASE WHEN brand_name IS NULL THEN 'GENERICO' ELSE brand_name END),
          r'\s{2,}', ' '
        ) AS brand_name,
        REGEXP_REPLACE(
          LOWER(CASE WHEN model IS NULL THEN sku_seller ELSE model END),
          r'\s{2,}', ' '
        ) AS model,
        main_sku,
        a.shop_sku,
        LOWER(primary_category) AS primary_category,
        global_identifier,
        a.G1,
        g1.N1,
        a.G2,
        CASE WHEN g2.N2 IS NULL OR LENGTH(g2.N2)<=3 THEN g1.N1 ELSE g2.N2 END AS N2,
        a.G3,
        CASE WHEN g3.N3 IS NULL OR LENGTH(g3.N3)<=3 THEN g2.N2 ELSE g3.N3 END AS N3,
        a.G4,
        CASE WHEN g4.N4 IS NULL OR LENGTH(g4.N4)<=3 THEN g3.N3 ELSE g4.N4 END AS N4,
        status,
        approval_status,
        created_at,
        updated_at,
        image_meta,
        url,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(color_basico) AS color_basico,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(color_secundario) AS color_secundario,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(talla) AS talla,
        f.name AS condiciones_del_producto,
        condiciones_de_garantia_del_vendedor,
        detalle_de_la_condicion,
        condiciones_de_garantia,
        g.name AS garantia_del_proveedor,
        contenido_del_paquete,
        numero_de_piezas,
        cantidad_de_paquetes,
        IFNULL(COALESCE(width,width_in_cm),0.4) AS width, --S
        IFNULL(COALESCE(length,length_in_cm),0.4) AS length, --S
        IFNULL(COALESCE(height,height_in_cm),0.4) AS height, --S
        IFNULL(COALESCE(weight,weight_in_kg),0.4) AS weight, --S
        size,
        previous_item_size,
        new_item_size,
        --(width*height*length) / 1000000 AS volume_m3,
        --SAFE_DIVIDE(height,length) / 1000000 AS aspect_ratio,
        width_in_cm,
        length_in_cm,
        height_in_cm,
        weight_in_kg,
        --CASE WHEN width!=SAFE_CAST(width_in_cm AS FLOAT64) THEN 'Si' ELSE 'No' END AS width_diff,
        --CASE WHEN length!=SAFE_CAST(length_in_cm AS FLOAT64) THEN 'Si' ELSE 'No' END AS length_diff,
        --CASE WHEN height!=SAFE_CAST(height_in_cm AS FLOAT64) THEN 'Si' ELSE 'No' END AS height_diff,
        --CASE WHEN weight!=SAFE_CAST(weight_in_kg AS FLOAT64) THEN 'Si' ELSE 'No' END AS weight_diff,
        variation,
        attributes,
        price,
        --price_normal,
        --price_internet,
        --price_cmr,
        --price_event,
        --price_event_start_date,
        --price_event_end_date,
        --codigo_de_barras,
      FROM (
        SELECT DISTINCT
          a.fk_seller,
          sellerId,
          sellerName,
          CASE WHEN REGEXP_CONTAINS(UPPER(a.sku_seller),'_DELETED') THEN REGEXP_EXTRACT(UPPER(a.sku_seller), r'^(.*?)_DELETED') 
              ELSE UPPER(a.sku_seller) END
              AS sku_seller,
          a.name AS product_description,
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_REPLACE(
                          REGEXP_REPLACE(
                          REGEXP_REPLACE(
                            REGEXP_REPLACE(
                              REGEXP_REPLACE(
                                REGEXP_REPLACE(
                                  REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                      LOWER(
                                        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(
                                          a.name
                                          )
                                        ), 
                                      r'[^a-zA-Z0-9. ]', ''),
                                    r'\b(ml[a-z]{0,2}\b)|([0-9]{1,2})(m\b)', r'\1ml'),
                                  r'cc\b|cm3\b| ml\b| mlml\b', 'ml'),
                                r'\b(lt[a-z]{0,1}\b)|([0-9]{1,2})(lt[a-z]{0,1}\b)', r'\1l'),
                              r'\blitros\b|lt[a-z]{0,1}| litros\b| lt[a-z]{0,1}\b', 'l'),
                            r'\b(onz[a-z]{0,1}\b)|([0-9]{1,2})(onz[a-z]{0,1}\b)', r'\1oz'),
                            r' oz\b', 'oz'),
                          r'\b(kg[a-z]{0,1}\b)|([0-9]{1,2})(kg[a-z]{0,1}\b)', r'\1kg'),
                        r'kgkg|kilos|kilogramos| kg\b| kilos\b| kilogramos\b', 'kg'),
                      r'\b(gr[a-z]{0,1}\b)|([0-9]{1,2})(gr[a-z]{0,1}\b)', r'\1g'),
                    r'pulgadas|pulgada|pulg.|\"| pulgadas', 'in'),
                  r'centimetros|cms| centimetros\b| cms\b', 'cm'),
                r'\bmetros\b|\bmetro\b|\bmts\b', 'm'),
            r'television|televiso[a-z]{0,2}|tv', 'tv'),
          r'([0-9]{1,4})\s([cgiklmo]{1}\b)', r'\1\2') AS product_name, --cgiklmo --lg
          REGEXP_REPLACE(d.name, r'[^a-zA-Z0-9. ]', '') AS brand_name,
          CASE 
            WHEN REGEXP_CONTAINS(UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', '')),'_DELETED') 
            THEN REGEXP_EXTRACT(UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', '')), r'^(.*?)_DELETED') 
            ELSE UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', ''))
            END AS model,
          --description,
          country,
          i.src_id AS main_sku,
          a.sku AS shop_sku,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(e.name) AS primary_category,
          e.global_identifier,
          CASE WHEN LENGTH(e.global_identifier) >= 3 THEN SUBSTR(e.global_identifier, 1, 3)
              ELSE e.global_identifier END AS G1,
          CASE WHEN LENGTH(e.global_identifier) >= 5 THEN SUBSTR(e.global_identifier, 1, 5)
              WHEN LENGTH(e.global_identifier) >= 3 THEN SUBSTR(e.global_identifier, 1, 3)
              ELSE e.global_identifier END AS G2,
          CASE WHEN LENGTH(e.global_identifier) >= 7 THEN SUBSTR(e.global_identifier, 1, 7)
              WHEN LENGTH(e.global_identifier) >= 5 THEN SUBSTR(e.global_identifier, 1, 5)
              WHEN LENGTH(e.global_identifier) >= 3 THEN SUBSTR(e.global_identifier, 1, 3)
              ELSE e.global_identifier END AS G3,
          CASE WHEN LENGTH(e.global_identifier) >= 9 THEN SUBSTR(e.global_identifier, 1, 9)
              WHEN LENGTH(e.global_identifier) >= 7 THEN SUBSTR(e.global_identifier, 1, 7)
              WHEN LENGTH(e.global_identifier) >= 5 THEN SUBSTR(e.global_identifier, 1, 5)
              WHEN LENGTH(e.global_identifier) >= 3 THEN SUBSTR(e.global_identifier, 1, 3)
              ELSE e.global_identifier END AS G4,
          -- ESPECIFICACIONES DEL PRODUCTO
          -- GARANTÍA Y ENVÍO
          --origin_bu,
          JSON_EXTRACT_SCALAR(attributes, '$.22') AS condicion_del_producto_id,
          JSON_EXTRACT_SCALAR(attributes, '$.10206') AS condiciones_de_garantia_del_vendedor,
          JSON_EXTRACT_SCALAR(attributes, '$.49') AS detalle_de_la_condicion,
          JSON_EXTRACT_SCALAR(attributes, '$.35') AS condiciones_de_garantia,
          JSON_EXTRACT_SCALAR(attributes, '$.9') AS garantia_del_proveedor_id,
          JSON_EXTRACT_SCALAR(attributes, '$.19') AS contenido_del_paquete,
            --a.fk_catalog_product_set,
            --b.id_catalog_image_set,
            --c.fk_catalog_image_set,
            --b.variant,
          image_meta,
          url,
          -- Ancho
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                JSON_EXTRACT_SCALAR(attributes, '$.48'),
                JSON_EXTRACT_SCALAR(attributes, '$.55'),
                JSON_EXTRACT_SCALAR(attributes, '$.60'),
                JSON_EXTRACT_SCALAR(attributes, '$.72'),
                JSON_EXTRACT_SCALAR(attributes, '$.344'),
                JSON_EXTRACT_SCALAR(attributes, '$.375'),
                JSON_EXTRACT_SCALAR(attributes, '$.514'),
                JSON_EXTRACT_SCALAR(attributes, '$.548'),
                JSON_EXTRACT_SCALAR(attributes, '$.730'),
                JSON_EXTRACT_SCALAR(attributes, '$.762'),
                JSON_EXTRACT_SCALAR(attributes, '$.938'),
                JSON_EXTRACT_SCALAR(attributes, '$.962'),
                JSON_EXTRACT_SCALAR(attributes, '$.1082'),
                JSON_EXTRACT_SCALAR(attributes, '$.1210'),
                JSON_EXTRACT_SCALAR(attributes, '$.1295'),
                JSON_EXTRACT_SCALAR(attributes, '$.1314'),
                JSON_EXTRACT_SCALAR(attributes, '$.1333'),
                JSON_EXTRACT_SCALAR(attributes, '$.1405'),
                JSON_EXTRACT_SCALAR(attributes, '$.1431'),
                JSON_EXTRACT_SCALAR(attributes, '$.1495'),
                JSON_EXTRACT_SCALAR(attributes, '$.1719'),
                JSON_EXTRACT_SCALAR(attributes, '$.10794')
              )
            ,',','.')
          AS FLOAT64) AS width,
          -- Largo
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                JSON_EXTRACT_SCALAR(attributes, '$.21'),
                JSON_EXTRACT_SCALAR(attributes, '$.28'),
                JSON_EXTRACT_SCALAR(attributes, '$.33'),
                JSON_EXTRACT_SCALAR(attributes, '$.90'),
                JSON_EXTRACT_SCALAR(attributes, '$.117'),
                JSON_EXTRACT_SCALAR(attributes, '$.365'),
                JSON_EXTRACT_SCALAR(attributes, '$.512'),
                JSON_EXTRACT_SCALAR(attributes, '$.527'),
                JSON_EXTRACT_SCALAR(attributes, '$.713'),
                JSON_EXTRACT_SCALAR(attributes, '$.779'),
                JSON_EXTRACT_SCALAR(attributes, '$.937'),
                JSON_EXTRACT_SCALAR(attributes, '$.1002'),
                JSON_EXTRACT_SCALAR(attributes, '$.1135'),
                JSON_EXTRACT_SCALAR(attributes, '$.1208'),
                JSON_EXTRACT_SCALAR(attributes, '$.1315'),
                JSON_EXTRACT_SCALAR(attributes, '$.1340'),
                JSON_EXTRACT_SCALAR(attributes, '$.1387'),
                JSON_EXTRACT_SCALAR(attributes, '$.1436'),
                JSON_EXTRACT_SCALAR(attributes, '$.1514'),
                JSON_EXTRACT_SCALAR(attributes, '$.1729'),
                JSON_EXTRACT_SCALAR(attributes, '$.10793'),
                JSON_EXTRACT_SCALAR(attributes, '$.11186')
              )
            ,',','.')
          AS FLOAT64) AS length,
          -- Alto
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                JSON_EXTRACT_SCALAR(attributes, '$.34'),
                JSON_EXTRACT_SCALAR(attributes, '$.41'),
                JSON_EXTRACT_SCALAR(attributes, '$.47'),
                JSON_EXTRACT_SCALAR(attributes, '$.81'),
                JSON_EXTRACT_SCALAR(attributes, '$.311'),
                JSON_EXTRACT_SCALAR(attributes, '$.360'),
                JSON_EXTRACT_SCALAR(attributes, '$.490'),
                JSON_EXTRACT_SCALAR(attributes, '$.566'),
                JSON_EXTRACT_SCALAR(attributes, '$.740'),
                JSON_EXTRACT_SCALAR(attributes, '$.768'),
                JSON_EXTRACT_SCALAR(attributes, '$.848'),
                JSON_EXTRACT_SCALAR(attributes, '$.977'),
                JSON_EXTRACT_SCALAR(attributes, '$.1050'),
                JSON_EXTRACT_SCALAR(attributes, '$.1300'),
                JSON_EXTRACT_SCALAR(attributes, '$.1341'),
                JSON_EXTRACT_SCALAR(attributes, '$.1377'),
                JSON_EXTRACT_SCALAR(attributes, '$.1458'),
                JSON_EXTRACT_SCALAR(attributes, '$.1516'),
                JSON_EXTRACT_SCALAR(attributes, '$.1732'),
                JSON_EXTRACT_SCALAR(attributes, '$.10779'),
                JSON_EXTRACT_SCALAR(attributes, '$.10795')
              )
            ,',','.')
          AS FLOAT64) AS height,
          -- Peso
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                JSON_EXTRACT_SCALAR(attributes, '$.8'),
                JSON_EXTRACT_SCALAR(attributes, '$.11'),
                JSON_EXTRACT_SCALAR(attributes, '$.14'),
                JSON_EXTRACT_SCALAR(attributes, '$.278'),
                JSON_EXTRACT_SCALAR(attributes, '$.392'),
                JSON_EXTRACT_SCALAR(attributes, '$.447'),
                JSON_EXTRACT_SCALAR(attributes, '$.606'),
                JSON_EXTRACT_SCALAR(attributes, '$.908'),
                JSON_EXTRACT_SCALAR(attributes, '$.1108'),
                JSON_EXTRACT_SCALAR(attributes, '$.1163'),
                JSON_EXTRACT_SCALAR(attributes, '$.1306'),
                JSON_EXTRACT_SCALAR(attributes, '$.33438')
              )
            ,',','.')
          AS FLOAT64) AS weight,
          -- Tallas
          f.size,
          h.previous_item_size,
          h.new_item_size,
          -- Catalogo logistico
          CAST(f.length_in_cm AS FLOAT64) AS length_in_cm,
          CAST(f.width_in_cm AS FLOAT64) AS width_in_cm,
          CAST(f.height_in_cm AS FLOAT64) AS height_in_cm,
          CAST(f.weight_in_kg AS FLOAT64) AS weight_in_kg,
          -- Numero de piezas
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                JSON_EXTRACT_SCALAR(attributes, '$.245'),
                JSON_EXTRACT_SCALAR(attributes, '$.674'),
                JSON_EXTRACT_SCALAR(attributes, '$.761'),
                JSON_EXTRACT_SCALAR(attributes, '$.818'),
                JSON_EXTRACT_SCALAR(attributes, '$.1017'),
                JSON_EXTRACT_SCALAR(attributes, '$.1144'),
                JSON_EXTRACT_SCALAR(attributes, '$.1235'),
                JSON_EXTRACT_SCALAR(attributes, '$.34829'),
                JSON_EXTRACT_SCALAR(attributes, '$.39141')
              )
            ,',','.')
          AS FLOAT64) AS numero_de_piezas,
          -- Cantidad de paquetes
          SAFE_CAST(
            REGEXP_REPLACE(
              COALESCE(
                --JSON_EXTRACT_SCALAR(attributes, '$.20'), TEST
                JSON_EXTRACT_SCALAR(attributes, '$.10210'),
                JSON_EXTRACT_SCALAR(attributes, '$.10585'),
                JSON_EXTRACT_SCALAR(attributes, '$.10587'),
                JSON_EXTRACT_SCALAR(attributes, '$.10588'),
                JSON_EXTRACT_SCALAR(attributes, '$.10590'),
                JSON_EXTRACT_SCALAR(attributes, '$.10592'),
                JSON_EXTRACT_SCALAR(attributes, '$.10596'),
                JSON_EXTRACT_SCALAR(attributes, '$.10604'),
                JSON_EXTRACT_SCALAR(attributes, '$.10607'),
                JSON_EXTRACT_SCALAR(attributes, '$.11122'),
                JSON_EXTRACT_SCALAR(attributes, '$.11124'),
                JSON_EXTRACT_SCALAR(attributes, '$.11126'),
                JSON_EXTRACT_SCALAR(attributes, '$.22760'),
                --JSON_EXTRACT_SCALAR(attributes, '$.23338'), TEST
                JSON_EXTRACT_SCALAR(attributes, '$.31239')
              )
            ,',','.')
          AS FLOAT64) AS cantidad_de_paquetes,
          -- Colores
          COALESCE(
            JSON_EXTRACT_SCALAR(variation, '$.94'),
            JSON_EXTRACT_SCALAR(variation, '$.275'),
            JSON_EXTRACT_SCALAR(variation, '$.366'),
            JSON_EXTRACT_SCALAR(variation, '$.393'),
            JSON_EXTRACT_SCALAR(variation, '$.468'),
            JSON_EXTRACT_SCALAR(variation, '$.624'),
            JSON_EXTRACT_SCALAR(variation, '$.742'),
            JSON_EXTRACT_SCALAR(variation, '$.772'),
            JSON_EXTRACT_SCALAR(variation, '$.795'),
            JSON_EXTRACT_SCALAR(variation, '$.945'),
            JSON_EXTRACT_SCALAR(variation, '$.995'),
            JSON_EXTRACT_SCALAR(variation, '$.1076'),
            JSON_EXTRACT_SCALAR(variation, '$.1269'),
            JSON_EXTRACT_SCALAR(variation, '$.1283'),
            JSON_EXTRACT_SCALAR(variation, '$.1317'),
            JSON_EXTRACT_SCALAR(variation, '$.1347'),
            JSON_EXTRACT_SCALAR(variation, '$.1362'),
            JSON_EXTRACT_SCALAR(variation, '$.1450'),
            JSON_EXTRACT_SCALAR(variation, '$.1492'),
            JSON_EXTRACT_SCALAR(variation, '$.1532'),
            JSON_EXTRACT_SCALAR(variation, '$.1717')
          ) AS color,
          COALESCE(
            JSON_EXTRACT_SCALAR(variation, '$.62'),
            JSON_EXTRACT_SCALAR(variation, '$.210'),
            JSON_EXTRACT_SCALAR(variation, '$.381'),
            JSON_EXTRACT_SCALAR(variation, '$.410'),
            JSON_EXTRACT_SCALAR(variation, '$.517'),
            JSON_EXTRACT_SCALAR(variation, '$.587'),
            JSON_EXTRACT_SCALAR(variation, '$.687'),
            JSON_EXTRACT_SCALAR(variation, '$.765'),
            JSON_EXTRACT_SCALAR(variation, '$.789'),
            JSON_EXTRACT_SCALAR(variation, '$.815'),
            JSON_EXTRACT_SCALAR(variation, '$.976'),
            JSON_EXTRACT_SCALAR(variation, '$.1139'),
            JSON_EXTRACT_SCALAR(variation, '$.1165'),
            JSON_EXTRACT_SCALAR(variation, '$.1291'),
            JSON_EXTRACT_SCALAR(variation, '$.1310'),
            JSON_EXTRACT_SCALAR(variation, '$.1339'),
            JSON_EXTRACT_SCALAR(variation, '$.1399'),
            JSON_EXTRACT_SCALAR(variation, '$.1480'),
            JSON_EXTRACT_SCALAR(variation, '$.1509'),
            JSON_EXTRACT_SCALAR(variation, '$.1627'),
            JSON_EXTRACT_SCALAR(variation, '$.1734')
          ) AS color_basico, --789
          COALESCE(
            JSON_EXTRACT_SCALAR(variation, '$.468'),
            JSON_EXTRACT_SCALAR(variation, '$.795')
          ) AS color_secundario, --795
          COALESCE(--JSON_EXTRACT_SCALAR(variation, '$.399'),
            JSON_EXTRACT_SCALAR(variation, '$.502'),
            JSON_EXTRACT_SCALAR(variation, '$.794')
          ) AS talla, --794
          variation,
          attributes,
          --product_identifier AS codigo_de_barras,
          --variation,
          --price,
          --volumetric_weight,
          approval_status,
          status,
          created_at,
          updated_at,
        FROM (
          SELECT DISTINCT
            fk_seller,
            sku_seller,
            name,
            fk_catalog_product_set,
            fk_catalog_brand,
            id_catalog_product,
            sku,
            primary_category,
            product_identifier,
            variation,
            approval_status,
            status,
            created_at,
            updated_at,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product`
        ) a
        LEFT JOIN (
          SELECT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY sellerId ORDER BY sellerName DESC) AS row,
            FROM (
              SELECT
                id_seller AS fk_seller,
                src_id AS sellerId,
                UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name), r'[^a-zA-Z0-9. ]', '')) AS sellerName,
                JSON_EXTRACT_SCALAR(tmp_data, '$.country') AS country,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
            )
          )
          WHERE row=1
        ) b ON b.fk_seller=a.fk_seller
        LEFT JOIN (
          SELECT DISTINCT
            fk_catalog_product_set,
            --LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(description)) AS description,
            attributes, --JSON
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product_data`
        ) c ON c.fk_catalog_product_set=a.fk_catalog_product_set
        LEFT JOIN (
          SELECT DISTINCT
            id_catalog_brand,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name) AS name,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_brand` 
        ) d ON d.id_catalog_brand=a.fk_catalog_brand
        LEFT JOIN (
          SELECT
            id_catalog_category,
            global_identifier,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name) AS name
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_category` 
        ) e ON e.id_catalog_category=a.primary_category
        LEFT JOIN (
          -- LOGISTIC CATALOG
          SELECT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY sku ORDER BY event_tmst DESC) AS row,
            FROM (
              SELECT 
                CURRENT_DATE('America/Lima') AS event_tmst,
                wms_id AS sku,
                item_size AS size,
                length_in_cm,
                height_in_cm,
                width_in_cm,
                weight_in_kg
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_scha_corp_logistic_catalog`
              UNION ALL
              SELECT 
                DATE(last_updated_dttm_utc,'America/Lima') AS event_tmst,
                wms_id AS sku,
                item_size AS size,
                length_in_cm,
                height_in_cm,
                width_in_cm,
                weight_in_kg
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_scha_corp_logistic_catalog_historico` 
              UNION ALL
              SELECT DISTINCT
                DATE(COALESCE(event_attr.event_tmst,last_updated_dttm)) AS event_tmst,
                item_info.offering_id AS sku,
                presentation_footprints.item_size AS size,
                presentation_footprints.length_in_cm,
                presentation_footprints.width_in_cm,
                presentation_footprints.height_in_cm,
                presentation_footprints.weight_in_kg,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dfl_prod_trf_corp_corp_dfl_prod.svw_btd_scha_corp_logistic_catalog` a,
              UNNEST(item_info) item_info,
              UNNEST(presentation_footprints) presentation_footprints
            )
          )
          WHERE row=1
        ) f ON f.sku=a.sku
        LEFT JOIN (
          --SELECT DISTINCT
            --a.fk_catalog_product_set,
            --b.id_catalog_image_set,
            --c.fk_catalog_image_set,
            --b.variant,
            --c.image_meta,
            --c.url,
          --FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` a
          --LEFT JOIN (
          --  SELECT DISTINCT
          --    id_catalog_image_set,			
          --    fk_catalog_product_set,			
          --    --variant
          --  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_image_set`
          --) b ON a.fk_catalog_product_set=b.fk_catalog_product_set
          --LEFT JOIN (
            SELECT --DISTINCT
              --fk_catalog_image_set,
              fk_catalog_product_set,
              --COALESCE(display_url,import_url) AS url,
              --image_meta,
              MAX(image_meta) AS image_meta, -- Agrupación para evitar duplicidades (se complenta con b.variant)
              MAX(COALESCE(display_url,import_url)) AS url,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product_image`
            --WHERE deleted=1
            GROUP BY 1--,2
          --) c ON a.fk_catalog_product_set=c.fk_catalog_product_set
        ) g ON g.fk_catalog_product_set=a.fk_catalog_product_set
        -- Variables logísticas
        LEFT JOIN (
          SELECT * FROM (
            SELECT
              wms_id,
              previous_item_size,
              new_item_size,
              ROW_NUMBER() OVER (PARTITION BY wms_id ORDER BY last_size_update DESC) AS row,
            FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.RP_SX_SKU_Size_Corrections_BT` 
            /**
            UNION ALL
            SELECT
              wms_id,
              previous_item_size,
              new_item_size,
              ROW_NUMBER() OVER (PARTITION BY wms_id ORDER BY last_size_update DESC) AS row,
            FROM `bi-fcom-drmb-local-pe-sbx.VL_PE.RP_LC_3P_SKU_Verified` 
            **/
          )
          WHERE row=1
        ) h ON h.wms_id=a.sku
        -- SKUs padres
        LEFT JOIN (
          SELECT DISTINCT
            a.fk_catalog_product_set,
            sku,
            b.src_id
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` a
          LEFT JOIN (
            SELECT DISTINCT
              id_catalog_product_set AS fk_catalog_product_set,
              src_id,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product_set` 
          ) b ON a.fk_catalog_product_set=b.fk_catalog_product_set
        ) i ON i.sku=a.sku
      ) a
      LEFT JOIN (
        SELECT
          G1,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(MAX(N1)) AS N1
        FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
        GROUP BY 1
      ) g1 ON g1.G1=a.G1
      LEFT JOIN (
        SELECT 
          G2,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(MAX(N2)) AS N2
        FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
        GROUP BY 1
      ) g2 ON g2.G2=a.G2
      LEFT JOIN (
        SELECT
          G3,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(MAX(N3)) AS N3
        FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
        GROUP BY 1
      ) g3 ON g3.G3=a.G3
      LEFT JOIN (
        SELECT
          G4,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(MAX(N4)) AS N4
        FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
        GROUP BY 1
      ) g4 ON g4.G4=a.G4
      LEFT JOIN (
        SELECT
          *,
          CASE WHEN event=1 THEN price_event 
              WHEN internet=1 THEN price_internet
              --WHEN cmr=1 THEN price_cmr <-- Este es un precio especial solo para los que tienen CMR
              ELSE price_normal END AS price --(current_price)
        FROM (
          SELECT
            *,
            CASE WHEN price_internet>0 THEN 1 ELSE 0 END AS internet,
            CASE WHEN price_cmr>0 THEN 1 ELSE 0 END AS cmr,
            CASE WHEN price_event_end_date>=CURRENT_DATE('America/Lima') THEN 1 ELSE 0 END AS event,
            ROW_NUMBER() OVER (PARTITION BY shop_sku ORDER BY price_event_end_date DESC) AS row, 
          FROM (
            SELECT DISTINCT
              --PRODUCT_ID AS shop_sku,
              SKU_ID AS shop_sku,
              IFNULL(PRICE_NORMAL_DEFAULT,0) AS price_normal,
              IFNULL(PRICE_INTERNET_DEFAULT,0) AS price_internet,
              IFNULL(PRICE_CMR_DEFAULT,0) AS price_cmr,
              IFNULL(PRICE_EVENT_EVENT,0) AS price_event,
              DATE(PRICE_EVENT_START_DATE, 'America/Lima') AS price_event_start_date,
              DATE(PRICE_EVENT_END_DATE, 'America/Lima') AS price_event_end_date,
            FROM `bi-fcom-drmb-local-pe-sbx.catalogo.catalogo_fcom_bi_local_peru`
          )
        )
        WHERE row=1
      ) b ON b.shop_sku=a.shop_sku
      -- Condiciones de producto
      LEFT JOIN (
        SELECT DISTINCT
          fk_catalog_attribute,
          CAST(id_catalog_attribute_option AS STRING) AS id_catalog_attribute_option,
          name,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_attribute_option`
        WHERE fk_catalog_attribute=22
      ) f ON f.id_catalog_attribute_option=a.condicion_del_producto_id
      -- Condiciones de garantía
      LEFT JOIN (
        SELECT DISTINCT
          fk_catalog_attribute,
          CAST(id_catalog_attribute_option AS STRING) AS id_catalog_attribute_option,
          name,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_attribute_option`
        WHERE fk_catalog_attribute=9
      ) g ON g.id_catalog_attribute_option=a.garantia_del_proveedor_id
    )
  ),
  b AS (
    SELECT
      global_identifier,
      COUNT(DISTINCT CASE WHEN size='XS3' THEN shop_sku ELSE NULL END) AS dist_xs3, 
      COUNT(DISTINCT CASE WHEN size='XS2' THEN shop_sku ELSE NULL END) AS dist_xs2, 
      COUNT(DISTINCT CASE WHEN size='XS' THEN shop_sku ELSE NULL END) AS dist_xs, 
      COUNT(DISTINCT CASE WHEN size='S' THEN shop_sku ELSE NULL END) AS dist_s, 
      COUNT(DISTINCT CASE WHEN size='M' THEN shop_sku ELSE NULL END) AS dist_m, 
      COUNT(DISTINCT CASE WHEN size='LO' THEN shop_sku ELSE NULL END) AS dist_lo, 
      COUNT(DISTINCT CASE WHEN size='L' THEN shop_sku ELSE NULL END) AS dist_l, 
      COUNT(DISTINCT CASE WHEN size='XL' THEN shop_sku ELSE NULL END) AS dist_xl, 
      COUNT(DISTINCT CASE WHEN size='XXL' THEN shop_sku ELSE NULL END) AS dist_xxl, 
      COUNT(DISTINCT CASE WHEN size='O' THEN shop_sku ELSE NULL END) AS dist_o, 
      COUNT(DISTINCT CASE WHEN size='EO' THEN shop_sku ELSE NULL END) AS dist_eo, 
    FROM a
    GROUP BY 1
  )

  SELECT
    a.*,
    b.* EXCEPT (global_identifier)
  FROM a
  LEFT JOIN b ON a.global_identifier=b.global_identifier

)
WHERE sellerId IS NOT NULL
--WHERE country='PE' --AND status='active'
--ORDER BY created_at ASC
)
