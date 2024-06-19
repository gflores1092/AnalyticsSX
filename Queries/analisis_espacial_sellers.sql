CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.analisis_espacial_sellers` AS (

  WITH
  analisis_espacial AS (
    WITH 
    h AS ( -- Todo Ok!!
      SELECT
        fk_seller_warehouse_type,
        MAX(CASE WHEN name='customercare_country' THEN value ELSE NULL END) AS country,
        MAX(CASE WHEN name='customercare_city' THEN value ELSE NULL END) AS city,
        REGEXP_EXTRACT(MAX(CASE WHEN name='facility_id' THEN value ELSE NULL END), r'-(\b[A-Z][A-Z0-9]{6}\B)') AS seller_id,
        LOWER(MAX(CASE WHEN name='warehouse_name' THEN value ELSE NULL END)) AS warehouse_name, 
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(MAX(CASE WHEN name='customercare_name' THEN value ELSE NULL END))) AS contact_name,
        MAX(CASE WHEN name='customercare_phone' THEN value ELSE NULL END) AS phone,
        MAX(CASE WHEN name='customercare_email' THEN value ELSE NULL END) AS email, 
        REGEXP_REPLACE(INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(MAX(CASE WHEN name='customercare_address1' THEN value ELSE NULL END))), r'\s+', ' ') AS address, 
        REGEXP_REPLACE(INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(MAX(CASE WHEN name='customercare_address2' THEN value ELSE NULL END))), r'\s+', ' ') AS address2, 
        MAX(CASE WHEN name='customercare_postcode' THEN value ELSE NULL END) AS postcode, 
        MAX(CASE WHEN name='customercare_start_time' THEN value ELSE NULL END) AS start_time, 
        MAX(CASE WHEN name='customercare_close_time' THEN value ELSE NULL END) AS close_time,
        MAX(CASE WHEN name='customercare_work_days' THEN value ELSE NULL END) AS work_days,
        MAX(CASE WHEN name='customercare_gps' THEN value ELSE NULL END) AS location
      FROM (
        SELECT DISTINCT 
          id_seller_warehouse,
          fk_seller_warehouse_type,
          name,
          REGEXP_REPLACE(value, r'\s+', ' ') AS value,
        FROM (
          SELECT * FROM (
            SELECT DISTINCT 
              id_seller_warehouse,
              fk_seller_warehouse_type,
              fk_field,
              value,
              updated_at,
              ROW_NUMBER() OVER (PARTITION BY fk_seller_warehouse_type, fk_field ORDER BY updated_at DESC) AS row,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller_warehouse`
            )
          WHERE row=1
        ) a
        JOIN (
          SELECT DISTINCT
            id_seller_profile_configuration_field,
            name
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller_profile_configuration_field` 
        ) b ON a.fk_field=b.id_seller_profile_configuration_field
      ) c
      GROUP BY 1
    ),
    j AS (
      SELECT DISTINCT
        h.seller_id,
        d.seller_name,
        e.name AS shipment_provider,
        CASE 
          WHEN REGEXP_CONTAINS(h.warehouse_name, 'main') OR REGEXP_CONTAINS(h.warehouse_name, 'principal') THEN 'Main warehouse'
          WHEN REGEXP_CONTAINS(h.warehouse_name, 'return') OR REGEXP_CONTAINS(h.warehouse_name, 'secund') OR REGEXP_CONTAINS(h.warehouse_name, 'retorno') OR REGEXP_CONTAINS(h.warehouse_name, 'devolu') THEN 'Return warehouse'
          ELSE h.warehouse_name END AS warehouse_name,
        h.contact_name,
        h.phone,
        h.email,
        d.active,
        h.country,
        f.region,
        f.city,
        g.name AS district,
        h.address,
        h.postcode,
        CASE WHEN i.big_ticket IS NULL THEN 0 ELSE i.big_ticket END AS big_ticket,
        h.start_time,
        h.close_time,
        h.work_days,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Mon') THEN 1 ELSE 0 END AS monday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Tue') THEN 1 ELSE 0 END AS tuesday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Wed') THEN 1 ELSE 0 END AS wednesday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Thu') THEN 1 ELSE 0 END AS thursday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Fri') THEN 1 ELSE 0 END AS friday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Sat') THEN 1 ELSE 0 END AS saturday,
        CASE WHEN REGEXP_CONTAINS(h.work_days, 'Sun') THEN 1 ELSE 0 END AS sunday,
        CAST(REGEXP_EXTRACT(h.location, r'^([-0-9.]+)') AS FLOAT64) AS latitude,
        CAST(REGEXP_EXTRACT(h.location, r'([-0-9.]+)$') AS FLOAT64) AS longitude,
        ROW_NUMBER() OVER (PARTITION BY h.seller_id ORDER BY warehouse_name) AS row,
      FROM h
      LEFT JOIN (
        SELECT
          id_seller AS fk_seller, -- llave shipment
          src_id AS seller_id,
          UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name)) AS seller_name,
          CASE WHEN delist_status=1 THEN 0 
              WHEN delist_status=0 THEN 1
              ELSE NULL END AS active,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
      ) d ON d.seller_id=h.seller_id
      LEFT JOIN (
        SELECT DISTINCT	
          x.fk_seller,		
          x.fk_shipment_provider,
          y.name,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller_shipment_provider` x
        JOIN (
          SELECT DISTINCT
            id_shipment_provider,
            name
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_shipment_provider`
        ) y ON x.fk_shipment_provider=y.id_shipment_provider
        WHERE x.is_seller_default=1
        AND x.active=1
      ) e ON e.fk_seller=d.fk_seller
      LEFT JOIN (
        SELECT DISTINCT
          CAST(id_country_city AS STRING) AS id_country_city,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name)) AS city,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(region_name)) AS region,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_country_city`
      ) f ON f.id_country_city=h.city
      LEFT JOIN (
        SELECT DISTINCT
          CAST(id_country_municipal AS STRING) AS id_country_municipal,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name)) AS name,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_country_municipal`
      ) g ON g.id_country_municipal=h.address2
      LEFT JOIN (
        SELECT
          seller_id,
          CASE WHEN SAFE_ADD(L, SAFE_ADD(XL, SAFE_ADD(XXL, SAFE_ADD(O, EO))))>0 THEN 1 ELSE 0 END AS big_ticket,
        FROM (
          SELECT
            seller_id,
            COUNT(DISTINCT CASE WHEN size='XS3' THEN shop_sku ELSE NULL END) AS XS3,
            COUNT(DISTINCT CASE WHEN size='XS2' THEN shop_sku ELSE NULL END) AS XS2,
            COUNT(DISTINCT CASE WHEN size='XS' THEN shop_sku ELSE NULL END) AS XS,
            COUNT(DISTINCT CASE WHEN size='S' THEN shop_sku ELSE NULL END) AS S,
            COUNT(DISTINCT CASE WHEN size='M' THEN shop_sku ELSE NULL END) AS M,
            COUNT(DISTINCT CASE WHEN size='LO' THEN shop_sku ELSE NULL END) AS LO,
            COUNT(DISTINCT CASE WHEN size='L' THEN shop_sku ELSE NULL END) AS L,
            COUNT(DISTINCT CASE WHEN size='XL' THEN shop_sku ELSE NULL END) AS XL,
            COUNT(DISTINCT CASE WHEN size='XXL' THEN shop_sku ELSE NULL END) AS XXL,
            COUNT(DISTINCT CASE WHEN size='O' THEN shop_sku ELSE NULL END) AS O,
            COUNT(DISTINCT CASE WHEN size='EO' THEN shop_sku ELSE NULL END) AS EO,
          FROM (
            SELECT
              seller_id,
              x.sku AS shop_sku,
              COALESCE(b.sku,JSON_EXTRACT_SCALAR(x.variation, '$.502')) AS size,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` x
            LEFT JOIN (
              SELECT * FROM (
                SELECT DISTINCT
                  id_seller,
                  short_code AS seller_id,
                  ROW_NUMBER() OVER (PARTITION BY short_code ORDER BY updated_at DESC) AS row,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
                WHERE JSON_EXTRACT_SCALAR(tmp_data, '$.country')='PE'
                )
              WHERE row=1
            ) a ON a.id_seller=x.fk_seller
            LEFT JOIN (
              SELECT DISTINCT
                sku,
                variation,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
            ) b ON b.sku=x.sku
            WHERE seller_id IS NOT NULL
            AND x.sku IS NOT NULL
          )
        GROUP BY 1--,2
        )
      ) i ON i.seller_id=h.seller_id
      WHERE h.country='PE'
    ),
    k AS (
      SELECT
        seller_id,
        CASE WHEN main_address!=return_address THEN 1 ELSE 0 END AS diff_address
      FROM (
        SELECT
          seller_id,
          MAX(CASE WHEN warehouse_name='Main warehouse' THEN address ELSE NULL END) AS main_address,
          MAX(CASE WHEN warehouse_name='Return warehouse' THEN address ELSE NULL END) AS return_address
        FROM j
        GROUP BY 1
      )
    ),
    ruc AS (
      SELECT * FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY updated_at DESC) AS row,
        FROM (
          SELECT DISTINCT
            short_code AS seller_id,
            COALESCE(business_reg_number,JSON_EXTRACT_SCALAR(tmp_data, '$.business_reg_number')) AS business_reg_number,
            COALESCE(name_company,JSON_EXTRACT_SCALAR(tmp_data, '$.name_company')) AS name_company,
            updated_at,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
        )
      )
      WHERE row=1
    ),
    coordenadas_sellers AS (
      SELECT 
        seller_id,
        CASE WHEN CHAR_LENGTH(CAST(latitude AS STRING))=20 THEN latitude/100000000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=19 THEN latitude/10000000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=18 THEN latitude/1000000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=17 THEN latitude/100000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=16 THEN latitude/10000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=15 THEN latitude/1000000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=14 THEN latitude/100000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=13 THEN latitude/10000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=12 THEN latitude/1000000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=11 THEN latitude/100000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=10 THEN latitude/10000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=9 THEN latitude/1000000
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=8 THEN latitude/100000 
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=7 THEN latitude/10000 
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=6 THEN latitude/1000 
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=5 THEN latitude/100 
            WHEN CHAR_LENGTH(CAST(latitude AS STRING))=4 THEN latitude/10 
            ELSE latitude END AS latitude,
        CASE WHEN CHAR_LENGTH(CAST(longitude AS STRING))=20 THEN longitude/100000000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=19 THEN longitude/10000000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=18 THEN longitude/1000000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=17 THEN longitude/100000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=16 THEN longitude/10000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=15 THEN longitude/1000000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=14 THEN longitude/100000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=13 THEN longitude/10000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=12 THEN longitude/1000000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=11 THEN longitude/100000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=10 THEN longitude/10000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=9 THEN longitude/1000000
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=8 THEN longitude/100000 
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=7 THEN longitude/10000 
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=6 THEN longitude/1000 
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=5 THEN longitude/100 
            WHEN CHAR_LENGTH(CAST(longitude AS STRING))=4 THEN longitude/10 
            ELSE longitude END AS longitude,
        REGEXP_REPLACE(address, r'\s+', ' ') AS address,
        CASE WHEN district=' Lurigancho' THEN 'Lurigancho'
            WHEN district='Cercado De Lima' THEN 'Lima'
            WHEN district='Jsus Maria' THEN 'Jesus Maria'
            ELSE district END AS district,
        phone,
        shipment_provider,
        macronodo,
        fecha_actualizacion_direccion 
      FROM (
        SELECT
          CODIGO AS seller_id,
          SAFE_CAST(REGEXP_REPLACE(LATITUD, r'[\.,]', '') AS INT64) AS latitude,
          SAFE_CAST(REGEXP_REPLACE(LONGITUD, r'[\.,]', '') AS INT64) AS longitude,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(DIRECCION)) AS address,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(DISTRITO)) AS district,
          TELEFONO AS phone,
          CASE WHEN LOWER(CARRIER)='ownfleet' THEN 'ibis'
              WHEN LOWER(CARRIER)='dropoff pickit' THEN 'Dropoff - Pickit'
              WHEN LOWER(CARRIER)='drop off plazatec' THEN 'Dropoff - Plaza Tec'
              WHEN LOWER(CARRIER)='despacho directo' THEN 'ibisdirecto'
              WHEN LOWER(CARRIER)='ownfleet- mar y vier' THEN 'ibis - mar-vier'
              ELSE LOWER(CARRIER)
              END AS shipment_provider,
          MACRONODO AS macronodo,
          FECHA_DE_ACTUALIZACION_DE_DIRECCION AS fecha_actualizacion_direccion,
        FROM (
          SELECT
            a.CODIGO,
            COALESCE(a.LATITUD, b.LATITUD) AS LATITUD,
            COALESCE(a.LONGITUD, b.LONGITUD) AS LONGITUD,
            DIRECCION,
            DISTRITO,
            TELEFONO,
            CARRIER,
            MACRONODO,
            FECHA_DE_ACTUALIZACION_DE_DIRECCION
          FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.coordenadas_sellers` a
          LEFT JOIN (
            SELECT 
              CODIGO_SELLER AS CODIGO,
              LATITUD,
              LONGITUD
            FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.data_dropoff_vl` 
          ) b ON b.CODIGO=a.CODIGO
        )
      )
    ),
    horarios_especiales AS (
      SELECT
        seller_id,
        --Horario_Especial,
        REGEXP_REPLACE(REGEXP_REPLACE(horario_especial, '0:00 AM','0 AM'), '0:00 PM', '0 PM') AS horario_especial,
        motivo
      FROM (
        SELECT
          Seller_ID AS seller_id,
          --Horario_Especial,
          CASE WHEN REGEXP_CONTAINS(Horario_Especial, r'\d{1,2}:\d{2} AM') THEN Horario_Especial
              WHEN REGEXP_CONTAINS(Horario_Especial, r'\d{1,2}:\d{2} PM') THEN Horario_Especial
              ELSE CONCAT(Horario_Especial, ' AM')
              END AS horario_especial,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Motivo) AS motivo,
        FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.horarios_especiales`
      )
    ),
    -- Dropoff
    dropoff AS (
      SELECT 
        seller_id,
        CASE WHEN REGEXP_CONTAINS(punto_dropoff, 'PLAZA TEC') THEN 'Dropoff - Plaza Tec'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'LINCE') THEN 'Dropoff - Lince'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'ATE') THEN 'Pickit 1 - ATE'
            WHEN punto_dropoff='PUNTO 1 BRENA' OR REGEXP_CONTAINS(punto_dropoff, 'PICKIT 1 BRENA') THEN 'Pickit 1 - BRENA'
            WHEN punto_dropoff='PUNTO 2 BRENA' THEN 'Pickit 2 - BRENA'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'COMAS') THEN 'Pickit 1 - COMAS'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'LA MOLINA') THEN 'Pickit 1 - LA MOLINA'
            WHEN punto_dropoff='PUNTO 1 LA VICTORIA' OR REGEXP_CONTAINS(punto_dropoff, 'PUNTO 1 LA VICTORIA|PICKIT 1 LA VICTORIA') THEN 'Pickit 1 - LA VICTORIA'
            WHEN punto_dropoff='PUNTO 2 LA VICTORIA' OR REGEXP_CONTAINS(punto_dropoff, 'PUNTO 2 LA VICTORIA|PICKIT 2 LA VICTORIA') THEN 'Pickit 2 - LA VICTORIA'
            WHEN punto_dropoff='PUNTO 3 LA VICTORIA' OR REGEXP_CONTAINS(punto_dropoff, 'PUNTO 3 LA VICTORIA|PICKIT 3 LA VICTORIA') THEN 'Pickit 3 - LA VICTORIA'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'LOS OLIVOS') THEN 'Pickit 1 - LOS OLIVOS'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'MDM') THEN 'Pickit 1 - MAGDALENA DEL MAR'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'MIRAFLORES') THEN 'Pickit 1 - MIRAFLORES'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SAN BORJA') THEN 'Pickit 1 - SAN BORJA'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SAN MIGUEL') THEN 'Pickit 1 - SAN MIGUEL'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SANTA ANITA') THEN 'Pickit 1 - SANTA ANITA'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SJL') AND REGEXP_CONTAINS(LOWER(nombre_dropoff),'yaq') THEN 'Pickit 1 - SJL'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SJL') AND REGEXP_CONTAINS(LOWER(nombre_dropoff),'dhl') THEN 'Pickit 2 - SJL'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'SJM') THEN 'Pickit 1 - SJM'
            WHEN punto_dropoff='PUNTO 1 SURCO' OR punto_dropoff='PICKIT 1 SURCO' THEN 'Pickit 1 - SURCO'
            WHEN punto_dropoff='PUNTO 2 SURCO' OR punto_dropoff='PICKIT 2 SURCO' THEN 'Pickit 2 - SURCO'
            WHEN punto_dropoff='PUNTO 3 SURCO' THEN 'Pickit 3 - SURCO'
            WHEN REGEXP_CONTAINS(punto_dropoff, 'LIMA') THEN 'Pickit 2 - CERCADO DE LIMA'
            ELSE punto_dropoff END AS punto_dropoff,
        nombre_dropoff,
        solicitud_dropoff,
      FROM (
        SELECT * FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY fecha_de_inicio DESC) AS row,
          FROM (
            SELECT * FROM (
              SELECT
                a.ID AS seller_id,
                UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(a.Distrito_modalidad), r'\s{2,}', ' ')) AS punto_dropoff,
                `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(a.Tienda) AS nombre_dropoff,
                SAFE_CAST(PARSE_DATE('%d/%m/%Y', a.Fecha_de_inicio) AS DATE) AS fecha_de_inicio,
                INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.Solicitado)) AS solicitud_dropoff,
                SAFE_CAST(PARSE_DATE('%d/%m/%Y', b.Fecha_de_retiro) AS DATE) AS fecha_de_retiro,
                INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.Motivos)) AS solicitud_retiro_dropoff,
              FROM `bi-fcom-drmb-local-pe-sbx.Drowzee_Dropoff.sellers_dropoff` a
              LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Drowzee_Dropoff.sellers_retirados_dropoff` b ON a.ID=b.sellerID
              WHERE ID IS NOT NULL
            )
            WHERE fecha_de_retiro IS NULL
          )
        )
        WHERE row=1
      )
    ),
    -- Dropoff (Coordenadas)
    dropoff_coord AS (
      SELECT
        CASE WHEN REGEXP_CONTAINS(punto_dropoff,'PLAZA TEC') THEN 'Dropoff - Plaza Tec'
            WHEN REGEXP_CONTAINS(punto_dropoff,'LINCE') THEN 'Dropoff - Lince'
            WHEN punto_dropoff='PUNTO 1 ATE' THEN 'Pickit 1 - ATE'
            WHEN punto_dropoff='PUNTO 1 BRENA' THEN 'Pickit 1 - BRENA'
            WHEN punto_dropoff='PUNTO 2 BRENA' THEN 'Pickit 2 - BRENA'
            WHEN punto_dropoff='PUNTO 1 CALLAO' THEN 'Pickit 1 - CALLAO'
            WHEN punto_dropoff='PUNTO 1 COMAS' THEN 'Pickit 1 - COMAS'
            WHEN punto_dropoff='PUNTO 1 LA MOLINA' THEN 'Pickit 1 - LA MOLINA'
            WHEN punto_dropoff='PUNTO 1 LA VICTORIA' THEN 'Pickit 1 - LA VICTORIA'
            WHEN punto_dropoff='PUNTO 2 LA VICTORIA' THEN 'Pickit 2 - LA VICTORIA'
            WHEN punto_dropoff='PUNTO 3 LA VICTORIA' THEN 'Pickit 3 - LA VICTORIA'
            WHEN punto_dropoff='PUNTO 1 LOS OLIVOS' THEN 'Pickit 1 - LOS OLIVOS'
            WHEN punto_dropoff='PUNTO 1 MDM' THEN 'Pickit 1 - MAGDALENA DEL MAR'
            WHEN punto_dropoff='PUNTO 1 MIRAFLORES' THEN 'Pickit 1 - MIRAFLORES'
            WHEN punto_dropoff='PUNTO 1 SAN BORJA' THEN 'Pickit 1 - SAN BORJA'
            WHEN punto_dropoff='PUNTO 1 SAN MIGUEL' THEN 'Pickit 1 - SAN MIGUEL'
            WHEN punto_dropoff='PUNTO 1 SANTA ANITA' THEN 'Pickit 1 - SANTA ANITA'
            WHEN punto_dropoff='PUNTO 1 SJL' AND REGEXP_CONTAINS(LOWER(nombre_dropoff),'yaq') THEN 'Pickit 1 - SJL'
            WHEN punto_dropoff='PUNTO 2 SJL' AND REGEXP_CONTAINS(LOWER(nombre_dropoff),'dhl') THEN 'Pickit 2 - SJL'
            WHEN punto_dropoff='PUNTO 1 SJM' THEN 'Pickit 1 - SJM'
            WHEN punto_dropoff='PUNTO 1 SURCO' OR punto_dropoff='PICKIT 1 SURCO' THEN 'Pickit 1 - SURCO'
            WHEN punto_dropoff='PUNTO 2 SURCO' OR punto_dropoff='PICKIT 2 SURCO' THEN 'Pickit 2 - SURCO'
            WHEN punto_dropoff='PUNTO 3 SURCO' OR punto_dropoff='PICKIT 3 SURCO' THEN 'Pickit 3 - SURCO'
            WHEN punto_dropoff='PUNTO 2 CERCADO DE LIMA' THEN 'Pickit 2 - CERCADO DE LIMA'
            ELSE punto_dropoff END AS punto_dropoff,
        direccion_dropoff,
        nombre_dropoff,
        horario_dropoff,
        frecuencia_dropoff,
        CAST(REGEXP_EXTRACT(location_dropoff, r'^([-0-9.]+)') AS FLOAT64) AS latitude_dropoff,
        CAST(REGEXP_EXTRACT(location_dropoff, r'([-0-9.]+)$') AS FLOAT64) AS longitude_dropoff,
        location_dropoff
      FROM (
        SELECT 
          UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Dropoff), r'\s{2,}', ' ')) AS punto_dropoff,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Direccion_de_pickup) AS direccion_dropoff,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Nombre_del_punto) AS nombre_dropoff,
          Horario_de_atencion AS horario_dropoff,
          Frecuencia AS frecuencia_dropoff,
          COORDENADAS AS location_dropoff,
        FROM `bi-fcom-drmb-local-pe-sbx.Drowzee_Dropoff.puntos_dropoff` 
      )
    ),
    -- Atención Sábados
    atencion_sabados AS (
      SELECT 
        ID_SELLER AS seller_id,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(STATUS) AS status_sabado,
      FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.sellers_atencion_sabados`
    ),
    -- Comercial
    kam AS (
      SELECT 
        id_seller AS seller_id,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(PM) AS PM,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(CT) AS CT,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Division) AS division,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(cat) AS category
      FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.comerciales_kam`
    ),
    final AS (
    SELECT
      business_reg_number AS ruc,
      name_company AS razon_social,
      j.seller_id,
      seller_name,
      CASE WHEN d.punto_dropoff IS NOT NULL THEN d.punto_dropoff
          WHEN c.shipment_provider IS NOT NULL AND d.punto_dropoff IS NULL THEN c.shipment_provider 
          ELSE j.shipment_provider END AS shipment_provider,
      CASE WHEN warehouse_name!='Main warehouse' AND j.row=1 THEN 'Main warehouse'
          WHEN warehouse_name!='Return warehouse' AND j.row=2 THEN 'Return warehouse'
          ELSE warehouse_name END AS warehouse_name,
      contact_name,
      CASE WHEN c.phone IS NOT NULL THEN c.phone ELSE j.phone END AS phone,
      email,
      country,
      region,
      city,
      CASE WHEN c.district IS NOT NULL THEN c.district ELSE j.district END AS district,
      CASE WHEN c.address IS NOT NULL AND LENGTH(c.address)>=10 THEN c.address ELSE j.address END AS address,
      postcode,
      active,
      CASE WHEN city='Lima' 
          AND j.district IN (
                          'Ancon', 
                          'Carabayllo', 'Chaclacayo', 'Cieneguilla',
                          'Lurigancho', 'Lurin',
                          'Pachacamac', 'Pucusana', 'Puente Piedra', 'Punta Hermosa', 'Punta Negra',
                          'San Bartolo', 'Santa Maria del Mar', 'Santa Rosa'
                          )

          THEN 1 ELSE 0 END AS periferico,
      big_ticket,
      k.diff_address,
      CASE WHEN he.horario_especial IS NOT NULL THEN he.horario_especial ELSE j.start_time END AS start_time,
      close_time,
      work_days,
      monday,
      tuesday,
      wednesday,
      thursday,
      friday,
      saturday,
      sunday,
      CASE WHEN c.latitude IS NOT NULL THEN c.latitude ELSE j.latitude END AS latitude,
      CASE WHEN c.longitude IS NOT NULL THEN c.longitude ELSE j.longitude END AS longitude,
      c.macronodo,
      --
      he.motivo,
      y.status_sabado,
      --
      d.punto_dropoff,
      x.direccion_dropoff,
      x.nombre_dropoff,
      x.horario_dropoff,
      x.frecuencia_dropoff,
      d.solicitud_dropoff,
      --
      --
      kam.PM,
      kam.CT,
      kam.division,
      kam.category,
      --
      CONCAT(j.latitude,",",j.longitude) AS location_fsc,
      CONCAT(c.latitude,",",c.longitude) AS location_hd,
      j.phone AS phone_fsc,
      c.phone AS phone_hd,
      j.district AS district_fsc,
      c.district AS district_hd,
      j.address AS address_fsc,
      c.address AS address_hd,
      j.start_time AS start_time_fsc,
      he.horario_especial AS start_time_hd,
      bqutil.fn.levenshtein(LOWER(j.address),LOWER(COALESCE(c.address,j.address))) AS address_similarity,
      IFNULL(ROUND(ST_DISTANCE(ST_GEOGPOINT(j.longitude, j.latitude), ST_GEOGPOINT(c.longitude, c.latitude), TRUE), 2),0) AS dist_fsc_hd,
      CASE WHEN ROUND(ST_DISTANCE(ST_GEOGPOINT(j.longitude, j.latitude), ST_GEOGPOINT(c.longitude, c.latitude), TRUE), 2)>=25 THEN 'Corregir' ELSE 'Ok' END AS location_corr,
      CASE WHEN j.phone!=c.phone THEN 'Corregir' ELSE 'Ok' END AS phone_corr,
      CASE WHEN j.district!=c.district THEN 'Corregir' ELSE 'Ok' END AS district_corr,
      CASE WHEN bqutil.fn.levenshtein(LOWER(j.address),LOWER(COALESCE(c.address,j.address)))>=5 THEN 'Corregir' ELSE 'Ok' END AS address_corr,
      CASE WHEN j.start_time!=he.horario_especial THEN 'Corregir' ELSE 'Ok' END AS schedule_corr,
      --ROW_NUMBER() OVER (PARTITION BY seller_id, warehouse_name ORDER BY status_time DESC) AS row,
      --CODIGOS POSTALES PERIFERICOS
      --
      -- Datos geograficos
      ---- Puntos geográficos
      ST_GEOGPOINT(j.longitude, j.latitude) AS geolocation_fsc,
      CASE WHEN ST_GEOGPOINT(c.longitude, c.latitude) IS NULL THEN ST_GEOGPOINT(j.longitude, j.latitude) ELSE ST_GEOGPOINT(c.longitude, c.latitude) END AS geolocation_hd,
      ST_GEOGPOINT(-76.958073, -12.217170) AS geolocation_hub,
      ST_GEOGPOINT(-77.080875, -12.047548) AS geolocation_3pl,
      ST_GEOGPOINT(x.longitude_dropoff, x.latitude_dropoff) AS geolocation_dropoff,
      ---- Hexagonos H3
      jslibs.h3.ST_H3(ST_GEOGPOINT(j.longitude, j.latitude), 8) AS h3_8_geo_fsc,
      jslibs.h3.ST_H3(CASE WHEN ST_GEOGPOINT(c.longitude, c.latitude) IS NULL THEN ST_GEOGPOINT(j.longitude, j.latitude) ELSE ST_GEOGPOINT(c.longitude, c.latitude) END , 8) AS h3_8_geo_hd,
      jslibs.h3.ST_H3(ST_GEOGPOINT(x.longitude_dropoff, x.latitude_dropoff), 8) AS h3_8_geo_dropoff,
      
      jslibs.h3.ST_H3(ST_GEOGPOINT(j.longitude, j.latitude), 12) AS h3_12_geo_fsc,
      jslibs.h3.ST_H3(CASE WHEN ST_GEOGPOINT(c.longitude, c.latitude) IS NULL THEN ST_GEOGPOINT(j.longitude, j.latitude) ELSE ST_GEOGPOINT(c.longitude, c.latitude) END , 12) AS h3_12_geo_hd,
      jslibs.h3.ST_H3(ST_GEOGPOINT(x.longitude_dropoff, x.latitude_dropoff), 12) AS h3_12_geo_dropoff,

    FROM j
    LEFT JOIN k ON k.seller_id=j.seller_id
    LEFT JOIN ruc ON ruc.seller_id=j.seller_id
    LEFT JOIN coordenadas_sellers c ON c.seller_id=j.seller_id
    LEFT JOIN horarios_especiales he ON he.seller_id=j.seller_id
    LEFT JOIN dropoff d ON d.seller_id=j.seller_id
    LEFT JOIN dropoff_coord x ON x.punto_dropoff=d.punto_dropoff
    LEFT JOIN atencion_sabados y ON y.seller_id=j.seller_id
    LEFT JOIN kam ON kam.seller_id=j.seller_id
    ORDER BY 2,4
    ),
    dropoff_polygons AS (
      SELECT
        punto_dropoff,
        ST_CONVEXHULL(ST_UNION_AGG(geolocation_hd)) AS dropoff_polygon,
      FROM final
      GROUP BY 1
    )


    SELECT * EXCEPT(row) FROM (
      SELECT
        a.*,
        b.dropoff_polygon,
        ROUND(ST_DISTANCE(geolocation_fsc, geolocation_hub, TRUE)/1000, 2) AS geodesic_dist_fsc_hub,
        ROUND(ST_DISTANCE(geolocation_fsc, geolocation_3pl, TRUE)/1000, 2) AS geodesic_dist_fsc_3pl,
        ROUND(ST_DISTANCE(geolocation_fsc, geolocation_dropoff, TRUE)/1000, 2) AS geodesic_dist_fsc_dropoff,
        ROUND(ST_DISTANCE(geolocation_hd, geolocation_hub, TRUE)/1000, 2) AS geodesic_dist_hd_hub,
        ROUND(ST_DISTANCE(geolocation_hd, geolocation_3pl, TRUE)/1000, 2) AS geodesic_dist_hd_3pl,
        ROUND(ST_DISTANCE(geolocation_hd, geolocation_dropoff, TRUE)/1000, 2) AS geodesic_dist_hd_dropoff,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_fsc, geolocation_hub), 2) AS manhattan_dist_fsc_hub,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_fsc, geolocation_3pl), 2) AS manhattan_dist_fsc_3pl,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_fsc, geolocation_dropoff), 2) AS manhattan_dist_fsc_dropoff,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_hd, geolocation_hub), 2) AS manhattan_dist_hd_hub,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_hd, geolocation_3pl), 2) AS manhattan_dist_hd_3pl,
        ROUND(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.ST_MANHATTAN(geolocation_hd, geolocation_dropoff), 2) AS manhattan_dist_hd_dropoff,
        ROW_NUMBER() OVER (PARTITION BY a.seller_id,a.warehouse_name ORDER BY a.location_fsc DESC) AS row,
        --
        ---- Agregar timeframes (similar a fill rate en peya)
      FROM final a
      LEFT JOIN dropoff_polygons b ON a.punto_dropoff=b.punto_dropoff
      --WHERE a.punto_dropoff='Pickit 1 - SAN MIGUEL'
      )
    WHERE row=1
  ),
  sellers_duplicados AS (

    WITH
    data AS (
      SELECT DISTINCT
        a.ruc,
        a.razon_social,
        a.seller_id,
        a.seller_name,
        a.contact_name,
        b.admin_rut,
        a.phone,
        a.email,
        a.h3_12_geo_hd,
        a.location_fsc,
        a.phone_fsc,
        a.district_fsc,
        a.address_fsc,
        a.start_time_fsc,
      FROM analisis_espacial a
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.informacion_sellers` b ON a.seller_id=b.seller_id
      WHERE a.warehouse_name='Main warehouse'
      AND contact_name!='Pedro Arias'
    ),
    val_rut AS (
      SELECT * FROM (
        SELECT
          a.seller_id,
          b.seller_rut,
        FROM data a
        LEFT JOIN (
          SELECT DISTINCT
            admin_rut,
            seller_id AS seller_rut
          FROM data
        ) b ON a.admin_rut=b.admin_rut
      )
      WHERE seller_id!=seller_rut
    ),
    val_contact AS (
      SELECT * FROM (
        SELECT
          a.seller_id,
          b.seller_contact,
        FROM data a
        LEFT JOIN (
          SELECT DISTINCT
            contact_name,
            seller_id AS seller_contact
          FROM data
        ) b ON a.contact_name=b.contact_name
      )
      WHERE seller_id!=seller_contact
    ),
    val_phone AS (
      SELECT * FROM (
        SELECT
          a.seller_id,
          b.seller_phone,
        FROM data a
        LEFT JOIN (
          SELECT DISTINCT
            phone,
            seller_id AS seller_phone
          FROM data
        ) b ON a.phone=b.phone
      )
      WHERE seller_id!=seller_phone
    ),
    val_email AS (
      SELECT * FROM (
        SELECT
          a.seller_id,
          b.seller_email,
        FROM data a
        LEFT JOIN (
          SELECT DISTINCT
            email,
            seller_id AS seller_email
          FROM data
        ) b ON a.email=b.email
      )
      WHERE seller_id!=seller_email
    ),
    val_h3 AS (
      SELECT * FROM (
        SELECT
          a.seller_id,
          b.seller_h3,
        FROM data a
        LEFT JOIN (
          SELECT DISTINCT
            h3_12_geo_hd,
            seller_id AS seller_h3
          FROM data
        ) b ON a.h3_12_geo_hd=b.h3_12_geo_hd
      )
      WHERE seller_id!=seller_h3
    ),
    val_dup AS (
      SELECT 
        seller_id,
        COUNT(DISTINCT seller_dup) AS n_seller_dup,
        MAX(CASE WHEN row=1 THEN seller_dup ELSE NULL END) AS seller_id_dup1,
        MAX(CASE WHEN row=2 THEN seller_dup ELSE NULL END) AS seller_id_dup2,
        MAX(CASE WHEN row=3 THEN seller_dup ELSE NULL END) AS seller_id_dup3,
        MAX(CASE WHEN row=4 THEN seller_dup ELSE NULL END) AS seller_id_dup4,
        MAX(CASE WHEN row=5 THEN seller_dup ELSE NULL END) AS seller_id_dup5,
        MAX(CASE WHEN row=6 THEN seller_dup ELSE NULL END) AS seller_id_dup6,
        MAX(CASE WHEN row=7 THEN seller_dup ELSE NULL END) AS seller_id_dup7,
        MAX(CASE WHEN row=8 THEN seller_dup ELSE NULL END) AS seller_id_dup8,
        MAX(CASE WHEN row=9 THEN seller_dup ELSE NULL END) AS seller_id_dup9,
      FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_dup DESC) AS row
        FROM (
          SELECT DISTINCT * FROM (
            SELECT seller_id,seller_rut AS seller_dup FROM val_rut
            UNION ALL
            SELECT seller_id,seller_contact AS seller_dup FROM val_contact
            UNION ALL
            SELECT seller_id,seller_phone AS seller_dup FROM val_phone
            UNION ALL
            SELECT seller_id,seller_email AS seller_dup FROM val_email
          )
        )
      )
      GROUP BY 1
    )
    /**
    SELECT
      a.* EXCEPT (seller_rut,seller_contact,seller_phone,seller_email),
      b.* EXCEPT (seller_id)
    FROM val_final a
    LEFT JOIN val_dup b ON a.seller_id=b.seller_id
    **/


    SELECT 
      a.*,
      b.* EXCEPT (seller_id) 
    FROM data a
    LEFT JOIN val_dup b ON a.seller_id=b.seller_id
  )
  /**
  ,
  contactos AS (

    WITH
    data AS (
      SELECT 
        ruc,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(representante_legal))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS representante_legal,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(administrador_cuenta))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS administrador_cuenta,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(contacto_logistico_1))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS contacto_logistico_1,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(contacto_logistico_2))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS contacto_logistico_2,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(contacto_devoluciones))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS contacto_devoluciones,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(contacto_pagos))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS contacto_pagos,
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(contactos_adicionales))
                  ,':|-|,|/', ' ')
                ,'\\+51', '')
              ,'\\s{2,}', ' ')
            ,'(\\d)\\s+(\\d)', '\\1\\2')
          ,'telefono|correo', ''
        ) AS contactos_adicionales
      FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.contactos_adicionales_sellers` 
    )

    SELECT DISTINCT 
      ruc,
      tipo,
      INITCAP(CASE WHEN LENGTH(nombre)<5 THEN NULL ELSE nombre END) AS nombre,
      CASE WHEN LENGTH(telefono)!=9 THEN NULL ELSE telefono END AS telefono,
      email
    FROM (
      SELECT
          ruc,
          'Representante legal' AS tipo,
          REGEXP_EXTRACT(representante_legal, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(representante_legal, r'\d+') AS telefono,
          REGEXP_EXTRACT(representante_legal, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Administrador de cuenta' AS tipo,
          REGEXP_EXTRACT(administrador_cuenta, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(administrador_cuenta, r'\d+') AS telefono,
          REGEXP_EXTRACT(administrador_cuenta, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Contacto logistico' AS tipo,
          REGEXP_EXTRACT(contacto_logistico_1, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(contacto_logistico_1, r'\d+') AS telefono,
          REGEXP_EXTRACT(contacto_logistico_1, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Contacto logistico' AS tipo,
          REGEXP_EXTRACT(contacto_logistico_2, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(contacto_logistico_2, r'\d+') AS telefono,
          REGEXP_EXTRACT(contacto_logistico_2, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Contacto devoluciones' AS tipo,
          REGEXP_EXTRACT(contacto_devoluciones, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(contacto_devoluciones, r'\d+') AS telefono,
          REGEXP_EXTRACT(contacto_devoluciones, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Contacto pagos' AS tipo,
          REGEXP_EXTRACT(contacto_pagos, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(contacto_pagos, r'\d+') AS telefono,
          REGEXP_EXTRACT(contacto_pagos, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
      UNION ALL
      SELECT
          ruc,
          'Contacto adicional' AS tipo,
          REGEXP_EXTRACT(contactos_adicionales, r'^[^\d]+') AS nombre,
          REGEXP_EXTRACT(contactos_adicionales, r'\d+') AS telefono,
          REGEXP_EXTRACT(contactos_adicionales, r'[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}') AS email
      FROM data
    )
  )
  **/

SELECT 
  a.*,
  b.* EXCEPT(
    ruc,razon_social,seller_id,seller_name,contact_name,admin_rut,phone,email,h3_12_geo_hd,location_fsc,phone_fsc,district_fsc,address_fsc,start_time_fsc
  ) 
FROM analisis_espacial a
LEFT JOIN sellers_duplicados b ON a.seller_id=b.seller_id

)

