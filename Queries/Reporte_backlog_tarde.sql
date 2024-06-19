CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_backlog_tarde` AS (

  WITH 
  test_sellers AS 
  (
    SELECT 
      seller_id
    FROM UNNEST([
      'SC48A7F','SC01B79','SCB821F','SC36C1D','SC24301','SC015FC','SCDC772','SCBAC4A',
      'SC9CADA','SC22020','SCEFFB7','SC89B3D','SC786B7','SC68E19','SC4FB63','SC95BC3','SC5DC3C'
    ]) AS seller_id
  ),
  gsc AS (
    SELECT
      a.shipping_provider_product,
      a.fk_shipment_type,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.GMT_5(a.created_at_utc) AS created_at,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.GMT_5(a.updated_at_utc) AS updated_at,
      c.src_id,
      c.name,
      b.order_nr,
      a.id_sales_order_item,
      a.tracking_code,
      a.sku,
      a.name sku_name,
      a.paid_price,
      d.name AS GSC_Status,
      a.src_status,
      DATE(a.target_to_ship) AS target_to_ship,
      a.no_of_parts_of_sku,
      LAX_STRING(SAFE.PARSE_JSON(b.address_billing).region) AS department,
      LAX_STRING(SAFE.PARSE_JSON(b.address_billing).city) AS province,
      LAX_STRING(SAFE.PARSE_JSON(b.address_billing).ward) AS district,
      b.national_registration_number AS DNI,
      CONCAT(b.customer_first_name," ",b.customer_last_name) AS client_name,
      LAX_STRING(SAFE.PARSE_JSON(b.address_billing).phone) AS client_phone,
      LAX_STRING(SAFE.PARSE_JSON(b.address_billing).customer_email) AS client_email,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a
    LEFT JOIN `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` b ON a.fk_sales_order=b.id_sales_order
    LEFT JOIN `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`c ON a.fk_seller=c.id_seller
    LEFT JOIN `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status` d ON a.fk_sales_order_item_status = d.id_sales_order_item_status
    WHERE b.fk_operator=2
    AND c.geography='national'
    AND c.src_id NOT IN (SELECT * FROM TEST_SELLERS)
    AND src_status IN ('handled_by_marketplace','packed_by_marketplace','ready_to_ship','awaiting_fulfillment','item_received','picked')
  ),
  tms AS (
    SELECT * FROM (  
      SELECT DISTINCT * FROM (
        /**
        SELECT DISTINCT
          ordenServicio AS deliveryOrderNumber,
          numero AS tracking,
          nombreEstadoEnvio AS estado,
          creacion AS fecha,
        FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
        UNION ALL
        SELECT DISTINCT
          numeroExterno AS deliveryOrderNumber,
          numero AS tracking,
          nombreEstadoEnvio AS estado,
          creacion AS fecha,
        FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
        UNION ALL
        SELECT DISTINCT
          ordenServicio AS deliveryOrderNumber,
          numero AS tracking,
          nombreEstadoEnvio AS estado,
          creacion AS fecha,
        FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio`
        UNION ALL
        SELECT DISTINCT
          numeroExterno AS deliveryOrderNumber,
          numero AS tracking,
          nombreEstadoEnvio AS estado,
          creacion AS fecha,
        FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio`
        **/
        SELECT DISTINCT
          deliveryOrderNumber,
          tracking,
          estado,
          fecha
        FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX
        )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha DESC)=1
    )
  ),
  urbano AS (
    SELECT * FROM (
      SELECT * FROM (
        SELECT
          cod_rastreo AS tracking,
          estado,
          datetime AS fecha,
        FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.urbano.Api3Pl_Urbano_Prod`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha DESC)=1
        UNION ALL
        SELECT
          cod_rastreo AS tracking,
          estado,
          datetime AS fecha,
        FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_Urbano`
        QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha DESC)=1
      )
      WHERE tracking IS NOT NULL
    )
    QUALIFY ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha DESC)=1
  ),
  catalyst AS (
    SELECT
      a.deliveryOrderNumber,
      a.orderNumber,
      a.deliveryMethod,
      a.piso_promesa,
      a.techo_promesa,
      a.createdAt,
      b.status_catalyst AS status,
      TIMESTAMP(SUBSTR(b.status_time, 1, 24)) AS fecha,
    FROM (
      SELECT
        deliveryOrderNumber,
        orderNumber,
        deliveryMethod,
        DATE(SAFE_CAST(promisedByDeliveryInfofromDateTime AS TIMESTAMP),"America/Lima") AS piso_promesa,
        DATE(SAFE_CAST(promisedByDeliveryInfotoDateTime AS TIMESTAMP),"America/Lima") AS techo_promesa,
        DATETIME(SAFE_CAST(createdAt AS TIMESTAMP),"America/Lima") AS createdAt
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
      QUALIFY ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY PARTITION_DATE DESC)=1
    ) a
    LEFT JOIN (
      SELECT DISTINCT
        CAST(deliveryOrderNumber AS STRING) AS deliveryOrderNumber,
        Status AS status_catalyst,
        Fecha_actualizacion AS status_time,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_shipment_status_updated` a
      LEFT JOIN (
        SELECT DISTINCT
          orderLineId,
          deliveryOrderNumber,
          orderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
        WHERE PARTITION_DATE>="2022-08-01"
      ) b ON a.Orderline_Id=b.orderLineId
      WHERE PARTITION_DATE>="2022-08-01"
      QUALIFY ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time DESC)=1
    ) b ON a.deliveryOrderNumber=b.deliveryOrderNumber
  ),
  loyalty AS (
    SELECT
      id_seller,
      valor AS segmento,
    FROM `bi-fcom-drmb-local-pe-sbx.Zapdos_NPS.espejo_segmentacion_sellers`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_seller ORDER BY valor DESC)=1
  ),
  comercial AS (
    SELECT 
      id_seller AS sellerId,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(PM) AS PM,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(CT) AS CT,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Division) AS Division,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(cat) AS Categoria
    FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.comerciales_kam`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_seller ORDER BY PM DESC)=1
  ),
  datos_sellers AS (
    SELECT DISTINCT
      seller_id AS sellerId, 
      phone AS sellerPhone,
      email AS sellerEmail 
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.analisis_espacial_sellers`
    WHERE warehouse_name='Main warehouse'
  ),
  sellers_aaa AS (
    SELECT 
      id_seller AS sellerId,
      CONCAT('+',REGEXP_REPLACE(numero,' ','')) AS loyaltyPhone,
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.asignacion_sellers_aaa_pe`
  ),
  sellers_dropoff AS (
    SELECT DISTINCT
      seller_id AS sellerId,
      punto_dropoff,
      direccion_dropoff,
      nombre_dropoff,
      horario_dropoff,
      frecuencia_dropoff
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.analisis_espacial_sellers`
    WHERE warehouse_name='Main warehouse'
    AND punto_dropoff IS NOT NULL
  ),
  final AS (
    SELECT
      gsc.created_at AS createdAt,
      DATE(gsc.created_at) AS datecreatedAt,
      gsc.src_id AS sellerId,
      INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.name)) AS sellerName,
      catalyst.orderNumber,
      gsc.order_nr AS deliveryOrderNumber,
      COALESCE(gsc.tracking_code,tms.tracking) AS trackingCode,
      --urbano.tracking AS tracking_urbano,
      --tms.tracking AS tracking_tms,
      gsc.id_sales_order_item AS orderItemId,
      gsc.GSC_Status AS lastStatusGSC,
      --DATE(gsc.updated_at) AS lastStatusDateGSC,
      gsc.updated_at AS lastStatusDatetimeGSC,
      --
      tms.estado AS lastStatusTMS,
      DATETIME(tms.fecha,'America/Lima') AS lastStatusDatetimeTMS,
      --
      ----catalyst.status AS lastStatusJARVIS,
      ----DATETIME(catalyst.fecha,'America/Lima') AS lastStatusDatetimeJARVIS,
      --urbano.estado AS Estado_urbano,
      --
      gsc.sku AS sku,
      INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.sku_name)) AS description,
      DATE(gsc.target_to_ship) AS promisedBySeller,
      LEAST(COALESCE(catalyst.piso_promesa,catalyst.techo_promesa),COALESCE(catalyst.techo_promesa,catalyst.piso_promesa)) AS promisedToClient,
      --NULL AS deliveredDate,
      CASE WHEN sellers_dropoff.punto_dropoff IS NOT NULL THEN 'dropoff' ELSE shipping_provider_product END AS carrierCode, 
      CASE WHEN gsc.fk_shipment_type=1 THEN 'FBF' ELSE 'FBS' END AS Fulfillment,
      CASE WHEN UPPER(gsc.department)='LIMA' THEN 'Lima' ELSE 'Provincia' END AS LimaProv,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.department)) AS department,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.province)) AS province,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.district)) AS district,
      loyaltyPhone,
      CONCAT('+51',sellerPhone) AS sellerPhone,
      sellerEmail,
      gsc.DNI AS clientDNI,
      INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.client_name)) AS clientName,
      gsc.client_phone AS clientPhone,
      gsc.client_email AS clientEmail,
      gsc.paid_price AS TotalPrecio,
      IFNULL(gsc.no_of_parts_of_sku,1) AS Cantidad_multipartes,
      CASE WHEN gsc.no_of_parts_of_sku IS NOT NULL THEN 'Si' ELSE 'No' END AS Multiparte,
      catalyst.deliveryMethod AS Tipo_envio,
      -- Segmentación
      loyalty.segmento,
      -- Comercial
      comercial.PM,
      comercial.CT,
      comercial.Division,
      comercial.Categoria,
      -- Dropoff
      sellers_dropoff.punto_dropoff,
      sellers_dropoff.direccion_dropoff,
      sellers_dropoff.nombre_dropoff,
      sellers_dropoff.horario_dropoff,
      sellers_dropoff.frecuencia_dropoff,
      -- Condicionales
      CASE WHEN CURRENT_DATE()>DATE(target_to_ship) THEN 'En retraso' ELSE 'Orden en tiempo' END AS estatus_general,
      DATE_DIFF(CURRENT_DATE(), target_to_ship, DAY) AS dias_de_retraso,
    FROM gsc
    LEFT JOIN tms ON gsc.tracking_code=tms.tracking
    LEFT JOIN urbano ON gsc.tracking_code=urbano.tracking
    LEFT JOIN catalyst ON gsc.order_nr=catalyst.deliveryOrderNumber
    LEFT JOIN loyalty ON gsc.src_id=loyalty.id_seller
    LEFT JOIN comercial ON gsc.src_id=comercial.sellerId
    LEFT JOIN datos_sellers ON gsc.src_id=datos_sellers.sellerId
    LEFT JOIN sellers_aaa ON gsc.src_id=sellers_aaa.sellerId
    LEFT JOIN sellers_dropoff ON gsc.src_id=sellers_dropoff.sellerId
    WHERE gsc.shipping_provider_product IN ('ibis','urbano')
    AND gsc.fk_shipment_type!=1 
    AND tms.estado NOT IN ('Anulado','Retorno a seller')
    AND gsc.GSC_Status NOT IN ('delivered','canceled','failed')
    --AND gsc.target_to_ship<=CURRENT_DATE('America/Lima') -- Filtro de pendientes
    ORDER BY createdAt ASC
  ),
  final_bl AS (
    SELECT
      a.* EXCEPT (estatus_general, dias_de_retraso),
      CASE WHEN dias_de_retraso>=30 THEN 'Cambio de estado' ELSE estatus_general END AS estatus_general,
      dias_de_retraso,
      CASE 
        WHEN dias_de_retraso>=30 THEN 'Cambio de estado' 
        WHEN dias_de_retraso>3 AND dias_de_retraso<30 THEN 'Cancelar'
        WHEN dias_de_retraso=3 THEN 'Comunicar 3er refuerzo'
        WHEN dias_de_retraso=2 THEN 'Comunicar 2do refuerzo'
        WHEN dias_de_retraso=1 THEN 'Comunicar 1er refuerzo'
        WHEN dias_de_retraso=0 THEN 'A tiempo'
        WHEN dias_de_retraso=-1 THEN 'Comunicar envío día siguiente'
        WHEN dias_de_retraso<-1 THEN 'No accionar'
        ELSE 'NA' END AS accion,
      --CASE WHEN dias_de_retraso=-1 THEN 'Comunicar envio dia siguiente' ELSE 'NA' END AS Tomorrowland
    FROM final a
  ),
  reporte_bl AS (
    SELECT 
      * 
    FROM final_bl
    WHERE accion NOT IN ('Cancelar', 'Cambio de estado', 'No accionar', 'Comunicar envío día siguiente')
    AND TIMESTAMP(lastStatusDatetimeGSC) <= TIMESTAMP(DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY) || 'T22:00:00')
  )

  SELECT
    datecreatedAt,
    deliveryOrderNumber,
    trackingCode,
    lastStatusGSC,
    sellerId,
    sellerName,
    sku, --
    description,
    promisedBySeller,
    Fulfillment,
    segmento,
    carrierCode,
    punto_dropoff,
    direccion_dropoff,
    nombre_dropoff,
    horario_dropoff,
    frecuencia_dropoff,
    estatus_general,
    dias_de_retraso,
    accion,
    sellerEmail,
    sellerPhone,
    loyaltyPhone,
    CASE
        WHEN sellerId IN (
            SELECT sellerId
            FROM Reporte_BL
            WHERE segmento != 'AAA'
            GROUP BY sellerId
        ) THEN 'Speech_3'
        WHEN sellerId IN (
            SELECT sellerId
            FROM Reporte_BL
            GROUP BY sellerId
            HAVING COUNT(DISTINCT segmento) = 1 AND COUNT(DISTINCT lastStatusGSC) > 1
        ) THEN 'Speech_2'
        WHEN sellerId IN (
            SELECT sellerId
            FROM Reporte_BL
            GROUP BY sellerId
            HAVING COUNT(*) = COUNT(CASE WHEN lastStatusGSC = 'pending' THEN 1 END)
        ) THEN 'Speech_4'
        ELSE 'Otro caso'
    END AS Speech,
    CASE
      WHEN accion = 'A tiempo' THEN 'A tiempo'
      WHEN accion = 'Comunicar 1er refuerzo' THEN '1 dia de retraso'
      WHEN accion = 'Comunicar 2do refuerzo' THEN '2 dias de retraso'
      WHEN accion = 'Comunicar 3er refuerzo' THEN '3 dias de retraso'
      ELSE NULL  -- Manejo de valores no especificados
    END AS Estatusenvio,
    CASE
      WHEN loyaltyPhone = '+573174415797' THEN 'wa.link/1apugh'
      WHEN loyaltyPhone = '+573188067327' THEN 'wa.link/ycwgfj'
      ELSE NULL
    END AS EnlaceAAA,
    --CASE WHEN dias_de_retraso=-1 THEN 'Comunicar envio dia siguiente' ELSE 'NA' END AS Tomorrowland
  FROM reporte_bl

)
