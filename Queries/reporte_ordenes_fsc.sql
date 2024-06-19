CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` AS (

WITH
-- B. Historial de estados y tiempos
--- 1. FSC (PESADO)
a AS (
  SELECT * FROM ( 
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY fk_sales_order ORDER BY shipping_type DESC) AS row,
    FROM (
      SELECT
        fk_seller,
        a.fk_sales_order,
        shipping_type,
        LOWER(shipping_provider_product) AS shipping_provider_product,
        c.src_status,
        MAX(a.created_at) AS created_at,
        MAX(c.created_at) AS updated_at,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a
      JOIN (
        SELECT DISTINCT
          id_shipment_type,
          name AS shipping_type,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_shipment_type`
      ) b ON b.id_shipment_type=a.fk_shipment_type
      LEFT JOIN (
        SELECT
          fk_sales_order,
          src_status,
          created_at,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status_history` a
        LEFT JOIN (
          SELECT DISTINCT
            fk_sales_order,
            id_sales_order_item,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
        ) b ON a.fk_sales_order_item=b.id_sales_order_item
        LEFT JOIN (
          SELECT DISTINCT
            id_sales_order_item_status,
            name AS src_status,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status`
        ) c ON a.fk_sales_order_item_status=c.id_sales_order_item_status
      ) c ON c.fk_sales_order=a.fk_sales_order
      GROUP BY 1,2,3,4,5
      )
      PIVOT (
        MAX(updated_at) AS time
        FOR src_status IN (
                'handled_by_marketplace', 'packed_by_marketplace', 'item_received', 'awaiting_fulfillment',
                'ready_to_ship', 'shipped', 'picked', 
                'delivered', 'canceled', 'failed_delivery', 'failed',
                'return_shipped_by_customer', 'return_waiting_for_approval','returned','return_rejected'
        )
      )
    )
    WHERE row=1
),
--- 2. Ingresos --OK
p AS (
  -- TMS
  SELECT DISTINCT
    numeroEnvio AS rastreo,
    time_ingreso,
  FROM (
    -- En bodega Crossdock
    /**
    SELECT
      numeroEnvio,
      MAX(DATETIME(creacion, "America/Lima")) AS time_ingreso,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env`
    WHERE nombreEstadoEnvio='En bodega Crossdock'
    GROUP BY 1
    **/
    -- 2do estado (siempre que en bodega crossdock sea nulo)
    SELECT
      tracking AS numeroEnvio,
      MAX(DATETIME(fecha, "America/Lima")) AS time_ingreso,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX`
    WHERE estado='En bodega Crossdock'
    GROUP BY 1
    UNION ALL
    SELECT
      numeroEnvio,
      time_ingreso,
    FROM (
      /**
      SELECT
        numeroEnvio,
        nombreEstadoEnvio,
        DATETIME(creacion, "America/Lima") AS time_ingreso,
        ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY creacion ASC) AS row,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env`
      **/
      SELECT
        tracking AS numeroEnvio,
        estado AS nombreEstadoEnvio,
        DATETIME(fecha, "America/Lima") AS time_ingreso,
        ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha ASC) AS row,        
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX`
    )
    WHERE row=2 AND nombreEstadoEnvio!='Anulado'
  )
  UNION ALL
  -- Urbano
  SELECT
    cod_rastreo AS rastreo,
    DATE(MAX(CAST(datetime AS DATETIME))) AS fecha_registro,
  FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_Urbano` -- OK -- NO HAY EN PROD
  WHERE estado='ADMITIDO EN HUB'
  GROUP BY 1
),
--- 4. Catalyst
-- C. Estados finales y previos
--- 1. FSC
x AS (
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_fsc, 1) OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS prev_status_fsc,
      LEAD(status_time_fsc, 1) OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS prev_status_time_fsc,
      ROW_NUMBER() OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS row,
      --LEAD(status_fsc, 1) OVER (PARTITION BY fk_sales_order,id_sales_order_item ORDER BY status_time_fsc DESC) AS prev_status_fsc,
      --LEAD(status_time_fsc, 1) OVER (PARTITION BY fk_sales_order,id_sales_order_item ORDER BY status_time_fsc DESC) AS prev_status_time_fsc,
      --ROW_NUMBER() OVER (PARTITION BY fk_sales_order,id_sales_order_item ORDER BY status_time_fsc DESC) AS row,
    FROM (
      SELECT
        fk_sales_order,
        --id_sales_order_item,
        src_status AS status_fsc,
        MAX(created_at) AS status_time_fsc,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status_history` a
      LEFT JOIN (
        SELECT DISTINCT
          fk_sales_order,
          id_sales_order_item,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
      ) b ON a.fk_sales_order_item=b.id_sales_order_item
      LEFT JOIN (
        SELECT DISTINCT
          id_sales_order_item_status,
          name AS src_status,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status`
      ) c ON a.fk_sales_order_item_status=c.id_sales_order_item_status
      GROUP BY 1,2--,3
    )
  )
  WHERE row=1
),
--- 2. TMS + Urbano
y AS ( -- OK
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time DESC) AS prev_status_tms,
      LEAD(status_time, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time DESC) AS prev_status_time,
      --LEAD(oficina_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time DESC) AS prev_oficina_tms,
      --LEAD(usuario_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time DESC) AS prev_usuario_tms,
      ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY status_time DESC) AS row,
    FROM (
      -- TMS
      /**
      SELECT DISTINCT
        a.numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreEstadoEnvio)) AS status_tms,
        DATETIME(a.creacion, "America/Lima") AS status_time,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreOficina)) AS oficina_tms,  
        INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS usuario_tms, 
        INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` a -- OK -- NO HAY EN PROD
      LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env` b ON a.numeroEnvio=b.numeroExterno
      **/
      SELECT DISTINCT
        a.tracking AS numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.estado)) AS status_tms,
        DATETIME(a.fecha, "America/Lima") AS status_time,
        --INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreOficina)) AS oficina_tms,  
        --INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS usuario_tms, 
        --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX` a         
      LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX` b ON a.tracking=b.tracking
      UNION ALL
      -- Urbano
      SELECT DISTINCT
        cod_rastreo AS numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(estado)) AS status_tms,
        CAST(datetime AS DATETIME) AS status_time,
        --agencia AS oficina_tms,
        --'Urbano' AS usuario_tms,
        --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Direccion_cliente),",|;",""),'"',"")) AS direccion,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.urbano.Api3Pl_Urbano_Prod` -- OK -- NO HAY EN PROD
    ) 
  )
  WHERE row=1
),
--- 4. Catalyst
-- TAB 1
t AS ( -- OK
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_catalyst, 1) OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time DESC) AS prev_status_catalyst,
      LEAD(status_time, 1) OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time DESC) AS prev_status_time,
      ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time DESC) AS row,
    FROM (
      SELECT DISTINCT
        CAST(deliveryOrderNumber AS STRING) AS deliveryOrderNumber,
        Status AS status_catalyst,
        Fecha_actualizacion AS status_time,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_shipment_status_updated` a
      JOIN (
        SELECT DISTINCT
          orderLineId,
          deliveryOrderNumber,
          orderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
        WHERE PARTITION_DATE>="2022-08-01"
        ) b ON a.Orderline_Id=b.orderLineId
      WHERE PARTITION_DATE>="2022-08-01"
      )
    )
  WHERE row=1
),
-- TAB 2
t2 AS (
  SELECT 
    b.deliveryOrderNumber,
    a.orderNumber,
    a.status AS status_catalyst,
    a.eventTime,
    a.reasonCodeCategory,
    a.reasonCodeSubCategory,
  FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_btd_seller_order_line_cancelled` a
  JOIN (
        SELECT DISTINCT
          deliveryOrderNumber,
          orderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
        WHERE PARTITION_DATE>="2022-08-01"
  ) b ON a.orderNumber=b.orderNumber
),
--- QUERIES PENDIENTES POR OPTIMIZAR (estos 3 queries son pesados)
-- Rastreos 
d AS ( 
  SELECT 
    fk_sales_order,
    cancel_reason,
    COALESCE(a.tracking_code_fsc,c.tracking_code_fsc) AS tracking_code_fsc,
    c.tracking_code,
    printed,
    target_to_ship,
    multibulto,
    n_rastreos,
    rastreos_asociados,
  FROM (
    SELECT
      fk_sales_order,
      fk_sales_order_reason,
      CASE WHEN LENGTH(tracking_code_fsc)=0 THEN NULL ELSE tracking_code_fsc END AS tracking_code_fsc,
      printed,
      target_to_ship,
      CASE WHEN no_of_parts_of_sku>=2 THEN 'Si' ELSE 'No' END AS multibulto,
    FROM (
      SELECT
        fk_sales_order,
        MAX(fk_sales_order_reason) AS fk_sales_order_reason,
        MAX(tracking_code) AS tracking_code_fsc,
        MAX(printed) AS printed,
        MAX(target_to_ship) AS target_to_ship,
        MAX(no_of_parts_of_sku) AS no_of_parts_of_sku,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` -- OK
      GROUP BY 1
    )
  ) a
  LEFT JOIN (
    SELECT DISTINCT
      id_sales_order_reason,
      description AS cancel_reason,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_reason` -- OK
  ) b ON a.fk_sales_order_reason=b.id_sales_order_reason
  LEFT JOIN (
    SELECT DISTINCT
      a.id_sales_order,
      deliveryOrderNumber,
      tracking_code,
      tracking_code_fsc,
      n_rastreos,
      rastreos_asociados,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` a
    LEFT JOIN (
     SELECT 
        deliveryOrderNumber,
        MAX(n_rastreos) AS n_rastreos,
        MAX(CASE WHEN row_desc=1 OR estado='Entregado' THEN tracking_code ELSE NULL END) AS tracking_code,
        MAX(CASE WHEN row_asc=1 THEN tracking_code ELSE NULL END) AS tracking_code_fsc,
        STRING_AGG(DISTINCT tracking_code, " | ") AS rastreos_asociados,
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY creacion ASC) AS row_asc,
          ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY creacion DESC) AS row_desc, --creacion DESC
        FROM (
          WITH a AS (
            SELECT DISTINCT * FROM (
              /**
              SELECT DISTINCT
                ordenServicio AS deliveryOrderNumber,
                numero AS tracking_code,
                nombreEstadoEnvio AS estado,
                creacion,
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              UNION ALL
              SELECT DISTINCT
                numeroExterno AS deliveryOrderNumber,
                numero AS tracking_code,
                nombreEstadoEnvio AS estado,
                creacion,
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              **/
              SELECT
                deliveryOrderNumber,
                tracking AS tracking_code,
                estado,
                fecha AS creacion
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX`
            )
          ),
          b AS (
            SELECT
              deliveryOrderNumber,
              COUNT(DISTINCT tracking_code) AS n_rastreos
            FROM a
            GROUP BY 1
          )

          SELECT
            a.deliveryOrderNumber,
            n_rastreos,
            tracking_code,
            estado,
            creacion,
          FROM a
          LEFT JOIN b ON a.deliveryOrderNumber=b.deliveryOrderNumber
        )
      )
      GROUP BY 1
    ) b ON a.order_nr=b.deliveryOrderNumber
    WHERE fk_operator=2
  ) c ON c.id_sales_order=a.fk_sales_order -- Rastreos FBF
),
-- Rechazos en puerta
h AS (
  SELECT * FROM (
    /**
    SELECT
      numeroEnvio,
      REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(nombreEstadoEnvio)),' ','_') AS nombreEstadoEnvio,
      MAX(DATETIME(creacion, "America/Lima")) AS creacion,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env`
    WHERE LOWER(nombreEstadoEnvio)='retorno a seller'
    GROUP BY 1,2
    **/
    SELECT
      tracking AS numeroEnvio,
      REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(estado)),' ','_') AS nombreEstadoEnvio,
      MAX(DATETIME(fecha, "America/Lima")) AS creacion,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX`
    WHERE LOWER(estado)='retorno a seller'
    GROUP BY 1,2    
    )
    PIVOT (
      MAX(creacion) AS time
      FOR nombreEstadoEnvio IN (
        'retorno_a_seller'
      )
    )
),
-- Info de productos y valorizados
q AS ( -- NO ACTUALIZA
  SELECT
    fk_sales_order,
    sku,
    product_name,
    brand_name,
    primary_category,
    global_identifier,
    size,
    COUNT(sku) AS items,
    SUM(paid_price) AS value,
  FROM (
    SELECT DISTINCT
      fk_sales_order,
      id_sales_order_item,
      a.sku,
      INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name),",|;",""),'"',"")) AS product_name,
      brand_name,
      primary_category,
      global_identifier,
      variation AS size,
      paid_price,
      --paid_commission,
      --shipping_fee,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a -- OK
    LEFT JOIN (
      SELECT * FROM (
        SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY sku ORDER BY brand_name DESC) AS row
        FROM (
          SELECT DISTINCT
            sku,
            c.name AS brand_name,
            INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(d.name),",|;",""),'"',"")) AS primary_category,
            d.global_identifier,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` x
          JOIN (
            SELECT DISTINCT
              id_catalog_brand,
              INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name)) AS name,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_brand` 
          ) c ON c.id_catalog_brand=x.fk_catalog_brand
          JOIN (
            SELECT
              id_catalog_category,
              global_identifier,
              `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name) AS name
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_category` 
          ) d ON d.id_catalog_category=x.primary_category
          WHERE fk_seller IS NOT NULL
          AND sku IS NOT NULL
        )
      )
      WHERE row=1
    ) b ON a.sku=b.sku
  )
  GROUP BY 1,2,3,4,5,6,7
),
-- Ordenes (PESADO)
b AS ( -- OK
  SELECT DISTINCT
    id_sales_order,
    CAST(order_nr AS STRING) AS deliveryOrderNumber,
    --national_registration_number,
    --INITCAP(CONCAT((customer_first_name),' ',(customer_last_name))) AS customerName,
    --JSON_EXTRACT_SCALAR(address_shipping, '$.phone') AS customerPhone,
    --JSON_EXTRACT_SCALAR(address_billing, '$.customer_email') AS customerEmail,
    --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(CONCAT(JSON_EXTRACT_SCALAR(address_shipping, '$.address1'),' - ',JSON_EXTRACT_SCALAR(address_shipping, '$.address2'))),",|;",""),'"',"")) AS customerAddress,
    payment_method,
  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` -- OK
  WHERE fk_operator=2
),
-- RLOs
r AS (
  SELECT * FROM (
    SELECT 
      *,
      ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber,sku ORDER BY rlo_id DESC) AS row
    FROM (
      SELECT 
        deliveryOrderNumber, 
        sku,
        rlo_id,
        status AS status_rlo,
        reasonCodeCategory AS return_reason,
        reasonCodeSubCategory AS return_sub_reason,
        tracking_code AS tracking_code_il,
        visitas_il,
        ticket_devolucion,
        ticket_reembolso,
      FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_vista_rlos`
      UNION ALL
      SELECT 
        deliveryOrderNumber, 
        sku,
        rlo_id,
        status AS status_rlo,
        reasonCodeCategory AS return_reason,
        reasonCodeSubCategory AS return_sub_reason,
        tracking_code AS tracking_code_il,
        visitas_il,
        ticket_devolucion,
        ticket_reembolso,
      FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_vista_offline`
    )
  )
  WHERE row=1
),
-- Planificación
/**
s AS ( --OK
  SELECT
    rastreo,
    COUNT(DISTINCT Fecha_Planificacion) AS visitas,
    MAX(CASE WHEN row=1 THEN Fecha_Planificacion ELSE NULL END) AS fecha_planificacion,
    MAX(CASE WHEN row=2 THEN Fecha_Planificacion ELSE NULL END) AS fecha_planificacion_2,
    MAX(CASE WHEN row=3 THEN Fecha_Planificacion ELSE NULL END) AS fecha_planificacion_3,
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY rastreo ORDER BY Fecha_Planificacion ASC) AS row,
    FROM (
      SELECT DISTINCT
        rastreo,
        DATE(Fecha_Planificacion) AS Fecha_Planificacion
      FROM `tc-sc-bi-bigdata-dlk-hdrl-dev.PURecoleccion_BI_HD_PE.Planificacion_nuv` -- PENDIENTE PROYECTO BI HD
    )
  )
  GROUP BY 1
),
**/
-- Última Milla
um AS ( --OK
  SELECT
    rastreo,
    COUNT(DISTINCT fecha_visita) AS visitas_cliente,
    MAX(CASE WHEN row=1 THEN fecha_visita ELSE NULL END) AS fecha_visita,
    MAX(CASE WHEN row=2 THEN fecha_visita ELSE NULL END) AS fecha_visita_2,
    MAX(CASE WHEN row=3 THEN fecha_visita ELSE NULL END) AS fecha_visita_3,
  FROM (
      -- TMS
      /**
      SELECT
        numeroEnvio AS rastreo,
        DATETIME(creacion, "America/Lima") AS fecha_visita,
        ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY creacion ASC) AS row,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` -- OK -- NO HAY EN PROD
      WHERE nombreEstadoEnvio IN ('Excepción de Entrega', 'Entregado')
      **/
      SELECT
        tracking AS rastreo,
        DATETIME(fecha, "America/Lima") AS fecha_visita,
        ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha ASC) AS row,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX`
      UNION ALL
      -- Urbano
      SELECT
        cod_rastreo AS rastreo,
        CAST(datetime AS DATETIME) AS fecha_visita,
        ROW_NUMBER() OVER (PARTITION BY cod_rastreo ORDER BY datetime ASC) AS row,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.urbano.Api3Pl_Urbano_Prod` -- OK -- NO HAY EN PROD 
      WHERE estado IN ('VISITADO SIN ENTREGA', 'ENTREGADO')
  )
  GROUP BY 1  
),
-- Backstore
u AS ( --OK
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_backstore, 1) OVER (PARTITION BY orderNumber ORDER BY status_time_backstore DESC) AS prev_status_backstore,
      LEAD(status_time_backstore, 1) OVER (PARTITION BY orderNumber ORDER BY status_time_backstore DESC) AS prev_status_time_backstore,
      ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY status_time_backstore DESC) AS row,
    FROM (
      SELECT
        orderNumber,
        UPPER(status_backstore) AS status_backstore,
        CASE WHEN status_backstore='ready_for_delivery' 
             THEN DATETIME_ADD(status_time_backstore, INTERVAL -1 MINUTE) 
             ELSE status_time_backstore END AS status_time_backstore,
      FROM (
        SELECT DISTINCT
          orderNumber,
          on_route,
          waiting_for_location,
          ready_for_delivery,
          delivered,
          cancelled,
          annulled,
        FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_bksm_Collect`
      )
      UNPIVOT (
        status_time_backstore 
        FOR status_backstore IN (on_route,waiting_for_location,ready_for_delivery,delivered,cancelled,annulled)
        )
    ) 
  )
  WHERE row=1
),
-- Novedades
v AS ( --OK
  SELECT
    numeroEnvio,
    MAX(CASE WHEN row=1 THEN nombreNovedadEnvio ELSE NULL END) AS novedad_1,
    MAX(CASE WHEN row=1 THEN observacion ELSE NULL END) AS observacion_novedad1,
    MAX(CASE WHEN row=1 THEN solucion ELSE NULL END) AS solucion_novedad1,
    MAX(CASE WHEN row=1 THEN tipoSolucion ELSE NULL END) AS tipo_solucion_novedad1,
    MAX(CASE WHEN row=1 THEN usuario ELSE NULL END) AS usuario_novedad1,
    --MAX(CASE WHEN row=1 THEN creacion ELSE NULL END) AS creacion_novedad1,
    MAX(CASE WHEN row=2 THEN nombreNovedadEnvio ELSE NULL END) AS novedad_2,
    MAX(CASE WHEN row=2 THEN observacion ELSE NULL END) AS observacion_novedad2,
    MAX(CASE WHEN row=2 THEN solucion ELSE NULL END) AS solucion_novedad2,
    MAX(CASE WHEN row=2 THEN tipoSolucion ELSE NULL END) AS tipo_solucion_novedad2,
    MAX(CASE WHEN row=2 THEN usuario ELSE NULL END) AS usuario_novedad2,
    --MAX(CASE WHEN row=2 THEN creacion ELSE NULL END) AS creacion_novedad2
  FROM (
    /**
    SELECT 
      numeroEnvio,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreNovedadEnvio,",|;","")) AS nombreNovedadEnvio,
      --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreEstadoEnvio,",|;","")) AS nombreEstadoEnvio,
      --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreEstadoNovedad,",|;","")) AS nombreEstadoNovedad,
      LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(observacion,",|;",""))) AS observacion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(solucion,",|;","")) AS solucion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(tipoSolucion,",|;","")) AS tipoSolucion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(usuario,",|;","")) AS usuario,
      creacion,
      ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY creacion ASC) AS row,
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envionovedadlog_env`
    **/
    SELECT
      numeroEnvio,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreNovedadEnvio,",|;","")) AS nombreNovedadEnvio,
      --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreEstadoEnvio,",|;","")) AS nombreEstadoEnvio,
      --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(nombreEstadoNovedad,",|;","")) AS nombreEstadoNovedad,
      LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(observacion,",|;",""))) AS observacion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(solucion,",|;","")) AS solucion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(tipoSolucion,",|;","")) AS tipoSolucion,
      `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(usuario,",|;","")) AS usuario,
      --creacion,
      --ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY creacion ASC) AS row,
      row
    FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envionovedadlog_env_SX`
    )
  GROUP BY 1
),
com AS (
  SELECT DISTINCT
    G4 AS global_identifier,
    Comisiones_f_com AS comision
  FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
),
m AS (
    SELECT
      c.deliveryOrderNumber,
      shipping_type,
      LOWER(shipping_provider_product) AS shipping_provider_product,
      customerDocument,
      customerName,
      customerPhone,
      customerEmail,
      UPPER(customerRegion) AS customerRegion,
      UPPER(customerCity) AS customerCity,
      UPPER(customerDistrict) AS customerDistrict,
      customerAddress,
      paid_price,
      shipping_fee,
      shipping_service_cost,
      paid_commission
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a
    LEFT JOIN (
      SELECT DISTINCT
        id_shipment_type,
        name AS shipping_type,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_global_seller_center.svw_shipment_type`
    ) b ON b.id_shipment_type=a.fk_shipment_type
    LEFT JOIN (
      SELECT DISTINCT
        id_sales_order,
        CAST(order_nr AS STRING) AS deliveryOrderNumber,
        national_registration_number AS customerDocument,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(CONCAT(customer_first_name,' ',customer_last_name))) AS customerName,
        JSON_EXTRACT_SCALAR(address_shipping, '$.phone') AS customerPhone,
        JSON_EXTRACT_SCALAR(address_billing, '$.customer_email') AS customerEmail,
        JSON_EXTRACT_SCALAR(address_billing, '$.region') AS customerRegion,
        JSON_EXTRACT_SCALAR(address_billing, '$.city') AS customerCity,
        JSON_EXTRACT_SCALAR(address_billing, '$.ward') AS customerDistrict,
        INITCAP(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(
                CONCAT(
                  JSON_EXTRACT_SCALAR(address_shipping, '$.address1'),' - ',JSON_EXTRACT_SCALAR(address_shipping, '$.address2')
                )
              ),",|;","")
            ,'"',"")
        ) AS customerAddress,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order`
      WHERE fk_operator=2
    ) c ON c.id_sales_order=a.fk_sales_order
),


-- Tickets de devoluciones

--- CRUCE POR OPTIMIZAR
-- Cruce final
l AS (
SELECT
  --a.fk_sales_order,
  DATE(a.created_at) AS date,
  c.sellerId,
  REGEXP_REPLACE(c.seller_name, '"', '') AS sellerName,
  c.orderNumber,
  b.deliveryOrderNumber,
  CASE WHEN a.shipping_type='Dropshipping' THEN 'FBS'
      WHEN a.shipping_type='Own Warehouse' THEN 'FBF'
      WHEN a.shipping_type='Cross docking' THEN 'XD'
      ELSE a.shipping_type END
      AS shipping_type,
  a.shipping_provider_product,
  d.tracking_code,
  d.tracking_code_fsc,
  d.multibulto,
  d.n_rastreos,
  d.rastreos_asociados,
  c.deliveryMethod,
  c.pickupPoint,
  u.status_backstore,
  x.status_fsc,
  y.status_tms,
  --y.usuario_tms,
  t.status_catalyst,
  --CASE WHEN t2.status_catalyst IS NOT NULL THEN t2.status_catalyst ELSE t.status_catalyst END AS status_catalyst,
  --y.oficina_tms AS oficina,
  --y.usuario_tms AS usuario,
  x.prev_status_fsc,
  y.prev_status_tms,
  t.prev_status_catalyst,
  --y.prev_oficina_tms AS prev_oficina,
  --y.prev_usuario_tms AS prev_usuario,
  y.status_time AS max_status_time_tms,
  q.sku,
  q.product_name,
  q.brand_name,
  q.primary_category,
  q.global_identifier,
  q.size,
  q.items,
  q.value,
  NULL AS visitas,
  --s.visitas,
  um.visitas_cliente,
  d.printed,
  CASE WHEN a.created_at IS NOT NULL THEN a.created_at -- Todos
      WHEN a.time_handled_by_marketplace IS NOT NULL THEN a.time_handled_by_marketplace -- FBS
      WHEN a.time_item_received IS NOT NULL THEN a.time_item_received -- FBF
      WHEN a.time_awaiting_fulfillment IS NOT NULL THEN a.time_awaiting_fulfillment --
      ELSE a.time_packed_by_marketplace --
      END AS created_at, -- Creado
  CASE WHEN a.time_ready_to_ship IS NOT NULL THEN a.time_ready_to_ship -- FBS
      WHEN a.time_picked IS NOT NULL THEN a.time_picked -- FBF
      ELSE a.time_ready_to_ship
      END AS ready_to_ship, -- Listo para enviar FSC
  d.target_to_ship, -- Tiempo límite Lpe
  NULL AS planned_at,
  --s.Fecha_Planificacion AS planned_at, -- Fecha de planificación
  CASE WHEN p.time_ingreso IS NOT NULL THEN p.time_ingreso -- Ingresos TMS + Urbano
       ELSE a.time_shipped END AS shipped_at, -- Enviado
  um.fecha_visita AS visited_customer_at,
  CASE WHEN a.time_delivered IS NOT NULL THEN a.time_delivered -- Entregado FSC
      WHEN a.time_canceled IS NOT NULL THEN a.time_canceled -- Cancelado
      WHEN a.time_failed_delivery IS NOT NULL THEN a.time_failed_delivery -- Fallo de entrega -- AGREGAR CASOS URBANO 3PL (EJEMPLO: WYB172645305)
      WHEN a.time_failed IS NOT NULL THEN a.time_failed -- Fallo (campo nulo)
      ELSE NULL END AS terminated_at, --OK
  c.target_to_customer,
  CASE WHEN a.time_return_shipped_by_customer IS NOT NULL THEN a.time_return_shipped_by_customer -- Retornos FSC (entrega de cliente)
      WHEN h.time_retorno_a_seller IS NOT NULL THEN h.time_retorno_a_seller -- Retornos TMS (salidas de almacén)
      ELSE NULL END AS return_shipped_by_customer, -- Pendiente revisar Urbano
  a.time_return_waiting_for_approval AS return_waiting_for_approval, -- Pendiente revisar TMS y/o Urbano
  --a.time_return_rejected, -- OK
  a.time_returned AS returned_at,  -- Pendiente revisar TMS y/o Urbano
  cancel_reason,
  r.return_reason,
  r.return_sub_reason,
  r.rlo_id,
  r.status_rlo,
  r.tracking_code_il,
  r.visitas_il,
  m.customerDocument,
  m.customerName,
  m.customerPhone,
  m.customerEmail,
  m.customerRegion,
  m.customerCity,
  m.customerDistrict,
  m.customerAddress,
  --k.latitud AS delivery_latitude,
  --k.longitud AS delivery_longitude,
  b.payment_method,
  paid_price,
  pay.shipping_fee,
  shipping_service_cost,
  paid_commission,
  com.comision,
  pay.ingreso,
  pay.egreso,
  pay.pago,
  pay.descuento,
  pay.ajuste,
  pay.fecha_ingreso,
  pay.fecha_egreso,
  pay.fecha_pago,
  pay.fecha_descuento,
  pay.fecha_ajuste,
  pay.ticket_reembolso,
  v.novedad_1,
  v.observacion_novedad1,
  v.solucion_novedad1,
  v.tipo_solucion_novedad1,
  v.usuario_novedad1,
  --v.creacion_novedad1,
  v.novedad_2,
  v.observacion_novedad2,
  v.solucion_novedad2,
  v.tipo_solucion_novedad2,
  v.usuario_novedad2,
  --v.creacion_novedad2,
  CONCAT('https://sellercenter.falabella.com/order/admin/order-detail/filteredStatus/0/id/',CONCAT(a.fk_sales_order, CONCAT('/sellerId/', c.id_seller))) AS fsc_url,
  CASE WHEN LENGTH(d.tracking_code)>10 THEN CONCAT("https://storage.googleapis.com/marketplace-package-labels-production/shipping-labels/", CONCAT(d.tracking_code, ".pdf")) 
      ELSE NULL END AS label_url,
  CASE WHEN a.shipping_provider_product IN ('courier internacional') AND d.tracking_code IS NOT NULL THEN NULL
      WHEN a.shipping_provider_product IN ('ibis', 'ibisdirecto') AND d.tracking_code IS NOT NULL THEN NULL
      WHEN a.shipping_provider_product='urbano' AND d.tracking_code IS NOT NULL THEN CONCAT('https://app.urbano.com.pe/plugin/etracking/etracking/?guia=', d.tracking_code)
      --https://portal.urbano.com.pe/rastrear/WYB177887633 -- ESTA ES GUIA DE ESTADOS DESDE ADMITIDO EN HUB. NO APARECE NADA DE VISITAS DE RECOLECCION, ESTAS TIENEN UN CODIGO "R".
      WHEN d.tracking_code IS NULL THEN NULL
      ELSE NULL END AS tracking_url,
  CASE WHEN LENGTH(r.tracking_code_il)>10 THEN CONCAT('https://app.urbano.com.pe/plugin/etracking/etracking/?guia=', r.tracking_code_il)
      WHEN r.tracking_code_il IS NULL THEN NULL
      ELSE NULL END AS tracking_il_url,
  ROW_NUMBER() OVER (PARTITION BY b.deliveryOrderNumber,q.sku ORDER BY a.created_at DESC) AS row,
FROM (
  SELECT DISTINCT
    id_seller,
    UPPER(a.sellerId) AS sellerId,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(COALESCE(b.seller_name, a.sellerName))) AS seller_name,
    a.orderNumber,
    CAST(a.deliveryOrderNumber AS STRING) AS deliveryOrderNumber,
    a.deliveryMethod,
    --a.variantId AS sku, <-- PRUEBA
    --a.displayName AS product_name, <-- PRUEBA
    --a.quantityNumber AS items, <-- PRUEBA
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(pickupPointadressName)) AS pickupPoint,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddressstateName)) AS region,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddresscityName)) AS ciudad,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddressmunicipalName)) AS distrito,
    DATETIME(SAFE_CAST(COALESCE(promisedByDeliveryInfofromDateTime,promisedByDeliveryInfotoDateTime) AS TIMESTAMP), 'America/Lima') AS target_to_customer,
    --a.orderLineId <-- PRUEBA
  FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders` a
  LEFT JOIN ( -- OK pero no jala con -1 debe -7
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY seller_name DESC) AS row,
      FROM (
        SELECT
          id_seller,
          src_id AS seller_id,
          UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name),",","")) AS seller_name
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
        )
      )
    WHERE row=1
  ) b ON a.sellerId=b.seller_id
  WHERE PARTITION_DATE>="2022-08-01"
) c
LEFT JOIN b ON c.deliveryOrderNumber=b.deliveryOrderNumber -- OK CORREGIDO
LEFT JOIN a ON b.id_sales_order=a.fk_sales_order -- Historial FSC --No actualiza
LEFT JOIN d ON d.fk_sales_order=a.fk_sales_order -- Rastreos + razones de cancelación -- OK CORREGIDO
LEFT JOIN x ON x.fk_sales_order=a.fk_sales_order -- Estados FSC -- OK CORREGIDO
LEFT JOIN y ON y.numeroEnvio=d.tracking_code -- Estados TMS + Urbano
LEFT JOIN t ON t.deliveryOrderNumber=b.deliveryOrderNumber -- Estados Catalyst TAB1
--LEFT JOIN t2 ON t2.deliveryOrderNumber=b.orderNumber -- Estados Catalyst TAB2
LEFT JOIN h ON h.numeroEnvio=d.tracking_code -- Rechazos en puerta
LEFT JOIN p ON p.rastreo=d.tracking_code -- Ingresos TMS + Urbano
--LEFT JOIN k ON k.tracking_code=d.tracking_code -- Info Extra OPL (TMS+Urbano)
LEFT JOIN q ON q.fk_sales_order=a.fk_sales_order -- Info de productos y valorizados -- OK CORREGIDO
LEFT JOIN r ON r.deliveryOrderNumber=b.deliveryOrderNumber AND r.sku=q.sku --RLOs
--LEFT JOIN s ON s.rastreo=d.tracking_code -- Planificación
LEFT JOIN m ON m.deliveryOrderNumber=c.deliveryOrderNumber -- Planificación
LEFT JOIN um ON um.rastreo=d.tracking_code -- Ultima Milla
LEFT JOIN u ON u.orderNumber=b.deliveryOrderNumber -- Estados Backstore
LEFT JOIN v ON v.numeroEnvio=d.tracking_code -- Novedades
LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` pay ON pay.sellerId=c.sellerId AND pay.deliveryOrderNumber=b.deliveryOrderNumber AND pay.sku=q.sku
LEFT JOIN com ON com.global_identifier=q.global_identifier
WHERE a.created_at IS NOT NULL
)

SELECT
  --fk_sales_order,
  date,
  sellerId,
  sellerName,
  orderNumber,
  deliveryOrderNumber,
  shipping_type,
  shipping_provider_product,
  tracking_code,
  tracking_code_fsc,
  multibulto,
  n_rastreos,
  rastreos_asociados,
  deliveryMethod,
  pickupPoint,
  status_backstore,
  status_fsc,
  status_tms,
  status_catalyst,
  --oficina,
  --usuario,
  prev_status_fsc,
  prev_status_tms,
  prev_status_catalyst,
  --prev_oficina,
  --prev_usuario,
  max_status_time_tms,
  sku,
  product_name,
  brand_name,
  primary_category,
  global_identifier,
  size,
  items,
  value,
  CASE WHEN shipping_type!='FBS' THEN 1
       WHEN shipping_type='FBS' AND shipping_provider_product='courier internacional' THEN 1 
       WHEN shipping_type='FBS' AND LENGTH(tracking_code)>10 THEN 1 
       ELSE printed END AS printed,
  CASE WHEN shipping_type='FBS' AND ready_to_ship IS NULL THEN 0 ELSE 1 END AS readied,
  CASE WHEN planned_at IS NOT NULL THEN 1 ELSE 0 END AS planned,
  CASE WHEN visitas>1 THEN 1 ELSE 0 END AS not_collected, --REVISAR ESTA LOGICA
  CASE WHEN shipping_type!='FBF' AND DATE_DIFF(target_to_ship, created_at, DAY)<1 THEN 1 ELSE 0 END AS error_dts,
  CASE WHEN DATE_DIFF(CURRENT_DATE('America/Lima'), max_status_time_tms, DAY)>=2 
            AND status_fsc NOT IN ('delivered','canceled', 'failed', 'failed_delivery','returned','return_shipped_by_customer','return_waiting_for_approval') 
            AND DATE_DIFF(CURRENT_DATE('America/Lima'), target_to_ship, DAY)>=1 THEN 1 
            ELSE 0 END 
            AS not_moving,
  CASE WHEN shipped_at IS NOT NULL THEN 1 ELSE 0 END AS shipped,
  CASE WHEN shipping_type='FBS' AND shipped_at>=DATE_ADD(target_to_ship, INTERVAL -7 DAY) THEN 0
      WHEN shipped_at IS NULL THEN 0
      ELSE 1 END AS advanced,
  CASE WHEN shipping_type='FBS' AND shipped_at>=target_to_ship OR ready_to_ship>=target_to_ship THEN 1
      WHEN shipped_at IS NULL THEN 0
      ELSE 0 END AS delayed, --Revisar logica OTS y OTD
  CASE WHEN status_fsc IN ('canceled', 'failed', 'failed_delivery') THEN 1 ELSE 0 END AS canceled,
  CASE WHEN status_tms='Retorno A Seller' THEN 1 ELSE 0 END AS rejected,
  CASE WHEN status_fsc IN ('failed', 'failed_delivery') THEN 1
       WHEN status_tms IN ('Fallo De Entrega', 'Fallo De Entrega Ct') THEN 1
       WHEN status_tms='Devolucion Al Shipper' THEN 1
      ELSE 0 END AS failed_delivery,
  CASE WHEN status_fsc IN ('canceled','failed','failed_delivery','returned', 'return_shipped_by_customer', 'return_waiting_for_approval') 
      AND shipped_at IS NOT NULL THEN 1 
      WHEN rlo_id IS NOT NULL THEN 1
      ELSE 0 END AS return,
  --CASE WHEN return_wh, cuando el producto es devuelto por almacén (rejected=1, failed_delivery=1)
  --CASE WHEN return_customer, cuando el producto es devuelto por cliente (return=1 y )
  CASE WHEN rlo_id IS NOT NULL THEN 1 ELSE 0 END AS has_rlo,
  CASE WHEN status_tms='Siniestro' THEN 1 -- Siniestros TMS
       WHEN status_tms='Envio Siniestrado' THEN 1 -- Siniestros Urbano
       WHEN status_tms='Entregado' AND status_catalyst IN ('CANCELLED','UNDELIVERED') THEN 1 -- Siniestros entregados a clientes UM
       WHEN status_tms='Entregado' AND status_fsc IN ('canceled', 'failed', 'failed_delivery') THEN 1 -- Siniestros entregados a clientes UM
       WHEN status_backstore='DELIVERED' AND status_catalyst IN ('CANCELLED','UNDELIVERED') THEN 1 -- Siniestros entregados a clientes TIENDA
       WHEN status_backstore='DELIVERED' AND status_fsc IN ('canceled', 'failed', 'failed_delivery') THEN 1 -- Siniestros entregados a clientes TIENDA
       WHEN status_fsc IN ('canceled', 'failed', 'failed_delivery') AND status_catalyst IN ('DELIVERED') THEN 1 -- Siniestros FSC-Catalyst
       ELSE 0 END AS siniestro,
  --payout_pending,
  --gift_option,
  DATE_DIFF(target_to_ship, created_at, DAY) AS days_to_ship,
  visitas,
  visitas_cliente,
  created_at,
  ready_to_ship,
  target_to_ship,
  planned_at,
  shipped_at,
  visited_customer_at,
  terminated_at,
  target_to_customer,
  return_shipped_by_customer,
  return_waiting_for_approval,
  returned_at,
  cancel_reason,
  return_reason,
  return_sub_reason,
  --shipped_to_warehouse, <- IMPORTANTE
  --shipped_to_customer, <- IMPORTANTE
  --return_shipped_to_warehouse, <- IMPORTANTE
  --return_shipped_to_seller,
  rlo_id,
  status_rlo,
  tracking_code_il,
  visitas_il,
  customerDocument,
  customerName,
  customerPhone,
  customerEmail,
  customerRegion,
  customerCity,
  customerDistrict,
  customerAddress,
  --delivery_latitude,
  --delivery_longitude,
  payment_method,
  paid_price,
  shipping_fee,
  shipping_service_cost,
  paid_commission,
  comision,
  ingreso,
  egreso,
  pago,
  descuento,
  ajuste,
  fecha_ingreso,
  fecha_egreso,
  fecha_pago,
  fecha_descuento,
  fecha_ajuste,
  ticket_reembolso,
  novedad_1,
  observacion_novedad1,
  solucion_novedad1,
  tipo_solucion_novedad1,
  usuario_novedad1,
  --creacion_novedad1,
  novedad_2,
  observacion_novedad2,
  solucion_novedad2,
  tipo_solucion_novedad2,
  usuario_novedad2,
  --creacion_novedad2,
  CASE WHEN status_catalyst='DELIVERED' THEN 'delivered'
       WHEN status_catalyst IN ('CANCELLED','UNDELIVERED') THEN 'canceled'
       WHEN status_catalyst IN ('CREATED','CONFIRMED','IN_TRANSIT','AVAILABLE_FOR_PICKUP','OUT_FOR_DELIVERY','DELIVERY_ATTEMPTED','DELIVERED_WITH_EXCEPTION','EXCEPTION') OR status_catalyst IS NULL THEN 'in_progress'
       WHEN status_fsc IN ('returned', 'return_shipped_by_customer', 'return_waiting_for_approval','return_rejected') THEN 'returned'
       ELSE NULL END AS status,
  --fsc_url,
  --label_url,
  ---tracking_url,
  --tracking_il_url,
FROM l
WHERE row=1 -- CASOS RAROS QUE DUPLICAN: 61683598495536, 61683683632783
--AND orderNumber='2075282028'
--ORDER BY 1
--AND document='10277695'

)
