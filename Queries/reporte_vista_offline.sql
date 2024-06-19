CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_vista_offline` AS (

WITH 
d AS (
  WITH
  -- Ingresos y egresos
  debe AS (
    SELECT
      sellerId,
      sellerName,
      orderNumber,
      sku,
      CAST(CASE WHEN egreso*-1>ingreso THEN egreso*-1 ELSE ingreso END AS FLOAT64) AS ingreso,
      CAST(egreso AS FLOAT64) AS egreso,
      fecha_ingreso,
      fecha_egreso,
      refund_ticket,
    FROM (
      SELECT
        a.sellerId,
        a.sellerName,
        a.orderNumber,
        a.sku,
        IFNULL(a.ingreso,0) AS ingreso,
        IFNULL(ABS(CASE WHEN a.egreso IS NULL OR a.egreso=0 THEN b.refund_amount ELSE a.egreso END)*-1,0) AS egreso,
        a.fecha_ingreso,
        CASE WHEN a.fecha_egreso IS NULL THEN b.refund_date ELSE a.fecha_egreso END AS fecha_egreso,
        refund_ticket
      FROM (
        SELECT
          sellerId,
          sellerName,
          deliveryOrderNumber AS orderNumber,
          sku,
          SUM(CASE WHEN event IN ('PAYMENT','ORDERLINE') AND event_status='CHARGED' THEN total ELSE NULL END) AS ingreso,
          SUM(CASE WHEN event IN ('CANCELLATION','RETURN') AND event_status IN ('CANCELLED','RECEIVED','REJECTED') THEN total ELSE NULL END) AS egreso,
          DATE(MAX(CASE WHEN event IN ('PAYMENT','ORDERLINE') AND event_status='CHARGED' THEN event_date ELSE NULL END)) AS fecha_ingreso,
          DATE(MAX(CASE WHEN event IN ('CANCELLATION','RETURN') AND event_status IN ('CANCELLED','RECEIVED','REJECTED') THEN event_date ELSE NULL END)) AS fecha_egreso,
        FROM (
          SELECT DISTINCT
            sellerId,
            sellerName,
            deliveryOrderNumber,
            sku,
            event,
            event_status,
            event_date,
            total
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.reporting.svw_pe_orderlines_event_history`
          --WHERE PARTITION_DATE>='2023-01-01'
          --AND sellerId NOT IN ('FALABELLA','SODIMAC','TOTTUS','FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU')
        )
        GROUP BY 1,2,3,4
      ) a
      LEFT JOIN (
        SELECT 
          orderNumber,
          SUM(refund_amount)*-1 AS refund_amount,
          MAX(refund_date) AS refund_date,
          MAX(caseNumber) AS refund_ticket,
        FROM (
          SELECT
            REGEXP_REPLACE(REGEXP_REPLACE(FC_ExternalIDOrderSeller__c,',| - ','|'), r'\s', '') AS orderNumber,
            a.CaseNumber AS caseNumber,
            FC_GrandTotal__c AS refund_amount,
            DATE(FC_RefundEventDate__c) AS refund_date,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case` a
          WHERE FC_GrandTotal__c>0
          AND FC_RefundStatus__c='paymentRefundSuccess'
        )
        GROUP BY 1
      ) b ON a.orderNumber=b.orderNumber
    )
  ),
  -- Pagos y descuentos
  haber AS (
    SELECT
      sellerId,
      orderNumber,
      sku,
      MAX(comision) AS comision,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='ITEM_PRICE_CREDIT' THEN total ELSE NULL END),0) AS FLOAT64) AS pago,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='ITEM_PRICE' THEN total ELSE NULL END)*-1,0) AS FLOAT64) AS descuento,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('APPEALED_RETURN_CREDIT','LOST_OR_DAMAGED_ORDER_ITEM_LEVEL_CREDIT') THEN total ELSE NULL END),0) AS FLOAT64) AS ajuste,
      DATE(MAX(CASE WHEN transaction_type_name='ITEM_PRICE_CREDIT' THEN event_date ELSE NULL END)) AS fecha_pago,
      DATE(MAX(CASE WHEN transaction_type_name='ITEM_PRICE' THEN event_date ELSE NULL END)) AS fecha_descuento,
      DATE(MAX(CASE WHEN transaction_type_name IN ('APPEALED_RETURN_CREDIT','LOST_OR_DAMAGED_ORDER_ITEM_LEVEL_CREDIT') THEN event_date ELSE NULL END)) AS fecha_ajuste,
    FROM (
      SELECT DISTINCT
        seller_id AS sellerId,
        delivery_order_number AS orderNumber,
        variant_id AS sku,
        transaction_type_name,
        event_arrived_at AS event_date,
        SAFE_DIVIDE(CAST(net_amount_cent AS INT), CAST(net_amount_fraction AS INT)) AS total,
        commission AS comision,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_fcm_corp_drmb_slfi_prd.svw_agg_pe_seller_settlement_transaction_detail`
      --WHERE PARTITION_DATE>='2023-01-01'
      --AND seller_id NOT IN ('FALABELLA','SODIMAC','TOTTUS','FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU')
    )
    GROUP BY 1,2,3
  ),
  -- Items y valorizado
  orden AS (
    SELECT
      orderNumber,
      sku,
      items,
      value,
      (items*value) AS total_value,
    FROM (
      SELECT
        b.orderNumber,
        a.sku,
        CAST(COUNT(a.sku) AS INT64) AS items,
        CAST(SUM(a.paid_price) AS FLOAT64) AS value,
      FROM (
        SELECT DISTINCT
          fk_sales_order,
          sku,
          paid_price,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
      ) a
      LEFT JOIN (
        SELECT DISTINCT
          id_sales_order,
          CAST(order_nr AS STRING) AS orderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order`
        WHERE fk_operator=2
      ) b ON b.id_sales_order=a.fk_sales_order
      GROUP BY 1,2
    )
  )

  SELECT
    sellerId,
    sellerName,
    orderNumber,
    sku,
    items,
    value,
    total_value,
    comision,
    ingreso,
    egreso,
    CASE WHEN pago>ingreso THEN ingreso ELSE pago END AS pago,
    CASE WHEN ABS(descuento)>ABS(egreso) THEN egreso ELSE descuento END AS descuento,
    ajuste,
    fecha_ingreso,
    fecha_egreso,
    fecha_pago,
    fecha_descuento,
    fecha_ajuste,
    ticket_reembolso,
  FROM (
    SELECT
      sellerId,
      sellerName,
      orderNumber,
      sku,
      items,
      value,
      total_value,
      comision,
      ingreso,
      egreso,
      IFNULL(CASE WHEN descuento<0 AND pago=0 THEN ingreso ELSE pago END,0) AS pago,
      descuento,
      ajuste,
      fecha_ingreso,
      fecha_egreso,
      fecha_pago,
      fecha_descuento,
      fecha_ajuste,
      ticket_reembolso,
    FROM (
        SELECT
          a.sellerId,
          a.sellerName,
          a.orderNumber,
          a.sku,
          o.items,
          o.value,
          o.total_value,
          b.comision,
          CASE WHEN a.ingreso>o.total_value THEN o.total_value ELSE a.ingreso END AS ingreso,
          CASE WHEN ABS(a.egreso)>(o.total_value) THEN o.total_value*-1 ELSE a.egreso END AS egreso,
          b.pago AS pago,
          b.descuento AS descuento,
          b.ajuste AS ajuste,
          a.fecha_ingreso,
          a.fecha_egreso,
          b.fecha_pago,
          b.fecha_descuento,
          b.fecha_ajuste,
          refund_ticket AS ticket_reembolso,
        FROM debe a 
        LEFT JOIN haber b ON a.sellerId=b.sellerId AND a.orderNumber=b.orderNumber AND a.sku=b.sku
        LEFT JOIN orden o ON a.orderNumber=o.orderNumber AND a.sku=o.sku
    )
  )
),
a AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY orderNumber,sku ORDER BY fecha_rlo) AS row 
  FROM (
    SELECT
      a.fecha_creacion AS fecha_rlo,
      a.returnMethod,
      a.Tienda,
      a.rlo_id,
      COALESCE(d.sellerId,e.sellerId) AS sellerId,
      COALESCE(d.sellerName,e.sellerName) AS sellerName,
      b.shipping_type, 
      b.shipping_provider_product,
      b.customerDocument,
      b.customerName,
      b.customerPhone,
      b.customerEmail,
      b.customerRegion,
      b.customerCity,
      b.customerDistrict,
      b.customerAddress, 
      a.orderNumber,
      a.sku,
      a.description,
      a.quantityNumber,
      SAFE_DIVIDE(c.value,c.items) AS price,
      c.primary_category AS global_identifier,
      a.size,
      i.created_at,
      i.ready_to_ship,
      i.shipped_at,
      i.delivered_at,
      i.return_shipped_by_customer,
      i.return_waiting_for_approval,
      i.returned_at,
      l.caseNumber AS ticket_devolucion,
      l.caseCreatedDate AS fecha_ticket_devolucion,
      l.razon_devolucion,
      --a.reasonCodeCategory,
      --a.reasonCodeSubCategory,
      a.tracking_number AS tracking_num,
      a.status,
      --a.fecha_registro,
      --a.fecha_planificacion,
      --a.fecha_ruta,
      --a.fecha_devolucion,
      SAFE_CAST(a.visitas_il AS INT64) AS visitas_il,
      SAFE_CAST(h.tickets AS INT64) AS tickets,
      ingreso,
      egreso,
      pago,
      descuento,
      ajuste,
      d.fecha_ingreso,
      d.fecha_egreso,
      d.fecha_pago,
      d.fecha_descuento,
      d.fecha_ajuste,
      d.ticket_reembolso,
      EXTRACT(YEAR FROM a.fecha_creacion) AS year,
      EXTRACT(MONTH FROM a.fecha_creacion) AS month,
      --EXTRACT(YEAR FROM b.created_at) AS year,
      --EXTRACT(MONTH FROM b.created_at) AS month,
      --CASE WHEN DATE_DIFF(COALESCE(a.fecha_creacion, c.fecha_ingreso), c.fecha_egreso, MONTH)<=1 THEN 'Si' ELSE 'No' END AS condicional,
    FROM ( --6823 (TODO BIEN HASTA AQUI)
      SELECT
        a.fecha_creacion,
        a.returnMethod,
        a.Tienda, -- CASE WHEN (corrección tiendas)
        a.rlo_id,
        --c.sellerId, d
        --c.sellerName, d
        --m.shipping_type, b
        --m.shipping_provider_product, b
        a.orderNumber,
        a.sku,
        d.name AS description,
        CAST(a.quantityNumber AS INT64) AS quantityNumber,
        --CAST(COALESCE(f.price,SAFE_DIVIDE(c.ingreso,a.quantityNumber)) AS FLOAT64) AS price,
        CAST(d.primary_category AS STRING) AS category,
        d.variation AS size,
        a.offline,
        --l.caseNumber AS ticket_devolucion,
        --l.caseCreatedDate AS fecha_ticket_devolucion,
        --l.razon_devolucion,
        a.reasonCodeCategory,
        a.reasonCodeSubCategory,
        a.status,
        a.tracking_number,
        --b.created_at,
        --b.delivered_at,
        --b.return_shipped_by_customer,
        --b.return_waiting_for_approval,
        --b.returned_at,
        a.fecha_registro,
        CAST(TIMESTAMP(NULL) AS DATETIME) AS fecha_planificacion,
        CAST(TIMESTAMP(NULL) AS DATETIME) AS fecha_ruta,
        CAST(TIMESTAMP(NULL) AS DATETIME) AS fecha_devolucion,
        CAST(a.visitas_il AS INT64) AS visitas_il,
        --CAST(e.tickets AS INT64) AS tickets,
        --CAST(c.ingreso AS FLOAT64) AS ingreso,
        --CAST(c.egreso AS FLOAT64) AS egreso,
        --CAST(c.pago AS FLOAT64) AS pago,
        --CAST(c.descuento AS FLOAT64) AS descuento,
        --CAST(c.ajuste AS FLOAT64) AS ajuste,
        --c.fecha_ingreso,
        --c.fecha_egreso,
        --c.fecha_pago,
        --c.fecha_descuento,
        --c.fecha_ajuste,
        a.ticket_reembolso,
        EXTRACT(YEAR FROM a.fecha_creacion) AS year,
        EXTRACT(MONTH FROM a.fecha_creacion) AS month,
        --EXTRACT(YEAR FROM b.created_at) AS year,
        --EXTRACT(MONTH FROM b.created_at) AS month,
        --CASE WHEN DATE_DIFF(COALESCE(a.fecha_creacion, c.fecha_ingreso), c.fecha_egreso, MONTH)<=1 THEN 'Si' ELSE 'No' END AS condicional,
      FROM ( -- PENDIENTE REVISAR!
        -- Devoluciones Offline 2023 -- FALTA CORREGIR FECHAS Y DATETIME
        SELECT * FROM (
          SELECT
            SAFE_CAST(PARSE_DATE('%d/%m/%Y', CONCAT(day_solicitud,'/',month_solicitud,'/',year_solicitud)) AS DATETIME) AS fecha_creacion,
            'OFFLINE' AS returnMethod,
            TIENDA AS Tienda,
            'HOME_PICKUP' AS rlo_id,
            ORDEN_SELLER AS orderNumber,
            SKU AS sku,
            SAFE_CAST(1 AS INT64) AS quantityNumber,
            'Yes' AS offline,
            'NA' AS razon_devolucion,
            'NA' AS reasonCodeCategory,
            'NA' AS reasonCodeSubCategory,
            UPPER(COALESCE(ESTADO_ENTREGA,ESTADO_DE_RECOLECCION)) AS status,
            GUIA AS tracking_number,
            SAFE_CAST(PARSE_DATE('%d/%m/%Y', CONCAT(day_solicitud,'/',month_solicitud,'/',year_solicitud)) AS DATETIME) AS fecha_registro,
            SAFE_CAST(NULL AS DATETIME) AS fecha_planificacion,
            SAFE_CAST(NULL AS DATETIME) AS fecha_ruta,
            SAFE_CAST(NULL AS DATETIME) AS fecha_devolucion,
            NULL AS visitas_il,
            NULL AS ticket_reembolso
          FROM (
            SELECT
              * EXCEPT (day_solicitud,month_solicitud,year_solicitud,day_recoleccion,month_recoleccion,year_recoleccion,day_entrega,month_entrega,year_entrega),
              CASE WHEN LENGTH(month_solicitud)=1 THEN CONCAT('0',month_solicitud) ELSE month_solicitud END AS month_solicitud,
              CASE WHEN LENGTH(day_solicitud)=1 THEN CONCAT('0',day_solicitud) ELSE day_solicitud END AS day_solicitud,
              CASE WHEN LENGTH(year_solicitud)=2 THEN CONCAT('20',year_solicitud) ELSE year_solicitud END AS year_solicitud,
              CASE WHEN LENGTH(month_recoleccion)=1 THEN CONCAT('0',month_recoleccion) ELSE month_recoleccion END AS month_recoleccion,
              CASE WHEN LENGTH(day_recoleccion)=1 THEN CONCAT('0',day_recoleccion) ELSE day_recoleccion END AS day_recoleccion,
              CASE WHEN LENGTH(year_recoleccion)=2 THEN CONCAT('20',year_recoleccion) ELSE year_recoleccion END AS year_recoleccion,
              CASE WHEN LENGTH(month_entrega)=1 THEN CONCAT('0',month_entrega) ELSE month_entrega END AS month_entrega,
              CASE WHEN LENGTH(day_entrega)=1 THEN CONCAT('0',day_entrega) ELSE day_entrega END AS day_entrega,
              CASE WHEN LENGTH(year_entrega)=2 THEN CONCAT('20',year_entrega) ELSE year_entrega END AS year_entrega
            FROM (
              SELECT 
                * EXCEPT (Fecha_solicitud,Fecha_Recoleccion,Fecha_entrega),
                SUBSTR(Fecha_solicitud, 1, STRPOS(Fecha_solicitud, '/') - 1) AS month_solicitud,
                SUBSTR(Fecha_solicitud, STRPOS(Fecha_solicitud, '/') + 1, STRPOS(SUBSTR(Fecha_solicitud, STRPOS(Fecha_solicitud, '/') + 1), '/') - 1) AS day_solicitud,
                REGEXP_EXTRACT(Fecha_solicitud, r'[^/]+$') AS year_solicitud,
                SUBSTR(Fecha_Recoleccion, 1, STRPOS(Fecha_Recoleccion, '/') - 1) AS month_recoleccion,
                SUBSTR(Fecha_Recoleccion, STRPOS(Fecha_Recoleccion, '/') + 1, STRPOS(SUBSTR(Fecha_Recoleccion, STRPOS(Fecha_Recoleccion, '/') + 1), '/') - 1) AS day_recoleccion,
                REGEXP_EXTRACT(Fecha_Recoleccion, r'[^/]+$') AS year_recoleccion,
                SUBSTR(Fecha_entrega, 1, STRPOS(Fecha_entrega, '/') - 1) AS month_entrega,
                SUBSTR(Fecha_entrega, STRPOS(Fecha_entrega, '/') + 1, STRPOS(SUBSTR(Fecha_entrega, STRPOS(Fecha_entrega, '/') + 1), '/') - 1) AS day_entrega,
                REGEXP_EXTRACT(Fecha_entrega, r'[^/]+$') AS year_entrega,
              FROM (
                SELECT
                  * EXCEPT(Fecha_solicitud,Fecha_Recoleccion,Fecha_entrega),
                  REGEXP_REPLACE(Fecha_solicitud, '72024', '/2024') AS Fecha_solicitud,
                  REGEXP_REPLACE(Fecha_Recoleccion, '72024', '/2024') AS Fecha_Recoleccion,
                  REGEXP_REPLACE(Fecha_entrega, '72024', '/2024') AS Fecha_entrega,
                FROM (
                  SELECT
                    * EXCEPT(ESTADO_DE_ENTREGA,Fecha_solicitud,FECHA_DE_RECOLECCION,FECHA_DE_ENTREGA),
                    CASE WHEN ESTADO_DE_ENTREGA='0' THEN NULL ELSE ESTADO_DE_ENTREGA END AS ESTADO_ENTREGA,
                    CASE WHEN Fecha_solicitud='0/01/1900' OR Fecha_solicitud='#N/A' OR LENGTH(Fecha_solicitud)<8 THEN NULL ELSE Fecha_solicitud END AS Fecha_solicitud,
                    CASE WHEN FECHA_DE_RECOLECCION='0/01/1900' OR FECHA_DE_RECOLECCION='#N/A' OR LENGTH(FECHA_DE_RECOLECCION)<8  THEN NULL ELSE FECHA_DE_RECOLECCION END AS Fecha_Recoleccion,
                    CASE WHEN FECHA_DE_ENTREGA='0/01/1900' OR FECHA_DE_ENTREGA='#N/A' OR LENGTH(FECHA_DE_ENTREGA)<8  THEN NULL ELSE FECHA_DE_ENTREGA END AS Fecha_entrega,
                  FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.Devoluciones_Offline_2023` 
                )
              )
            )
          )
        )
        UNION ALL
        SELECT 
          PARSE_DATE('%m/%d/%Y',Fecha_registro_Postventa) AS fecha_creacion,
          'HOME_PICKUP' AS returnMethod,
          'HOME_PICKUP' AS Tienda,
          'BIG_TICKET' AS rlo_id,
          Orden_Seller AS orderNumber,
          Sku,
          SAFE_CAST(1 AS INT64) AS quantityNumber,
          'Yes' AS offline,
          Motivo_Cliente AS razon_devolucion,
          'NA' AS reasonCodeCategory,
          'NA' AS reasonCodeSubCategory,
          Estado_de_Entrega_Seller AS status,
          Guia AS tracking_number,
          PARSE_DATE('%m/%d/%Y',Fecha_registro_Postventa) AS fecha_registro,
          PARSE_DATE('%m/%d/%Y',Fecha_registro_Postventa) AS fecha_planificacion, --fecha_planificacion
          NULL AS fecha_ruta,
          NULL AS fecha_devolucion,
          NULL AS visitas_il,
          NULL AS ticket_reembolso,
        FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.Home_Pickup`
        WHERE Fecha_registro_Postventa IS NOT NULL
        UNION ALL
        SELECT 
          PARSE_DATE('%m/%d/%Y',fecha_registro_postventa) AS fecha_creacion,
          'HOME_PICKUP' AS returnMethod,
          'HOME_PICKUP' AS Tienda,
          'BIG_TICKET' AS rlo_id,
          orden_seller AS orderNumber,
          sku,
          SAFE_CAST(1 AS INT64) AS quantityNumber,
          'Yes' AS offline,
          Motivo_Cliente AS razon_devolucion,
          'NA' AS reasonCodeCategory,
          'NA' AS reasonCodeSubCategory,
          Estado_Entrega AS status,
          COALESCE(
            CASE WHEN Guia_Integracion='-' THEN NULL ELSE Guia_Integracion END,
            CASE WHEN Guia_Manual_revision='-' THEN NULL ELSE Guia_Manual_revision END
          ) AS tracking_number,
          PARSE_DATE('%m/%d/%Y',fecha_registro_postventa) AS fecha_registro,
          PARSE_DATE('%m/%d/%Y',fecha_registro_postventa) AS fecha_planificacion, --fecha_planificacion
          NULL AS fecha_ruta,
          NULL AS fecha_devolucion,
          NULL AS visitas_il,
          NULL AS ticket_reembolso,
        FROM bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.Home_Pickup_2024
        WHERE fecha_registro_postventa IS NOT NULL
        --WHERE Tipo_guia='GUÍA MANUAL'
      ) a
      -- Productos 
      LEFT JOIN (
        SELECT DISTINCT
          --fk_seller,
          --sku_seller,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name) AS name,
          --fk_catalog_product_set,
          --fk_catalog_brand,
          --id_catalog_product,
          a.sku,
          primary_category,
          --product_identifier,
          b.size AS variation,
          --approval_status,
          --status,
          --created_at,
          --updated_at,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` a
        LEFT JOIN (
          -- LOGISTIC CATALOG
          SELECT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY sku ORDER BY event_tmst DESC) AS row,
            FROM (
              SELECT DISTINCT
                item_info.offering_id AS sku,
                presentation_footprints.item_size AS size,
                presentation_footprints.length_in_cm,
                presentation_footprints.width_in_cm,
                presentation_footprints.height_in_cm,
                presentation_footprints.weight_in_kg,
                event_attr.event_tmst,
                --presentation_footprints.type,
                --presentation_footprints.units,
                --presentation_footprints.units_UOM
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dfl_prod_trf_corp_corp_dfl_prod.svw_btd_scha_corp_logistic_catalog` a,
              UNNEST(item_info) item_info,
              UNNEST(presentation_footprints) presentation_footprints--,
              --UNNEST(event_attr) event_attr
              --WHERE item_info.offering_id='120472852'
            )
          )
          WHERE row=1
        ) b ON a.sku=b.sku
      ) d ON d.sku=a.sku
    ) a 
    -- Shipping type and provider
    LEFT JOIN (
      SELECT * FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY shipping_type DESC) AS row,
        FROM (
          SELECT
            c.orderNumber,
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
              CAST(order_nr AS STRING) AS orderNumber,
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
        )
      )
      WHERE row=1
    ) b ON b.orderNumber=a.orderNumber
    -- Precios de productos
    LEFT JOIN (
      SELECT * FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY orderNumber,sku ORDER BY product_name DESC) AS row,
        FROM (
          SELECT
            --fk_sales_order,
            orderNumber,
            sku,
            product_name,
            brand_name,
            primary_category,
            global_identifier,
            variation,
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
              variation,
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
          ) a
          LEFT JOIN (
            SELECT DISTINCT
              id_sales_order,
              CAST(order_nr AS STRING) AS orderNumber,
              payment_method,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` -- OK
            WHERE fk_operator=2
          ) b ON a.fk_sales_order=b.id_sales_order
          GROUP BY 1,2,3,4,5,6,7
        )
      )
      WHERE row=1
    ) c ON c.orderNumber=a.orderNumber AND c.sku=a.sku
    -- Cuadre contable
    LEFT JOIN (
      SELECT * FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY orderNumber,sku ORDER BY fecha_ingreso ASC) AS row,
        FROM d
      )
      WHERE row=1
    ) d ON d.orderNumber=a.orderNumber AND d.sku=a.sku
    LEFT JOIN (
      SELECT DISTINCT
        orderNumber,
        deliveryOrderNumber,
        UPPER(a.sellerId) AS sellerId,
        UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(COALESCE(b.sellerName, a.sellerName))) AS sellerName,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`  a
      LEFT JOIN ( -- OK pero no jala con -1 debe -7
        SELECT * FROM (
          SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY sellerName DESC) AS row,
          FROM (
            SELECT
              id_seller,
              src_id AS seller_id,
              UPPER(REGEXP_REPLACE(name,",","")) AS sellerName
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
          )
        )
      WHERE row=1
      ) b ON a.sellerId=b.seller_id
    ) e ON e.deliveryOrderNumber=a.orderNumber
    -- Tickets Clientes
    LEFT JOIN (
      SELECT
        uniqueorderNumber AS orderNumber,
        COUNT(DISTINCT CaseNumber) AS tickets,
      FROM (
        SELECT
          CaseNumber,
          uniqueorderNumber
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`,
        UNNEST(REGEXP_EXTRACT_ALL(FC_ExternalIDOrderSeller__c, r'20.{8}')) AS uniqueorderNumber
        WHERE FC_Level_1__c IN ('Consulta','Interno','Reclamo','Pedido Solicitud')
      )
      GROUP BY 1
    ) h ON h.orderNumber=a.orderNumber
    -- Cruce tiempos FSC
    LEFT JOIN (
      SELECT 
        b.orderNumber,
        --orderLineId,
        MAX(created_at) AS created_at,
        MAX(COALESCE(time_ready_to_ship,time_picked)) AS ready_to_ship,
        MAX(target_to_ship) AS target_to_ship,
        MAX(time_shipped) AS shipped_at,
        MAX(time_delivered) AS delivered_at,
        MAX(time_return_shipped_by_customer) AS return_shipped_by_customer,
        MAX(time_return_waiting_for_approval) AS return_waiting_for_approval,
        MAX(time_returned) AS returned_at,
      FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY fk_sales_order ORDER BY created_at ASC) AS row,
        FROM (
          SELECT
            a.fk_sales_order,
            c.src_status,
            MAX(a.created_at) AS created_at,
            MAX(target_to_ship) AS target_to_ship,
            CASE WHEN c.src_status='delivered' THEN MIN(c.created_at) ELSE MAX(c.created_at) END AS updated_at,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a
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
                target_to_ship,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
            ) b ON a.fk_sales_order_item=b.id_sales_order_item
            LEFT JOIN (
              SELECT DISTINCT
                id_sales_order_item_status,
                name AS src_status,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status`
            ) c ON a.fk_sales_order_item_status=c.id_sales_order_item_status
          ) c ON c.fk_sales_order=a.fk_sales_order
          GROUP BY 1,2
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
        ) a
        LEFT JOIN (
          SELECT DISTINCT
            id_sales_order AS fk_sales_order,
            CAST(order_nr AS STRING) AS orderNumber,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` -- OK
          WHERE fk_operator=2
        ) b ON a.fk_sales_order=b.fk_sales_order
        LEFT JOIN (
          SELECT DISTINCT
            deliveryOrderNumber AS orderNumber,
            lineId AS orderLineId,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_order_lines` a
          LEFT JOIN (
            SELECT
            orderNumber,
            deliveryOrderNumber
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders` 
          ) b ON b.orderNumber=a.orderNumber
        ) c ON c.orderNumber=b.orderNumber
        GROUP BY 1
    ) i ON i.orderNumber=a.orderNumber
    -- Tickets Devoluciones
    LEFT JOIN (
    SELECT
        orderNumber,
        caseCreatedDate,
        caseNumber,
        caseLevel2,
        caseDescription,
        caseClosureComment,
        CASE WHEN REGEXP_CONTAINS(caseDescription,'me equivoque de tamano color o modelo') THEN 'Me equivoque de tamano color o modelo'
            WHEN REGEXP_CONTAINS(caseDescription,'las caracteristicas no coinciden con las de la publicacion|las caracteristicas no corresponden|no es lo que esperaba') THEN 'Las caracteristicas no coinciden con las de la publicacion'
            WHEN REGEXP_CONTAINS(caseDescription,'tiene una falla de funcionamiento|no funciona|falla|fallas|fallado|defectuoso|defecto|no enciende|bloqueado|mal funcionamiento|danado|defectuosa') THEN 'Tiene una falla de funcionamiento'
            WHEN REGEXP_CONTAINS(caseDescription,'me llego un producto distinto al que compre|otro producto|entrega incorrecta|producto no corresponde|producto diferente|producto incorrecto|producto distinto|producto incorrecto|producto equivocado|producto que no corresponde|modelo diferente|diferente modelo') THEN 'Llego un producto distinto al que compre'
            WHEN REGEXP_CONTAINS(caseDescription,'me arrepenti y ya no lo quiero') THEN 'Me arrepenti y ya no lo quiero'
            WHEN REGEXP_CONTAINS(caseDescription,'llego en mal estado o tiene dano estetico|dano estetico|falla estetica|mal estado|roto|rota|malas condiciones') THEN 'Llego en mal estado o tiene dano estetico'
            WHEN REGEXP_CONTAINS(caseDescription,'esta incompleto|incompleto|incompleta|le falta|me falto|/bfaltante/b|/bllego sin/b|/ble llego/b') THEN 'Producto incompleto'
            WHEN REGEXP_CONTAINS(caseDescription,'lo encontre a mejor precio') THEN 'Lo encontre a mejor precio'
            WHEN REGEXP_CONTAINS(caseDescription,'la entrega se atraso y ya no necesito el producto|retraso|retrasado|courrier deleyed') THEN 'La entrega se atraso y ya no necesito el producto'
            WHEN REGEXP_CONTAINS(caseDescription,'el empaque original llego danado') THEN 'Empaque danado'
            WHEN REGEXP_CONTAINS(caseDescription,'cambio de parecer') THEN 'Cambio de parecer'
            WHEN REGEXP_CONTAINS(caseDescription,'entrega no reconocida|siniestro') THEN 'Entrega no reconocida'
            WHEN REGEXP_CONTAINS(caseDescription,'/btalla/b|tamano|no le queda|no me quedaron') THEN 'Error de talla'
            WHEN REGEXP_CONTAINS(caseDescription,'garantia') THEN 'Garantia'
            WHEN REGEXP_CONTAINS(caseDescription,'falsa entrega') THEN 'Falsa entrega'
            WHEN REGEXP_CONTAINS(caseDescription,'direccion incorrecta') THEN 'Direccion incorrecta'
            WHEN REGEXP_CONTAINS(caseDescription,'cliente ausente') THEN 'Cliente ausente'
            WHEN REGEXP_CONTAINS(caseDescription,'fuera de horario') THEN 'Fuera de horario'
            WHEN REGEXP_CONTAINS(caseDescription,'fuera de cobertura|domicilio sin acceso') THEN 'Fuera de cobertura'
            WHEN REGEXP_CONTAINS(caseDescription,'/breprogramacion/b') THEN 'Cliente solicito reprogramacion'
            WHEN REGEXP_CONTAINS(caseDescription,'doble|duplicado|duplicidad') THEN 'Compra duplicada' 
            WHEN REGEXP_CONTAINS(caseDescription,'publicidad enganosa|/benganosa/b') THEN 'Publicidad enganosa'
            WHEN REGEXP_CONTAINS(caseDescription,'/btercera persona/b|/botra persona/b|autorizar tercero|/btercero/b') THEN 'Entrega a tercero'
            WHEN REGEXP_CONTAINS(caseDescription,'problema de cobro') THEN 'Problema de cobro'
            WHEN REGEXP_CONTAINS(caseDescription,'cambio|cambiar') THEN 'Cambio de producto' --SIEMPRE AL ULTIMO
            WHEN REGEXP_CONTAINS(caseDescription,'envio de boleta|envio de factura|/bboleta/b|/bfactura/b') THEN 'Envio de comprobante'
            ELSE 'Otro' END AS razon_devolucion,
        CASE WHEN REGEXP_CONTAINS(caseDescription,'offline|off') THEN 'Si' ELSE 'No' END AS offline
      FROM (    
        SELECT
          orderNumber,
          caseCreatedDate,
          --delivered_at,
          caseNumber,   
          --caseBU,
          --caseLevel1,
          caseLevel2,
          --caseTipification,
          --caseClosureType,
          caseDescription,
          caseClosureComment,
          final_row,
          ROW_NUMBER() OVER (PARTITION BY orderNumber,final_row ORDER BY caseCreatedDate DESC) AS row_num,
        FROM (
          SELECT
            *,
            CASE WHEN row_devoluciones>=1 THEN row_devoluciones ELSE row END AS final_row
          FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY caseCreatedDate ASC) AS row,
              CASE WHEN CaseLevel2='Devoluciones' THEN ROW_NUMBER() OVER (PARTITION BY CASE WHEN CaseLevel2='Devoluciones' THEN orderNumber END ORDER BY caseCreatedDate ASC) ELSE 0 END AS row_devoluciones
            FROM (
              SELECT
                REGEXP_REPLACE(REGEXP_REPLACE(FC_ExternalIDOrderSeller__c,',| - ','|'), r'\s', '') AS orderNumber,
                CreatedDate AS caseCreatedDate,
                i.delivered_at,
                CaseNumber AS caseNumber,   
                --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_BusinessUnit__c) AS caseBU,
                --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_Level_1__c) AS caseLevel1,
                `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Level_2__c) AS caseLevel2,
                --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_TipificationName__c) AS caseTipification,
                --`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_ClosureType__c) AS caseClosureType,
                LOWER(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Description),",|;",""),'"',"")) AS caseDescription,
                LOWER(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_ClosureComment__c),",|;",""),'"',"")) AS caseClosureComment,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case` a
              LEFT JOIN (
                SELECT 
                  b.orderNumber,
                  --orderLineId,
                  MAX(created_at) AS created_at,
                  MAX(time_delivered) AS delivered_at,
                  MAX(time_return_shipped_by_customer) AS return_shipped_by_customer,
                  MAX(time_return_waiting_for_approval) AS return_waiting_for_approval,
                  MAX(time_returned) AS returned_at,
                FROM (
                  SELECT 
                    *,
                    ROW_NUMBER() OVER (PARTITION BY fk_sales_order ORDER BY created_at ASC) AS row,
                  FROM (
                    SELECT
                      a.fk_sales_order,
                      c.src_status,
                      MAX(a.created_at) AS created_at,
                      CASE WHEN c.src_status='delivered' THEN MIN(c.created_at) ELSE MAX(c.created_at) END AS updated_at,
                    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` a
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
                    GROUP BY 1,2
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
                  ) a
                  LEFT JOIN (
                    SELECT DISTINCT
                      id_sales_order AS fk_sales_order,
                      CAST(order_nr AS STRING) AS orderNumber,
                    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` -- OK
                    WHERE fk_operator=2
                  ) b ON a.fk_sales_order=b.fk_sales_order
                  GROUP BY 1
              ) i ON i.orderNumber=a.FC_ExternalIDOrderSeller__c
              --WHERE FC_Country__c ='PE'
              --AND FC_Commerce_Name__c IN ('F.com','GSC')
              WHERE FC_Level_1__c IN ('Consulta','Interno','Reclamo','Pedido Solicitud') -- no debe ser consulta
              --WHERE FC_Level_2__c='Devoluciones'
            ) 
          )
        )
        WHERE caseCreatedDate>=delivered_at
      )
      WHERE row_num=1 AND final_row=1
    ) l ON l.orderNumber=a.orderNumber
  ) a
),
b AS (
  SELECT
    orderNumber,
    sku,
    MAX(row) AS max_row,
  FROM a
  GROUP BY 1,2
),
f AS (
  SELECT 
    COALESCE(a.fecha_rlo,n.fecha_planificacion) AS fecha_creacion,
    a.returnMethod,
    CASE WHEN a.Tienda IS NULL THEN 'NA'
        WHEN REGEXP_CONTAINS(a.Tienda,'GSC') THEN 'NA'
        ELSE a.Tienda 
        END AS Tienda,
    a.rlo_id,
    customerDocument,
    customerName,
    a.sellerId,
    a.sellerName,
    a.shipping_type, 
    a.shipping_provider_product AS shipment_provider, 
    deliveryMethod,
    pickupPoint,
    d.orderNumber AS orderNumber,
    a.orderNumber AS deliveryOrderNumber,
    a.sku,
    a.description,
    brand_name,
    model,
    color_basico,
    color_secundario,
    talla,
    condiciones_del_producto,
    condiciones_de_garantia_del_vendedor,
    detalle_de_la_condicion,
    condiciones_de_garantia,
    garantia_del_proveedor,
    contenido_del_paquete,
    CAST(a.quantityNumber AS INT64) AS quantityNumber,
    CAST(a.price AS FLOAT64) AS price,
    a.global_identifier,
    a.size,
    rec.target_to_ship,
    d.target_to_customer,
    rec.tracking_code_fsc,
    rec.tracking_code,
    rec.printed,
    rec.multibulto,
    rec.n_rastreos,
    rec.rastreos_asociados,
    --visitas_rec,
    a.created_at,
    a.ready_to_ship,
    a.shipped_at,
    a.delivered_at,
    a.return_shipped_by_customer,
    a.return_waiting_for_approval,
    a.returned_at,
    ticket_devolucion,
    fecha_ticket_devolucion,
    razon_devolucion,
    NULL AS reasonCodeCategory,
    NULL AS reasonCodeSubCategory,
    a.fecha_rlo AS fecha_registro,
    n.fecha_planificacion,
    n.fecha_ruta,
    n.fecha_devolucion AS fecha_devolucion,
    a.tracking_num,
    COALESCE(n.status_urbano,a.status) AS status,
    COALESCE(n.visitas_il,a.visitas_il) AS visitas_il,
    a.tickets,
    z.comision,
    SAFE_DIVIDE(a.ingreso,b.max_row) AS ingreso,
    SAFE_DIVIDE(a.egreso,b.max_row) AS egreso,
    SAFE_DIVIDE(a.pago,b.max_row) AS pago,
    SAFE_DIVIDE(a.descuento,b.max_row) AS descuento,
    SAFE_DIVIDE(a.ajuste,b.max_row) AS ajuste,
    COALESCE(fecha_ingreso,DATE(a.created_at)) AS fecha_ingreso,
    fecha_egreso,
    fecha_pago,
    fecha_descuento,
    fecha_ajuste,
    ticket_reembolso,
    --returnOrderLineId,
    --returnOrderLineNumber,
    --a.orderLineId,
    IFNULL(apelacion,0) AS apelacion,
    fecha_apelacion,
    motivo_apelacion,
    estado_apelacion,
    customerPhone,
    customerEmail,
    customerRegion,
    customerCity,
    customerDistrict,
    customerAddress,
    --ST_GEOGPOINT(q.ship_to_long, q.ship_to_lat) AS geolocation_customer,
    --ST_GEOGPOINT(p.ship_from_long, p.ship_from_lat) AS geolocation_rlo,
    --ST_GEOGPOINT(p.ship_to_long, p.ship_to_lat) AS geolocation_return,
    jslibs.h3.ST_H3(ST_GEOGPOINT(q.ship_to_long, q.ship_to_lat), 8) AS h3_8_geo_customer,
    jslibs.h3.ST_H3(ST_GEOGPOINT(p.ship_from_long, p.ship_from_lat), 8) AS h3_8_geo_rlo,
    jslibs.h3.ST_H3(ST_GEOGPOINT(p.ship_to_long, p.ship_to_lat), 8) AS h3_8_geo_return,
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
    EXTRACT(YEAR FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_year,
    EXTRACT(MONTH FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_month,
    EXTRACT(WEEK FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_week,
    EXTRACT(DAY FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_day,
    EXTRACT(DAYOFWEEK FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_weekday,
    EXTRACT(HOUR FROM COALESCE(a.created_at,fecha_ingreso)) AS creation_hour,
    EXTRACT(YEAR FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_year,
    EXTRACT(MONTH FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_month,
    EXTRACT(WEEK FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_week,
    EXTRACT(DAY FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_day,
    EXTRACT(DAYOFWEEK FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_weekday,
    EXTRACT(HOUR FROM COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso))) AS return_hour,
    --CASE WHEN DATE_DIFF(COALESCE(a.created_at,fecha_ingreso), COALESCE(a.return_shipped_by_customer,COALESCE(fecha_rlo,fecha_egreso)), MONTH)<=1 THEN 'Si' ELSE 'No' END AS condicional,
  FROM a
  LEFT JOIN b ON a.orderNumber=b.orderNumber AND a.sku=b.sku
  -- Cruce Urbano
  LEFT JOIN (
    SELECT
      *
    FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY tracking_num ORDER BY FECHA_ESTADO) AS row 
      FROM (
        SELECT 
          GUIA AS tracking_num,
          ESTADO_ACTUAL AS status_urbano,
          DETALLE_ESTADO,
          SAFE_CAST(NRO_VISITAS AS INT64) AS visitas_il,
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', FECHA_SS)) AS fecha_planificacion,
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', FECHA_AO)) AS fecha_ruta,
          --ARRIBO_DESTINO AS fecha_devolucion,
          SAFE_CAST(CASE WHEN ESTADO_ACTUAL='ENTREGADO' THEN FECHA_ESTADO END AS DATETIME) AS fecha_devolucion,
          FECHA_ESTADO,
        FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.ESPEJO_URBANO`
        UNION ALL
        SELECT  
          GUIA AS tracking_num,
          ESTADO_ACTUAL AS status_urbano,
          DETALLE_ESTADO,
          SAFE_CAST(NRO_VISITAS AS INT64) AS visitas_il,
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', FECHA_SS)) AS fecha_planificacion,
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', FECHA_AO)) AS fecha_ruta,
          --ARRIBO_DESTINO AS fecha_devolucion,
          SAFE_CAST(CASE WHEN ESTADO_ACTUAL='ENTREGADO' THEN FECHA_ESTADO END AS DATETIME) AS fecha_devolucion,
          FECHA_ESTADO
        FROM `bi-fcom-drmb-local-pe-sbx.Pidgeotto_Devolucion.IL_URBANO`
      )
    )
    WHERE row=1
  ) n ON n.tracking_num=a.tracking_num
  -- Catalyst
  LEFT JOIN (
    SELECT DISTINCT
      orderNumber,
      deliveryOrderNumber,
      deliveryMethod,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(pickupPointadressName)) AS pickupPoint,
      DATETIME(SAFE_CAST(COALESCE(promisedByDeliveryInfofromDateTime,promisedByDeliveryInfotoDateTime) AS TIMESTAMP), 'America/Lima') AS target_to_customer,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
  ) d ON d.deliveryOrderNumber=a.orderNumber
  -- Coordenadas Return
  LEFT JOIN (
    SELECT DISTINCT
      ord_num AS rlo_id,
      CAST(ship_from.lat AS FLOAT64) AS ship_from_lat,
      CAST(ship_from.long AS FLOAT64) AS ship_from_long,
      CAST(ship_to.lat AS FLOAT64) AS ship_to_lat,
      CAST(ship_to.long AS FLOAT64) AS ship_to_long,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_pe_dfl_prod.svw_svw_vw_acc_scha_3pl_logistics_shipment_created` 
    WHERE shipment_typ='RETURN'
  ) p ON p.rlo_id=a.rlo_id
  -- Coordenadas Forward
  LEFT JOIN (
    SELECT DISTINCT
      ord_num AS orderNumber,
      CAST(ship_from.lat AS FLOAT64) AS ship_from_lat,
      CAST(ship_from.long AS FLOAT64) AS ship_from_long,
      CAST(ship_to.lat AS FLOAT64) AS ship_to_lat,
      CAST(ship_to.long AS FLOAT64) AS ship_to_long,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_pe_dfl_prod.svw_svw_vw_acc_scha_3pl_logistics_shipment_created` 
    WHERE shipment_typ='FORWARD'
  ) q ON q.orderNumber=a.orderNumber
  -- Comisiones
  LEFT JOIN (
    SELECT DISTINCT
      G4 AS global_identifier,
      CAST(Comisiones_f_com AS FLOAT64) AS comision
    FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
  ) z ON z.global_identifier=a.global_identifier
  -- Apelaciones
  LEFT JOIN (
    SELECT DISTINCT
      deliveryOrderNumber,
      sku,
      1 AS apelacion,
      fecha_apelacion,
      INITCAP(
        CASE WHEN REGEXP_CONTAINS(motivo_apelacion,'dano atribuible al ol') THEN 'producto danado (atribuible ol)'
            WHEN REGEXP_CONTAINS(motivo_apelacion,'dano atribuible al cliente') THEN 'producto danado (atribuible usuario)'
            WHEN REGEXP_CONTAINS(motivo_apelacion,'producto incompleto') THEN 'producto incompleto'
            WHEN REGEXP_CONTAINS(motivo_apelacion,'producto no perten') THEN 'producto no pertenece al catalogo del seller'
            ELSE motivo_apelacion END
      ) AS motivo_apelacion,
      estado_apelacion,
    FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber,sku ORDER BY estado_apelacion ASC) AS row,
      FROM (
        SELECT 
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', fecha_de_atencion)) AS fecha_apelacion,
          orden AS deliveryOrderNumber,
          sku_falabella AS sku,
          id_item AS id_sales_order_item,
          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(motivo_seller)) AS motivo_apelacion,
          INITCAP(estado_apelacion) AS estado_apelacion,
          CAST(REGEXP_REPLACE(monto_bruto,'\\,','') AS FLOAT64) AS monto_apelacion,
          'Ticket' AS canal,
        FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.apelaciones_externas_sf`
        UNION ALL
        SELECT 
          DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', fecha_de_atencion)) AS fecha_apelacion,
          orden AS deliveryOrderNumber,
          sku_falabella AS sku, 
          id_sales_order_item,
          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(motivo_seller)) AS motivo_apelacion,
          INITCAP(estado_de_solicitud) AS estado_apelacion,
          CAST(REGEXP_REPLACE(total_bruto,'\\,','') AS FLOAT64) AS monto_apelacion,
          'Modulo' AS canal,
        FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.modulo_de_fsc`
      )
    )
    WHERE row=1
  ) ap ON ap.deliveryOrderNumber=a.orderNumber AND ap.sku=a.sku
    -- Tracking Code Recolección
  LEFT JOIN (
    SELECT 
      fk_sales_order,
      deliveryOrderNumber,
      COALESCE(a.tracking_code_fsc,b.tracking_code_fsc) AS tracking_code_fsc,
      b.tracking_code,
      printed,
      target_to_ship,
      multibulto,
      n_rastreos,
      rastreos_asociados,
    FROM (
      SELECT
        fk_sales_order,
        CASE WHEN LENGTH(tracking_code_fsc)=0 THEN NULL ELSE tracking_code_fsc END AS tracking_code_fsc,
        printed,
        target_to_ship,
        CASE WHEN no_of_parts_of_sku>=2 THEN 'Si' ELSE 'No' END AS multibulto,
      FROM (
        SELECT
          fk_sales_order,
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
                FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio`
                UNION ALL
                SELECT DISTINCT
                  numeroExterno AS deliveryOrderNumber,
                  numero AS tracking_code,
                  nombreEstadoEnvio AS estado,
                  creacion,
                FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio`
                **/
                SELECT DISTINCT
                  deliveryOrderNumber,
                  tracking AS tracking_code,
                  estado,
                  fecha AS creacion
                FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX
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
    ) b ON b.id_sales_order=a.fk_sales_order
  ) rec ON rec.deliveryOrderNumber=a.orderNumber
  -- Novedades
  LEFT JOIN (
    SELECT
      numeroEnvio,
      --MAX(row) AS visitas_rec,
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
      --FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_novedad` -- OK -- NO HAY EN PROD 
      FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envionovedadlog_env_SX
      )
    GROUP BY 1
  ) nov ON nov.numeroEnvio=rec.tracking_code
    -- Logistic Catalog
  LEFT JOIN (
    SELECT 
      sku,
      REGEXP_REPLACE(
        LOWER(CASE WHEN brand_name IS NULL THEN 'GENERICO' ELSE brand_name END),
        r'\s{2,}', ' '
      ) AS brand_name,
      REGEXP_REPLACE(
        LOWER(CASE WHEN model IS NULL THEN sku ELSE model END),
        r'\s{2,}', ' '
      ) AS model,
      LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(color_basico)) AS color_basico,
      LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(color_secundario)) AS color_secundario,
      UPPER(REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(talla)),'talla ','')) AS talla,
      LOWER(f.name) AS condiciones_del_producto,
      LOWER(condiciones_de_garantia_del_vendedor) AS condiciones_de_garantia_del_vendedor,
      LOWER(detalle_de_la_condicion) AS detalle_de_la_condicion,
      LOWER(condiciones_de_garantia) AS condiciones_de_garantia,
      LOWER(g.name) AS garantia_del_proveedor,
      LOWER(contenido_del_paquete) AS contenido_del_paquete
    FROM (
      SELECT 
        a.sku,
        REGEXP_REPLACE(d.name, r'[^a-zA-Z0-9. ]', '') AS brand_name,
        CASE 
          WHEN REGEXP_CONTAINS(UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', '')),'_DELETED') 
          THEN REGEXP_EXTRACT(UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', '')), r'^(.*?)_DELETED') 
          ELSE UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(JSON_EXTRACT_SCALAR(attributes, '$.32')), r'[^a-zA-Z0-9. ]', ''))
          END AS model,
        JSON_EXTRACT_SCALAR(attributes, '$.22') AS condicion_del_producto_id,
        JSON_EXTRACT_SCALAR(attributes, '$.10206') AS condiciones_de_garantia_del_vendedor,
        JSON_EXTRACT_SCALAR(attributes, '$.49') AS detalle_de_la_condicion,
        JSON_EXTRACT_SCALAR(attributes, '$.35') AS condiciones_de_garantia,
        JSON_EXTRACT_SCALAR(attributes, '$.9') AS garantia_del_proveedor_id,
        JSON_EXTRACT_SCALAR(attributes, '$.19') AS contenido_del_paquete,
        COALESCE(JSON_EXTRACT_SCALAR(variation, '$.517'),JSON_EXTRACT_SCALAR(variation, '$.789')) AS color_basico, --789
        COALESCE(JSON_EXTRACT_SCALAR(variation, '$.468'),JSON_EXTRACT_SCALAR(variation, '$.795')) AS color_secundario, --795
        COALESCE(JSON_EXTRACT_SCALAR(variation, '$.502'),JSON_EXTRACT_SCALAR(variation, '$.794')) AS talla
    FROM (
      SELECT DISTINCT
            fk_seller,
            sku_seller,
            REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name)), r'[^a-zA-Z0-9. ]', '') AS name,
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
        SELECT DISTINCT
          fk_catalog_product_set,
          attributes, --JSON
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product_data`
      ) c ON c.fk_catalog_product_set=a.fk_catalog_product_set
      LEFT JOIN (
        SELECT DISTINCT
          id_catalog_brand,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name) AS name,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_brand` 
      ) d ON d.id_catalog_brand=a.fk_catalog_brand
    ) a
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
  ) catalog ON catalog.sku=a.sku
),
off AS (
  SELECT
    fecha_creacion,
    returnMethod,
    Tienda,
    rlo_id,
    customerDocument,
    customerName,
    sellerId,
    sellerName,
    shipping_type,
    shipment_provider,
    deliveryMethod,
    pickupPoint,
    orderNumber,
    deliveryOrderNumber,
    sku,
    description,
    brand_name,
    model,
    color_basico,
    color_secundario,
    talla,
    condiciones_del_producto,
    condiciones_de_garantia_del_vendedor,
    detalle_de_la_condicion,
    condiciones_de_garantia,
    garantia_del_proveedor,
    contenido_del_paquete,
    quantityNumber,
    price,
    global_identifier,
    size,
    target_to_ship,
    target_to_customer,
    tracking_code_fsc,
    tracking_code,
    printed,
    multibulto,
    n_rastreos,
    rastreos_asociados,
    --visitas_rec,
    created_at,
    ready_to_ship,
    shipped_at,
    delivered_at,
    return_shipped_by_customer,
    return_waiting_for_approval,
    returned_at,
    ticket_devolucion,
    fecha_ticket_devolucion,
    razon_devolucion,
    CAST(reasonCodeCategory AS STRING) AS reasonCodeCategory,
    CAST(reasonCodeSubCategory AS STRING) AS reasonCodeSubCategory,
    tracking_num AS tracking_code_il,
    status,
    SAFE_CAST(fecha_registro AS DATETIME) AS fecha_registro,
    SAFE_CAST(fecha_planificacion AS DATETIME) AS fecha_planificacion,
    SAFE_CAST(fecha_ruta AS DATETIME) AS fecha_ruta,
    SAFE_CAST(fecha_devolucion AS DATETIME) AS fecha_devolucion,
    visitas_il,
    tickets,
    comision,
    IFNULL(ingreso,0) AS ingreso,
    IFNULL(egreso,0) AS egreso,
    IFNULL(pago,0) AS pago,
    IFNULL(descuento,0) AS descuento,
    IFNULL(ajuste,0) AS ajuste,
    --SAFE_DIVIDE(CAST(ajuste AS FLOAT64),(1-z.comision)) AS ajuste,
    fecha_ingreso,
    fecha_egreso,
    fecha_pago,
    fecha_descuento,
    fecha_ajuste,
    ticket_reembolso,
    --returnOrderLineId,
    --returnOrderLineNumber,
    --a.orderLineId,
    apelacion,
    fecha_apelacion,
    motivo_apelacion,
    estado_apelacion,
    customerPhone,
    customerEmail,
    customerRegion,
    customerCity,
    customerDistrict,
    customerAddress,
    --ST_GEOGPOINT(q.ship_to_long, q.ship_to_lat) AS geolocation_customer,
    --ST_GEOGPOINT(p.ship_from_long, p.ship_from_lat) AS geolocation_rlo,
    --ST_GEOGPOINT(p.ship_to_long, p.ship_to_lat) AS geolocation_return,
    h3_8_geo_customer,
    h3_8_geo_rlo,
    h3_8_geo_return,
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
    creation_year,
    creation_month,
    creation_week,
    creation_day,
    creation_weekday,
    creation_hour,
    return_year,
    return_month,
    return_week,
    return_day,
    return_weekday,
    return_hour,
    CASE WHEN DATE_DIFF(fecha_ingreso,fecha_egreso, MONTH)<=1 THEN 'Si' ELSE 'No' END AS condicional,
  FROM f
)

SELECT 
    fecha_creacion,
    returnMethod,
    Tienda,
    rlo_id,
    customerDocument,
    customerName,
    sellerId,
    sellerName,
    shipping_type,
    shipment_provider,
    deliveryMethod,
    pickupPoint,
    orderNumber,
    deliveryOrderNumber,
    sku,
    description,
    brand_name,
    model,
    color_basico,
    color_secundario,
    talla,
    condiciones_del_producto,
    condiciones_de_garantia_del_vendedor,
    detalle_de_la_condicion,
    condiciones_de_garantia,
    garantia_del_proveedor,
    contenido_del_paquete,
    quantityNumber,
    price,
    global_identifier,
    size,
    target_to_ship,
    target_to_customer,
    tracking_code_fsc,
    tracking_code,
    printed,
    multibulto,
    n_rastreos,
    rastreos_asociados,
    --visitas_rec,
    created_at,
    ready_to_ship,
    shipped_at,
    delivered_at,
    return_shipped_by_customer,
    return_waiting_for_approval,
    returned_at,
    ticket_devolucion,
    fecha_ticket_devolucion,
    razon_devolucion,
    reasonCodeCategory,
    reasonCodeSubCategory,
    tracking_code_il,
    UPPER(REGEXP_REPLACE(status,'-','')) AS status,
    fecha_registro,
    fecha_planificacion,
    fecha_ruta,
    fecha_devolucion,
    visitas_il,
    tickets,
    comision,
    ingreso,
    egreso,
    pago,
    descuento,
    ajuste,
    --SAFE_DIVIDE(CAST(ajuste AS FLOAT64),(1-z.comision)) AS ajuste,
    fecha_ingreso,
    fecha_egreso,
    fecha_pago,
    fecha_descuento,
    fecha_ajuste,
    ticket_reembolso,
    ingreso + egreso AS debe,
    pago + descuento AS haber,
    (ingreso+egreso)-(pago+descuento) AS saldo,
    CASE WHEN (ingreso+egreso)-(pago+descuento)>=0 THEN 'CxP' ELSE 'CxC' END AS tipo,
    apelacion,
    fecha_apelacion,
    motivo_apelacion,
    estado_apelacion,
    customerPhone,
    customerEmail,
    customerRegion,
    customerCity,
    customerDistrict,
    customerAddress,
    --ST_GEOGPOINT(q.ship_to_long, q.ship_to_lat) AS geolocation_customer,
    --ST_GEOGPOINT(p.ship_from_long, p.ship_from_lat) AS geolocation_rlo,
    --ST_GEOGPOINT(p.ship_to_long, p.ship_to_lat) AS geolocation_return,
    h3_8_geo_customer,
    h3_8_geo_rlo,
    h3_8_geo_return,
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
    creation_year,
    creation_month,
    creation_week,
    creation_day,
    creation_weekday,
    creation_hour,
    return_year,
    return_month,
    return_week,
    return_day,
    return_weekday,
    return_hour,
    condicional,
FROM off


--WHERE a.orderNumber='2108794216'

--CASO RARO: 2061276330 ESTA MAL EL SKU EN DATA OFFLINE PERO EN DATA ORDER NO. SE DEBE REVISAR 
--CASOS DE ORDENES CON INGRESO NULO
--CRUZAR TIENDAS OFFLINE CON TIENDAS DE LAS RLOS
-- SOLO HAY 2 ORDENES QUE ESTAN DUPLICANDO!!! --6823 -> 6825
)
