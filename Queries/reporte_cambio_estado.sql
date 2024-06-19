CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` AS (

WITH
saldos AS (
  WITH
  -- Ingresos y egresos
  debe AS (
    SELECT
      sellerId,
      sellerName,
      deliveryOrderNumber,
      sku,
      CAST(CASE WHEN egreso*-1>ingreso THEN egreso*-1 ELSE ingreso END AS FLOAT64) AS ingreso,
      CAST(egreso AS FLOAT64) AS egreso,
      fecha_ingreso,
      fecha_egreso,
      refund_sol_ticket AS ticket_sol_reembolso,
      refund_ticket AS ticket_reembolso,
      refund_sol_ticket_date AS fecha_sol_ticket_reembolso,
      refund_ticket_date AS fecha_ticket_reembolso,
      cupon
    FROM (
      SELECT
        a.sellerId,
        a.sellerName,
        a.deliveryOrderNumber,
        a.sku,
        IFNULL(a.ingreso,0) AS ingreso,
        IFNULL(ABS(CASE WHEN a.egreso IS NULL OR a.egreso=0 THEN b.refund_amount ELSE a.egreso END)*-1,0) AS egreso,
        a.fecha_ingreso,
        CASE WHEN a.fecha_egreso IS NULL THEN b.refund_date ELSE a.fecha_egreso END AS fecha_egreso,
        refund_sol_ticket,
        refund_ticket,
        refund_sol_ticket_date,
        refund_ticket_date,
        cupon
      FROM (
        SELECT
          sellerId,
          sellerName,
          deliveryOrderNumber,
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
        )
        GROUP BY 1,2,3,4
      ) a
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reembolsos_salesforce` b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
    )
  ),
  -- Pagos y descuentos -- OK!!
  haber AS (
    SELECT
      sellerId,
      a.deliveryOrderNumber,
      a.sku,
      MAX(comision) AS comision,

      -- Cuadre contable
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='ITEM_PRICE_CREDIT' THEN total ELSE NULL END),0) AS FLOAT64) AS pago,
      CAST(IFNULL(COALESCE(MAX(b.descuento),SUM(CASE WHEN transaction_type_name='ITEM_PRICE' THEN total ELSE NULL END)*-1),0) AS FLOAT64) AS descuento,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('APPEALED_RETURN_CREDIT','LOST_OR_DAMAGED_ORDER_ITEM_LEVEL','LOST_OR_DAMAGED_ORDER_ITEM_LEVEL_CREDIT') THEN total ELSE NULL END),0) AS FLOAT64) AS ajuste,

      -- Comisiones
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('COMMISSION_CREDIT','COMMISSION_CREDIT_ADJUSTMENT') THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('COMMISSION','COMMISSION_ADJUSTMENT') THEN total ELSE NULL END),0) AS FLOAT64) 
      AS cobro_comision, 

      -- Oportunidades únicas
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('PROMO_ITEM_PRICE_CREDIT','PROMO_ITEM_PRICE_CREDIT_ADJUST') THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('PROMO_ITEM_PRICE','PROMO_ITEM_PRICE_ADJUST') THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_ou,

      -- Cofinanciamiento logístico
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('FREE_SHIPPING_SELLER_COST_CREDIT','FREE_SHIPPING_SELLER_COST_CREDIT_ADJUSTMENT') THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('FREE_SHIPPING_SELLER_COST','FREE_SHIPPING_SELLER_COST_ADJUSTMENT') THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_cofilog,

      -- Logistics Reprocess Fee
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='LOGISTICS_REPROCESS_FEE_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='LOGISTICS_REPROCESS_FEE' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_logrep,

      -- Reverse Logistics Fee
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='REVERSE_LOGISTICS_FEE_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='REVERSE_LOGISTICS_FEE' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_revlog,

      -- Shipping Fee
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SHIPPING_FEE_RETURN_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SHIPPING_FEE_RETURN' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_loginv,

      -- Shipping Fee Item
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('SHIPPING_FEE_ITEM_LEVEL_CREDIT','SHIPPING_FEE_ITEM_LEVEL_CREDIT_ADJU') THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name IN ('SHIPPING_FEE_ITEM_LEVEL','SHIPPING_FEE_ITEM_LEVEL_ADJU') THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_loginv_item,
      
      -- Shipping Fee Adjust
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SHIPPING_FEE_ADJUST' THEN total ELSE NULL END),0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SHIPPING_FEE_CREDIT_ADJUST' THEN total ELSE NULL END)*-1,0) AS FLOAT64) --descuento 
      AS cobro_loginv_ajuste,

      -- Cobros Fby
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SHIPPING_FEE_FBY' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_ship_fby,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='INBOUND_FEE_FBY' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_inb_fby,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='INVENTORY_REMOVAL_FEE_FBY_CREDIT' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_invrem_fby,
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='LOGIS_REW_REPRO_FEE_FBY' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_logrep_fby,

      -- Cancellation Fee
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='CANCELLATION_FEE_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='CANCELLATION_FEE' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_cancel,

      -- Handling Fee
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='HANDLING_FEE_ITEM_LEVEL_ADJUSTMENT' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_handling_fee,

      -- Productos Patrocinados
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SPONSORED_PRODUCTS_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='SPONSORED_PRODUCTS' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_vas, 
      
      -- Media Management
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='MEDIA_MANAGEMENT_ONLINE_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='MEDIA_MANAGEMENT_ONLINE' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_medios, 

      -- Banners
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='BANNERS_ON_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='BANNERS_ON' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_banner,      

      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='BANNERS_HOME' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_banner_home,

      -- Mailings
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='MAILINGS' THEN total ELSE NULL END),0) AS FLOAT64) AS cobro_mailings,

      -- Falabella Flex
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='FALABELLA_FLEX_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='FALABELLA_FLEX' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_flex,

      -- Apelaciones
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='APPEALED_RETURN_CREDIT' THEN total ELSE NULL END)*-1,0) AS FLOAT64) + --pago
      CAST(IFNULL(SUM(CASE WHEN transaction_type_name='APPEALED_RETURN' THEN total ELSE NULL END),0) AS FLOAT64) --descuento
      AS cobro_apelacion, 

      -- Fechas
      DATE(MAX(CASE WHEN transaction_type_name='ITEM_PRICE_CREDIT' THEN event_date ELSE NULL END)) AS fecha_pago,
      DATE(MAX(CASE WHEN transaction_type_name='ITEM_PRICE' THEN event_date ELSE NULL END)) AS fecha_descuento,
      DATE(MAX(CASE WHEN transaction_type_name IN ('APPEALED_RETURN_CREDIT','LOST_OR_DAMAGED_ORDER_ITEM_LEVEL_CREDIT') THEN event_date ELSE NULL END)) AS fecha_ajuste,

    FROM (
      SELECT DISTINCT
        seller_id AS sellerId,
        delivery_order_number AS deliveryOrderNumber,
        variant_id AS sku,
        transaction_type_name,
        event_arrived_at AS event_date,
        SAFE_DIVIDE(CAST(net_amount_cent AS INT), CAST(net_amount_fraction AS INT)) AS total,
        commission AS comision,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_fcm_corp_drmb_slfi_prd.svw_agg_pe_seller_settlement_transaction_detail`
      --WHERE PARTITION_DATE>='2023-01-01'
      --AND seller_id NOT IN ('FALABELLA','SODIMAC','TOTTUS','FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU')
    ) a
    LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Raichu_Transacciones.descuentos_fuera_sistema` b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
    GROUP BY 1,2,3
  ),
  -- Cuadre contable
  cuadre_contable AS (
    SELECT
      a.sellerId,
      a.sellerName,
      a.deliveryOrderNumber,
      a.sku,
      --o.items,
      --o.value,
      --o.total_value,
      b.comision,
      --ROUND(CASE WHEN a.ingreso>o.total_value THEN o.total_value ELSE a.ingreso END, 2) AS ingreso,
      --ROUND(CASE WHEN ABS(a.egreso)>(o.total_value) THEN o.total_value*-1 ELSE a.egreso END,2) AS egreso,
      ROUND(IFNULL(a.ingreso,0),2) AS ingreso,
      ROUND(IFNULL(a.egreso,0),2) AS egreso,
      ROUND(IFNULL(b.pago,0),2) AS pago,
      ROUND(IFNULL(b.descuento,0),2) AS descuento,
      ROUND(IFNULL(b.ajuste,0),2) AS ajuste,
      a.fecha_ingreso,
      a.fecha_egreso,
      b.fecha_pago,
      b.fecha_descuento,
      b.fecha_ajuste,
      ticket_sol_reembolso,
      ticket_reembolso,
      fecha_sol_ticket_reembolso,
      fecha_ticket_reembolso,
      b.* EXCEPT (sellerId,deliveryOrderNumber,sku,comision,pago,descuento,ajuste,fecha_pago,fecha_descuento,fecha_ajuste)
    FROM debe a 
    LEFT JOIN haber b ON a.sellerId=b.sellerId AND a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
    --LEFT JOIN orden o ON a.deliveryOrderNumber=o.deliveryOrderNumber AND a.sku=o.sku
  ),

  -- REVISAR DESDE AQUI!

  -- Items y valorizado
  orden AS (
    SELECT
      deliveryOrderNumber,
      sku,
      items,
      value,
      value AS total_value,
    FROM (
      SELECT
        b.deliveryOrderNumber,
        a.sku,
        CAST(COUNT(a.sku) AS INT64) AS items,
        CAST(SUM(a.paid_price) AS FLOAT64) AS value,
      FROM (
        SELECT DISTINCT
          fk_sales_order,
          id_sales_order_item,
          sku,
          paid_price,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item`
      ) a
      LEFT JOIN (
        SELECT DISTINCT
          id_sales_order,
          CAST(order_nr AS STRING) AS deliveryOrderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order`
        WHERE fk_operator=2
      ) b ON b.id_sales_order=a.fk_sales_order
      GROUP BY 1,2
    )
  )

  SELECT
    sellerId,
    sellerName,
    deliveryOrderNumber,
    sku,
    --items,
    --value,
    --total_value,
    comision,
    ingreso,
    egreso,
    CASE WHEN pago>ingreso THEN ingreso ELSE pago END AS pago,
    CASE WHEN ABS(descuento)>ABS(egreso) THEN egreso ELSE descuento END AS descuento,
    a.* EXCEPT(sellerId, sellerName, deliveryOrderNumber, sku, items, value, total_value, comision, ingreso, egreso, pago, descuento)
  FROM (
    SELECT
      sellerId,
      sellerName,
      deliveryOrderNumber,
      sku,
      items,
      value,
      total_value,
      comision,
      ingreso,
      egreso,
      IFNULL(CASE WHEN descuento<0 AND pago=0 THEN ingreso ELSE pago END,0) AS pago,
      a.* EXCEPT(sellerId, sellerName, deliveryOrderNumber, sku, items, value, total_value, comision, ingreso, egreso, pago) 
    FROM (
        SELECT
          a.sellerId,
          a.sellerName,
          a.deliveryOrderNumber,
          a.sku,
          o.items,
          o.value,
          o.total_value,
          b.comision,
          ROUND(CASE WHEN a.ingreso>o.total_value THEN o.total_value ELSE a.ingreso END, 2) AS ingreso,
          ROUND(CASE WHEN ABS(a.egreso)>(o.total_value) THEN o.total_value*-1 ELSE a.egreso END,2) AS egreso,
          ROUND(IFNULL(b.pago,0),2) AS pago,
          ROUND(IFNULL(b.descuento,0),2) AS descuento,
          ROUND(IFNULL(b.ajuste,0),2) AS ajuste,
          a.fecha_ingreso,
          a.fecha_egreso,
          b.fecha_pago,
          b.fecha_descuento,
          b.fecha_ajuste,
          ticket_sol_reembolso,
          ticket_reembolso,
          fecha_sol_ticket_reembolso,
          fecha_ticket_reembolso,
          cupon,
          b.* EXCEPT (sellerId,deliveryOrderNumber,sku,comision,pago,descuento,ajuste,fecha_pago,fecha_descuento,fecha_ajuste)
        FROM debe a 
        LEFT JOIN haber b ON a.sellerId=b.sellerId AND a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
        LEFT JOIN orden o ON a.deliveryOrderNumber=o.deliveryOrderNumber AND a.sku=o.sku
        /**
        SELECT * FROM cuadre_contable
        LEFT JOIN orden
        **/
    ) a
  ) a
),
-- Productos
productos AS (
  SELECT
    *,
    SAFE_DIVIDE(value,items) AS unit_price,
  FROM (
    SELECT
      fk_sales_order,
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
    )
    GROUP BY 1,2,3,4,5,6,7
  )
),
-- Rastreos
rastreos AS ( 
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
      orderNumber,
      tracking_code,
      tracking_code_fsc,
      n_rastreos,
      rastreos_asociados,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` a
    LEFT JOIN (
     SELECT 
        orderNumber,
        MAX(n_rastreos) AS n_rastreos,
        MAX(CASE WHEN row_desc=1 OR estado='Entregado' THEN tracking_code ELSE NULL END) AS tracking_code,
        MAX(CASE WHEN row_asc=1 THEN tracking_code ELSE NULL END) AS tracking_code_fsc,
        STRING_AGG(DISTINCT tracking_code, " | ") AS rastreos_asociados,
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY creacion ASC) AS row_asc,
          ROW_NUMBER() OVER (PARTITION BY orderNumber ORDER BY creacion DESC) AS row_desc, --creacion DESC
        FROM (
          WITH a AS (
            SELECT DISTINCT * FROM (
              /**
              SELECT DISTINCT
                ordenServicio AS orderNumber,
                numero AS tracking_code,
                nombreEstadoEnvio AS estado,
                creacion,
              FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio` 
              --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              UNION ALL
              SELECT DISTINCT
                numeroExterno AS orderNumber,
                numero AS tracking_code,
                nombreEstadoEnvio AS estado,
                creacion,
              FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio` 
              --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              **/
              SELECT DISTINCT 
                deliveryOrderNumber AS orderNumber,
                tracking AS tracking_code,
                estado,
                fecha AS creacion
              FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX
            )
          ),
          b AS (
            SELECT
              orderNumber,
              COUNT(DISTINCT tracking_code) AS n_rastreos
            FROM a
            GROUP BY 1
          )

          SELECT
            a.orderNumber,
            n_rastreos,
            tracking_code,
            estado,
            creacion,
          FROM a
          LEFT JOIN b ON a.orderNumber=b.orderNumber
        )
      )
      GROUP BY 1
    ) b ON a.order_nr=b.orderNumber
    WHERE fk_operator=2
  ) c ON c.id_sales_order=a.fk_sales_order -- Rastreos FBF
),
-- Envios
envios AS (
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
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_global_seller_center.svw_shipment_type`
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
---  Ingresos
ingresos AS (
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
    --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env`
    FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS`  
    WHERE nombreEstadoEnvio='En bodega Crossdock'
    GROUP BY 1
    **/
    SELECT
      tracking AS numeroEnvio,
      MAX(DATETIME(fecha, "America/Lima")) AS time_ingreso,
    FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX
    WHERE estado='En bodega Crossdock'
    GROUP BY 1
    UNION ALL
    -- 2do estado (siempre que en bodega crossdock sea nulo)
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
      --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` -- OK -- NO HAY EN PROD
      FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS`
      **/
      SELECT
        tracking AS numeroEnvio,
        estado AS nombreEstadoEnvio,
        DATETIME(fecha, "America/Lima") AS time_ingreso,
        ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha ASC) AS row,
      FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX
    )
    WHERE row=2 AND nombreEstadoEnvio!='Anulado'
  )
  UNION ALL
  -- Urbano
  SELECT
    cod_rastreo AS rastreo,
    DATE(MAX(CAST(datetime AS DATETIME))) AS fecha_registro,
  FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.urbano.Api3Pl_Urbano_Prod` -- OK -- NO HAY EN PROD
  WHERE estado='ADMITIDO EN HUB'
  GROUP BY 1
),
-- Rechazos en puerta
rechazos AS (
  SELECT * FROM (
    /**
    SELECT
      numeroEnvio,
      REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(nombreEstadoEnvio)),' ','_') AS nombreEstadoEnvio,
      MAX(DATETIME(creacion, "America/Lima")) AS creacion,
    --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` -- OK -- NO HAY EN PROD
    FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS`
    WHERE LOWER(nombreEstadoEnvio)='retorno a seller'
    GROUP BY 1,2
    **/
    SELECT
      tracking AS numeroEnvio,
      REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(estado)),' ','_') AS nombreEstadoEnvio,
      MAX(DATETIME(fecha, "America/Lima")) AS creacion,
    FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX
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
-- Ultima Milla
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
      --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` -- OK -- NO HAY EN PROD
      FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS`
      WHERE nombreEstadoEnvio IN ('Excepción de Entrega', 'Entregado')
      **/
      SELECT
        tracking AS rastreo,
        DATETIME(fecha, "America/Lima") AS fecha_visita,
        ROW_NUMBER() OVER (PARTITION BY tracking ORDER BY fecha ASC) AS row,
      FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX
      WHERE estado IN ('Excepción de Entrega', 'Entregado')
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
-- ** ESTADOS **
-- Estados FSC
fsc AS (
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_fsc, 1) OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS prev_status_fsc,
      LEAD(status_time_fsc, 1) OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS prev_status_time_fsc,
      ROW_NUMBER() OVER (PARTITION BY fk_sales_order ORDER BY status_time_fsc DESC) AS row,
    FROM (
      SELECT
        fk_sales_order,
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
      GROUP BY 1,2
    )
  )
  WHERE row=1
),
-- Estados Catalyst
catalyst AS (
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_catalyst, 1) OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time_catalyst DESC) AS prev_status_catalyst,
      LEAD(status_time_catalyst, 1) OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time_catalyst DESC) AS prev_status_time_catalyst,
      ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY status_time_catalyst DESC) AS row,
    FROM (
      SELECT 
        deliveryOrderNumber,
        status_catalyst,
        status_time_catalyst
      FROM (
        SELECT DISTINCT
          orderNumber,
          lineId AS orderLineId,
          lineNumber,
          status_orderLine AS status_catalyst,
          DATETIME(CAST(statusEventTime_orderLine AS TIMESTAMP)) AS status_time_catalyst
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_order_lines`
      ) a
      LEFT JOIN (
        SELECT DISTINCT
          orderLineId,
          deliveryOrderNumber,
          orderNumber,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
        --WHERE PARTITION_DATE>="2022-08-01"
      ) b ON a.orderLineId=b.orderLineId
      --WHERE PARTITION_DATE>="2022-08-01"
    )
  )
  WHERE row=1
),
--- Estados TMS + Urbano
tms AS ( -- OK
  SELECT * FROM (
    SELECT
      *,
      LEAD(status_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time_tms DESC) AS prev_status_tms,
      LEAD(status_time_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time_tms DESC) AS prev_status_time_tms,
      --LEAD(oficina_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time_tms DESC) AS prev_oficina_tms,
      --LEAD(usuario_tms, 1) OVER (PARTITION BY numeroEnvio ORDER BY status_time_tms DESC) AS prev_usuario_tms,
      ROW_NUMBER() OVER (PARTITION BY numeroEnvio ORDER BY status_time_tms DESC) AS row,
    FROM (
      -- TMS
      /**
      SELECT DISTINCT
        a.numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreEstadoEnvio)) AS status_tms,
        DATETIME(a.creacion, "America/Lima") AS status_time_tms,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreOficina)) AS oficina_tms,  
        INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS usuario_tms, 
        INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
      --FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` a -- OK -- NO HAY EN PROD
      FROM `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS` a
      --LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.seller_experience_reporting.copia_TMS_envio` b ON a.numeroEnvio=b.numeroExterno
      **/
      SELECT DISTINCT
        a.tracking AS numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.estado)) AS status_tms,
        DATETIME(a.fecha, "America/Lima") AS status_time_tms,
      FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX a
      LEFT JOIN tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX b ON a.tracking=b.tracking
      UNION ALL
      -- Urbano
      SELECT DISTINCT
        cod_rastreo AS numeroEnvio,
        INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(estado)) AS status_tms,
        CAST(datetime AS DATETIME) AS status_time_tms,
        --agencia AS oficina_tms,
        --'Urbano' AS usuario_tms,
        --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Direccion_cliente),",|;",""),'"',"")) AS direccion,
      FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.urbano.Api3Pl_Urbano_Prod` -- OK -- NO HAY EN PROD
    ) 
  )
  WHERE row=1
),
-- Estados Backstore
backstore AS (
  SELECT 
    order_number AS deliveryOrderNumber, 
    order_date AS status_time_backstore, 
    pckg_status AS status_backstore
  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_svw_backstore_customer_care_product`
  WHERE country = 'PE'
  AND order_number like '2%'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY order_number ORDER BY pckg_udpt_dt DESC) = 1
),
-- Comisiones
com AS (
  SELECT DISTINCT
    G4 AS global_identifier,
    Comisiones_f_com AS comision
  FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_reporting.svw_comisiones_fcom_pe
),
-- Segmentacion y status seller
datos_seller AS (
  SELECT
    a.*,
    b.seller_status,
    c.last_delivered_order_date
  FROM (
    SELECT
      id_seller AS sellerId,
      valor AS segmento,
    FROM `bi-fcom-drmb-sell-in-sbx.segmentacion_valor_sellers.peru`
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id_seller ORDER BY valor DESC)=1
  ) a
  LEFT JOIN (
    SELECT
      src_id AS sellerId,
      id_seller AS fk_seller,
      CASE WHEN delist_status=1 THEN 'Inactivo'
           WHEN delist_status=0 THEN 'Activo'
           ELSE NULL END AS seller_status,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
  ) b ON a.sellerId=b.sellerId
  LEFT JOIN (
    SELECT 
      fk_seller,
      DATE(MAX(CASE WHEN name='delivered' THEN created_at ELSE NULL END)) AS last_delivered_order_date
    FROM (
      SELECT DISTINCT
        fk_seller,
        fk_sales_order,
        id_sales_order_item,
        src_id AS seller_order_line_id,
        sku,
        fk_sales_order_item_status,
        created_at,
        updated_at,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item` 
    ) a
    LEFT JOIN (
      SELECT DISTINCT
        id_sales_order_item_status,
        name
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order_item_status`
    ) b ON a.fk_sales_order_item_status=b.id_sales_order_item_status
    GROUP BY 1
  ) c ON b.fk_seller=c.fk_seller
),
-- Escalado postventa
escalamientos AS (
  SELECT * FROM (
    SELECT
      *,
      COALESCE(
        CASE WHEN resolucion_sx='Notificacion a seller' THEN NULL ELSE resolucion_sx END,
        CASE WHEN resolucion_postventa='Flujo OPL sin errores' THEN NULL ELSE resolucion_postventa END,
        resolucion_sac,
        resolucion_caso
      ) AS resolucion_final
    FROM (
      SELECT 
        *,
        CASE 
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'pedido entregado|compra entregada|orden entregada|tms entregado|se verifica entrega|seller confirma entrega|cliente confirma entrega|orden fue entregada|cliente acepto la entrega|seller se comunico con nosotros y nos menciona que ya realizo la entrega al cliente') THEN 'Pedido entregado al cliente'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'cliente no respond[a-z]{1,2}|cliente sin exito|falta de respuesta del cliente|no hubo confirmacion del cliente|cliente no contesta|no recibir respuesta de parte del cliente|cliente no contesta|cl no responde correo|cl no contesta|cliente no ha respondido|sin respuesta por parte de[a-z]{0,1} cliente|falta de respuesta del cl|falta de contacto con el cliente|se llama sin exito al cliente') THEN 'Cliente no responde'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'improcedente|reclamo no corresponde|cliente no mando evidencia|no envia evidencia|no fue facilitada la evidencia|cliente no ha enviado las evidencias|falta de evidencia por parte de cliente|reclamo no procede') THEN 'Caso improcedente o sin evidencias'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'no haber respuesta por parte del seller|al haber excedido el plazo de respuesta por parte del seller|habiendo superado el tiempo para que el seller de una solucion|no tener rpta del seller|seller no brind[a-z]{1,2} rpta|sin respuesta de seller|sin respuesta por parte de seller|falta de respuesta de seller|falta de respuesta por parte del seller|seller no respond[a-z]{1,2}|seller excede tiempo de respuesta|no hay respuesta de seller|no hay respuesta de parte de seller|seller no ha respondido|seller no se contacto|seller no se volvio a contactar|falta de respuesta seller|falta de respuesta del seller|seller no ha dado respuesta|no se tuvo respuesta por parte del seller|seller sin exito|seller excede el tiempo de respuesta|proveedor no contacto|proveedor de la compra no respondio|no se obtiene respuesta de seller|seller se puso en contacto pero cliente indica que no|no se obtuvo respuesta de seller|al no haber respuesta del seller|proveedor no respondio|no hubo respuesta por parte de seller|falta de respuesta del sx|no existe respuesta por parte de proveedor|senala que seller no se comunico') THEN 'Seller no responde'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'hd indica proceder a favor del cl|hd proceder a favor del cliente|hd se procede a favor del cliente|hd se crea caso de reembolso|hd se procede a crear|hd se procede a favor del cliente|se crea caso de reembolso con aprobacion de hd|cargo a hd|entregado sin evidencia|error del opl|evidencia tms no existe|evidencia tms ilegible') THEN 'Error de OPL'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'autorizacion de sx|seller responde e indica que se proceda a favor del cliente|seller solicita proceder a favor del cl|seller indica proceder con reembolso|seller indica proceder a favor del cl|seller menciona proceder a favor del cl|sin sustento seller|sx indica a favor del cl|sx se procede a favor del cliente|seller indica reembolsar|sx proceder a favor de[a-z]{0,1} cl|sx proceder con reembolso|seller no cumplio|falta de stock|no contar con stock|no tener stock|sx indica procede a favor del cl|seller indica que no realizo el envio|no cuenta con stock|seller indica que no realizo el envio|con aprobacion de sx|seller confirma producto faltante|seller indica que se proceda con la devolucion|seller indica que no tiene evidencia|seller en[a-z]{1,2}a evidencia|seller respondio indicando que se proceda con el reembolso|seller responde correo e indica que se proceda a favor del cliente|seller nos informa que no cuentan con stock|seller no podra atender al cliente|seller da respuesta se proceda a favor de cliente|seller indica proceder al reembolso|seller confirma envio de pedido incompleto|seller no despacho|seller indica que no cuenta con el stock') THEN 'Error de seller'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'cliente realizar la devolucion|cliente devolvera el producto incorrecto') THEN 'Producto devuelto por el cliente'
          WHEN REGEXP_CONTAINS(caso_comentario_sac,'mediacion') THEN 'Caso cerrado por mediacion'
          --WHEN REGEXP_CONTAINS(caso_comentario_sac,'se pasa atencion en|se cier[a-z]{0,1}a caso') THEN 'Caso cerrado'
          ELSE NULL END AS resolucion_sac,
        CASE 
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'devuelto al seller') THEN 'Devuelto a seller'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'no fue entregado a cliente|cliente no cuenta con pedido|no se entregara a cliente|pedido no logro ser entregado') THEN 'Pedido no entregado a cliente'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'opl tiene el pedido en su poder|opl no logr[a-z]{1,2} realizar pedido|opl no logro regularizar entrega de pedido|opl no cuenta con sustento|opl senala que por error|opl marco por error|opl no cuenta con las evidencias|opl no cuenta con evidencias|no brindo los sustentos|opl confirma proceder a|opl confirma se proceda|opl no cuenta con evidencias de entrega|transporte indica proceder a favor de cliente|opl no cuenta con los sustentos de entrega|opl no presenta sustento|opl no logro realizar la entrega cliente') THEN 'Error de OPL'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'opl no responde|opl no brind[a-z]{0,1}|opl no brind[a-z]{0,1} respuesta|opl sin rpta|sin rpta|transportista no brinda respuesta|no brinda respuesta|encargado no brind[a-z]{0,1} rpta ni sustento') THEN 'OPL no responde'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'segun despacho seller|segun despacho de[a-z]{0,1} seller|segun el despacho de[a-z]{0,1} seller') THEN 'Flujo OPL sin errores'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'siniestro|siniestrado') THEN 'Siniestro OPL'
          WHEN REGEXP_CONTAINS(caso_comentario_postventa,'falta de respuesta del seller') THEN 'Seller no responde' 
          ELSE NULL END AS resolucion_postventa,
        CASE 
          WHEN REGEXP_CONTAINS(caso_comentario_sx,'seller indica por correo que se proceda a favor del cliente|proceda a favor del cliente|proceder a favor del cliente|proceder a favor de cliente con reembolso|favor de[a-z]{0,1} cliente|proceda con reembolso|proceder con reembolso') THEN 'Error de seller'
          WHEN REGEXP_CONTAINS(caso_comentario_sx,'se le notific[a-z]{0,1} al seller') THEN 'Notificacion a seller' 
          WHEN REGEXP_CONTAINS(caso_comentario_sx,'seller no respond[a-z]{0,4}|no brind[a-z]{1,2} respuesta') THEN 'Seller no responde' 
          ELSE NULL END AS resolucion_sx,
        CASE 
          WHEN REGEXP_CONTAINS(caso_cierre_comentario,'devuelto al seller') THEN 'Devuelto a seller'
          WHEN REGEXP_CONTAINS(caso_cierre_comentario,'opl tiene el pedido en su poder|opl no logr[a-z]{1,2} realizar pedido') THEN 'Error de OPL'
          WHEN REGEXP_CONTAINS(caso_cierre_comentario,'opl no responde|opl no brind[a-z]{0,1}|opl no brind[a-z]{0,1} respuesta|opl sin rpta|sin rpta|transportista no brinda respuesta') THEN 'OPL no responde'
          WHEN REGEXP_CONTAINS(caso_cierre_comentario,'segun despacho seller|segun despacho de[a-z]{0,1} seller|segun el despacho de[a-z]{0,1} seller') THEN 'Flujo OPL sin errores'
          WHEN REGEXP_CONTAINS(caso_cierre_comentario,'siniestro|siniestrado') THEN 'Siniestro OPL'
          ELSE NULL END AS resolucion_caso,
      FROM (
        SELECT
          x.deliveryOrderNumber,
          x.caseNumber AS ticket_reclamo,
          x.tipificacion_reclamo,
          x.tipo_reclamo,
          x.fecha_reclamo,
          IFNULL(a.escalado_postventa,'No') AS escalado_postventa,
          IFNULL(b.cancelado_postventa,'No') AS cancelado_postventa,
          IFNULL(c.escalado_sx,'No') AS escalado_sx,
          e.caseDescription AS caso_descripcion,
          a.taskNumber AS tarea_esc_postventa,
          a.taskResolutionType AS tarea_tipo_esc_postventa,
          a.taskResolutionReason AS tarea_res_esc_postventa,
          a.taskRequiredSolution AS tarea_sol_esc_postventa,
          b.taskNumber AS tarea_canc_postventa,
          b.taskResolutionType AS tarea_tipo_canc_postventa,
          b.taskResolutionReason AS tarea_res_canc_postventa,
          b.taskRequiredSolution AS tarea_sol_canc_postventa,
          a.caseClosureType AS caso_cierre_tipo,
          a.caseClosureComment AS caso_cierre_comentario,
          d.commentBody AS caso_comentario_sac,
          a.commentBody AS caso_comentario_postventa,
          c.commentBody AS caso_comentario_sx
        FROM (
          --SELECT * FROM (
            SELECT DISTINCT
              deliveryOrderNumber,
              caseNumber,-- AS ticket_reclamo,
              caseTipification AS tipificacion_reclamo,
              tipoReclamo AS tipo_reclamo,
              date AS fecha_reclamo,
              caseCreatedDate
            FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce`
            WHERE caseLevel1='Reclamo'
          --)
          --QUALIFY ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY caseCreatedDate ASC)=1
        ) x
        LEFT JOIN (
          SELECT * FROM (
            SELECT
              a.caseNumber,
              a.caseClosureType,
              a.caseClosureComment,
              b.commentBody,
              a.taskNumber,
              a.taskLastModifiedBy,
              a.taskResolutionType,
              a.taskResolutionReason,
              a.taskRequiredSolution,
              a.taskCompletedDate,
              a.escalado_postventa,
            FROM (
              SELECT 
                caseNumber,
                caseClosureType,
                caseClosureComment,
                taskNumber,
                taskLastModifiedBy,
                taskResolutionType,
                taskResolutionReason,
                taskRequiredSolution,
                taskCompletedDate,
                CASE WHEN taskBU='AC_HD_PE_PostVenta3P' OR taskLastModifiedBy IN ('Nataly Flores','Sofia Tacuchi Cruz') THEN 'Si' ELSE 'No' END AS escalado_postventa,
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_tareas_salesforce` 
              WHERE taskType='Task'
            ) a
            LEFT JOIN (
              SELECT
                caseNumber,
                commentOwner AS taskLastModifiedBy,
                commentBody
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comentarios_casos_salesforce`
            ) b ON a.caseNumber=b.caseNumber AND a.taskLastModifiedBy=b.taskLastModifiedBy
            WHERE a.escalado_postventa='Si'
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY taskCompletedDate DESC)=1
        ) a ON x.caseNumber=a.caseNumber
        LEFT JOIN (
          SELECT * FROM (
            SELECT
              a.caseNumber,
              a.caseClosureType,
              a.caseClosureComment,
              b.commentBody,
              a.taskNumber,
              a.taskLastModifiedBy,
              a.taskResolutionType,
              a.taskResolutionReason,
              a.taskRequiredSolution,
              a.taskCompletedDate,
              a.cancelado_postventa,
            FROM (
              SELECT 
                caseNumber,
                caseClosureType,
                caseClosureComment,
                taskNumber,
                taskLastModifiedBy,
                taskResolutionType,
                taskResolutionReason,
                taskRequiredSolution,
                taskCompletedDate,
                CASE WHEN taskBU='AC_HD_PE_tareas_Anulaciones' AND caseClosureType='Procede' THEN 'Si'
                    WHEN taskLastModifiedBy IN ('Karla Huaytalla Cardoso') AND caseClosureType='Procede' THEN 'Si'
                    ELSE 'No' END AS cancelado_postventa 
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_tareas_salesforce` 
              WHERE taskType='Task'
            ) a
            LEFT JOIN (
              SELECT
                caseNumber,
                commentOwner AS taskLastModifiedBy,
                commentBody
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comentarios_casos_salesforce`
            ) b ON a.caseNumber=b.caseNumber AND a.taskLastModifiedBy=b.taskLastModifiedBy
            WHERE a.cancelado_postventa='Si'
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY taskCompletedDate DESC)=1
        ) b ON x.caseNumber=b.caseNumber
        LEFT JOIN (
          SELECT * FROM (
            SELECT
              a.caseNumber,
              a.caseClosureType,
              a.caseClosureComment,
              b.commentBody,
              a.taskNumber,
              a.taskLastModifiedBy,
              a.taskResolutionType,
              a.taskResolutionReason,
              a.taskRequiredSolution,
              a.taskCompletedDate,
              a.escalado_sx,
            FROM (
              SELECT 
                caseNumber,
                caseClosureType,
                caseClosureComment,
                taskNumber,
                taskLastModifiedBy,
                taskResolutionType,
                taskResolutionReason,
                taskRequiredSolution,
                taskCompletedDate,
                CASE WHEN taskLastModifiedBy IN ('Darwing Arroyo','Elida  Castro','Fernando Changanaqui','Javier La Torre','Michelle Banon','Rodrigo Teran','Royer Tacuri') THEN 'Si' ELSE 'No' END AS escalado_sx
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_tareas_salesforce` 
              WHERE taskType='Task'
            ) a
            LEFT JOIN (
              SELECT
                caseNumber,
                commentOwner AS taskLastModifiedBy,
                commentBody
              FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comentarios_casos_salesforce`
            ) b ON a.caseNumber=b.caseNumber AND a.taskLastModifiedBy=b.taskLastModifiedBy
            WHERE a.escalado_sx='Si'
          )
          QUALIFY ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY taskCompletedDate DESC)=1
        ) c ON x.caseNumber=c.caseNumber
        LEFT JOIN (
          SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comentarios_casos_salesforce`
          QUALIFY ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY CommentDate DESC)=1
        ) d ON x.caseNumber=d.caseNumber
        LEFT JOIN (
          SELECT DISTINCT
            caseNumber,
            caseDescription
          FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` 
        ) e ON x.caseNumber=e.caseNumber
      )
    )
    --WHERE NOT REGEXP_CONTAINS(caso_comentario_sac,'se pasa atencion|fuera de sla|se crea caso nuevo')
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY fecha_reclamo ASC)=1
),
-- Devoluciones Todo
devoluciones AS (
  WITH 
  devoluciones AS (
    SELECT DISTINCT * FROM (
      SELECT 
        deliveryOrderNumber,
        sku,
        CASE
          WHEN rlo_id='HOME_PICKUP' AND Tienda IN ('NA','HOME_PICKUP') THEN 'HOME_PICKUP'
          WHEN REGEXP_CONTAINS(Tienda,'GSC-SC') THEN NULL
          WHEN Tienda='NA' THEN NULL
          ELSE Tienda END AS Tienda,
        reason_code_category,
        reason_code_sub_category,
        estado_consolidado_il,
        guia_il,
        CASE WHEN rlo_id='NA' THEN NULL ELSE rlo_id END AS rlo_id,
        estado_il,
        fecha_egreso,
        fecha_solicitud_il,
        fecha_recoleccion_il,
        fecha_entrega_il,
        rango_dias_devolucion
      FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_devoluciones` 
      WHERE deliveryOrderNumber IS NOT NULL
      AND sku IS NOT NULL
    )
  ),
  n AS (
    SELECT
      deliveryOrderNumber,
      sku,
      COUNT(DISTINCT guia_il) AS n_guias,
      COUNT(DISTINCT rlo_id) AS n_rlos,
      COUNT(DISTINCT Tienda) AS n_tiendas,
      COUNT(DISTINCT reason_code_category) AS n_reason_cd,
      COUNT(DISTINCT reason_code_sub_category) AS n_reason_cds,
    FROM devoluciones
    GROUP BY 1,2
  ),
  devoluciones_todo AS (
    SELECT
      a.*,
      b.n_guias,
      b.n_rlos,
      b.n_tiendas,
      b.n_reason_cd,
      b.n_reason_cds
    FROM devoluciones a
    LEFT JOIN n b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
  )

  SELECT
    deliveryOrderNumber,
    sku,
    CASE 
      WHEN fecha_recoleccion_il IS NOT NULL THEN 'Entregado'
      ELSE estado_consolidado_il END AS estado_consolidado_il,
    * EXCEPT(deliveryOrderNumber,sku,estado_consolidado_il)
  FROM (
    SELECT
      deliveryOrderNumber,
      sku,
      MAX(estado_consolidado_il) AS estado_consolidado_il,
      COUNT(DISTINCT guia_il) AS n_guias,
      COUNT(DISTINCT rlo_id) AS n_rlos,
      CASE WHEN MAX(n_guias)=1 THEN MAX(guia_il) ELSE STRING_AGG(guia_il, '|') END AS guia_il,
      CASE WHEN MAX(n_rlos)=1 THEN MAX(rlo_id) ELSE STRING_AGG(rlo_id, '|') END AS rlo_id,
      CASE WHEN MAX(n_tiendas)=1 THEN MAX(Tienda) ELSE STRING_AGG(Tienda, '|') END AS Tienda,
      CASE WHEN MAX(n_reason_cd)=1 THEN MAX(reason_code_category) ELSE STRING_AGG(reason_code_category, '|') END AS reason_code_category,
      CASE WHEN MAX(n_reason_cds)=1 THEN MAX(reason_code_sub_category) ELSE STRING_AGG(reason_code_sub_category, '|') END AS reason_code_sub_category,
      MAX(fecha_solicitud_il) AS fecha_solicitud_il,
      MAX(fecha_recoleccion_il) AS fecha_recoleccion_il,
      MAX(fecha_entrega_il) AS fecha_entrega_il,
      COUNT(DISTINCT CASE WHEN rango_dias_devolucion='30+' THEN guia_il ELSE NULL END) AS guias_il_fuera_de_plazo,
    FROM devoluciones_todo a
    GROUP BY 1,2
  )
),
devoluciones_salesforce AS (
  SELECT
    deliveryOrderNumber,
    DATE(caseCreatedDate) AS fecha_ticket_devolucion,
    caseNumber AS ticket_devolucion,
    caseLevel2,
    caseDescription,
    caseClosureComment,
    CASE 
      WHEN REGEXP_CONTAINS(caseDescription,'me equivoque de tamano color o modelo') THEN 'Me equivoque de tamano color o modelo'
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
      deliveryOrderNumber,
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
      ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber,final_row ORDER BY caseCreatedDate DESC) AS row_num,
    FROM (
      SELECT
        *,
        CASE WHEN row_devoluciones>=1 THEN row_devoluciones ELSE row END AS final_row
      FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY caseCreatedDate ASC) AS row,
          CASE 
            WHEN CaseLevel2='Devoluciones' THEN ROW_NUMBER() OVER (PARTITION BY CASE WHEN CaseLevel2='Devoluciones' THEN deliveryOrderNumber END ORDER BY caseCreatedDate ASC) 
            ELSE 0 END AS row_devoluciones
        FROM (
          SELECT
            REGEXP_REPLACE(REGEXP_REPLACE(FC_ExternalIDOrderSeller__c,',| - ','|'), r'\s', '') AS deliveryOrderNumber,
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
              b.deliveryOrderNumber,
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
                    CAST(order_nr AS STRING) AS deliveryOrderNumber,
                  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` -- OK
                  WHERE fk_operator=2
                ) b ON a.fk_sales_order=b.fk_sales_order
                GROUP BY 1
            ) i ON i.deliveryOrderNumber=a.FC_ExternalIDOrderSeller__c
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
),
tickets_cambio_estado AS (

  WITH
  cambio_estado AS (
    SELECT 
      *
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`
    WHERE COALESCE(FC_TipificationName__c, Subject) IN ('Deseo cambiar el estatus de una orden enviada','Deseo cambiar el estado de una orden')
  )

  SELECT
    uniqueorderNumber AS deliveryOrderNumber,
    COUNT(DISTINCT CaseNumber) AS n_tickets_cambio_estado,
    MIN(CaseNumber) AS ticket_cambio_estado,
    MIN(CreatedDate) AS fecha_ticket_cambio_estado,
  FROM (
    SELECT DISTINCT * FROM (
      SELECT
        CaseNumber,
        CreatedDate,
        uniqueorderNumber
      FROM cambio_estado,
      UNNEST(REGEXP_EXTRACT_ALL(FC_ExternalIDOrderSeller__c, r'2[0-9]{8,9}$')) AS uniqueorderNumber
      UNION ALL
      SELECT
        CaseNumber,
        CreatedDate,
        uniqueorderNumber
      FROM cambio_estado,
      UNNEST(REGEXP_EXTRACT_ALL(Description, r'2[0-9]{8,9}$')) AS uniqueorderNumber
      UNION ALL
      SELECT 
        CaseNumber,
        CreatedDate,
        uniqueorderNumber
      FROM cambio_estado,
      UNNEST(REGEXP_EXTRACT_ALL(Description, r'\b2[0-9]{1,9}\b')) AS uniqueorderNumber
    )
  )
  GROUP BY 1
),

-- Saldos final
saldos_final AS (
  SELECT DISTINCT
    datos.sellerId,
    datos.sellerName,
    --id_sales_order,
    CASE WHEN datos.sellerId IN ('FALABELLA','SODIMAC','TOTTUS','FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU') THEN '1P' 
         WHEN REGEXP_CONTAINS(datos.sellerId,'^SC') THEN '3P'
         ELSE '1P' END AS sellerType,
    segmento,
    CASE WHEN seller_status='0' THEN 'Inactivo' ELSE seller_status END AS seller_status,
    last_delivered_order_date,
    datos.orderNumber,
    datos.deliveryOrderNumber,
    datos.sku,
    CASE WHEN shipping_type='Dropshipping' THEN 'FBS'
        WHEN shipping_type='Own Warehouse' THEN 'FBF'
        WHEN shipping_type='Cross docking' THEN 'XD'
        ELSE shipping_type END
        AS shipping_type,
    shipping_provider_product,
    deliveryMethod,
    pickupPoint,
    CAST(tracking_code_fsc AS STRING) AS tracking_code_fsc,
    CAST(tracking_code AS STRING) AS tracking_code,
    multibulto,
    n_rastreos,
    rastreos_asociados,
    --productos.primary_category,
    --productos.global_identifier,
    catalyst.status_catalyst,
    status_fsc,
    status_tms,
    status_backstore,
    printed,
    CASE WHEN envios.created_at IS NOT NULL THEN envios.created_at -- Todos
        WHEN envios.time_handled_by_marketplace IS NOT NULL THEN envios.time_handled_by_marketplace -- FBS
        WHEN envios.time_item_received IS NOT NULL THEN envios.time_item_received -- FBF
        WHEN envios.time_awaiting_fulfillment IS NOT NULL THEN envios.time_awaiting_fulfillment --
        ELSE envios.time_packed_by_marketplace --
        END AS created_at, -- Creado
    CASE WHEN envios.time_ready_to_ship IS NOT NULL THEN envios.time_ready_to_ship -- FBS
        WHEN envios.time_picked IS NOT NULL THEN envios.time_picked -- FBF
        ELSE envios.time_ready_to_ship
        END AS ready_to_ship, -- Listo para enviar FSC
    target_to_ship,
    NULL AS planned_at,
    CASE WHEN ingresos.time_ingreso IS NOT NULL THEN ingresos.time_ingreso -- Ingresos TMS + Urbano
        ELSE envios.time_shipped END AS shipped_at, -- Enviado
    um.fecha_visita AS visited_customer_at,
    CASE WHEN envios.time_delivered IS NOT NULL THEN envios.time_delivered -- Entregado FSC
        WHEN envios.time_canceled IS NOT NULL THEN envios.time_canceled -- Cancelado
        WHEN envios.time_failed_delivery IS NOT NULL THEN envios.time_failed_delivery -- Fallo de entrega -- AGREGAR CASOS URBANO 3PL (EJEMPLO: WYB172645305)
        WHEN envios.time_failed IS NOT NULL THEN envios.time_failed -- Fallo (campo nulo)
        ELSE NULL END AS terminated_at, --OK
    target_to_customer,
    CASE WHEN envios.time_return_shipped_by_customer IS NOT NULL THEN envios.time_return_shipped_by_customer -- Retornos FSC (entrega de cliente)
        WHEN rechazos.time_retorno_a_seller IS NOT NULL THEN rechazos.time_retorno_a_seller -- Retornos TMS (salidas de almacén)
        ELSE NULL END AS return_shipped_by_customer, -- Pendiente revisar Urbano
    envios.time_return_waiting_for_approval AS return_waiting_for_approval, -- Pendiente revisar TMS y/o Urbano
    --a.time_return_rejected, -- OK
    envios.time_returned AS returned_at,  -- Pendiente revisar TMS y/o Urbano
    variation,
    productos.items,
    productos.unit_price,
    productos.value,
    com.comision,
    IFNULL(shipping_fee,0) AS shipping_fee,
    IFNULL(ingreso,0) AS ingreso,
    IFNULL(egreso,0) AS egreso,
    IFNULL(pago,0) AS pago,
    IFNULL(descuento,0) AS descuento,
    IFNULL(ajuste,0) AS ajuste,
    COALESCE(fecha_ingreso,DATE(created_at)) AS fecha_ingreso,
    fecha_egreso,
    fecha_pago,
    fecha_descuento,
    fecha_ajuste,
    n_tickets_cambio_estado,
    ticket_cambio_estado,
    fecha_ticket_cambio_estado,
    razon_devolucion,
    tipificacion_reclamo,
    tipo_reclamo,
    ticket_reclamo,
    ticket_sol_reembolso,
    ticket_reembolso,
    ticket_devolucion,
    fecha_reclamo,
    fecha_sol_ticket_reembolso,
    fecha_ticket_reembolso,
    fecha_egreso AS fecha_reembolso,
    fecha_ticket_devolucion,
    IFNULL(escalado_postventa,'No') AS escalado_postventa,
    IFNULL(cancelado_postventa,'No') AS cancelado_postventa,
    IFNULL(escalado_sx,'No') AS escalado_sx,
    tarea_esc_postventa,
    tarea_canc_postventa,
    tarea_sol_esc_postventa,
    tarea_sol_canc_postventa,
    caso_cierre_tipo,
    caso_cierre_comentario,
    caso_comentario_sac,
    caso_comentario_postventa,
    caso_comentario_sx,
    resolucion_sac,
    resolucion_postventa,
    resolucion_sx,
    resolucion_caso,
    resolucion_final,
    COALESCE(estado_consolidado_il_j,estado_consolidado_il) AS estado_consolidado_il,
    Tienda,
    reason_code_category,
    reason_code_sub_category,
    n_rlos,
    CAST(rlo_id AS STRING) AS rlo_id,
    n_guias,
    CAST(COALESCE(guia_il_j,guia_il) AS STRING) AS guia_il,
    fecha_solicitud_il,
    fecha_recoleccion_il,
    COALESCE(fecha_entrega_il_j,fecha_entrega_il) AS fecha_entrega_il,
    guias_il_fuera_de_plazo,
    saldos.* EXCEPT(
      sellerId, sellerName, deliveryOrderNumber, sku, comision, 
      ingreso, egreso, pago, descuento, ajuste, 
      fecha_ingreso, fecha_egreso, fecha_pago, fecha_descuento, fecha_ajuste, 
      ticket_sol_reembolso, ticket_reembolso, fecha_sol_ticket_reembolso, fecha_ticket_reembolso
    ),
    SAFE_DIVIDE(egreso,unit_price) AS mod,
    CEIL(SAFE_DIVIDE(egreso,unit_price)) AS round_mod,
  FROM (
    SELECT DISTINCT
      --id_seller,
      UPPER(a.sellerId) AS sellerId,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(COALESCE(b.sellerName, a.sellerName))) AS sellerName,
      id_sales_order,
      a.orderNumber,
      CAST(a.deliveryOrderNumber AS STRING) AS deliveryOrderNumber,
      orderLineId,
      variantId AS sku,
      a.deliveryMethod,
      UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(pickupPointadressName)) AS pickupPoint,
      --UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddressstateName)) AS region,
      --UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddresscityName)) AS ciudad,
      --UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(shippingAddressmunicipalName)) AS distrito,
      DATETIME(SAFE_CAST(COALESCE(promisedByDeliveryInfofromDateTime,promisedByDeliveryInfotoDateTime) AS TIMESTAMP), 'America/Lima') AS target_to_customer,
      COALESCE(deliveryCostAmount,deliveryCostV2Amount) AS shipping_fee,
    FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders` a
    LEFT JOIN ( -- OK pero no jala con -1 debe -7
      SELECT * FROM (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY sellerName DESC) AS row,
        FROM (
          SELECT
            id_seller,
            src_id AS seller_id,
            UPPER(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(name),",","")) AS sellerName
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_seller`
          )
        )
      WHERE row=1
    ) b ON a.sellerId=b.seller_id
    LEFT JOIN (
      SELECT DISTINCT
        id_sales_order,
        order_nr AS orderNumber
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order`
      WHERE fk_operator=2
    ) c ON c.orderNumber=a.deliveryOrderNumber
    --WHERE PARTITION_DATE>="2022-08-01"
  ) datos
  LEFT JOIN saldos ON datos.deliveryOrderNumber=saldos.deliveryOrderNumber AND saldos.sku=datos.sku
  LEFT JOIN productos ON productos.fk_sales_order=datos.id_sales_order AND productos.sku=datos.sku
  LEFT JOIN rastreos ON rastreos.fk_sales_order=datos.id_sales_order
  LEFT JOIN envios ON envios.fk_sales_order=datos.id_sales_order
  LEFT JOIN fsc ON fsc.fk_sales_order=datos.id_sales_order 
  LEFT JOIN ingresos ON ingresos.rastreo=rastreos.tracking_code
  LEFT JOIN rechazos ON rechazos.numeroEnvio=rastreos.tracking_code
  LEFT JOIN um ON um.rastreo=rastreos.tracking_code
  LEFT JOIN catalyst ON catalyst.deliveryOrderNumber=datos.deliveryOrderNumber --revisar (estados a nivel Orden no Orden-SKU)
  LEFT JOIN backstore ON backstore.deliveryOrderNumber=datos.deliveryOrderNumber --revisar (estados a nivel Orden no Orden-SKU)
  LEFT JOIN tms ON tms.numeroEnvio=rastreos.tracking_code
  LEFT JOIN com ON com.global_identifier=productos.global_identifier
  --LEFT JOIN reclamos ON reclamos.deliveryOrderNumber=datos.deliveryOrderNumber
  LEFT JOIN datos_seller ON datos_seller.sellerId=datos.sellerId
  LEFT JOIN escalamientos ON escalamientos.deliveryOrderNumber=saldos.deliveryOrderNumber
  LEFT JOIN devoluciones ON datos.deliveryOrderNumber=devoluciones.deliveryOrderNumber AND datos.sku=devoluciones.sku
  LEFT JOIN devoluciones_salesforce ON datos.deliveryOrderNumber=devoluciones_salesforce.deliveryOrderNumber
  LEFT JOIN tickets_cambio_estado ON datos.deliveryOrderNumber=tickets_cambio_estado.deliveryOrderNumber
  LEFT JOIN (
    SELECT DISTINCT
    deliveryOrderNumber,
    sku,
    guia AS guia_il_j,
    CASE WHEN ESTADO_TRANSPORTE_GENERAL='ENTREGADO' THEN 'Entregado' ELSE NULL END AS estado_consolidado_il_j,
    SAFE.PARSE_DATE('%m/%d/%Y', fecha_estado) AS fecha_entrega_il_j
  FROM `bi-fcom-drmb-local-pe-sbx.Raichu_Transacciones.rev_devs_j`
  ) devs_rev_j ON datos.deliveryOrderNumber=devs_rev_j.deliveryOrderNumber AND datos.sku=devs_rev_j.sku
)

SELECT
  * EXCEPT(comentario_final),
  CASE 
    WHEN accion_final_devolucion='Saldo por recuperar Seller' THEN 'Saldo por recuperar Seller'
    --WHEN segmento='Longtail' AND comentario_final IS NULL AND DATE_DIFF(CURRENT_DATE(),fecha_egreso,DAY)>=45 THEN 'Saldo por recuperar Seller'
  ELSE comentario_final END AS comentario_final,

FROM (
  SELECT 
    *,
    CASE 
      WHEN estado_consolidado_il IN ('Entregado','Custodia','En Devolucion','Siniestro') THEN 'Saldo por recuperar Seller' 
      ELSE NULL END AS accion_final_devolucion,
    CASE 
      WHEN resolucion_final IN ('Devuelto a seller','Error de seller','Pedido no entregado a cliente','Seller no responde') THEN 'Saldo por recuperar Seller'
      WHEN resolucion_final IN ('Error de OPL','OPL no responde','Siniestro OPL') THEN 'Saldo por recuperar HD'  
      WHEN resolucion_final IN ('Caso cerrado por mediacion','Caso improcedente o sin evidencias','Cliente no responde','Error sistemico','Pedido entregado al cliente','Reembolso fuera de plazo') THEN 'Gasto'
      ELSE NULL END AS comentario_final,
  FROM (
    SELECT 
      * EXCEPT(row,resolucion_final),
      CASE 
        WHEN resolucion_final IS NULL AND status_catalyst='CANCELLED' AND status_tms='Entregado' THEN 'Error sistemico'
        WHEN resolucion_final IS NULL AND status_catalyst='CANCELLED' AND status_backstore='DELIVERED' THEN 'Error sistemico'
        ---
        WHEN resolucion_final IS NULL AND segmento IN ('Longtail','Nuevo','Sin venta') AND DATE_DIFF(CURRENT_DATE(),fecha_egreso,DAY)>=30 THEN 'Seller no responde'
        WHEN resolucion_final IS NULL AND segmento IN ('Midtail','AAA') AND DATE_DIFF(CURRENT_DATE(),fecha_egreso,DAY)>=120 THEN 'Seller no responde'
        ---
        ELSE resolucion_final END AS resolucion_final,
    FROM (
      SELECT DISTINCT
        a.* EXCEPT (
          n_tickets_cambio_estado,ticket_cambio_estado,fecha_ticket_cambio_estado,
          razon_devolucion,tipificacion_reclamo,tipo_reclamo,
          ticket_reclamo,ticket_sol_reembolso,ticket_reembolso,ticket_devolucion,
          fecha_reclamo,fecha_sol_ticket_reembolso,fecha_ticket_reembolso,fecha_reembolso,fecha_ticket_devolucion,
          cupon,
          escalado_postventa,cancelado_postventa,escalado_sx,
          tarea_esc_postventa,tarea_canc_postventa,tarea_sol_esc_postventa,tarea_sol_canc_postventa,
          caso_cierre_tipo,caso_cierre_comentario,
          caso_comentario_sac,caso_comentario_postventa,caso_comentario_sx,
          resolucion_sac,resolucion_postventa,resolucion_sx,resolucion_caso,resolucion_final,
          Tienda,reason_code_category,reason_code_sub_category,
          n_rlos,rlo_id,
          estado_consolidado_il,
          n_guias,guia_il,
          fecha_solicitud_il,fecha_recoleccion_il,fecha_entrega_il,guias_il_fuera_de_plazo
        ),
        n_tickets_cambio_estado,
        ticket_cambio_estado,
        fecha_ticket_cambio_estado,
        CASE
            WHEN status_fsc IN ('delivered','canceled','failed') THEN 'Si'
            WHEN status_catalyst IN ('DELIVERED','CANCELLED','UNDELIVERED') THEN 'Si'
            WHEN status_tms IN ('Entregado','Anulado','Devolucion Al Shipper','Fallo De Entrega','Retorno A Seller','Siniestro') THEN 'Si'
            WHEN status_backstore IN ('DELIVERED','ANNULLED','CANCELLED') THEN 'Si'
            ELSE 'No' END AS estado_final,
        CASE 
            -- Cuentas por Pagar (CxP)
            WHEN tipo='CxP' AND saldo>0.1 AND status_fsc IN ('canceled','failed') THEN 'Reembolsar a cliente'
            WHEN tipo='CxP' AND saldo>0.1 AND status_catalyst IN ('CANCELLED','UNDELIVERED') THEN 'Reembolsar a cliente'
            WHEN tipo='CxP' AND saldo>0.1 AND status_tms IN ('Anulado','Devolucion Al Shipper','Fallo De Entrega','Retorno A Seller','Siniestro') THEN 'Reembolsar a cliente'
            WHEN tipo='CxP' AND saldo>0.1 AND status_backstore IN ('ANNULLED','CANCELLED') THEN 'Reembolsar a cliente'
            WHEN tipo='CxP' AND saldo>0.1 AND status_fsc IN ('delivered') THEN 'Pagar a seller'
            WHEN tipo='CxP' AND saldo>0.1 AND status_catalyst IN ('DELIVERED') THEN 'Pagar a seller'
            WHEN tipo='CxP' AND saldo>0.1 AND status_tms IN ('Entregado') THEN 'Pagar a seller'
            WHEN tipo='CxP' AND saldo>0.1 AND status_backstore IN ('DELIVERED') THEN 'Pagar a seller'
            WHEN tipo='CxP' AND saldo>0.1 AND DATE_DIFF(CURRENT_DATE(), fecha_ingreso, DAY)>=30 THEN 'Escalera'
            -- Cuentas por Cobrar (CxC)
            WHEN tipo='CxC' AND saldo<-0.1 AND status_fsc IN ('canceled','failed') THEN 'Descontar a seller'
            WHEN tipo='CxC' AND saldo<-0.1 AND status_catalyst IN ('CANCELLED','UNDELIVERED') THEN 'Descontar a seller'
            WHEN tipo='CxC' AND saldo<-0.1 AND DATE_DIFF(CURRENT_DATE(), fecha_egreso, DAY)>=30 THEN 'Escalera'
            ELSE 'No aplica' END AS accion,
        ROW_NUMBER() OVER (PARTITION BY orderNumber,deliveryOrderNumber,sku ORDER BY created_at DESC) AS row,
        IFNULL(Tienda,'NA') AS Tienda,
        n_rlos,
        rlo_id,
        estado_consolidado_il,
        n_guias,
        guia_il,
        razon_devolucion,
        reason_code_category,
        reason_code_sub_category,
        tipificacion_reclamo,
        tipo_reclamo,
        ticket_reclamo,
        ticket_devolucion,
        ticket_sol_reembolso,
        ticket_reembolso,
        cupon,
        fecha_reclamo,
        fecha_ticket_devolucion,
        fecha_sol_ticket_reembolso,
        fecha_ticket_reembolso,
        fecha_reembolso,
        fecha_solicitud_il,
        fecha_recoleccion_il,
        fecha_entrega_il,
        guias_il_fuera_de_plazo,
        escalado_postventa,
        cancelado_postventa,
        escalado_sx,
        tarea_esc_postventa,
        tarea_canc_postventa,
        tarea_sol_esc_postventa,
        tarea_sol_canc_postventa,
        caso_cierre_tipo,
        caso_cierre_comentario,
        caso_comentario_sac,
        caso_comentario_postventa,
        caso_comentario_sx,
        resolucion_sac,
        resolucion_postventa,
        resolucion_sx,
        resolucion_caso,
        CASE 
          WHEN caso_comentario_sac='Reembolso fuera de plazo' THEN 'Reembolso fuera de plazo'
          ELSE resolucion_final END AS resolucion_final
      FROM (
        SELECT
          a.*,
          ingreso + egreso AS debe,
          pago + descuento AS haber,
          (ingreso+egreso)-(pago+descuento) AS saldo,
          CASE WHEN (ingreso+egreso)-(pago+descuento)>=0 THEN 'CxP' ELSE 'CxC' END AS tipo,
          --SAFE_ADD(cobro_comision,SAFE_ADD(cobro_comision_ajuste,SAFE_ADD(cobro_cofilog,SAFE_ADD(cobro_cofilog_ajuste,SAFE_ADD(cobro_loginv,SAFE_ADD(cobro_loginv_item,SAFE_ADD(cobro_loginv_ajuste,SAFE_ADD(cobro_fby,SAFE_ADD(cobro_cancel,SAFE_ADD(cobro_handling_fee,SAFE_ADD(cobro_vas,SAFE_ADD(cobro_medios,SAFE_ADD(cobro_banner,SAFE_ADD(cobro_mailings,SAFE_ADD(cobro_flex,cobro_apelacion))))))))))))))) AS cobros_total,
          --SAFE_ADD(reembolso_comision,SAFE_ADD(reembolso_comision_ajuste,SAFE_ADD(reembolso_cofilog,SAFE_ADD(reembolso_cofilog_ajuste,SAFE_ADD(reembolso_loginv,SAFE_ADD(reembolso_loginv_item,SAFE_ADD(reembolso_loginv_ajuste,SAFE_ADD(reembolso_cancel,SAFE_ADD(reembolso_vas,SAFE_ADD(reembolso_medios,SAFE_ADD(reembolso_banner,SAFE_ADD(reembolso_flex,reembolso_apelacion)))))))))))) AS reembolsos_total,
          --CASE WHEN status_fsc IN ('canceled','failed') AND status_tms='Entregado' THEN 'Si' ELSE 'No' END AS siniestro,
        FROM (
          SELECT
            EXTRACT(YEAR FROM COALESCE(fecha_ingreso,created_at)) AS year,
            EXTRACT(MONTH FROM COALESCE(fecha_ingreso,created_at)) AS month,
            a.* EXCEPT (
              mod,round_mod,
              egreso,pago,descuento,ajuste,fecha_ingreso,fecha_egreso,fecha_pago,fecha_descuento,fecha_ajuste,
              razon_devolucion,
              tipificacion_reclamo,tipo_reclamo,
              ticket_reclamo,ticket_sol_reembolso,ticket_reembolso,ticket_devolucion,
              cupon,
              fecha_reclamo,fecha_sol_ticket_reembolso,fecha_ticket_reembolso,fecha_reembolso,fecha_ticket_devolucion,
              ---
              cobro_comision,cobro_ou,cobro_cofilog,cobro_logrep,cobro_revlog,
              cobro_loginv,cobro_loginv_item,cobro_loginv_ajuste,
              cobro_ship_fby,cobro_inb_fby,cobro_invrem_fby,cobro_logrep_fby,
              cobro_cancel,cobro_handling_fee,
              cobro_vas,cobro_medios,cobro_banner,cobro_banner_home,cobro_mailings,
              cobro_flex,cobro_apelacion, 
              ---
              escalado_postventa,cancelado_postventa,escalado_sx,
              tarea_esc_postventa,tarea_canc_postventa,tarea_sol_esc_postventa,tarea_sol_canc_postventa,caso_cierre_tipo,caso_cierre_comentario,
              caso_comentario_sac,caso_comentario_postventa,caso_comentario_sx,
              resolucion_sac,resolucion_postventa,resolucion_sx,resolucion_caso,resolucion_final
            ),
            ROUND(CASE WHEN mod!=round_mod THEN round_mod*unit_price ELSE egreso END,2) AS egreso,
            pago,
            descuento,
            ajuste,
            fecha_ingreso,
            fecha_egreso,
            fecha_pago,
            fecha_descuento,
            fecha_ajuste,
            razon_devolucion,
            tipificacion_reclamo,
            tipo_reclamo,
            ticket_reclamo,
            ticket_sol_reembolso,
            ticket_reembolso,
            ticket_devolucion,
            fecha_reclamo,
            fecha_sol_ticket_reembolso,
            fecha_ticket_reembolso,
            fecha_reembolso,
            fecha_ticket_devolucion,
            cupon,
            escalado_postventa,
            cancelado_postventa,
            escalado_sx,
            tarea_esc_postventa,
            tarea_canc_postventa,
            tarea_sol_esc_postventa,
            tarea_sol_canc_postventa,
            caso_cierre_tipo,
            caso_cierre_comentario,
            CASE
              WHEN caso_comentario_sac IS NULL AND DATE_DIFF(fecha_egreso,fecha_ingreso,DAY)>30 THEN 'Reembolso fuera de plazo'
              ELSE caso_comentario_sac END AS caso_comentario_sac,
            caso_comentario_postventa,
            caso_comentario_sx,
            resolucion_sac,
            resolucion_postventa,
            resolucion_sx,
            resolucion_caso,
            resolucion_final
          FROM saldos_final a
        ) a
      ) a 
    )

    WHERE row=1
    --AND sellerId NOT IN ('SC48A7F','SC01B79','SCB821F','SC36C1D','SC24301','SC015FC','SCDC772','SCBAC4A','SC9CADA','SC22020','SCEFFB7','SC89B3D','SC786B7','SC68E19','SC4FB63','SC95BC3')
    --AND sellerId NOT IN ('FALABELLA','SODIMAC','TOTTUS','FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU')
    --AND saldo!=0
  )
)
)
