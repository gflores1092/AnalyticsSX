CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.seguimiento_apelaciones` AS (

  WITH 
  data AS (
    SELECT DISTINCT--16475
      a.*,
      b.pago AS value,
      DATE(b.event_date) AS fecha_transaccion,
      c.fecha_factura,
    FROM (
      SELECT DISTINCT
        canal,
        COALESCE(b.sellerId,c.sellerId,i.sellerId) AS sellerId,
        COALESCE(b.sellerName,c.sellerName,i.sellerName) AS sellerName,
        COALESCE(c.segmento,i.segmento) AS segmento,
        COALESCE(c.seller_status,i.seller_status) AS seller_status,
        a.caseNumber,
        c.orderNumber,
        a.deliveryOrderNumber,
        a.sku,
        COALESCE(c.shipping_type,i.shipping_type) AS shipping_type,
        COALESCE(c.shipping_provider_product,i.shipping_provider_product) AS shipping_provider_product,
        COALESCE(c.deliveryMethod,i.deliveryMethod) AS deliveryMethod,
        COALESCE(c.pickupPoint,i.pickupPoint) AS pickupPoint,
        c.tracking_code_fsc,
        --c.tracking_code,
        --c.value AS paid_price,
        COALESCE(a.paid_price,c.value) AS paid_price, --<--- REVISAR!!! ESTE DATO TICKET: 33304205
        c.fecha_egreso AS fecha_devolucion_tienda,
        a.fecha_apelacion AS fecha_solicitud_apelacion,
        COALESCE(a.fecha_estado_actual,DATE(e.caseClosedDate),a.fecha_apelacion) AS fecha_gestion_apelacion,
        a.comentario_apelacion,
        IFNULL(a.motivo_apelacion,'NA') AS motivo_apelacion,
        --a.submotivo_apelacion,
        a.status,
        CASE 
          WHEN a.status IN ('SELLER_REJECTION_ACCEPTED','SELLER_REAPPEAL_ACCEPTED','Aceptado') THEN 'Aceptado'
          WHEN a.status IN ('SELLER_REJECTION_REFUSED','SELLER_REAPPEAL_REFUSED','SELLER_REJECTION_REJECTED','SELLER_REAPPEAL_REJECTED','Rechazado') THEN 'Rechazado'
          WHEN a.status='Closed' THEN COALESCE(h.estado_apelacion,'Rechazado') --'Por validar'
          ELSE 'Pendiente' END AS estado_apelacion,  
        b.product_description,
        b.global_identifier,
        b.G1,
        b.N1,
        b.G2,
        b.N2,
        b.G3,
        b.N3,
        b.G4,
        b.N4,
        --
        COALESCE(c.variation,b.size) AS size, -- primera de la orden luego del catalogo
        b.brand_name,
        --
        d.customerDocument,
        d.customerName,
        UPPER(COALESCE(CASE WHEN c.Tienda='NA' THEN NULL ELSE c.Tienda END,i.Tienda)) AS Tienda,
        c.ingreso,
        c.egreso,
        c.pago,
        c.descuento,
        c.ajuste,
        c.fecha_ingreso,
        c.fecha_egreso,
        c.fecha_pago,
        c.fecha_descuento,
        c.fecha_ajuste,
        c.n_rlos,
        c.rlo_id,
        c.estado_consolidado_il,
        c.n_guias,
        c.guia_il,
        DATE_DIFF(c.fecha_entrega_il,c.fecha_egreso, DAY) AS dias_devolucion_seller,
        c.razon_devolucion,
        c.reason_code_category,
        c.reason_code_sub_category,
        c.tipificacion_reclamo,
        c.tipo_reclamo,
        c.ticket_reclamo,
        c.ticket_devolucion,
        c.ticket_sol_reembolso,
        c.ticket_reembolso,
        c.fecha_reclamo,
        c.fecha_ticket_devolucion,
        c.fecha_sol_ticket_reembolso,
        c.fecha_ticket_reembolso,
        c.fecha_reembolso,
        c.fecha_solicitud_il,
        c.fecha_recoleccion_il,
        c.fecha_entrega_il,
        e.caseOwner AS agente,
        CASE WHEN g.fecha_inicio_piloto IS NULL THEN '2024-05-01' ELSE g.fecha_inicio_piloto END AS fecha_inicio_piloto,

      FROM ( -- TODO OK INCLUSO VALORIZADOS

        WITH
        tickets_sheet AS (
          SELECT * FROM (
            SELECT
              'Salesforce' AS canal,
              --DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', a.fecha_de_atencion)) AS fecha_apelacion,
              COALESCE(b.fecha_apelacion,a.fecha_apelacion) AS fecha_apelacion,
              a.caseNumber,
              a.deliveryOrderNumber,
              a.sku,
              a.paid_price,
              b.comentario_apelacion,
              a.motivo_apelacion,
              --'NA' AS submotivo_apelacion,
              a.status,
              b.fecha_estado_actual
            FROM (
              SELECT * FROM (
                SELECT 
                  *,
                  ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY fecha_apelacion DESC) AS row, 
                FROM (
                  SELECT
                    DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', fecha_de_atencion)) AS fecha_apelacion,
                    n_caso AS caseNumber,
                    orden AS deliveryOrderNumber,
                    sku_falabella AS sku,
                    SAFE_CAST(valor_item_a_favor_del_seller AS FLOAT64) AS paid_price,
                    LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(motivo_seller)) AS motivo_apelacion,
                    INITCAP(estado_apelacion) AS status,
                  FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.apelaciones_externas_sf`
                )
              )
              WHERE row=1
            ) a
            LEFT JOIN (
                SELECT DISTINCT 
                  caseNumber,
                  date AS fecha_apelacion,
                  LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(caseDescription)) AS comentario_apelacion,
                  DATE(caseClosedDate) AS fecha_estado_actual
                FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce`
            ) b ON a.caseNumber=b.caseNumber
          )
          WHERE fecha_apelacion IS NOT NULL
        ),
        tickets_salesforce AS (
          SELECT * EXCEPT (origen) FROM (
            SELECT 
              'Salesforce' AS canal,
              a.date AS fecha_apelacion,
              a.caseNumber,
              CASE WHEN LENGTH(a.deliveryOrderNumber)<10 THEN NULL ELSE a.deliveryOrderNumber END AS deliveryOrderNumber,
              CASE WHEN LENGTH(a.sku)<8 THEN NULL ELSE a.sku END AS sku,
              a.value AS paid_price,
              LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(caseDescription)) AS comentario_apelacion,
              'NA' AS motivo_apelacion,
              --'NA' AS submotivo_apelacion,
              a.caseStatus AS status,
              DATE(a.caseClosedDate) AS fecha_estado_actual,
              IFNULL(b.origen,'Salesforce') AS origen
            FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` a 
            LEFT JOIN (
              SELECT DISTINCT 
                caseNumber,
                'Sheet' AS origen
              FROM tickets_sheet
            ) b ON a.caseNumber=b.caseNumber
            WHERE a.caseTipification='Quiero rechazar una devolucion'
          )
          WHERE origen!='Sheet'
        ),
        casos_modulo AS (
          SELECT
            'Modulo' AS canal,
            DATE(a.cro_created_at) AS fecha_apelacion,
            'NA' AS caseNumber,
            a.delivery_order_number AS deliveryOrderNumber,
            b.sku AS sku,
            SAFE_CAST(b.paid_price AS FLOAT64) AS paid_price,
            LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(comment)) AS comentario_apelacion,
            CASE
              WHEN COALESCE(reason,comment)='Damaged product' THEN 'producto danado' 
              WHEN COALESCE(reason,comment)='Used product' THEN 'producto usado' 
              WHEN COALESCE(reason,comment)='Product arrived with damaged or dirty packaging' THEN 'producto llegó con el empaque dañado o sucio' 
              WHEN COALESCE(reason,comment)='Product arrived without packaging' THEN 'producto llegó sin empaque' 
              WHEN COALESCE(reason,comment)='Incomplete product' THEN 'producto incompleto' 
              WHEN COALESCE(reason,comment)='Product does not work' THEN 'producto no funciona' 
              WHEN COALESCE(reason,comment)='Product does not belong to my catalog' THEN 'producto no pertenece al catalogo del seller' 
              WHEN COALESCE(reason,comment)='Product not delivered to my warehouses' THEN 'producto no entregado' 
              WHEN COALESCE(reason,comment)='Empty box' THEN 'caja vacia' 
              ELSE NULL END AS motivo_apelacion,
            --subStatus AS submotivo_apelacion,
            CASE 
              WHEN a.SRO_status='SELLER_REJECT' AND SRO_days_status_changed>30 THEN 'SELLER_REJECTION_REFUSED'
              ELSE a.SRO_status END AS status,
            DATE(a.SRO_event_time) AS fecha_estado_actual,
            --return_order_line_id,
          FROM `bi-fcom-drmb-sell-in-sbx.panel_sell_in.perfect_return_orders` a
          LEFT JOIN (
            SELECT DISTINCT
              returnLineId,
              variantId AS sku,
              unitPricecentAmountCalc AS paid_price,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_catalyst_prd_pe.svw_customer_order_return`,
            UNNEST(lineItemConditions) lineItemConditions,
            UNNEST(linePolicies) linePolicies,
            UNNEST(totals) totals
          ) b ON a.return_order_line_id=b.returnLineId
          LEFT JOIN (
            SELECT DISTINCT * FROM (
              /**
              SELECT 
                return_order_line_id,
                deliveryOrderNumber,
                reasonCodeSubCategory,
                current_status,
                REGEXP_REPLACE(MAX(CASE WHEN name='returnId' THEN values ELSE NULL END),r'^\["|"\]$','') AS returnId,
                REGEXP_REPLACE(MAX(CASE WHEN name='reason' THEN values ELSE NULL END),r'^\["|"\]$','') AS reason,
                REGEXP_REPLACE(MAX(CASE WHEN name='comment' THEN values ELSE NULL END),r'^\["|"\]$','') AS comment,
                REGEXP_REPLACE(MAX(CASE WHEN name='status' THEN values ELSE NULL END),r'^\["|"\]$','') AS status,
                REGEXP_REPLACE(MAX(CASE WHEN name='userEmail' THEN values ELSE NULL END),r'^\["|"\]$','') AS userEmail,
                REGEXP_REPLACE(MAX(CASE WHEN name='subStatus' THEN values ELSE NULL END),r'^\["|"\]$','') AS subStatus
              FROM (
                SELECT
                  return_order_line_id,
                  REGEXP_REPLACE(associated_delivery_orders,r'^\["|"\]$','') AS deliveryOrderNumber,
                  rejection_info_reasonCodeSubCategory AS reasonCodeSubCategory,
                  current_status,
                  --rejection_info,
                  rejection_info_customInfo.name AS name,
                  rejection_info_customInfo.values AS values,
                FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_fcm_corp_seller_order_slor_prd.vw_seller_return_order_lines_pe a,
                UNNEST(rejection_info_customInfo) rejection_info_customInfo
              )
              GROUP BY 1,2,3,4
              UNION ALL
              **/
              SELECT
                return_order_line_id, 
                REGEXP_REPLACE(associated_delivery_orders,r'^\["|"\]$','') AS deliveryOrderNumber,
                JSON_EXTRACT_SCALAR(rejection_info, '$.reasonCodeSubCategory') AS reasonCodeSubCategory,
                current_status,
                --JSON_EXTRACT_SCALAR(rejection_info, '$.mediaURL[0]') AS mediaURL,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[0].values[0]') AS returnId,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[1].values[0]') AS reason,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[2].values[0]') AS comment,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[3].values[0]') AS status,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[4].values[0]') AS userEmail,
                JSON_EXTRACT_SCALAR(rejection_info, '$.customInfo[5].values[0]') AS subStatus
              FROM  (
                SELECT 
                  return_order_line_id,
                  associated_delivery_orders,
                  current_status,
                  rejection_info,
                  return_option
                FROM tc-sc-bi-bigdata-cons-fcom-prd.svw_fcm_corp_seller_order_slor_prd.vw_btd_seller_return_order_lines_pe
              )
            )
          ) c ON a.return_order_line_id=c.return_order_line_id
          WHERE a.pais='Perú'
          AND a.sellerType='3P'
          AND a.SRO_status IN (
            'AGENT_REVIEW','AGENT_REAPPEAL_REVIEW',
            'SELLER_REJECT',
            'SELLER_REJECTION_REFUSED','SELLER_REJECTION_REJECTED','SELLER_REJECTION_ACCEPTED',
            'SELLER_REAPPEAL',
            'SELLER_REAPPEAL_REFUSED','SELLER_REAPPEAL_REJECTED','SELLER_REAPPEAL_ACCEPTED'
          )
        ),
        casos_modulo_sheet AS (
          SELECT * EXCEPT (origen) FROM (
            SELECT -- 19577
              'Modulo' AS canal,
              DATE(SAFE.PARSE_DATETIME('%m/%d/%Y', fecha_de_atencion)) AS fecha_apelacion,
              'NA' AS caseNumber,
              orden AS deliveryOrderNumber,
              sku_falabella AS sku,
              SAFE_CAST(unit_price AS FLOAT64) AS paid_price,
              'NA' AS comentario_apelacion,
              LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(motivo_seller)) AS motivo_apelacion,
              --'NA' AS submotivo_apelacion,
              INITCAP(estado_de_solicitud) AS status,
              SAFE_CAST(NULL AS DATE) AS fecha_estado_actual,
              IFNULL(b.origen,'Sheet') AS origen
            FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.modulo_de_fsc` a
            LEFT JOIN (
              SELECT DISTINCT
                deliveryOrderNumber,
                'Modulo' AS origen
              FROM casos_modulo
            ) b ON a.orden=b.deliveryOrderNumber
            WHERE a.fecha_de_atencion IS NOT NULL
          )
          WHERE origen!='Modulo'
        )

        SELECT 
          a.* 
        FROM (
          SELECT * FROM tickets_sheet
          UNION ALL
          SELECT * FROM tickets_salesforce 
          UNION ALL
          SELECT * FROM casos_modulo_sheet 
          UNION ALL
          SELECT * FROM casos_modulo 
        ) a

      ) a
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.modelo_tallas_skus` b ON a.sku=b.shop_sku
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` c ON a.deliveryOrderNumber=c.deliveryOrderNumber AND a.sku=c.sku
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` d ON a.deliveryOrderNumber=d.deliveryOrderNumber AND a.sku=d.sku
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` e ON a.caseNumber=e.caseNumber --FC_WorkOrder_orderNumber__c
      LEFT JOIN (
        SELECT 
          Tienda,
          DATE(SAFE.PARSE_DATETIME('%d/%m/%Y', piloto_tiendas)) AS fecha_inicio_piloto
        FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.piloto_tiendas`
      ) g ON c.Tienda=g.Tienda
      LEFT JOIN (
        SELECT 
          *,
          CASE
            WHEN REGEXP_CONTAINS(taskDescription,'no procede|no procedio|no podemos aceptar|no se acepta|no podemos|no es valido|generar un nuevo ticket|abrir un nuevo ticket|cierre de ticket|sin opcion a reclamo|crear un nuevo ticket|crear un ticket|un nuevo ticket|ya ha sido aceptado|no reabrir|no reaperturar|plataforma de falabella seller center|plataforma de fsc|modulo de devoluciones|evidencias no son validas|apelacion realizada por fsc|no es suficiente|fue aprobada en el ticket|se procede a rechazar su solicitud|su solicitud ha sido rechazada|generar un ticket por orden adjuntado las evidencias|gera flores|este caso ya cuenta con respuesta') THEN 'Rechazado'
            WHEN REGEXP_CONTAINS(taskSubject,'Necesitamos mas informacion|Aviso cierre caso') THEN 'Rechazado'
            WHEN taskDescription IS NULL THEN 'Rechazado'
            WHEN REGEXP_CONTAINS(taskDescription,'emitir la factura|se procedera a aceptar|de acuerdo a la evidencia brindada estaremos procediendo con aceptar su solicitud de rechazo|caso se encuentra cerrado') THEN 'Aceptado'
          ELSE 'Por validar' END AS estado_apelacion 
        FROM (
          SELECT
            caseNumber,
            taskBU,
            taskSubject,
            taskDescription,
            taskType,
            taskCreatedDate,
            ROW_NUMBER() OVER (PARTITION BY caseNumber ORDER BY taskCreatedDate DESC) AS row
          FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_tareas_salesforce` 
          WHERE taskType='Correo'
        )
        WHERE row=1
      ) h ON h.caseNumber=a.caseNumber
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` i ON a.deliveryOrderNumber=i.deliveryOrderNumber
    ) a
    LEFT JOIN (
      SELECT DISTINCT 
        seller_id AS sellerId,
        delivery_order_number AS deliveryOrderNumber,
        variant_id AS sku,
        transaction_type_name,
        DATETIME(SAFE_CAST(event_arrived_at AS TIMESTAMP)) AS event_date,
        SAFE_DIVIDE(CAST(net_amount_cent AS INT), CAST(net_amount_fraction AS INT)) AS pago,
        commission AS comision,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_fcm_corp_drmb_slfi_prd.svw_agg_pe_seller_settlement_transaction_detail`
      WHERE transaction_type_name='APPEALED_RETURN_CREDIT'
    ) b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
    LEFT JOIN (
      SELECT
        deliveryOrderNumber,
        fecha_factura
      FROM (
        SELECT DISTINCT
          deliveryOrderNumber,
          DATE(fecha_factura) AS fecha_factura
        FROM (
          SELECT 
            numero_orden AS deliveryOrderNumber,
            PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S',timestamp) AS fecha_factura 
          FROM bi-fcom-drmb-local-pe-sbx.Psyduck_Comprobantes.facturas_fcom_apelaciones
          UNION ALL
          SELECT 
            numero_orden AS deliveryOrderNumber,
            PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S',timestamp) AS fecha_factura
          FROM bi-fcom-drmb-local-pe-sbx.Psyduck_Comprobantes.facturas_fcom_modulo
          UNION ALL
          SELECT 
            numero_orden AS deliveryOrderNumber,
            PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S',timestamp) AS fecha_factura
          FROM bi-fcom-drmb-local-pe-sbx.Psyduck_Comprobantes.facturas_fcom_sc
          UNION ALL
          SELECT 
            numero_orden AS deliveryOrderNumber,
            PARSE_TIMESTAMP('%m/%d/%Y %H:%M:%S',timestamp) AS fecha_factura
          FROM bi-fcom-drmb-local-pe-sbx.Psyduck_Comprobantes.facturas_fcom_sf
          UNION ALL
          SELECT 
            numero_orden AS deliveryOrderNumber,
            PARSE_TIMESTAMP('%d/%m/%Y %H:%M:%S',timestamp) AS fecha_factura
          FROM bi-fcom-drmb-local-pe-sbx.Psyduck_Comprobantes.facturas_fcom_tickets
        )
      )
      QUALIFY ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber ORDER BY fecha_factura DESC)=1
    ) c ON c.deliveryOrderNumber=b.deliveryOrderNumber 
  ),
  estados_apelacion AS (
    SELECT
      deliveryOrderNumber,
      COUNT(DISTINCT estado_apelacion) AS n,
      MAX(CASE WHEN estado_apelacion='Aceptado' THEN 1 ELSE 0 END) AS aceptado
    FROM (
      SELECT DISTINCT
        deliveryOrderNumber,
        estado_apelacion
      FROM data
    )
    GROUP BY 1
  ),
  validacion AS (
    SELECT 
      a.*,
      b.* EXCEPT(deliveryOrderNumber)
    FROM data a
    LEFT JOIN estados_apelacion b ON a.deliveryOrderNumber=b.deliveryOrderNumber
  ),
  data_validacion AS (
    SELECT
      a.* EXCEPT(value,fecha_transaccion,n,aceptado),
      CASE WHEN estado_apelacion!='Aceptado' AND n>1 AND aceptado=1 THEN NULL ELSE value END AS value,
      SAFE_CAST((CASE WHEN estado_apelacion!='Aceptado' AND n>1 AND aceptado=1 THEN NULL ELSE fecha_transaccion END) AS DATE) AS fecha_transaccion
    FROM validacion a
  ),
  rows_apelacion AS (
    SELECT 
      deliveryOrderNumber,
      COUNT(CASE WHEN value!=0 THEN deliveryOrderNumber ELSE NULL END) AS n_rows
    FROM data_validacion
    GROUP BY 1
  ),
  final_data AS (
    SELECT
      a.* EXCEPT (value,fecha_transaccion),
      SAFE_DIVIDE(value,n_rows) AS value,
      ROUND(SAFE_DIVIDE(CASE WHEN value IS NULL THEN paid_price ELSE value END,ingreso),2) AS porc_aceptacion, -- ESTO SE DEBE REVISAR EN %
      fecha_transaccion
    FROM data_validacion a
    LEFT JOIN rows_apelacion b ON a.deliveryOrderNumber=b.deliveryOrderNumber
  ),
  data_final AS (
    SELECT
      a.* EXCEPT(flujo,brand_name,size,n_apelacion),
      CASE 
        WHEN flujo='Otro' AND pago=0 THEN 'Fallo de entrega'
        ELSE flujo END AS flujo,
      size,
      brand_name,
      n_apelacion
    FROM (
      SELECT
        EXTRACT(YEAR FROM fecha_transaccion) AS year,
        EXTRACT(MONTH FROM fecha_transaccion) AS month,
        EXTRACT(YEAR FROM fecha_egreso) AS year_dev_tienda,
        EXTRACT(MONTH FROM fecha_egreso) AS month_dev_tienda,
        EXTRACT(YEAR FROM fecha_solicitud_apelacion) AS year_sol_apelacion,
        EXTRACT(MONTH FROM fecha_solicitud_apelacion) AS month_sol_apelacion,
        EXTRACT(YEAR FROM fecha_gestion_apelacion) AS year_ges_apelacion,
        EXTRACT(MONTH FROM fecha_gestion_apelacion) AS month_ges_apelacion,
        a.* EXCEPT(fecha_factura, porc_aceptacion,size,brand_name),
        CONCAT(
          EXTRACT(YEAR FROM fecha_devolucion_tienda),
          '-',
          CASE 
            WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_devolucion_tienda) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_devolucion_tienda) AS STRING))
            ELSE CAST(EXTRACT(MONTH FROM fecha_devolucion_tienda) AS STRING) END
        ) AS periodo_devolucion_tienda,
        CONCAT(
          EXTRACT(YEAR FROM fecha_entrega_il),
          '-',
          CASE 
            WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_entrega_il) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_entrega_il) AS STRING))
            ELSE CAST(EXTRACT(MONTH FROM fecha_entrega_il) AS STRING) END
        ) AS periodo_devolucion_seller,
        CONCAT(
          EXTRACT(YEAR FROM fecha_solicitud_apelacion),
          '-',
          CASE 
            WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_solicitud_apelacion) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_solicitud_apelacion) AS STRING))
            ELSE CAST(EXTRACT(MONTH FROM fecha_solicitud_apelacion) AS STRING) END
        ) AS periodo_solicitud_apelacion,
        CONCAT(
          EXTRACT(YEAR FROM fecha_gestion_apelacion),
          '-',
          CASE 
            WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_gestion_apelacion) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_gestion_apelacion) AS STRING))
            ELSE CAST(EXTRACT(MONTH FROM fecha_gestion_apelacion) AS STRING) END
        ) AS periodo_gestion_apelacion,
        CONCAT(
          EXTRACT(YEAR FROM fecha_transaccion),
          '-',
          CASE 
            WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_transaccion) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_transaccion) AS STRING))
            ELSE CAST(EXTRACT(MONTH FROM fecha_transaccion) AS STRING) END
        ) AS periodo_transaccion,
        CASE WHEN fecha_transaccion IS NOT NULL THEN 'Si' ELSE 'No' END AS pagado,
        CASE 
          WHEN fecha_devolucion_tienda>=(CASE WHEN fecha_inicio_piloto IS NULL THEN '2024-05-01' ELSE fecha_inicio_piloto END) THEN 'Si' 
          ELSE 'No' END AS piloto,
        DATE_DIFF(CURRENT_DATE('America/Lima'),fecha_gestion_apelacion,DAY) AS dias_desde_gestion,
        DATE_DIFF(fecha_transaccion,fecha_gestion_apelacion,DAY) AS dias_de_gestion_a_pago,
        fecha_factura,
        DATE_DIFF(fecha_factura,fecha_gestion_apelacion,DAY) AS dias_de_gestion_a_factura,
        DATE_DIFF(fecha_transaccion,fecha_factura,DAY) AS dias_de_factura_a_pago,
        CASE
          WHEN REGEXP_CONTAINS(Tienda,'FALABELLA') THEN 'FALABELLA'
          WHEN REGEXP_CONTAINS(Tienda,'MAESTRO') THEN 'MAESTRO'
          WHEN REGEXP_CONTAINS(Tienda,'SODIMAC') THEN 'SODIMAC'
          WHEN REGEXP_CONTAINS(Tienda,'TOTTUS') THEN 'TOTTUS'
          WHEN REGEXP_CONTAINS(Tienda,'HOME_PICKUP') THEN 'HOME_PICKUP'
          WHEN REGEXP_CONTAINS(Tienda,'MODULO|F.COM|FALABELLA.COM') THEN 'MODULO'
          WHEN Tienda='NA' THEN 'NA'
        ELSE 'Otro' END AS BU,
        CASE
          WHEN porc_aceptacion<0.05 THEN '1. 0 a 5%' 
          WHEN porc_aceptacion>=0.05 AND porc_aceptacion<0.30 THEN '2. 5 a 30%'
          WHEN porc_aceptacion>=0.30 AND porc_aceptacion<0.50 THEN '3. 30% a 50%'
          WHEN porc_aceptacion>=0.50 AND porc_aceptacion<1 THEN '4. 50% a 100%'
          WHEN porc_aceptacion>=1 THEN '5. 100%'
          ELSE NULL END AS porc_aceptacion,
        CASE 
          WHEN Tienda='HOME_PICKUP' THEN 'Devoluciones - Home Pickup'
          WHEN REGEXP_CONTAINS(Tienda,'FALABELLA|MAESTRO|SODIMAC|TOTTUS') THEN 'Devoluciones - Tienda'
          WHEN REGEXP_CONTAINS(reason_code_category,'NON_SHOW') THEN 'Fallo de entrega'
          ELSE 'Otro' END AS flujo,
        size,
        brand_name,
        ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber,sku ORDER BY fecha_solicitud_apelacion ASC) AS n_apelacion,
      FROM final_data a
    ) a
  ),
  max_apelaciones AS (
    SELECT
      *,
      CASE WHEN max_apelacion>1 THEN 'Si' ELSE 'No' END AS reapelacion,
    FROM (
      SELECT
        deliveryOrderNumber,
        sku,
        MAX(n_apelacion) AS max_apelacion
      FROM data_final
      GROUP BY 1,2
    )
  ) 

  SELECT 
    a.*,
    b.* EXCEPT (deliveryOrderNumber,sku),
    CASE
      WHEN razon_devolucion IN ('Cambio de parecer','Lo encontre a mejor precio','Me arrepenti y ya no lo quiero','Me equivoque de tamano color o modelo') THEN 'Arrepentimiento'
      WHEN razon_devolucion IN ('Garantia','Tiene una falla de funcionamiento') THEN 'Falla de funcionamiento'
      WHEN razon_devolucion IN ('Cambio de producto','Error de talla','Fuera de cobertura','La entrega se atraso y ya no necesito el producto','Las caracteristicas no coinciden con las de la publicacion','Llego en mal estado o tiene dano estetico','Llego un producto distinto al que compre','Producto incompleto','Publicidad enganosa') THEN 'Problemas con calidad del producto'
    ELSE 'No identificado' END AS motivo_devolucion
  FROM data_final a
  LEFT JOIN max_apelaciones b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku

)
