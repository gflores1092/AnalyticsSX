CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comprobantes` AS (

  WITH 
    sellers_integrados AS (
      SELECT 
        sellerId,
        'Si' AS integrado
      FROM UNNEST([
      'SC7B531','SCAC0C5','SCD1016','SC11B4E','SC4FF3F','SC9DA49',
      'SC27E46','SC87A21','SC1B5A1','SC5A821','SC2BD7E','SCF7299',
      'SCBDBE6','SC21945','SC46CC7','SCC4125','SC1B42D'
      ]) AS sellerId
    ),
    invoices AS (
      SELECT 
        orderLineId,
        --date_time,
        invoiceDate,
        invoiceType,
        invoiceNumber,
        --quantityNumber,
        --centAmount,
        --lineCentAmount,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_catalyst_prd_pe.svw_invoice_attached`
      --WHERE eventType='invoiceAttached'
    ),
    gsc AS (
      SELECT
        created_at,
        src_id,
        name,
        order_nr,
        lineId,
        target_to_ship,
        department,
        province,
        district,
        DNI,
        client_name,
        client_phone,
        client_email,
        MAX(tracking_code) AS tracking_code,
        MAX(GSC_Status) AS GSC_Status,
        MAX(src_status) AS src_status,
        SUM(paid_price) AS paid_price,
      FROM (
        SELECT
          a.shipping_provider_product,
          a.fk_shipment_type,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.GMT_5(a.created_at_utc) AS created_at,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.GMT_5(a.updated_at_utc) AS updated_at,
          c.src_id,
          c.name,
          b.order_nr,
          e.lineId,
          a.id_sales_order_item,
          a.tracking_code,
          a.sku,
          a.name AS sku_name,
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
        LEFT JOIN `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_order_lines` e ON e.orderNumber=b.order_nr
        WHERE b.fk_operator=2
        AND c.geography='national'
      )
      GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13
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
        a.orderLineId,
        a.deliveryOrderNumber,
        a.orderNumber,
        a.deliveryMethod,
        a.piso_promesa,
        a.techo_promesa,
        a.createdAt,
        b.status_catalyst AS status,
        TIMESTAMP(SUBSTR(b.status_time, 1, 24)) AS fecha,
        a.shipping_fee,
      FROM (
        SELECT
          orderLineId,
          deliveryOrderNumber,
          orderNumber,
          deliveryMethod,
          deliveryCostAmount AS shipping_fee,
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
      FROM `bi-fcom-drmb-sell-in-sbx.segmentacion_valor_sellers.peru`
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
    datos_factura AS (
      SELECT * FROM (
        SELECT
          order_nr AS deliveryOrderNumber,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_legal_name'),'"','') AS razon_social,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_address'),'"','') AS direccion,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_region'),'"','') AS region,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_municipality'),'"','') AS provincia,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.fiscal_person'),'"','') AS persona_fiscal,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.document_type'),'"','') AS tipo_de_documento,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.legal_id'),'"','') AS identificacion_legal,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_type_regimen'),'"','') AS actividad_economica,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.customer_verifier_digit'),'"','') AS digito_verificador,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_locality'),'"','') AS distrito,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_email'),'"','') AS correo_electronico,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_phonenumber'),'"','') AS telefono,
          REGEXP_REPLACE(JSON_EXTRACT(extra_billing_attributes, '$.receiver_postcode'),'"','') AS codigo_postal
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order` a
      )
      WHERE razon_social IS NOT NULL
    ),
montos AS (
  SELECT  
    deliveryOrderNumber,
    MAX(created_at) AS createdAt,
    MAX(DATE(created_at)) AS datecreatedAt,
    SUM(shipping_fee) AS shipping_fee,
    SUM(TotalPrecio) AS TotalPrecio,
    SUM(monto_total) AS monto_total
  FROM (
    SELECT
      catalyst.deliveryOrderNumber,
      gsc.created_at,
      IFNULL(catalyst.shipping_fee,0) AS shipping_fee,
      IFNULL(gsc.paid_price,0) AS TotalPrecio,
      IFNULL(catalyst.shipping_fee,0) + IFNULL(gsc.paid_price,0) AS monto_total
    FROM catalyst
    LEFT JOIN gsc ON gsc.order_nr=catalyst.deliveryOrderNumber
  )
  GROUP BY 1
),
data_final AS (
  SELECT
    montos.createdAt AS createdAt,
    DATE(montos.datecreatedAt) AS datecreatedAt,
    gsc.src_id AS sellerId,
    gsc.name AS sellerName,
    loyalty.segmento,
    catalyst.deliveryOrderNumber,
    COALESCE(gsc.tracking_code,tms.tracking) AS trackingCode,
    catalyst.deliveryMethod,
    gsc.GSC_Status AS lastStatusGSC,
    tms.estado AS lastStatusTMS,
    DATE(gsc.target_to_ship) AS promisedBySeller,
    LEAST(COALESCE(catalyst.piso_promesa,catalyst.techo_promesa),COALESCE(catalyst.techo_promesa,catalyst.piso_promesa)) AS promisedToClient,
    DATE(cambio_estado.terminated_at) AS terminatedDate,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.department)) AS department,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.province)) AS province,
    UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.district)) AS district,
    CONCAT('+51',sellerPhone) AS sellerPhone,
    sellerEmail,
    gsc.DNI AS clientDNI,
    INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(gsc.client_name)) AS clientName,
    gsc.client_phone AS clientPhone,
    gsc.client_email AS clientEmail,
    IFNULL(montos.shipping_fee,0) AS shipping_fee,
    IFNULL(montos.TotalPrecio,0) AS TotalPrecio,
    IFNULL(montos.monto_total,0) AS monto_total,
    --
    CASE WHEN datos_factura.razon_social IS NULL THEN 'Boleta' ELSE 'Factura' END AS tipo_documento,
    datos_factura.razon_social,
    direccion,
    region,
    provincia,
    persona_fiscal,
    tipo_de_documento,
    identificacion_legal,
    actividad_economica,
    digito_verificador,
    distrito,
    correo_electronico,
    telefono,
    codigo_postal,
    --
    DATE(invoices.invoiceDate) AS invoiceDate,
    invoices.invoiceType,
    invoices.invoiceNumber,
    --

    --
    IFNULL(integrado,'No') AS integrado,
    loyaltyPhone,
    ruc.ruc,
    ruc.razon_social AS razon_social_seller,
    CASE
      WHEN REGEXP_CONTAINS(ruc.ruc,'^10') THEN 'RUC10'
      WHEN REGEXP_CONTAINS(ruc.ruc,'^20') THEN 'RUC20'
      ELSE 'Otro' END AS tipo_ruc
  FROM catalyst
  LEFT JOIN gsc ON gsc.order_nr=catalyst.deliveryOrderNumber
  LEFT JOIN montos ON montos.deliveryOrderNumber=catalyst.deliveryOrderNumber
  LEFT JOIN invoices ON invoices.orderLineId=catalyst.orderLineId
  LEFT JOIN tms ON gsc.tracking_code=tms.tracking
  LEFT JOIN urbano ON gsc.tracking_code=urbano.tracking
  LEFT JOIN loyalty ON gsc.src_id=loyalty.id_seller
  LEFT JOIN comercial ON gsc.src_id=comercial.sellerId
  LEFT JOIN datos_sellers ON gsc.src_id=datos_sellers.sellerId
  LEFT JOIN sellers_aaa ON gsc.src_id=sellers_aaa.sellerId
  LEFT JOIN datos_factura ON gsc.order_nr=datos_factura.deliveryOrderNumber
  LEFT JOIN sellers_integrados ON gsc.src_id=sellers_integrados.sellerId
  LEFT JOIN (
    SELECT DISTINCT
      seller_id AS src_id,
      ruc,
      razon_social,
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.analisis_espacial_sellers`
  ) ruc ON ruc.src_id=gsc.src_id
  LEFT JOIN (
    SELECT 
      deliveryOrderNumber,
      MAX(terminated_at) AS terminated_at
    FROM bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado
    GROUP BY 1
  ) cambio_estado ON cambio_estado.deliveryOrderNumber=gsc.order_nr
  WHERE gsc.src_id NOT IN ('FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU','FALABELLA PERU','SODIMAC PERU', 'TOTTUS PERU')
  AND gsc.name NOT IN ('FALABELLA','SODIMAC','TOTTUS')
  --AND invoiceDate IS NULL
  --AND gsc.src_id NOT IN (SELECT * FROM sellers_integrados)
  --AND catalyst.techo_promesa>=DATE_ADD(CURRENT_DATE('America/Lima'), INTERVAL -1 MONTH)
  --AND gsc.GSC_Status='delivered'
  ORDER BY gsc.created_at 
),
solicitudes AS (

  WITH 
  solicitudes AS (
    SELECT 
      deliveryOrderNumber,
      date,
      caseNumber,
      caseOwner,
      caseBU,
      caseLevel1,
      caseLevel2,
      caseTipification,
      caseStatus,
      caseCreatedDate,
      caseDueDate,
      caseClosedDate,
      case_completion_hours,
      --case_completion_work_hours
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` 
    WHERE caseLevel1='Pedido Solicitud'
    AND caseTipification IN ('Cambio de boleta a factura','Cambio/correccion de datos de boleta/factura','Copia/solicitud de boleta/factura')
    AND deliveryOrderNumber IS NOT NULL
  ),
  n_solicitudes AS (
    SELECT
      deliveryOrderNumber,
      COUNT(DISTINCT caseNumber) AS n_solicitudes,
      MAX(caseNumber) AS caseNumber,
      MIN(caseCreatedDate) AS caseCreatedDate,
      MAX(caseClosedDate) AS caseClosedDate,
      MIN(DATE(caseCreatedDate)) AS date,
    FROM solicitudes
    GROUP BY 1
  )

  SELECT * FROM (
    SELECT 
      a.deliveryOrdernumber,
      b.date AS fecha_solicitud,
      b.caseNumber AS ticket_solicitud,
      a.caseOwner,
      a.caseBU,
      a.caseLevel1,
      a.caseLevel2,
      a.caseTipification,
      a.caseCreatedDate,
      a.caseClosedDate,
      a.caseStatus,
      DATE(a.caseCreatedDate) AS fecha_inicio_solicitud,
      DATE(a.caseClosedDate) AS fecha_final_solicitud,
      DATE_DIFF(CASE WHEN a.caseClosedDate IS NULL THEN CURRENT_DATE() ELSE DATE(a.caseClosedDate) END, DATE(a.caseCreatedDate), DAY) AS dias_solicitud,
      b.n_solicitudes
    FROM solicitudes a
    LEFT JOIN n_solicitudes b ON a.caseNumber=b.caseNumber
  )
  WHERE ticket_solicitud IS NOT NULL

)

SELECT
  *,
  CASE WHEN status_comprobante='Enviado' THEN 1 ELSE 0 END AS comprobante_enviado,
  CASE WHEN status_comprobante='Pendiente' THEN 1 ELSE 0 END AS comprobante_pendiente,
  CASE WHEN tipo_documento='Boleta' THEN 1 ELSE 0 END AS comprobante_boleta,
  CASE WHEN tipo_documento='Factura' THEN 1 ELSE 0 END AS comprobante_factura
FROM (
  SELECT DISTINCT
    a.* EXCEPT (integrado,loyaltyPhone,ruc,razon_social_seller,tipo_ruc),
    b.* EXCEPT (deliveryOrdernumber,dias_solicitud,n_solicitudes),
    CASE WHEN a.invoiceDate IS NULL THEN 'Pendiente' ELSE 'Enviado' END AS status_comprobante,
    DATE_DIFF(CASE WHEN COALESCE(a.invoiceDate,b.fecha_final_solicitud) IS NULL THEN CURRENT_DATE() ELSE DATE(COALESCE(a.invoiceDate,b.fecha_final_solicitud)) END, DATE(a.promisedBySeller), DAY) AS dias_comprobante,
    b.dias_solicitud,
    b.n_solicitudes,
    a.integrado,
    CASE
      WHEN a.loyaltyPhone='+573174415797' THEN 'wa.link/1apugh'
      WHEN a.loyaltyPhone='+573188067327' THEN 'wa.link/ycwgfj'
      ELSE NULL END AS EnlaceAAA,
    a.loyaltyPhone,
    a.ruc,
    a.razon_social_seller,
    a.tipo_ruc,
    CONCAT(
      EXTRACT(YEAR FROM datecreatedAt),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM datecreatedAt) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM datecreatedAt) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM datecreatedAt) AS STRING) END
    ) AS periodo_creacion_orden,
    CONCAT(
      EXTRACT(YEAR FROM promisedBySeller),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM promisedBySeller) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM promisedBySeller) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM promisedBySeller) AS STRING) END
    ) AS periodo_promesa_seller,
    CONCAT(
      EXTRACT(YEAR FROM promisedToClient),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM promisedToClient) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM promisedToClient) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM promisedToClient) AS STRING) END
    ) AS periodo_promesa_cliente,
    CONCAT(
      EXTRACT(YEAR FROM invoiceDate),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM invoiceDate) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM invoiceDate) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM invoiceDate) AS STRING) END
    ) AS periodo_carga_comprobante,
    CONCAT(
      EXTRACT(YEAR FROM fecha_inicio_solicitud),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_inicio_solicitud) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_inicio_solicitud) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM fecha_inicio_solicitud) AS STRING) END
    ) AS periodo_inicio_solicitud,
    CONCAT(
      EXTRACT(YEAR FROM fecha_final_solicitud),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_final_solicitud) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_final_solicitud) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM fecha_final_solicitud) AS STRING) END
    ) AS periodo_final_solicitud
  FROM data_final a
  LEFT JOIN solicitudes b ON a.deliveryOrderNumber=b.deliveryOrderNumber
)

)




