CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_planificacion_transporte` AS (

  WITH
  tms AS (

    WITH 
    tms AS (
      SELECT
        a.GUIA,
        a.deliveryOrderNumber,
        b.sku,
        b.items,
        a.ESTADO,
        a.FECHA_ESTADO,
        a.flujo,
        a.row,
      FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY GUIA ORDER BY FECHA_ESTADO DESC) AS row,
          CASE WHEN REGEXP_CONTAINS(GUIA,'DEV|RET') THEN 'IL' ELSE 'Recoleccion' END AS flujo
        FROM (  
          SELECT DISTINCT
            a.*,
            b.deliveryOrderNumber
          FROM (
            SELECT DISTINCT
              a.numeroEnvio AS GUIA,
              INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.estado)) AS ESTADO,
              SAFE_CAST(a.fecha AS DATETIME) AS FECHA_ESTADO,
              INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.oficina)) AS OFICINA,  
              --INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS USUARIO, 
              --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
            FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_log_GSC a -- OK -- NO HAY EN PROD
            LEFT JOIN tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX b ON a.numeroEnvio=b.tracking
          ) a
          LEFT JOIN (
            SELECT DISTINCT * FROM (
              /**
              SELECT DISTINCT
                ordenServicio AS deliveryOrderNumber,
                numero AS GUIA
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              UNION ALL
              SELECT DISTINCT
                numeroExterno AS deliveryOrderNumber,
                numero AS GUIA
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env`
              UNION ALL
              SELECT DISTINCT 
                numeroExterno AS deliveryOrderNumber,
                numeroEnvio AS GUIA
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envioconsolidado_env`
            )
            **/
              SELECT DISTINCT
                deliveryOrderNumber,
                tracking AS GUIA
              FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX
              UNION ALL
              SELECT DISTINCT
                deliveryOrderNumber,
                GUIA
              FROM tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envioconsolidado_env_SX
            )
          ) b ON a.GUIA=b.GUIA
        )
      ) a
      LEFT JOIN (
        SELECT DISTINCT
          deliveryOrderNumber,
          sku,
          items
        FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` 
      ) b ON b.deliveryOrderNumber=a.deliveryOrderNumber
    )

    -- TMS Recolección
    SELECT * FROM tms
    WHERE flujo='Recoleccion'
    UNION ALL
    -- TMS IL
    SELECT * FROM (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY GUIA ORDER BY FECHA_ESTADO DESC) AS row,
      FROM (
        SELECT DISTINCT
          a.GUIA_IL AS GUIA,
          b.deliveryOrderNumber,
          b.sku,
          b.items,
          a.ESTADO,
          a.FECHA_ESTADO,
          a.flujo,
        FROM (
          SELECT
            a.GUIA AS GUIA_IL,
            REGEXP_REPLACE(a.GUIA,'RET|DEV|REC','') AS GUIA,
            a.* EXCEPT(GUIA,deliveryOrderNumber,sku,items)
          FROM tms a
        ) a
        LEFT JOIN (
          SELECT DISTINCT
            GUIA,
            deliveryOrderNumber,
            sku,
            items
          FROM tms b
        ) b ON a.GUIA=b.GUIA
        WHERE a.flujo='IL'
      )
    )
  ),
  planificacion AS ( -- OK!!
    SELECT
      CAST(numero_order AS STRING) AS deliveryOrderNumber,
      rastreo,
      'PLANIFICADO' AS Tipo,
      Estado,
      Motivo,
      Condicion,
      Fecha_Planificaci_n AS fecha_planificacion,
      ROW_NUMBER() OVER (PARTITION BY numero_order ORDER BY Fecha_Planificaci_n ASC) AS row
    FROM (
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_01`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_02`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_03`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_04`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_05`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gengar_Planificacion.2024_06`
    )
  ),
  ingreso_no_planif AS (
    SELECT
      b.deliveryOrderNumber,
      a.Rastreo AS rastreo,
      'NO PLANIFICADO' AS Tipo,
      'INGRESO SIN PLANIFICACION' AS Estado,
      'NA' AS Motivo,
      'NA' AS Condicion,
      Fecha_Registro AS fecha_planificacion,
      ROW_NUMBER() OVER (PARTITION BY b.deliveryOrderNumber ORDER BY Fecha_Registro ASC) AS row
    FROM ( --33123
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_01`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_02`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_03`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_04`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_05`
      UNION ALL
      SELECT * FROM `bi-fcom-drmb-local-pe-sbx.Gastly_IngresoNoPlanificado.2024_06`
    ) a
    LEFT JOIN (
      SELECT DISTINCT
        deliveryOrderNumber,
        GUIA AS Rastreo
      FROM tms
    ) b ON a.Rastreo=b.Rastreo
  ),
  tickets_fcom AS ( -- OK!!
    SELECT
      a.*,
      b.caseOwner AS agente,
      DATE(b.caseCreatedDate) AS fecha_creacion_ticket,
      DATE(b.caseClosedDate) AS fecha_cierre_ticket,
      ROW_NUMBER() OVER (PARTITION BY a.deliveryOrderNumber ORDER BY b.caseCreatedDate ASC) AS row
    FROM (
      SELECT DISTINCT
        Casos AS caseNumber,
        OC AS deliveryOrderNumber,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Motivo_0) AS motivo_seller,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Motivo_1) AS condicion_seller,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Observaciones) AS detalle_seller,
        CASE WHEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(LOWER(A_favor_del_Seller))='si' THEN 'A favor de seller' ELSE 'Error de seller' END AS revision_sx,
      FROM `bi-fcom-drmb-local-pe-sbx.sx_google_sheets.tickets_fcom`
    ) a
    LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` b ON a.caseNumber=b.caseNumber
  )

  SELECT
    *,
    CONCAT(
      EXTRACT(YEAR FROM fecha_planificacion),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_planificacion) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_planificacion) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM fecha_planificacion) AS STRING) END
    ) AS periodo_planificacion,
    CONCAT(
      EXTRACT(YEAR FROM fecha_creacion_ticket),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_creacion_ticket) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_creacion_ticket) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM fecha_creacion_ticket) AS STRING) END
    ) AS periodo_creacion_ticket,
    CONCAT(
      EXTRACT(YEAR FROM fecha_cierre_ticket),
      '-',
      CASE 
        WHEN LENGTH(CAST(EXTRACT(MONTH FROM fecha_cierre_ticket) AS STRING))=1 THEN CONCAT('0',CAST(EXTRACT(MONTH FROM fecha_cierre_ticket) AS STRING))
        ELSE CAST(EXTRACT(MONTH FROM fecha_cierre_ticket) AS STRING) END
    ) AS periodo_cierre_ticket
  FROM (
    SELECT
      b.sellerId,
      b.sellerName,
      b.segmento,
      b.orderNumber,
      a.deliveryOrderNumber,
      --b.sku,
      --b.variation,
      b.shipping_type,
      b.shipping_provider_product,
      b.deliveryMethod,
      b.pickupPoint,
      a.rastreo,
      a.* EXCEPT(deliveryOrderNumber,rastreo)
    FROM (
      SELECT
        a.* EXCEPT(row),
        b.* EXCEPT(deliveryOrderNumber,row)
      FROM ( -- 1006010
        SELECT * FROM planificacion
        UNION ALL
        SELECT * FROM ingreso_no_planif
      ) a
      LEFT JOIN tickets_fcom b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.row=b.row
    ) a
    LEFT JOIN (
      SELECT DISTINCT
        sellerId,
        sellerName,
        segmento,
        orderNumber,
        deliveryOrderNumber,
        shipping_type,
        shipping_provider_product,
        deliveryMethod,
        pickupPoint
      FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado`
    ) b ON b.deliveryOrderNumber=a.deliveryOrderNumber
  )

)
--WHERE a.deliveryOrderNumber in ('2199918698','2199891633','2199305768')



















