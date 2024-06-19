CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_devoluciones` AS (

  WITH 
  rlos AS ( --OK
    SELECT DISTINCT * FROM (
      SELECT
        a.* EXCEPT (Tienda),
        CASE
          WHEN REGEXP_CONTAINS(Tienda,'GSC-SC') THEN NULL
          WHEN Tienda='NA' THEN NULL
          ELSE Tienda END AS Tienda
      FROM (
        SELECT
          a.rlo_id,
          COALESCE(a.deliveryOrderNumber,d.deliveryOrderNumber) AS deliveryOrderNumber,
          COALESCE(b.sku,d.sku) AS sku,
          SAFE_CAST(COALESCE(b.items_il,d.items_il) AS INT64) AS items_il,
          COALESCE(c.guia_il,d.guia_il) AS guia_il,
          IFNULL(COALESCE(d.Tienda,a.Tienda),'NA') AS Tienda,
          COALESCE(a.reason_code_category,a.reason_code_category) AS reason_code_category,
          COALESCE(a.reason_code_sub_category,d.reason_code_sub_category) AS reason_code_sub_category
        FROM (
          SELECT 
            a.*,
            b.node_name AS Tienda,
          FROM (
            SELECT
              country,
              rlo_id,
              MAX(CASE WHEN reference_typ='returnOrderLineId' THEN reference_val ELSE NULL END) AS returnOrderLineId,
              MAX(CASE WHEN reference_typ='associatedDeliveryOrders' THEN reference_val ELSE NULL END) AS deliveryOrderNumber,
              MAX(node_id) AS node_id,
              MAX(reason_code_category) AS reason_code_category,
              MAX(reason_code_sub_category) AS reason_code_sub_category
            FROM (
              SELECT * FROM (
                SELECT DISTINCT
                  country,
                  rlo_id,
                  rlo_item_ref.reference_typ,
                  rlo_item_ref.reference_val,
                  return_option.node_id AS node_id,
                  return_reason.reason_code_category,
                  return_reason.reason_code_sub_category,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_vw_acc_scha_rlo_packaging_plan` a
                UNION ALL
                SELECT DISTINCT
                  country,
                  rlo_id,
                  'NA' AS reference_typ,
                  'NA' AS reference_val,
                  return_option.node_id AS node_id,
                  return_reason.reason_code_category,
                  return_reason.reason_code_sub_category,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_vw_acc_scha_rlo_packaging_plan` a
                UNION ALL
                SELECT DISTINCT
                  country,
                  rlo_id,
                  rlo_item_ref.reference_typ,
                  rlo_item_ref.reference_val,
                  return_option.node_id AS node_id,
                  return_reason.reason_cd_category AS reason_code_category,
                  return_reason.reason_cd_sub_category AS reason_code_sub_category,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_svw_vw_acc_scha_rlo_creation` a            
              )
              WHERE country='PE'
            )
          GROUP BY 1,2
          ) a
          LEFT JOIN (
            SELECT DISTINCT
              node_id,
              `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(node_name) AS node_name,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_pe_dfl_prod.svw_svw_vw_acc_scha_dntm_nodes` 
          ) b ON b.node_id=a.node_id
        ) a
        LEFT JOIN (
          SELECT DISTINCT
            returnLineId AS returnOrderLineId,
            variantId AS sku,
            quantityNumber AS items_il
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_catalyst_prd_pe.svw_customer_order_return`
        ) b ON b.returnOrderLineId=a.returnOrderLineId
        LEFT JOIN (
          SELECT DISTINCT * FROM (
            SELECT
              *,
              ROW_NUMBER() OVER (PARTITION BY country,rlo_id ORDER BY guia_il DESC) AS row,
            FROM (
              SELECT DISTINCT
                country,
                rlo_id,
                tracking_num AS guia_il,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_vw_acc_scha_rlo_packaging_plan` a
              UNION ALL
              SELECT DISTINCT
                country,
                rlo_id,
                tracking_num AS guia_il,
              FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_corp_dfl_prod.svw_svw_vw_acc_scha_rlo_creation` 
            )
          )
          WHERE row=1 AND country='PE'
        ) c ON c.rlo_id=a.rlo_id
        LEFT JOIN (
          SELECT
            rlo_id,
            deliveryOrderNumber,
            sku,
            items_il,
            guia_il,
            COALESCE(b.Tienda,a.Tienda) AS Tienda,
            reason_code_category,
            reason_code_sub_category
          FROM (
            SELECT
              rlo_id,
              OC AS deliveryOrderNumber,
              variant_id AS sku,
              SAFE_CAST(quantity AS STRING) AS items_il,
              tracking_num AS guia_il,
              CASE WHEN type='HOME_PICKUP' THEN 'HOME_PICKUP' ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(nodeName) END AS Tienda,
              reason_cd_category AS reason_code_category,
              reason_cd_sub_category AS reason_code_sub_category,
              dropoff_node_id
            FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.HD_RLO_PE`
          ) a
          LEFT JOIN (
            SELECT DISTINCT
              node_id,
              `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(node_name) AS Tienda,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_svw_tc_sc_bi_bigdata_dfl_prod_acc_corp_pe_dfl_prod.svw_svw_vw_acc_scha_dntm_nodes` 
          ) b ON b.node_id=a.dropoff_node_id
        ) d ON d.rlo_id=a.rlo_id
      ) a
    ) 
  ),
  tms AS ( --OK

    WITH 
    tms AS (
      SELECT
        a.GUIA,
        COALESCE(c.deliveryOrderNumber,a.deliveryOrderNumber) AS deliveryOrderNumber,
        COALESCE(b.sku,c.sku) AS sku,
        COALESCE(b.items,c.quantityNumber) AS items,
        a.ESTADO,
        a.FECHA_ESTADO,
        a.flujo,
        a.row,
      FROM (
        SELECT 
          *,
          ROW_NUMBER() OVER (PARTITION BY GUIA ORDER BY FECHA_ESTADO DESC) AS row,
          --CASE WHEN REGEXP_CONTAINS(GUIA,'DEV|REC|RET') THEN 'IL' ELSE 'Recoleccion' END AS flujo,
          CASE WHEN REGEXP_CONTAINS(GUIA,'RET') OR tipo='IL' THEN 'IL' ELSE 'Recoleccion' END AS flujo
        FROM (  
          SELECT DISTINCT
            a.*,
            b.deliveryOrderNumber
          FROM (
            
            WITH 
            data_tms AS (
              /**
              SELECT DISTINCT
                a.numeroEnvio AS GUIA,
                INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreEstadoEnvio)) AS ESTADO,
                SAFE_CAST(a.creacion AS DATETIME) AS FECHA_ESTADO,
                INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreOficina)) AS OFICINA,  
                INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS USUARIO, 
                --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.enviolog_env` a
              LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.REPLICA_ESPEJO_TMS.envio_env` b ON a.numeroEnvio=b.numeroExterno
              **/
              SELECT DISTINCT
                a.tracking AS GUIA,
                INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.estado)) AS ESTADO,
                SAFE_CAST(a.fecha AS DATETIME) AS FECHA_ESTADO,
                --INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.nombreOficina)) AS OFICINA,  
                --INITCAP(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(a.usuario),",","")) AS USUARIO, 
                --INITCAP(REGEXP_REPLACE(REGEXP_REPLACE(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.direccionDest),",|;",""),'"',"")) AS direccion, 
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_enviolog_env_SX` a
              LEFT JOIN `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX` b ON a.tracking=b.tracking
              --WHERE NOT REGEXP_CONTAINS(a.tracking,'240111000002670912')
            ),
            data_tms_il AS (
              SELECT * EXCEPT(GUIA_IL,ESTADO_IL) FROM (
                SELECT
                  a.GUIA,
                  a.tipo,
                  a.GUIA_IL,
                  b.ESTADO AS ESTADO_IL
                FROM (
                  SELECT DISTINCT
                    GUIA,
                    CONCAT('RET',GUIA) AS GUIA_IL,
                    'IL' AS tipo
                  FROM data_tms
                  WHERE --REGEXP_CONTAINS(OFICINA,'Zonasfcom_Il') OR 
                  ESTADO IN ('En Devolucion','Recojo seller')
                ) a
                LEFT JOIN (
                  SELECT
                    GUIA,
                    ESTADO
                  FROM data_tms
                  QUALIFY ROW_NUMBER() OVER (PARTITION BY GUIA ORDER BY FECHA_ESTADO DESC)=1
                ) b ON b.GUIA=a.GUIA_IL
              )
              WHERE ESTADO_IL IS NULL
            )

            SELECT 
              a.*,
              b.* EXCEPT(GUIA)
            FROM data_tms a
            LEFT JOIN data_tms_il b ON a.GUIA=b.GUIA

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
              **/
              SELECT DISTINCT 
                deliveryOrderNumber,
                tracking AS GUIA
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envio_env_SX`
              UNION ALL
              SELECT DISTINCT
                deliveryOrderNumber,
                GUIA
              FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_TMS_envioconsolidado_env_SX`
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
      LEFT JOIN (
        SELECT DISTINCT
          rlo_id,
          deliveryOrderNumber,
          sku,
          quantityNumber
        FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_vista_rlos`
      ) c ON c.rlo_id=a.deliveryOrderNumber
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
            --REGEXP_REPLACE(a.GUIA,'RET|DEV|REC','') AS GUIA, -- Parece que REC y DEV son recolecciones
            REGEXP_REPLACE(a.GUIA,'RET','') AS GUIA,
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
  urbano_il AS (

    WITH 
    urbano_il AS (
      SELECT
        *,
        CASE 
          --WHEN REGEXP_CONTAINS(deliveryOrderNumber,'^1') AND LENGTH(deliveryOrderNumber)=10 THEN 'Catalyst'
          WHEN REGEXP_CONTAINS(deliveryOrderNumber,'^2') AND LENGTH(deliveryOrderNumber)=10 THEN 'FSC'
          WHEN REGEXP_CONTAINS(deliveryOrderNumber,'^RLO') THEN 'RLO'
          WHEN LENGTH(deliveryOrderNumber)>12 THEN 'Rastreo'
          ELSE 'Otro' END AS tipo
      FROM (
        SELECT
          deliveryOrderNumber,
          CASE WHEN REGEXP_CONTAINS(sku,'^1') AND LENGTH(sku)=9 THEN sku ELSE NULL END AS sku,
          items_il,
          n_guia,
          guia_il,
          estado_il,
          fecha_solicitud_il,
          fecha_recoleccion_il,
          fecha_entrega_il,
          n_visitas_il
        FROM (
          SELECT
            REGEXP_REPLACE(REGEXP_EXTRACT(CODIGO_REFERENCIA, r'^([^-]*)'),' PROD','') AS deliveryOrderNumber,
            REGEXP_EXTRACT(CONTENIDO, r'^(\d+)') AS sku,
            SAFE_CAST(IFNULL(PIEZAS,'1') AS INT64) AS items_il,
            IFNULL(SAFE_CAST(REGEXP_EXTRACT(CODIGO_REFERENCIA, r'-([^.]*)$') AS INT64),1) AS n_guia,
            GUIA AS guia_il,
            ESTADO_ACTUAL AS estado_il,
            SAFE_CAST(FECHA_SS AS DATETIME) AS fecha_solicitud_il,
            SAFE_CAST(FECHA_AO AS DATETIME) AS fecha_recoleccion_il,
            SAFE_CAST((CASE WHEN ESTADO_ACTUAL='ENTREGADO' THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_entrega_il,
            SAFE_CAST(NRO_VISITAS AS INT64) AS n_visitas_il,
            --guias_entregadas,guias_en_proceso,guias_pendientes,guia_custodia,guias_siniestro
          FROM `bi-fcom-drmb-local-pe-sbx.Pidgeotto_Devolucion.IL_URBANO`
        )
      )
    )

    -- FSC todo ok
    SELECT * FROM urbano_il
    WHERE tipo='FSC' AND sku IS NOT NULL
    UNION ALL
    -- FSC con nulos
    SELECT * FROM (  
      SELECT 
        u.deliveryOrderNumber,
        o.sku,
        COALESCE(u.items_il,o.items) AS items_il,
        u.n_guia,
        u.guia_il,
        u.estado_il,
        u.fecha_solicitud_il,
        u.fecha_recoleccion_il,
        u.fecha_entrega_il,
        u.n_visitas_il,
        u.tipo
      FROM (
        SELECT * FROM urbano_il u
        WHERE u.tipo='FSC' AND u.sku IS NULL
      ) u
      LEFT JOIN (
        
        WITH 
        orders AS (  
          SELECT DISTINCT
            deliveryOrderNumber,
            sku,
            items
          FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` 
        )

          SELECT  
            o1.deliveryOrderNumber,
            o.sku,
            o.items,
          FROM (
            SELECT * FROM (
              SELECT 
                deliveryOrderNumber,
                COUNT(DISTINCT sku) AS n_skus
              FROM orders
              GROUP BY 1
            )
            WHERE n_skus=1
          ) o1
          LEFT JOIN orders o ON o1.deliveryOrderNumber=o.deliveryOrderNumber

      ) o ON u.deliveryOrderNumber=o.deliveryOrderNumber
    )
    UNION ALL
    -- RLO
    SELECT * FROM urbano_il
    WHERE tipo='RLO'
    UNION ALL
    SELECT * FROM urbano_il
    WHERE tipo='Rastreo'
    UNION ALL
    SELECT * FROM urbano_il
    WHERE tipo='Otro'

  )

-- Hasta aqui atrás TODO OK

  SELECT DISTINCT
    * EXCEPT(estado_consolidado_il),
    CASE 
      WHEN dias_devolucion<=30 THEN '00-30'
      WHEN dias_devolucion>30 THEN '30+'
      ELSE NULL END AS rango_dias_devolucion,
    estado_consolidado_il,
    CASE
      WHEN estado_consolidado_il IN ('Entregado','Custodia','Siniestro') THEN 1
      WHEN estado_consolidado_il IN ('En Reparto','En Almacén') THEN 2
      WHEN estado_consolidado_il IN ('Excepcion de Entrega') THEN 3
      WHEN estado_consolidado_il IN ('Registrado') THEN 4
      ELSE 5 END AS orden_estado_il
  FROM (
    SELECT 
      *,
      DATE_DIFF(fecha_entrega_il,fecha_egreso, DAY) AS dias_devolucion,
      CASE 
        WHEN UPPER(estado_il) IN ('CUSTODIA','CUSTODIA DE OPL','En Devolucion') THEN 'Custodia' --1
        WHEN UPPER(estado_il) IN ('DISPONIBLE PARA RETIRO','ADMITIDO EN HUB','EN BODEGA CROSSDOCK','IMPO (SE QUEDA EN QOLQAS)',
                                  'AVAILABLE_FOR_PICKUP'
                                  ) THEN 'En Almacén' --2
        WHEN UPPER(estado_il) IN ('DESPACHADO A DESTINO','REINGRESO','EN REPARTO','SALIO A RUTA','EN PROCESO DE ENTREGA',
                                  'ARRIBADO EN DESTINO','EN TRANSITO A ORIGEN','EN TRANSITO BODEGA OL','TRANSITO LOCAL',
                                  'EXCEPCION DE ENTREGA','VISITADO SIN ENTREGA',
                                  'DELIVERY_ATTEMPTED','IN_TRANSIT','ON_ROUTE','OUT_FOR_DELIVERY','SHIPMENT_LOADED'
                                  ) THEN 'En Reparto' --2
        WHEN UPPER(estado_il) IN ('ENTREGADO EN TIENDA') THEN 'En Tienda' --5
        WHEN UPPER(estado_il) IN ('ENTREGADO','RETORNO A SELLER','ENTREGADO AL SELLER','DEVOLUCION AL SHIPPER','DELIVERED') THEN 'Entregado' --1
        WHEN UPPER(estado_il) IN ('FALLO DE ENTREGA','FALLO DE ENTREGA CT') THEN 'Fallo de Entrega' --5
        WHEN UPPER(estado_il) IN ('SOLICITUD DE SERVICIO','REGISTRADO','RECEPCION',
                                  'PENDING','RECEIVED'
                                  ) THEN 'Registrado' --4
        WHEN UPPER(estado_il) IN ('ENVIO SINIESTRADO','SINIESTRO','SINIESTRO DE OPL') THEN 'Siniestro' --1
        WHEN UPPER(estado_il) IN ('ANULADO','MAL DESPACHO','ANNULLED') THEN 'Anulado' --6
        WHEN LENGTH(estado_il)=0 THEN NULL
        ELSE estado_il END AS estado_consolidado_il
    FROM (
      SELECT 
        CASE 
          WHEN REGEXP_CONTAINS(LOWER(a.opl),'99') THEN '99Minutos'
          WHEN REGEXP_CONTAINS(LOWER(a.opl),'andes') THEN 'Andes Express'
          WHEN REGEXP_CONTAINS(LOWER(a.opl),'tms|own') THEN 'Own Fleet'
          WHEN REGEXP_CONTAINS(UPPER(a.guia_il),'REC|RET|DEV') THEN 'Own Fleet'
          WHEN REGEXP_CONTAINS(LOWER(a.opl),'urbano') THEN 'Urbano'
          WHEN REGEXP_CONTAINS(UPPER(a.guia_il),'WYB') THEN 'Urbano'
          WHEN LOWER(a.opl)='-' THEN 'Own Fleet'
          ELSE INITCAP(a.opl) END AS opl,
        a.rlo_id,
        a.deliveryOrderNumber,
        a.sku,
        a.items_il,
        a.n_guia,
        SAFE_CAST(a.guia_il AS STRING) AS guia_il,
        COALESCE(a.estado_il,b.ESTADO) AS estado_il,
        c.fecha_egreso,
        DATE(COALESCE(a.fecha_solicitud_il,b.fecha_solicitud_il)) AS fecha_solicitud_il,
        DATE(COALESCE(a.fecha_recoleccion_il,b.fecha_recoleccion_il)) AS fecha_recoleccion_il,
        DATE(COALESCE(a.fecha_entrega_il,b.fecha_entrega_il)) AS fecha_entrega_il,
        COALESCE(a.n_visitas_il,b.n_visitas_il) AS n_visitas_il,
        a.tipo,
        c.items,
        c.egreso,
        a.Tienda,
        a.reason_code_category,
        a.reason_code_sub_category,
      FROM (
        -- Urbano
        SELECT 
          'Urbano' AS opl,
          'NA' AS rlo_id,
          'NA' AS Tienda,
          'NA' AS reason_code_category,
          'NA' AS reason_code_sub_category,
          *
        FROM urbano_il
        WHERE tipo='FSC'
        UNION ALL
        SELECT
          'Urbano' AS opl,
          a.rlo_id,          
          b.Tienda,
          b.reason_code_category,
          b.reason_code_sub_category,
          b.deliveryOrderNumber,
          b.sku,
          b.items_il,
          a.n_guia,
          COALESCE(a.guia_il,b.guia_il) AS guia_il,
          a.estado_il,
          a.fecha_solicitud_il,
          a.fecha_recoleccion_il,
          a.fecha_entrega_il,
          a.n_visitas_il,
          a.tipo
        FROM (
          SELECT 
            deliveryOrderNumber AS rlo_id,
            * EXCEPT(deliveryOrderNumber)
          FROM urbano_il
          WHERE tipo='RLO'
        ) a
        LEFT JOIN rlos b ON a.rlo_id=b.rlo_id
        UNION ALL
        -- Rastreo
        SELECT
          'Urbano' AS opl,
          'NA' AS Tienda,
          'NA' AS reason_code_category,
          'NA' AS reason_code_sub_category,
          a.rlo_id,
          b.deliveryOrderNumber,
          b.sku,
          b.items AS items_il,
          a.n_guia,
          a.guia_il,
          a.estado_il,
          a.fecha_solicitud_il,
          a.fecha_recoleccion_il,
          a.fecha_entrega_il,
          a.n_visitas_il,
          a.tipo
        FROM (
          SELECT
            'NA' AS rlo_id,
            deliveryOrderNumber AS GUIA,
            * EXCEPT(deliveryOrderNumber,sku,items_il)
          FROM urbano_il
          WHERE tipo='Rastreo'
        ) a
        LEFT JOIN (
          SELECT * FROM tms WHERE row=1
        ) b ON a.GUIA=b.GUIA
        UNION ALL
        -- Otros
        SELECT 
          'Urbano' AS opl,
          'NA' AS rlo_id,
          'NA' AS Tienda,
          'NA' AS reason_code_category,
          'NA' AS reason_code_sub_category,
          *
        FROM urbano_il
        WHERE tipo='Otro'
        UNION ALL
        -- TMS IL 
        SELECT * FROM (
          SELECT 
            'TMS' AS opl,
            'NA' AS rlo_id,
            'NA' AS Tienda,
            'NA' AS reason_code_category,
            'NA' AS reason_code_sub_category,
            deliveryOrderNumber,
            sku,
            items AS items_il,
            1 AS n_guia,
            GUIA AS guia_il,
            MAX(CASE WHEN row=1 THEN estado ELSE NULL END) AS estado_il,
            MAX(CASE WHEN estado='Registrado' THEN FECHA_ESTADO ELSE NULL END) AS fecha_solicitud_il,
            MAX(CASE WHEN estado='En Reparto' THEN FECHA_ESTADO ELSE NULL END) AS fecha_recoleccion_il,
            MAX(CASE WHEN estado='Entregado' THEN FECHA_ESTADO ELSE NULL END) AS fecha_entrega_il,
            IFNULL(COUNT(DISTINCT CASE WHEN estado='Excepcion De Entrega' THEN FECHA_ESTADO ELSE NULL END),0)+IFNULL(COUNT(DISTINCT CASE WHEN estado='Entregado' THEN FECHA_ESTADO ELSE NULL END),0) AS n_visitas_il,
            'Rastreo' AS tipo
          FROM tms
          WHERE flujo='IL'
        GROUP BY 1,2,3,4,5,6,7,8,9,10
        )
        -- Home Pickup
        UNION ALL
        SELECT
          a.* EXCEPT (estado_il,fecha_solicitud_il,fecha_recoleccion_il,fecha_entrega_il,n_visitas_il,tipo),
          COALESCE(b.ESTADO,a.estado_il) AS estado_il,
          COALESCE(b.fecha_solicitud_il,a.fecha_solicitud_il) AS fecha_solicitud_il,
          COALESCE(b.fecha_recoleccion_il,a.fecha_recoleccion_il) AS fecha_recoleccion_il,
          COALESCE(b.fecha_entrega_il,a.fecha_entrega_il) AS fecha_entrega_il,
          COALESCE(b.n_visitas_il,a.n_visitas_il) AS n_visitas_il,
          tipo
        FROM (
          -- Devoluciones Offline 2023 -- FALTA CORREGIR FECHAS Y DATETIME
          SELECT * FROM (
            SELECT
              CASE WHEN REGEXP_CONTAINS(OPL,'FLOTA IL|FLOTA UM CTs') THEN 'OWN FLEET' ELSE OPL END AS opl,
              'HOME_PICKUP' AS rlo_id,
              TIENDA AS Tienda,
              'NA' AS reason_code_category,
              'NA' AS reason_code_sub_category,
              ORDEN_SELLER AS deliveryOrderNumber,
              SKU AS sku,
              SAFE_CAST(1 AS INT64) AS items_il,
              1 AS n_guia,
              GUIA AS guia_il,
              UPPER(COALESCE(ESTADO_ENTREGA,ESTADO_DE_RECOLECCION)) AS estado_il,
              SAFE_CAST(PARSE_DATE('%d/%m/%Y', CONCAT(day_solicitud,'/',month_solicitud,'/',year_solicitud)) AS DATETIME) AS fecha_solicitud_il,
              SAFE_CAST(NULL AS DATETIME) AS fecha_recoleccion_il,
              SAFE_CAST(NULL AS DATETIME) AS fecha_entrega_il,
              --CONCAT(day_recoleccion,'/',month_recoleccion,'/',year_recoleccion)  AS fecha_recoleccion_il,  
              --CONCAT(day_entrega,'/',month_entrega,'/',year_entrega)  AS fecha_entrega_il,
              NULL AS n_visitas_il,
              'Offline' AS tipo,
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
          --WHERE fecha_solicitud_il IS NOT NULL
          UNION ALL
          -- Home Pickup 2023
          SELECT 
            CASE WHEN REGEXP_CONTAINS(Operador,'FLOTA IL|FLOTA UM CTs') THEN 'OWN FLEET' ELSE Operador END AS opl,
            'HOME_PICKUP' AS rlo_id,
            'NA' AS Tienda,
            'NA' AS reason_code_category,
            'NA' AS reason_code_sub_category,
            Orden_Seller AS deliveryOrderNumber,
            Sku AS sku,
            SAFE_CAST(1 AS INT64) AS items_il,
            1 AS n_guia,
            SAFE_CAST(Guia AS STRING) AS guia_il,
            REGEXP_REPLACE(Estado_de_Entrega_Seller,'-','') AS estado_il,
            SAFE_CAST(PARSE_DATE('%m/%d/%Y',Fecha_registro_Postventa) AS DATETIME) AS fecha_solicitud_il,
            SAFE_CAST(NULL AS DATETIME) AS fecha_recoleccion_il,
            SAFE_CAST(NULL AS DATETIME) AS fecha_entrega_il,
            NULL AS n_visitas_il,
            'Offline' AS tipo,
          FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.Home_Pickup`
          WHERE Fecha_registro_Postventa IS NOT NULL
          UNION ALL
          -- Home Pickup 2024
          SELECT
            opl,
            rlo_id,
            'NA' AS Tienda,
            'NA' AS reason_code_category,
            'NA' AS reason_code_sub_category,
            deliveryOrderNumber,
            sku,
            items_il,
            n_guia,
            SAFE_CAST(COALESCE(guia_il,Guia_Integracion) AS STRING) AS guia_il,
            COALESCE(estado_il,estado_recoleccion_il) AS estado_il,
            fecha_solicitud_il,
            fecha_recoleccion_il,
            fecha_entrega_il,
            n_visitas_il,
            tipo
          FROM (
            SELECT
              CASE 
                WHEN OPERADOR_FINAL_ASIGNADO IN ('ACJ','MOY','SEDEL','CTs UM') THEN 'OWN FLEET' 
                ELSE OPERADOR_FINAL_ASIGNADO END AS opl, -- Esta columna se ha cambiado
              'HOME_PICKUP' AS rlo_id,
              deliveryOrderNumber,
              sku,
              SAFE_CAST(1 AS INT64) AS items_il,
              1 AS n_guia,
              COALESCE(
                CASE WHEN GUIA_ENTREGA IS NULL THEN NULL ELSE GUIA_ENTREGA END,
                CASE WHEN LENGTH(Guia_Integracion)=0 OR Motivo_Cancelacion_Guia_Integracion!='-' THEN NULL ELSE Guia_Integracion END,
                CASE WHEN LENGTH(Guia_Manual_revision)=0 THEN NULL ELSE Guia_Manual_revision END
              ) AS guia_il,
              estado_il,
              fecha_solicitud_il,
              fecha_recoleccion_il,
              fecha_entrega_il,
              NULL AS n_visitas_il,
              'Offline' AS tipo,
              ---
              estado_recoleccion_il,
              Guia_Integracion,
            FROM (
              SELECT
                OPERADOR_FINAL_ASIGNADO,
                orden_seller AS deliveryOrderNumber,
                sku,
                REGEXP_REPLACE(SAFE_CAST(Estado_Entrega AS STRING),'-','') AS estado_il,
                SAFE_CAST(SAFE.PARSE_DATE('%m/%d/%Y',fecha_registro_postventa) AS DATETIME) AS fecha_solicitud_il,
                SAFE_CAST((CASE WHEN Estado_General_Recoleccion='RECOLECTADO' THEN Fecha_Estado ELSE NULL END) AS DATETIME) AS fecha_recoleccion_il,
                SAFE_CAST((CASE WHEN Estado_Entrega='ENTREGADO' THEN Fecha_Entrega ELSE NULL END) AS DATETIME) AS fecha_entrega_il,
                REGEXP_REPLACE(SAFE_CAST(GUIA_ENTREGA AS STRING),'-','') AS GUIA_ENTREGA,
                REGEXP_REPLACE(SAFE_CAST(Guia_Integracion AS STRING),'-','') AS Guia_Integracion,
                REGEXP_REPLACE(SAFE_CAST(Guia_Manual_revision AS STRING),'-','') AS Guia_Manual_revision,
                REGEXP_REPLACE(SAFE_CAST(Motivo_Cancelacion_Guia_Integracion AS STRING),'-','') AS Motivo_Cancelacion_Guia_Integracion,
                REGEXP_REPLACE(Estado_General_Recoleccion,'-','') AS estado_recoleccion_il
              FROM bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.Home_Pickup_2024
              WHERE fecha_registro_postventa IS NOT NULL
            )
          )        
        ) a
        LEFT JOIN (
          SELECT 
            GUIA,
            MAX(CASE WHEN row=1 THEN ESTADO ELSE NULL END) AS ESTADO,
            SAFE_CAST(MAX(CASE WHEN estado='Registrado' THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_solicitud_il,
            SAFE_CAST(MAX(CASE WHEN estado IN ('En Reparto','En bodega Crossdock') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_recoleccion_il,
            SAFE_CAST(MAX(CASE WHEN estado IN ('Entregado','Retorno A Seller') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_entrega_il,
            IFNULL(COUNT(DISTINCT CASE WHEN estado='Excepcion de Entrega' THEN FECHA_ESTADO ELSE NULL END),0)+IFNULL(COUNT(DISTINCT CASE WHEN estado='Entregado' THEN FECHA_ESTADO ELSE NULL END),0) AS n_visitas_il,
          FROM tms
          GROUP BY 1
          UNION ALL
          SELECT
            guia_il AS GUIA,
            MAX(estado_il) AS ESTADO,
            MAX(fecha_solicitud_il) AS fecha_solicitud_il,
            MAX(fecha_recoleccion_il) AS fecha_recoleccion_il,
            MAX(fecha_entrega_il) AS fecha_entrega_il,
            MAX(n_visitas_il) AS n_visitas_il
          FROM urbano_il
          GROUP BY 1
        ) b ON a.guia_il=b.GUIA 
        -- 99 minutos
        UNION ALL
        SELECT * FROM (
          SELECT
            a.opl,
            a.rlo_id,
            'NA' AS Tienda,
            'NA' AS reason_code_category,
            'NA' AS reason_code_sub_category,
            a.deliveryOrderNumber,
            COALESCE(b.sku,a.sku) AS sku,
            a.items_il,
            a.n_guia,
            a.guia_il,
            a.estado_il,
            a.fecha_solicitud_il,
            a.fecha_recoleccion_il,
            a.fecha_entrega_il,
            a.visitas_il,
            a.tipo
          FROM (
            SELECT DISTINCT
              a.opl,
              a.rlo_id,
              COALESCE(b.deliveryOrderNumber,a.deliveryOrderNumber) AS deliveryOrderNumber,
              --a.deliveryOrderNumber AS prueba,
              a.sku,
              a.items_il,
              a.n_guia,
              a.guia_il,
              a.estado_il,
              a.fecha_solicitud_il,
              a.fecha_recoleccion_il,
              a.fecha_entrega_il,
              a.visitas_il,
              a.tipo
            FROM (
              SELECT 
                '99_MINUTOS' AS opl,
                '99_MINUTOS' AS rlo_id,
                REGEXP_EXTRACT(CODIGO_REFERENCIA, r'(\d+)') AS deliveryOrderNumber,
                SAFE_CAST(NULL AS STRING) AS sku,
                1 AS items_il,
                1 AS n_guia,
                REGEXP_EXTRACT(GUIA, r'(\d+)') AS guia_il,  
                CASE 
                  WHEN ESTADO_ACTUAL IN ('DEVOLUCIÓN TERMINADA','ENTREGADO','ENTREGADA SEGUNDO INTENTO','ENTREGADA TERCER INTENTO') THEN 'Entregado'
                  WHEN ESTADO_ACTUAL IN ('REQUIERE CORRECIONES','ENTREGA PARCIAL','NA') THEN 'Custodia'
                  WHEN ESTADO_ACTUAL IN ('ROBO O EXTRAVIO','DAÑADO') THEN 'Siniestro'
                  ELSE ESTADO_ACTUAL END AS estado_il,
                SAFE_CAST(FECHA_SS AS DATE) AS fecha_solicitud_il,
                SAFE_CAST(FECHA_AO AS DATE) AS fecha_recoleccion_il,
                SAFE_CAST((CASE WHEN ESTADO_ACTUAL IN ('DEVOLUCIÓN TERMINADA','ENTREGADO','ENTREGADA SEGUNDO INTENTO','ENTREGADA TERCER INTENTO') THEN FECHA_ESTADO ELSE NULL END) AS DATE) AS fecha_entrega_il,
                CASE
                  WHEN ESTADO_ACTUAL IN ('DEVOLUCIÓN TERMINADA','ENTREGADO') THEN 1
                  WHEN ESTADO_ACTUAL='ENTREGADA SEGUNDO INTENTO' THEN 2
                  WHEN ESTADO_ACTUAL='ENTREGADA TERCER INTENTO' THEN 3
                  ELSE 1 END AS visitas_il,
                '99_Minutos' AS tipo
              FROM `bi-fcom-drmb-local-pe-sbx.Vulpix_Devoluciones.IL_99minutos`
              WHERE GUIA IS NOT NULL
            ) a
            LEFT JOIN tms b ON a.deliveryOrderNumber=b.GUIA
          ) a
          LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` b ON a.deliveryOrderNumber=b.deliveryOrderNumber
        )
        WHERE sku IS NOT NULL
        -- RLOs
        UNION ALL
        SELECT * FROM (
          WITH
          rlos AS (
            SELECT 
              a.* EXCEPT(status),
              COALESCE(b.ESTADO,a.status) AS status
            FROM (
              SELECT DISTINCT * FROM (
                SELECT 
                  a.deliveryOrderNumber,
                  a.sku,
                  a.rlo_id,
                  a.Tienda,
                  a.reason_code_category,
                  a.reason_code_sub_category,
                  CASE 
                    WHEN b.status IN ('RETURN_ACCEPTED','RETURN_REJECTED','SELLER_ACCEPT','SELLER_REJECT','SELLER_REJECTION_REFUSED') THEN 'DELIVERED'
                    WHEN b.status IN ('EXCEPTION','INTERNAL_EXCEPTION') THEN 'EXCEPTION'
                    ELSE b.status END AS status,
                  a.guia_il AS tracking_code_il,
                  a.items_il,
                  b.fecha_registro AS fecha_solicitud_il,
                  b.fecha_ruta AS fecha_recoleccion_il,
                  b.fecha_devolucion AS fecha_entrega_il,
                  b.visitas_il
                FROM rlos a
                LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_vista_rlos` b ON a.rlo_id=b.rlo_id
              )
              WHERE status NOT IN ('EXPIRED')
              AND tracking_code_il IS NOT NULL
            ) a
            LEFT JOIN (
              SELECT 
                GUIA,
                MAX(CASE WHEN row=1 THEN ESTADO ELSE NULL END) AS ESTADO,
                SAFE_CAST(MAX(CASE WHEN estado='Registrado' THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_solicitud_il,
                SAFE_CAST(MAX(CASE WHEN estado IN ('En Reparto','En bodega Crossdock') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_recoleccion_il,
                SAFE_CAST(MAX(CASE WHEN estado IN ('Entregado','Retorno A Seller') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_entrega_il,
                IFNULL(COUNT(DISTINCT CASE WHEN estado='Excepcion de Entrega' THEN FECHA_ESTADO ELSE NULL END),0)+IFNULL(COUNT(DISTINCT CASE WHEN estado='Entregado' THEN FECHA_ESTADO ELSE NULL END),0) AS n_visitas_il,
              FROM tms
              GROUP BY 1
              UNION ALL
              SELECT
                guia_il AS GUIA,
                MAX(estado_il) AS ESTADO,
                MAX(fecha_solicitud_il) AS fecha_solicitud_il,
                MAX(fecha_recoleccion_il) AS fecha_recoleccion_il,
                MAX(fecha_entrega_il) AS fecha_entrega_il,
                MAX(n_visitas_il) AS n_visitas_il
              FROM urbano_il
              GROUP BY 1
            ) b ON a.tracking_code_il=b.GUIA 
          ),
          status_rlos AS (
            SELECT
              deliveryOrderNumber,
              sku,
              COUNT(DISTINCT status) AS n_status,
              COUNT(DISTINCT Tienda) AS n_tiendas,
              COUNT(DISTINCT reason_code_category) AS n_reason_cd,
              COUNT(DISTINCT reason_code_sub_category) AS n_reason_cds
            FROM rlos
            GROUP BY 1,2
          )

          SELECT
            CASE
              WHEN REGEXP_CONTAINS(guia_il,'^600') THEN 'Own Fleet'
              WHEN REGEXP_CONTAINS(guia_il,'WYB') THEN 'Urbano'
              ELSE 'Otro' END AS opl,
            rlo_id,
            Tienda,
            reason_code_category,
            reason_code_sub_category,
            deliveryOrderNumber,
            sku,
            items_il,
            ROW_NUMBER() OVER (PARTITION BY deliveryOrderNumber,sku ORDER BY fecha_solicitud_il ASC) AS n_guia,
            guia_il,
            estado_il,
            fecha_solicitud_il,
            fecha_recoleccion_il,
            fecha_entrega_il,
            n_visitas_il,
            'RLO' AS tipo,
          FROM (
            SELECT 
              deliveryOrderNumber,
              sku,
              STRING_AGG(rlo_id, '|') AS rlo_id,
              CASE WHEN MAX(n_tiendas)=1 THEN MAX(Tienda) ELSE STRING_AGG(Tienda,'|') END AS Tienda,	
              CASE WHEN MAX(n_reason_cd)=1 THEN MAX(reason_code_category) ELSE STRING_AGG(reason_code_category,'|') END AS reason_code_category,	
              CASE WHEN MAX(n_reason_cds)=1 THEN MAX(reason_code_sub_category) ELSE STRING_AGG(reason_code_sub_category,'|') END AS reason_code_sub_category,	
              CASE WHEN MAX(n_status)=1 THEN MAX(status) ELSE STRING_AGG(status,'|') END AS estado_il,	
              SUM(items_il) AS items_il,
              COUNT(DISTINCT tracking_code_il) AS n_guias,	
              STRING_AGG(tracking_code_il, '|') AS guia_il,
              MAX(n_status) AS n_status,
              SUM(visitas_il) AS n_visitas_il,
              MAX(fecha_solicitud_il) AS fecha_solicitud_il,
              MAX(fecha_recoleccion_il) AS fecha_recoleccion_il,
              MAX(fecha_entrega_il) AS fecha_entrega_il,
            FROM (
              SELECT 
                a.*,
                b.n_status,
                b.n_tiendas,
                b.n_reason_cd,
                b.n_reason_cds
              FROM rlos a
              LEFT JOIN status_rlos b ON a.deliveryOrderNumber=b.deliveryOrderNumber AND a.sku=b.sku
            )
            GROUP BY 1,2
          )
        )
      ) a
      LEFT JOIN (
        SELECT 
          GUIA,
          MAX(CASE WHEN row=1 THEN ESTADO ELSE NULL END) AS ESTADO,
          SAFE_CAST(MAX(CASE WHEN estado='Registrado' THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_solicitud_il,
          SAFE_CAST(MAX(CASE WHEN estado IN ('En Reparto','En bodega Crossdock') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_recoleccion_il,
          SAFE_CAST(MAX(CASE WHEN estado IN ('Entregado','Retorno A Seller') THEN FECHA_ESTADO ELSE NULL END) AS DATETIME) AS fecha_entrega_il,
          IFNULL(COUNT(DISTINCT CASE WHEN estado='Excepcion de Entrega' THEN FECHA_ESTADO ELSE NULL END),0)+IFNULL(COUNT(DISTINCT CASE WHEN estado='Entregado' THEN FECHA_ESTADO ELSE NULL END),0) AS n_visitas_il,
        FROM tms
        GROUP BY 1
        UNION ALL
        SELECT
          guia_il AS GUIA,
          MAX(estado_il) AS ESTADO,
          MAX(fecha_solicitud_il) AS fecha_solicitud_il,
          MAX(fecha_recoleccion_il) AS fecha_recoleccion_il,
          MAX(fecha_entrega_il) AS fecha_entrega_il,
          MAX(n_visitas_il) AS n_visitas_il
        FROM urbano_il
        GROUP BY 1
      ) b ON a.guia_il=b.GUIA 
      LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` c ON a.deliveryOrderNumber=c.deliveryOrderNumber AND a.sku=c.sku
    )    
  ) 

)



