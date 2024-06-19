CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_macronodos` AS (

  --CONCAT("[",macronode,"]") AS macronode,
  --STRING_AGG(macronode,',') AS macronode,
  
  SELECT
    * EXCEPT(row)
  FROM (
    SELECT 
      country,
      sellerId,
      sellerName,
      node_id,
      macronode,
      --CONCAT('"',macronode,'"') AS macronode,
      deliveryType,
      deliveryMethod,
      n_deliveryMethod,
      dts,
      cot,
      frequency,
      region,
      province,
      district,
      node_lat,
      node_long,
      last_mod_dttm,
      ROW_NUMBER() OVER (PARTITION BY sellerId,deliveryType ORDER BY last_mod_dttm DESC) AS row,
    FROM (
      SELECT 
        a.*,
        b.* EXCEPT(sellerId)
      FROM (
        SELECT 
          a.*,
          b.* EXCEPT(node_id,last_mod_dttm),
          c.* EXCEPT(node_id),
          b.last_mod_dttm,
          CASE 
            WHEN deliveryMethod IN ('DR-IBIS','IBIS') THEN 'HOME_DELIVERY'
            WHEN deliveryMethod='C&C' THEN 'COLLECT'
            ELSE NULL END AS deliveryType
        FROM (
          SELECT
            cntry_cd AS country,
            REGEXP_EXTRACT(proc_name, r'-(SC\w{5})') AS sellerId,
            node_id,
          FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_acc_scha_dntm_process`
        ) a
        LEFT JOIN (
          SELECT DISTINCT
            lst_node_id AS node_id,
            node_name AS macronode,
            REGEXP_EXTRACT(node_name, r'^([^ ]+)') AS deliveryMethod,
            REGEXP_EXTRACT(node_name, r'^[^ ]+ (\d+)') AS n_deliveryMethod,
            REGEXP_EXTRACT(node_name, r'DTS (\d+)') AS dts,
            REGEXP_EXTRACT(node_name, r'\b(\d{2}:\d{2})\b') AS cot,
            TRIM(REPLACE(REGEXP_EXTRACT(node_name, r'\d{2}:\d{2}\s+(.*)'),")","")) AS frequency,
            last_mod_dttm
          FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_acc_scha_dntm_nodes` 
        ) b ON a.node_id=b.node_id
        LEFT JOIN (
          SELECT DISTINCT
            node_id,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(node_state_name) AS region,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(node_county_name) AS province,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(node_district_name) AS district,
            node_lat,
            node_long,
          FROM `tc-sc-bi-bigdata-hdpe-pjx-dev.cons_vl.vw_acc_scha_dntm_nodes` 
        ) c ON a.node_id=c.node_id
      ) a
      LEFT JOIN (
        SELECT DISTINCT
          seller_id AS sellerId,
          seller_name AS sellerName
        FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.analisis_espacial_sellers`
      ) b ON a.sellerId=b.sellerId
    )
    ORDER BY sellerId,macronode DESC
  )
  WHERE row=1
  AND sellerId IS NOT NULL
  AND deliveryType IS NOT NULL
  ORDER BY sellerId DESC
  
)
