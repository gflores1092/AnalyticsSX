CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_customer_reviews` AS (

  SELECT DISTINCT
    c.sellerId,
    c.sellerName,
    c.main_sku AS sku,
    --c.product_description,
    c.description,
    c.brand_name,
    c.primary_category,
    c.global_identifier,
    a.date,
    a.review_id,
    a.rating,
    a.customer,
    a.review_title,
    a.review_text,
    --a.moderation_status,
  FROM (
    SELECT * FROM (
      SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY process_date DESC) AS row, 
      FROM (
        SELECT DISTINCT
          DATE(SAFE_CAST(PROCESS_DATE AS TIMESTAMP)) AS process_date,
          DATE(SAFE_CAST(SUBMISSION_DATE AS TIMESTAMP)) AS date,
          PRODUCT_ID AS src_id,
          PRODUCT_BRAND AS brand,
          REVIEW_ID AS review_id,
          OVERALL_RATING AS rating,
          USER_NICKNAME AS customer,
          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(TITLE)) AS review_title,
          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(REGEXP_REPLACE(REVIEW_TEXT,',',''))) AS review_text,
          MODERATION_STATUS AS moderation_status,
          --PHOTOS.Sizes.normal.Url AS review_photo,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_pe_bazaar_voice.svw_ratings_and_reviews`-- a,
        --UNNEST(PHOTOS) AS PHOTOS
      )
    )
    WHERE row=1
  ) a
  LEFT JOIN (
    SELECT DISTINCT
        a.fk_catalog_product_set,
        sku AS shop_sku,
        b.src_id
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product` a
      LEFT JOIN (
        SELECT DISTINCT
          id_catalog_product_set AS fk_catalog_product_set,
          src_id,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_catalog_product_set` 
      ) b ON a.fk_catalog_product_set=b.fk_catalog_product_set
  ) b ON a.src_id=b.src_id
  LEFT JOIN (
    SELECT DISTINCT
      sellerId,
      sellerName,
      main_sku,
      --shop_sku AS sku,
      INITCAP(product_name) AS description,
      INITCAP(brand_name) AS brand_name,
      INITCAP(primary_category) AS primary_category,
      global_identifier
    FROM `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.modelo_tallas_skus`
  ) c ON b.src_id=c.main_sku
  WHERE sellerId IS NOT NULL
  
)
