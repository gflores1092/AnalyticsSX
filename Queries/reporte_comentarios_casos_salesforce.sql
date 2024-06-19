CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_comentarios_casos_salesforce` AS (
  
  SELECT * FROM (
    SELECT 
      b.CaseNumber,
      c.CommentOwner,
      REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(a.CommentBody)),r'\s{2,}',' ') AS CommentBody,
      DATETIME(a.CreatedDate,'America/Lima') AS CommentDate,
    FROM `bi-fcom-drmb-sell-in-sbx.svw_tc_sc_bi_bigdata_cust_dp_prod_acc_salesforce_v2_cust_dp_prod.svw_vw_salesforce_case_comment` a
    LEFT JOIN (
      SELECT 
        Id AS ParentId,
        CaseNumber 
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`
    ) b ON a.ParentId=b.ParentId
    LEFT JOIN (
      SELECT 
        Id AS CreatedById,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(CONCAT(FirstName,CONCAT(' ', LastName))) AS CommentOwner,
      FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_user`
    ) c ON a.CreatedById=c.CreatedById 
    WHERE a.IsDeleted=false
  )
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CaseNumber,CommentOwner ORDER BY CommentDate DESC)=1

)
