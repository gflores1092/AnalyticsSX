CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_tareas_salesforce` AS (

  SELECT
  *,
  SUM(task_completion_hours) OVER (PARTITION BY caseNumber ORDER BY taskCreatedDate ASC) AS cum_task_completion_hours,
  --diff_task_created_lag
  FROM (
    SELECT 
      *
    FROM (
      SELECT
        b.country,
        b.commerce,
        EXTRACT(YEAR FROM b.date) AS year,
        EXTRACT(MONTH FROM b.date) AS month,
        EXTRACT(WEEK FROM b.date)+1 AS week,
        b.date,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.customerDocument) AS customerDocument,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(b.customerName) AS customerName,
        UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(sellerId)) AS sellerId,
        UPPER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(sellerName)) AS sellerName,
        b.orderNumber,
        b.deliveryOrderNumber,
        b.sku,
        CASE WHEN customerDocument IS NULL AND sellerId IS NOT NULL THEN 'Seller'
            WHEN customerDocument IS NOT NULL THEN 'Customer'
            ELSE 'Other' END AS caseStakeholder,
        b.caseNumber,
        b.caseOwner,
        b.caseBU,
        b.caseLevel1,
        b.caseLevel2,
        b.caseTipification,
        b.caseStatus,
        --b.caseDescription,
        b.casePriority,
        a.taskNumber,
        COALESCE(a.taskBU,e.taskBU) AS taskBU,
        a.taskSubject,
        a.taskDescription,
        a.taskStatus,
        a.taskType,
        ROW_NUMBER() OVER (PARTITION BY b.caseNumber,a.taskType ORDER BY a.taskCreatedDate ASC) AS taskRow,
        a.taskCreatedDate,
        a.taskCompletedDate,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(c.taskCreatedBy) AS taskCreatedBy,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(d.taskLastModifiedBy) AS taskLastModifiedBy,
        LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(taskResolutionType)) AS taskResolutionType,
        LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(taskResolutionReason)) AS taskResolutionReason,
        REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(taskRequiredSolution)),r'\s{2,}',' ') AS taskRequiredSolution,
        b.caseCreatedDate,
        b.caseDueDate,
        b.caseClosedDate,
        b.caseReopenings,
        b.caseRecontacts,
        b.caseClosureType,
        b.caseClosureComment,
        b.regulatoryId,
        b.refund_id,
        b.refund_amount,
        b.refund_status,
        b.refund_date,
        b.value,
        b.orders,
        b.skus,
        b.segmento,
        CASE WHEN caseTipification IN ('Entrega incompleta','Entrega incompleta de promocion','Informacion entrega incompleta','Producto incompleto') THEN 'Entrega incompleta'
            WHEN caseTipification IN ('Informacion entrega incorrecta','Entrega parcial','Entrega incorrecta','Producto no corresponde','Producto no le queda') THEN 'Entrega incorrecta'
            WHEN caseTipification IN ('Cupon promocional','Error en publicacion - precio','Error en publicacion','Error en publicacion caracteristicas producto') THEN 'Error en publicacion'
            WHEN caseTipification IN ('Devolucion producto no original','Producto no original') THEN 'Producto no original'
            WHEN caseTipification IN ('Garantia rechazada','Informacion de garantia','Reclamo Garantia','Solicitud de garantia','Solicitud de uso de Garantia','Autogestion de garantia','Incumplimiento en tiempo de garantia') THEN 'Garantia'
            WHEN caseTipification IN ('Entrega parcial','Informacion pieza faltante','Pieza faltante','Producto faltante') THEN 'Pieza faltante'
            WHEN caseTipification IN ('Producto con empaque deteriorado','Producto con falla de funcionamiento/tecnica','Producto malogrado') THEN 'Producto con falla'
            WHEN caseTipification IN ('Producto en mal estado','Producto mal rotulado','Producto vencido') THEN 'Producto en mal estado'
            WHEN caseTipification IN ('Falta de surtido/ disponibilidad','Producto sin disponibilidad') THEN 'Producto sin disponibilidad'
            WHEN caseTipification IN ('Cancelacion por retraso','Estado de la entrega','Retraso en la entrega','Retraso en la entrega paqueteria','Retraso en la entrega seller') THEN 'Retraso en la entrega'
            WHEN caseTipification IN ('Reenvio intento de entrega','Reprogramacion de entrega','Reprogramacion direccion incorrecta','Reprogramacion nadie en casa') THEN 'Reprogramación'
            WHEN caseTipification IN ('Incumplimiento en prestacion del servicio','Problemas con Personal Transporte','Reclamo de Servicios','Reembolso por incumplimiento de envio express') THEN 'Términos y Condiciones'
            WHEN caseTipification IN ('Cancelar una orden','Cliente Ausente','Desiste de la compra sin motivo') THEN 'Cancelación'
            WHEN caseTipification IN ('Cambio de fecha de entrega','Cambio de quien retira') THEN 'Cambio de datos'
            WHEN caseTipification IN ('Arrepentimiento en la compra','Devolucion big ticket 3P','Devolucion rechazada','Devolver una orden','Error devolucion big ticket 3P','Producto cancelado y entregado') THEN 'Devoluciones'
            WHEN caseTipification IN ('Error sistemico interno','Problemas para cancelar la orden de compra','Problemas para devolver la orden de compra') THEN 'Plataforma'
            WHEN caseTipification IN ('Cambio de boleta a factura','Cambio/correccion de datos de boleta/factura','Copia/solicitud de boleta/factura','Error envio de boleta/factura') THEN 'Comprobante'
            WHEN caseTipification IN ('Cargo no reconocido','Cobro duplicado','Cobro sin orden de compra','Descuento no aplicado','Error en cobro','Impuestos','Liberacion de pedido') THEN 'Cobros'
            ELSE caseTipification END AS groupcaseTipification,
        CASE
          WHEN caseTipification IN ('Producto no le queda') THEN 'Cliente'
          WHEN caseTipification IN ('Cargo no reconocido','Cobro duplicado','Cobro sin orden de compra','Descuento no aplicado','Error en cobro','Error en cobro de intereses','Experiencia/Intangible','Falta de surtido/ disponibilidad','No se refleja linio cash','Problemas con documento bancario','Problemas con Personal Transporte','Producto cancelado y entregado','Reclamo de Servicios','Retiro en courrier/paquetera','Servicios de instalaciones','Tiempo para ser atendido') THEN 'Otros'
          WHEN caseTipification IN ('Devolver una orden','Error en reembolso','Incumplimiento en tiempo de reembolso o reversa','Problemas para cancelar la orden de compra','Problemas para devolver la orden de compra') THEN 'Prob. cambio y cancelacion del cliente'
          WHEN caseTipification IN ('Falsa entrega','Retraso en la entrega','Retraso en la entrega seller') THEN 'Problemas Entrega'
          WHEN caseTipification IN ('Cliente sospecha producto falso','Entrega incompleta','Entrega incorrecta','Pieza faltante','Producto en mal estado','Producto faltante','Producto incompleto','Producto mal rotulado','Producto no corresponde','Producto vencido') THEN 'Seller'
          ELSE 'Otro' END AS tipoReclamo,
        SAFE_CAST(b.caseSLA AS INT64) AS caseSLA,
        SAFE_CAST(b.caseSLA AS INT64)*24 AS caseSLAhours,
        DATETIME_DIFF(caseDueDate,caseCreatedDate, HOUR) AS case_due_created_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN DATETIME_DIFF(caseClosedDate,caseCreatedDate, HOUR) 
            ELSE DATETIME_DIFF(CURRENT_DATE(),caseCreatedDate, HOUR) END AS case_completion_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN DATETIME_DIFF(caseClosedDate,caseDueDate, HOUR) 
            ELSE DATETIME_DIFF(CURRENT_DATE(),caseDueDate, HOUR) END AS case_diff_completion_due_hours,
        CASE WHEN taskCompletedDate IS NOT NULL THEN DATETIME_DIFF(taskCompletedDate,taskCreatedDate, HOUR) 
            ELSE DATETIME_DIFF(CURRENT_DATE(),taskCreatedDate, HOUR) END AS task_completion_hours,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseDueDate),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours AS case_due_created_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseClosedDate),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours 
            ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours END AS case_completion_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseClosedDate),TIMESTAMP(caseDueDate), 9, 18, [1,2,3,4,5]).hours 
            ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(caseDueDate), 9, 18, [1,2,3,4,5]).hours END AS case_diff_completion_work_hours,
        CASE WHEN taskCompletedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(taskCompletedDate),TIMESTAMP(taskCreatedDate), 9, 18, [1,2,3,4,5]).hours 
            ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(taskCreatedDate), 9, 18, [1,2,3,4,5]).hours END AS task_completion_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL AND caseClosedDate>caseDueDate THEN 1 
            WHEN caseClosedDate IS NULL AND CURRENT_DATE()>caseDueDate THEN 1
            ELSE 0 END AS late,
        ROW_NUMBER() OVER (PARTITION BY b.caseNumber,a.taskNumber,b.orderNumber ORDER BY b.date DESC) AS row,
      FROM (
        SELECT
          country,
          commerce,
          a.caseNumber,   
          d.caseOwner,
          caseBU,
          caseLevel1,
          caseLevel2,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(caseTipification) AS caseTipification,
          casePriority,
          caseOrigin,
          caseMilestoneStatus,
          caseStatus,
          caseClosureType,
          caseClosureComment,
          CASE  WHEN caseTipification = '¿Por qué mis productos no sincronizan en Falabella Seller Center?' THEN 76
                WHEN caseTipification = 'Algunos de mis productos presentan error 404' THEN 64
                WHEN caseTipification = 'Las modificaciones de mis productos no se ven reflejados en f.com' THEN 40
                WHEN caseTipification = 'Necesito saber a que hacen referencia los conceptos cobrados' THEN 16
                WHEN caseTipification = 'Detalle de pagos MarketPlace en mi corte' THEN 28
                WHEN caseTipification = 'Tengo dudas sobre pagos recibidos' THEN 40
                WHEN caseTipification = 'Programar pagos que no han tenido cambio de estado en Falabella Seller Center' THEN 40
                WHEN caseTipification = 'Problema con la contraseña de Portal Fpay - Pagos' THEN 40
                WHEN caseTipification = 'Error en saldo funds out Fpay' THEN 40
                WHEN caseTipification = 'No me aparece el monto disponible en FPAY' THEN 40
                WHEN caseTipification = 'No puedo editar mis datos bancarios en FPAY' THEN 40
                WHEN caseTipification = 'Consultas y reclamos recaudación Fpay' THEN 72
                WHEN caseTipification = 'Solicitud de documentos, modificaciones, prepago, cierre productos' THEN 72
                WHEN caseTipification = 'Reclamos, desconocimiento transacciones' THEN 72
                WHEN caseTipification = 'Contacto Compliance' THEN 40
                WHEN caseTipification = 'Validación de documentos por parte de Legal' THEN 40
                WHEN caseTipification = 'Quiero reactivar mi cuenta de F.com' THEN 52
                WHEN caseTipification = 'Inactivación de cuenta solicitada por F.com Legal' THEN 40
                WHEN caseTipification = 'Requiero de su apoyo para gestionar solicitudes de los clientes' THEN 28
                WHEN caseTipification = 'Recibi un producto por garantía dañado' THEN 40
                WHEN caseTipification = '¿Cómo proceso una garantía?' THEN 40
                WHEN caseTipification = '¿Cómo aumento mis ordenes de venta diaria?' THEN 4
                WHEN caseTipification = 'No puedo generar la guía de mi orden para despacho' THEN 40
                WHEN caseTipification = 'Deseo cambiar el estatus de una orden enviada' THEN 64
                WHEN caseTipification = '¿Qué hago si mi producto es  extraviado por paqueterÍa?' THEN 40
                WHEN caseTipification = 'Activar cuenta en f.com' THEN 24
                WHEN caseTipification = '¿Cómo crear cuenta en Portal Fpay?' THEN 16
                WHEN caseTipification = '¿Cómo me registro a las capacitaciones?' THEN 16
                WHEN caseTipification = '¿Dónde consigo el material de capacitaciones?' THEN 16
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'En el indicador de envíos a tiempo hay órdenes que no corresponden' THEN 28
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'En el indicador de envíos a tiempo hay órdenes que no corresponden' THEN 16
                WHEN caseTipification = 'No se han pagado unidades en la Liquidación mensual FBy' THEN 4
                WHEN caseTipification = 'Necesito apoyo en el proceso de ingreso de unidades' THEN 28
                WHEN caseTipification = 'No recolectaron mi paquete. Necesito una nueva recolección' THEN 4
                WHEN caseTipification = '¿Cómo solicito un retiro de unidades desde la bodega FBy?' THEN 4
                WHEN caseTipification = '¿Qué documentación debo enviar para acreditar la originalidad de productos?' THEN 4
                WHEN caseTipification = 'Tengo un error en la plataforma de productos promocionados' THEN 4
                WHEN caseTipification = 'Mensajería - Servicio de publicidad' THEN 36
                WHEN caseTipification = 'Productos promocionados - Servicio de publicidad' THEN 40
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'No recolectaron mis envíos pendientes' THEN 40
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'No recolectaron mis envíos pendientes' THEN 28
                WHEN caseTipification = 'Requiero cambio de estatus ya que mi guía no posee movimiento' THEN 40
                WHEN caseTipification = 'La guia de mi envío no tiene movimiento' THEN 40
                WHEN caseTipification = 'Tengo diferencias de stock en F.com' THEN 100
                WHEN caseTipification = 'Necesito el IMEI de una orden FBY para facturar al cliente' THEN 64
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'Quiero cancelar una orden debido a que no actualice mi stock en el sistema' THEN 28
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'Quiero cancelar una orden debido a que no actualice mi stock en el sistema' THEN 16
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = '¿Cómo rastreo una devolución?' THEN 40
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = '¿Cómo rastreo una devolución?' THEN 24
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'Deseo cancelar una orden generada por error de Falabella Seller Center' THEN 28
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'Deseo cancelar una orden generada por error de Falabella Seller Center' THEN 16
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'Deseo cancelar una orden ya que los datos del cliente son errados' THEN 28
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'Deseo cancelar una orden ya que los datos del cliente son errados' THEN 16
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = 'Deseo cancelar una orden que se generó sin el precio actualizado' THEN 28
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = 'Deseo cancelar una orden que se generó sin el precio actualizado' THEN 16
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') < '2023-08-25'
                     AND caseTipification = '¿Cómo puedo saber el motivo de una devolución?' THEN 40
                WHEN DATETIME(TIMESTAMP(caseCreatedDate), 'America/Lima') >= '2023-08-25' 
                     AND caseTipification = '¿Cómo puedo saber el motivo de una devolución?' THEN 30
                ELSE SAFE_CAST(caseSLA AS INT64) END AS caseSLA,
          refund_id,
          refund_amount,
          refund_status,
          refund_date,
          Id,
          date,
          --Subject,
          caseDescription,
          caseCreatedDate,
          caseDueDate,
          caseClosedDate,
          b.orderNumber,
          b.deliveryOrderNumber,
          b.sku,
          b.value,
          b.orders,
          b.skus,
          b.customerDocument,
          b.customerName,
          COALESCE(b.sellerId, a.FC_SellerId__c, c.sellerId) AS sellerId,
          COALESCE(b.sellerName, a.FC_SellerName__c, c.sellerName) AS sellerName,
          segmento,
          regulatoryId,          
          caseReopenings,
          caseRecontacts,
          caseManager,
          caseResponsibilitySeller,
        FROM (
          SELECT
            FC_Country__c AS country,
            CASE
              WHEN FC_Commerce_Name__c IN ('F.com','GSC') THEN 'F.com' 
              WHEN FC_Commerce_Name__c NOT IN ('F.com','GSC') AND REGEXP_CONTAINS(FC_SellerId__c,'^SC') THEN 'F.com'
              WHEN FC_Commerce_Name__c NOT IN ('F.com','GSC') AND REGEXP_CONTAINS(FC_ExternalIDOrderSeller__c,'^2') THEN 'F.com'
              ELSE FC_Commerce_Name__c END AS commerce,
            CaseNumber AS caseNumber,   
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_BusinessUnit__c) AS caseBU,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Level_1__c) AS caseLevel1,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Level_2__c) AS caseLevel2,
            FC_TipificationName__c AS caseTipification,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Priority) AS casePriority,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Origin) AS caseOrigin,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(MilestoneStatus) AS caseMilestoneStatus,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Status) AS caseStatus,
            FC_SLA__c AS caseSLA,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_ClosureType__c) AS caseClosureType,
            REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_ClosureComment__c)),r'\s{2,}',' ') AS caseClosureComment,
            FC_Id_Refund__c AS refund_id,
            FC_GrandTotal__c AS refund_amount,
            FC_RefundStatus__c AS refund_status,
            DATE(FC_RefundEventDate__c) AS refund_date,
            Id,
            DATE(CAST(CreatedDate AS TIMESTAMP), 'America/Lima') AS date,
            --Subject,
            REGEXP_REPLACE(LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Description)),r'\s{2,}',' ') AS caseDescription,
            DATETIME(CAST(CreatedDate AS TIMESTAMP), 'America/Lima') AS caseCreatedDate,
            DATETIME(CAST(FC_Due_date__c AS TIMESTAMP), 'America/Lima') AS caseDueDate,
            DATETIME(CAST(ClosedDate AS TIMESTAMP), 'America/Lima') AS caseClosedDate,
            FC_ExternalIDOrderSeller__c,
            FC_ListOfSkuMulti__c,
            FC_SellerId__c,
            FC_SellerName__c,
            FC_Id_Regulatory__c AS regulatoryId,          
            FC_ReOpenings__c AS caseReopenings,
            FC_Recontacts__c AS caseRecontacts,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(FC_Manager__c) AS caseManager,
            FC_ResponsibilitySeller__c AS caseResponsibilitySeller,
            CreatedById,
            OwnerId,       
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case` 
        ) a
        LEFT JOIN (

            WITH 
            detalles AS (
              SELECT DISTINCT
                a.CaseNumber,
                d.customerDocument,
                d.customerName,
                b.sellerId,
                b.sellerName,
                c.valor AS segmento,
                e.orderNumber,
                b.deliveryOrderNumber,
                b.sku,
                b.value
              FROM (
                SELECT DISTINCT * FROM (
                  SELECT
                    CaseNumber,
                    uniqueorderNumber
                  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`,
                  UNNEST(REGEXP_EXTRACT_ALL(FC_ExternalIDOrderSeller__c, r'2[0-9]{8,9}$')) AS uniqueorderNumber
                  UNION ALL
                  SELECT
                    CaseNumber,
                    uniqueorderNumber
                  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`,
                  UNNEST(REGEXP_EXTRACT_ALL(Description, r'2[0-9]{8,9}$')) AS uniqueorderNumber
                )
              ) a
              LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_ordenes_fsc` b ON a.uniqueorderNumber=b.deliveryOrderNumber
              LEFT JOIN `bi-fcom-drmb-sell-in-sbx.segmentacion_valor_sellers.peru` c ON b.sellerId=c.id_seller
              LEFT JOIN (
                SELECT DISTINCT
                  CAST(order_nr AS STRING) AS deliveryOrderNumber,
                  national_registration_number AS customerDocument,
                  INITCAP(CONCAT((customer_first_name),' ',(customer_last_name))) AS customerName,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_trf_corp_drmb_sllm.svw_gsc_production_sales_order`
                WHERE fk_operator=2
              ) d ON b.deliveryOrderNumber=d.deliveryOrderNumber
              LEFT JOIN (
                SELECT DISTINCT
                  deliveryOrderNumber,
                  orderNumber,
                FROM `tc-sc-bi-bigdata-cons-fcom-prd.catalyst_prd_pe.svw_delivery_orders`
                WHERE PARTITION_DATE>="2022-08-01"
              ) e ON b.deliveryOrderNumber=e.deliveryOrderNumber
            ),
            n_detalles AS (
              SELECT
                CaseNumber,
                COUNT(DISTINCT customerDocument) AS customer_documents,
                COUNT(DISTINCT customerName) AS customer_names,
                COUNT(DISTINCT orderNumber) AS orders,
                COUNT(DISTINCT deliveryOrderNumber) AS deliveryOrders,
                COUNT(DISTINCT sku) AS skus,
              FROM detalles
              GROUP BY 1
            )

            SELECT 
              a.CaseNumber,
              CASE WHEN MAX(customer_documents)=1 THEN MAX(customerDocument) ELSE STRING_AGG(customerDocument, '|') END AS customerDocument,
              CASE WHEN MAX(customer_names)=1 THEN MAX(customerName) ELSE STRING_AGG(customerName, '|') END AS customerName,
              MAX(sellerId) AS sellerId,
              MAX(sellerName) AS sellerName,
              MAX(segmento) AS segmento,
              CASE WHEN MAX(orders)=1 THEN MAX(orderNumber) ELSE STRING_AGG(orderNumber, '|') END AS orderNumber,
              CASE WHEN MAX(deliveryOrders)=1 THEN MAX(deliveryOrderNumber) ELSE STRING_AGG(deliveryOrderNumber, '|') END AS deliveryOrderNumber,
              CASE WHEN MAX(skus)=1 THEN MAX(sku) ELSE STRING_AGG(sku, '|') END AS sku,
              SUM(value) AS value,
              COUNT(DISTINCT deliveryOrderNumber) AS orders,
              COUNT(DISTINCT sku) AS skus,
            FROM detalles a
            LEFT JOIN n_detalles b ON a.caseNumber=b.caseNumber
            GROUP BY 1

        ) b ON b.CaseNumber=a.CaseNumber 
        LEFT JOIN (
          SELECT
            Id AS CreatedById,
            Alias AS sellerId,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(BadgeText) AS sellerName,
          FROM`tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_user` 
        ) c ON c.CreatedById=a.CreatedById
        LEFT JOIN (
          SELECT 
            Id AS OwnerId,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(CONCAT(FirstName,CONCAT(' ', LastName))) AS caseOwner,
          FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_user`
        ) d ON d.OwnerId=a.OwnerId
      ) b 
      LEFT JOIN (
        SELECT DISTINCT
          FC_Case__c AS caseNumber,
          FC_Number__c AS taskNumber,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_BusinessUnitTask__c) AS taskBU,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Subject) AS taskSubject,
          CASE WHEN REGEXP_CONTAINS(Subject,'^Correo electr') 
              THEN 
              IFNULL(
                REGEXP_REPLACE(
                  REGEXP_EXTRACT(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_EXTRACT(
                          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Description))
                        ,r'cuerpo\s*([\s\S]*)agradecemos') -- extracion desde cuerpo hasta agradecemos
                      ,r'\n',' ') -- eliminar espacios (newlines -> enter) 
                    ,r'\s{2,}',' ') -- eliminar espacios dobles+ --AQUI
                    , r'^(.*con esta informacion)')
                  ,' con esta informacion','')
                ,
                REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REGEXP_EXTRACT(
                          LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Description))
                        ,r'cuerpo\s*([\s\S]*)agradecemos') -- extracion desde cuerpo hasta agradecemos
                      ,r'\n',' ') -- eliminar espacios (newlines -> enter) 
                    ,r'\s{2,}',' ') 
                )
              ELSE REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        LOWER(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(Description))
                      ,r'\n',' ')
                    ,r'\s{2,}',' ') 
              END AS taskDescription,
          --Description,
          DATETIME(CAST(CreatedDate AS TIMESTAMP), 'America/Lima') AS taskCreatedDate,
          DATETIME(CAST(CompletedDateTime AS TIMESTAMP), 'America/Lima') AS taskCompletedDate,
          `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(Status) AS taskStatus,
          CreatedById AS taskCreatedBy,
          LastModifiedById AS taskLastModifiedBy,
          FC_Resolution__c AS taskResolutionType,
          FC_ResolutionReason__c AS taskResolutionReason,
          FC_RequiredSolution__c AS taskRequiredSolution,
          CASE WHEN REGEXP_CONTAINS(Subject,'^Correo electr') THEN 'Correo' 
              WHEN REGEXP_CONTAINS(Subject,'^Recontacto') THEN 'Recontacto'
              ELSE 'Task' END AS taskType,
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_task`
      ) a ON b.Id=a.caseNumber
      LEFT JOIN (
        SELECT DISTINCT
          Id AS CreatedById,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(CONCAT(FirstName,' ',LastName))) AS taskCreatedBy
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_user`
      ) c ON c.CreatedById=a.taskCreatedBy
      LEFT JOIN (
        SELECT DISTINCT
          Id AS LastModifiedById,
          INITCAP(`bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(CONCAT(FirstName,' ',LastName))) AS taskLastModifiedBy
        FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_user`
      ) d ON d.LastModifiedById=a.taskLastModifiedBy
      LEFT JOIN (
        SELECT * FROM (
          SELECT 
            LastModifiedById,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Management_Group__c) AS taskBU,
            COUNT(DISTINCT FC_Number__c) AS tasks,
            FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_task`
            --WHERE LastModifiedById='0058a00000KvJMeAAN'
            GROUP BY 1,2
            ORDER BY 3 DESC
          )
        QUALIFY ROW_NUMBER() OVER (PARTITION BY LastModifiedById ORDER BY tasks DESC)=1
      ) e ON e.LastModifiedById=a.taskLastModifiedBy
    )
    WHERE country='PE'
    AND commerce IN ('F.com','GSC')
    AND sellerId NOT IN ('FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU','FALABELLA PERU','SODIMAC PERU', 'TOTTUS PERU')
    AND sellerName NOT IN ('FALABELLA','SODIMAC','TOTTUS')
  )
  WHERE row=1
  --AND caseTipification IN ('Falsa entrega','Entrega incompleta','Producto en mal estado','Pieza faltante','Informacion entrega incompleta','Informacion pieza faltante','Autogestion de falsa entrega')
  --AND caseClosedDate IS NULL
  --AND taskBU='AC_HD_PE_PostVenta3P'
  --WHERE CaseNumber='14452751'
  --WHERE caseLevel2='Reembolso'
  --WHERE CaseNumber='18222942'
  --WHERE REGEXP_CONTAINS(orderNumber,'2054371883')
  --WHERE orderNumber='6120658855'
)
