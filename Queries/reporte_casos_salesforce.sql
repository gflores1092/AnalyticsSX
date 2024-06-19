CREATE OR REPLACE TABLE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_casos_salesforce` AS (

  SELECT
    * EXCEPT(row),
    --SUM(task_completion_hours) OVER (PARTITION BY caseNumber ORDER BY taskCreatedDate ASC) AS cum_task_completion_hours,
    --diff_task_created_lag
  FROM (
    SELECT
      *,
      CASE WHEN Departamento!='Other' THEN 'Local' ELSE 'Regional' END AS Sector,
      CASE WHEN caseSLAhours >= case_completion_hours THEN '1' ELSE '0' END AS cumplimientoSLA
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
        b.caseDescription,
        b.Origin,
        b.casePriority,
        /**
        a.taskNumber,
        a.taskBU,
        a.taskSubject,
        a.taskDescription,
        a.taskStatus,
        a.taskType,
        ROW_NUMBER() OVER (PARTITION BY b.caseNumber,a.taskType ORDER BY a.taskCreatedDate ASC) AS taskRow,
        a.taskCreatedDate,
        a.taskCompletedDate,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(c.taskCreatedBy) AS taskCreatedBy,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING_NEW(d.taskLastModifiedBy) AS taskLastModifiedBy,
        **/
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
        CASE 
            WHEN caseOwner='Alejandra Bendrell' THEN 'Support SX'
            WHEN caseOwner='Andrea Cribillero Torres' THEN 'Logística SX'
            WHEN caseOwner='Andrea Nino' THEN 'Support SX'
            WHEN caseOwner='Angel Chavez' THEN 'Logística SX'
            WHEN caseOwner='Angel Rivera Moreno' THEN 'Logística SX'
            WHEN caseOwner='Antony Tello' THEN 'Devoluciones SX'
            WHEN caseOwner='Aranzazu Caballero' THEN 'Finanzas SX'
            WHEN caseOwner='Betty Aquino' AND caseCreatedDate < '2023-04-01' THEN 'Logística SX'
            WHEN caseOwner='Betty Aquino' AND caseCreatedDate >= '2023-04-01' THEN 'Devoluciones SX'
            WHEN caseOwner='Claudia Cavero' THEN 'Finanzas SX'
            WHEN caseOwner='Cristhian Alvarez' THEN 'Logística SX'
            WHEN caseOwner='Dalia Chalco Rojas' THEN 'Finanzas SX'
            WHEN caseOwner='Darwing Arroyo' THEN 'Devoluciones SX'
            WHEN caseOwner='Eduardo Sedano' THEN 'Finanzas SX'
            WHEN caseOwner='Elida Castro' THEN 'Devoluciones SX'
            WHEN caseOwner='Fernando Changanaqui' THEN 'Devoluciones SX'
            --WHEN caseOwner='Hans Romero' THEN 'Devoluciones SX'
            --WHEN caseOwner='Harumi Casas' THEN 'Devoluciones SX'
            WHEN caseOwner='Helber Huaman' THEN 'Logística SX'
            --WHEN caseOwner='Javier La Torre' THEN 'Devoluciones SX'
            WHEN caseOwner='Jeremy Caceres' THEN 'Finanzas SX'
            WHEN caseOwner='Johans Lopez' THEN 'Logística SX'
            WHEN caseOwner='Jose Diego Rondon' THEN 'Logística SX'
            WHEN caseOwner='Julissa Montalvo' THEN 'Support SX'
            WHEN caseOwner='Karol Fong Kuan' THEN 'Devoluciones SX'
            WHEN caseOwner='Melissa Huerta' THEN 'Logística SX'
            WHEN caseOwner='Michelle Bañon' THEN 'Devoluciones SX'
            WHEN caseOwner='Miguel Pazos' THEN 'Logística SX'
            WHEN caseOwner='Patricia Salinas' THEN 'Support SX'
            WHEN caseOwner='Rocio Serna' THEN 'Logística SX'
            WHEN caseOwner='Rodrigo Teran' THEN 'Devoluciones SX'
            --WHEN caseOwner='Royer Tacuri' THEN 'Devoluciones SX'
            WHEN caseOwner='Sheyla Leon' THEN 'Logística SX'
            WHEN caseOwner='Yeniffer Villanueva' THEN 'Finanzas SX'
            --WHEN 'Zeus Chavez' THEN 'Finanzas SX'
            ELSE 'Other' END AS Departamento,
        CASE WHEN caseTipification = 'No recolectaron mis envíos pendientes' THEN 'Logistica SX' 
            WHEN caseTipification = 'Deseo cancelar una orden' THEN 'Logistica SX'
            WHEN caseTipification = 'Dudas sobre una cancelación' THEN 'Logistica SX'
            WHEN caseTipification = 'Solicitudes adicionales de despacho' THEN 'Logistica SX'
            WHEN caseTipification = 'Errores en indicadores operativos' THEN 'Logistica SX'
            WHEN caseTipification = 'Cambio de Punto drop off' THEN 'Logistica SX'
            WHEN caseTipification = 'No recolectaron mis envíos pendientes' THEN 'Logistica SX'
            WHEN caseTipification = 'Deseo cambiar el estatus de una orden enviada' THEN 'Logistica SX'
            WHEN caseTipification = 'Requiero apoyo ya que despaché una orden cancelada' THEN 'Logistica SX'
            WHEN caseTipification = 'Deseo cambiar el estatus de una orden enviada' THEN 'Logistica SX'
            WHEN caseTipification = 'Despaché erradamente las órdenes al cliente final' THEN 'Logistica SX'
            WHEN caseTipification = 'Quiero modificar los datos de mi cuenta en Falabella Seller Center' THEN 'Logistica SX'
            WHEN caseTipification = 'Quiero formalizar reclamo por un producto dañado' THEN 'Post Venta SX'
            WHEN caseTipification = 'Quiero formalizar reclamo por una entrega que no reconozco' THEN 'Post Venta SX'
            WHEN caseTipification = 'No he recibido mi producto por fallo de entrega' THEN 'Post Venta SX'
            WHEN caseTipification = 'Dudas con un producto en devolución' THEN 'Post Venta SX'
            WHEN caseTipification = 'Quiero rechazar una devolución' THEN 'Post Venta SX'
            WHEN caseTipification = 'Información sobre una orden en devolución' THEN 'Post Venta SX'
            WHEN caseTipification = 'Tengo un cobro errado de guías por devolución' THEN 'Post Venta SX'
            WHEN caseTipification = 'Recibí una caja vacía o mi producto fue cambiado' THEN 'Post Venta SX'
            WHEN caseTipification = 'Deseo cambiar el estatus de una orden enviada' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo cobros errados por cancelaciones' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo un cobro errado de guÍas por envío gratis' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo un error en cálculo de comisión' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo un error en los cobros del programa f.plus+' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Requiero la constancia de detracción' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo un descuadre entre la factura y el corte de pago' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Detalle de pagos recibidos' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Estado de cuenta y dudas sobre pagos' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Pagos pendientes' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'Tengo pagos con error en Falabella Seller Center' THEN 'Finanzas (pagos)'
            WHEN caseTipification = 'No puedo generar la guía de mi orden para despacho' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Requiero de su apoyo para gestionar solicitudes de los clientes' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Descuadre de stock' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Problemas al agendar la cita en almacén' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Problemas en carga de template' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Problemas en la descarga de etiqueta bulto' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Solicitud de IMEI-N° Serie' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Apelación al cambio de medidas SKU' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Apelación de inautorización de productos' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Etiquetas de despacho (Envío Gratis y Envío Rápido' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Quiero activar / inactivar mi cuenta' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Retiro total/parcial de producto almacenado' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Problemas con Fulfillment by Falabella' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Requiero de su apoyo para gestionar solicitudes de los clientes' THEN 'BO General (soporte)'
            WHEN caseTipification = 'Deseo cambiar el estatus de una orden enviada' THEN 'BO General (soporte)'
            ELSE 'Other'
          END AS Area,
        SAFE_CAST(caseSLA AS INT64) AS caseSLA,
        SAFE_CAST(caseSLA AS INT64)*24 AS caseSLAhours,
        DATETIME_DIFF(caseDueDate,caseCreatedDate, HOUR) AS case_due_created_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN DATETIME_DIFF(caseClosedDate,caseCreatedDate, HOUR) 
            ELSE DATETIME_DIFF(CURRENT_DATE(),caseCreatedDate, HOUR) END AS case_completion_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN DATETIME_DIFF(caseClosedDate,caseDueDate, HOUR) 
            ELSE DATETIME_DIFF(CURRENT_DATE(),caseDueDate, HOUR) END AS case_diff_completion_due_hours,
        --CASE WHEN taskCompletedDate IS NOT NULL THEN DATETIME_DIFF(taskCompletedDate,taskCreatedDate, HOUR) 
        --    ELSE DATETIME_DIFF(CURRENT_DATE(),taskCreatedDate, HOUR) END AS task_completion_hours,
        `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseDueDate),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours AS case_due_created_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseClosedDate),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours 
            ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(caseCreatedDate), 9, 18, [1,2,3,4,5]).hours END AS case_completion_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(caseClosedDate),TIMESTAMP(caseDueDate), 9, 18, [1,2,3,4,5]).hours 
            ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(caseDueDate), 9, 18, [1,2,3,4,5]).hours END AS case_diff_completion_work_hours,
        --CASE WHEN taskCompletedDate IS NOT NULL THEN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(TIMESTAMP(taskCompletedDate),TIMESTAMP(taskCreatedDate), 9, 18, [1,2,3,4,5]).hours 
        --    ELSE `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.DATE_DIFF_WORK(CURRENT_TIMESTAMP(),TIMESTAMP(taskCreatedDate), 9, 18, [1,2,3,4,5]).hours END AS task_completion_work_hours,
        CASE WHEN caseClosedDate IS NOT NULL AND caseClosedDate>caseDueDate THEN 1 
            WHEN caseClosedDate IS NULL AND CURRENT_DATE()>caseDueDate THEN 1
            ELSE 0 END AS late,
        ROW_NUMBER() OVER (PARTITION BY b.caseNumber,b.deliveryOrderNumber ORDER BY b.date DESC) AS row,
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
          Origin,
          casePriority,
          caseOrigin,
          caseMilestoneStatus,
          caseStatus,
          caseClosureType,
          caseClosureComment,
          caseSLA,
          CASE 
                WHEN caseTipification='¿Cómo aumento mis ordenes de venta diaria?' THEN 4
                WHEN caseTipification='¿Cómo cargar productos?' THEN 4
                WHEN caseTipification='¿Cómo crear cuenta en Portal Fpay?' THEN 16
                WHEN caseTipification='¿Cómo descargar una guía de despacho?' THEN 4
                WHEN caseTipification='¿Cómo gestionar una garantia?' THEN 4
                WHEN caseTipification='¿Cómo incremento mis ventas?' THEN 4
                WHEN caseTipification='¿Cómo me registro a las capacitaciones?' THEN 16
                WHEN caseTipification='¿Cómo proceso una garantía?' THEN 40
                WHEN caseTipification='¿Cómo obtengo mayor apoyo comercial?' THEN 4
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='¿Cómo puedo saber el motivo de una devolución?' THEN 40
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='¿Cómo puedo saber el motivo de una devolución?' THEN 30
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='¿Cómo rastreo una devolución?' THEN 40
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='¿Cómo rastreo una devolución?' THEN 24
                ---          
                WHEN caseTipification='¿Cómo solicito un retiro de unidades desde la bodega FBy?' THEN 4
                WHEN caseTipification='¿Dónde consigo el material de capacitaciones?' THEN 16
                WHEN caseTipification='¿Por qué mis productos en el catálogo no han sido aprobados?' THEN 4
                WHEN caseTipification='¿Por qué mis productos no sincronizan en Falabella Seller Center?' THEN 76
                WHEN caseTipification='¿Qué documentación debo enviar para acreditar la originalidad de productos?' THEN 4
                WHEN caseTipification='¿Qué hago si mi producto es  extraviado por paqueterÍa?' THEN 40
                WHEN caseTipification='¿Quién es mi ejecutivo de cuenta?' THEN 4
                WHEN caseTipification='Agendar retiro/recolección' THEN 4
                WHEN caseTipification='Activar cuenta en f.com' THEN 24
                WHEN caseTipification='Algunos de mis productos presentan error 404' THEN 64 -- 40 (regional)
                WHEN caseTipification='Apelación apagado de productos del cátalogo' THEN 28
                WHEN caseTipification='Apelación de inautorización de productos ' THEN 28
                WHEN caseTipification='Apoyo en temas legales' THEN 40
                WHEN caseTipification='Beneficios y dudas del programa f.plus+' THEN 4
                WHEN caseTipification='Cambio de modalidad de despacho' THEN 16
                WHEN caseTipification='Cambio de punto drop off' THEN 16
                WHEN caseTipification='Capacitaciones' THEN 4
                WHEN caseTipification='Comisiones por devoluciones no reembolsadas' THEN 28
                WHEN caseTipification='Cómo emitir boleta/facturar al cliente final' THEN 4
                WHEN caseTipification='Cómo retirar unidades de la bodega Fulfillment by Falabella' THEN 4
                WHEN caseTipification='Consultas de adelantos de pago' THEN 40
                WHEN caseTipification='Consultas generales productos financieros' THEN 4
                WHEN caseTipification='Consultas y reclamos recaudación Fpay' THEN 72
                WHEN caseTipification='Contacto Compliance' THEN 40 -- 36 (regional)
                WHEN caseTipification='Descuadre de stock' THEN 88
                WHEN caseTipification='Deseo cambiar el estatus de una orden enviada' THEN 64 -- 28 (regional)
                WHEN caseTipification='Deseo cancelar una orden' THEN 28
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='Deseo cancelar una orden generada por error de Falabella Seller Center' THEN 28
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='Deseo cancelar una orden generada por error de Falabella Seller Center' THEN 16
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='Deseo cancelar una orden que se generó sin el precio actualizado' THEN 28
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='Deseo cancelar una orden que se generó sin el precio actualizado' THEN 16
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='Deseo cancelar una orden ya que los datos del cliente son errados' THEN 28
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='Deseo cancelar una orden ya que los datos del cliente son errados' THEN 16
                ---
                WHEN caseTipification='Deseo crear una marca' THEN 28
                WHEN caseTipification='Deseo negociar mis comisiones' THEN 24
                WHEN caseTipification='Despaché erradamente las órdenes al cliente final' THEN 28
                WHEN caseTipification='Desvinculación de Fulfillment by Falabella' THEN 4
                WHEN caseTipification='Detalle de pagos MarketPlace en mi corte' THEN 28
                WHEN caseTipification='Detalle de pagos recibidos' THEN 28
                WHEN caseTipification='Devolución de una orden que no fue entregada al cliente' THEN 36
                WHEN caseTipification='Diferencias en transacciones - Portal' THEN 28
                WHEN caseTipification='Dudas adicionales para despacho' THEN 4
                WHEN caseTipification='Dudas con la app del conductor' THEN 4
                WHEN caseTipification='Dudas con mi catálogo' THEN 4
                WHEN caseTipification='Dudas con producto despachado' THEN 4
                WHEN caseTipification='Dudas con productos patrocinados' THEN 4
                WHEN caseTipification='Dudas con un producto en devolución' THEN 4
                WHEN caseTipification='Dudas en cobros en otros servicios adicionales' THEN 4
                WHEN caseTipification='Dudas sobre el apagado de productos del catálogo' THEN 4
                WHEN caseTipification='Dudas sobre el modelo Falabella Directo' THEN 4
                WHEN caseTipification='Dudas sobre el modelo Fulfillment by Seller (drop off)' THEN 4
                WHEN caseTipification='Dudas sobre el servicio Productos patrocinados' THEN 4
                WHEN caseTipification='Dudas sobre la modalidad Fulfillment by Falabella' THEN 4
                WHEN caseTipification='Dudas sobre la sucursal para entregar las órdenes' THEN 48
                WHEN caseTipification='Dudas sobre mi cuenta Falabella Seller Center' THEN 4
                WHEN caseTipification='Dudas sobre mi liquidación de factura' THEN 36
                WHEN caseTipification='Dudas sobre otros servicios f.media' THEN 4
                WHEN caseTipification='Dudas sobre una cancelación' THEN 4
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='En el indicador de envíos a tiempo hay órdenes que no corresponden' THEN 28
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='En el indicador de envíos a tiempo hay órdenes que no corresponden' THEN 16
                ---
                WHEN caseTipification='En la creación de mi producto no aparecen disponible la lista de atributos' THEN 28
                WHEN caseTipification='Envío de reporte de acciones sugeridas' THEN 4
                WHEN caseTipification='Error en la cobertura de entrega' THEN 28
                WHEN caseTipification='Error en saldo funds out Fpay' THEN 40
                WHEN caseTipification='Errores en indicadores operativos' THEN 12
                WHEN caseTipification='Estado de cuenta y dudas sobre pagos' THEN 4
                WHEN caseTipification='Etiqueta de envío rápido' THEN 40
                WHEN caseTipification='Etiquetas de despacho' THEN 24
                WHEN caseTipification='Falta de skus en template' THEN 16
                WHEN caseTipification='FO Portal no reflejado' THEN 28
                WHEN caseTipification='Funcionamiento Portal Fpay' THEN 4
                WHEN caseTipification='Inactivación de cuenta solicitada por F.com Legal' THEN 40
                WHEN caseTipification='Información sobre mi factura ' THEN 4
                WHEN caseTipification='Información sobre una orden en devolución' THEN 4
                WHEN caseTipification='Informes y estadísticas' THEN 40
                WHEN caseTipification='La guia de mi envío no tiene movimiento' THEN 40 -- 28 (regional)
                WHEN caseTipification='Las modificaciones de mis productos no se ven reflejados en f.com' THEN 40
                WHEN caseTipification='Marketing y publicidad' THEN 40
                WHEN caseTipification='Mensajería - Servicio de publicidad' THEN 36
                WHEN caseTipification='Mis productos cuentan con los requerimientos legales para la aprobación' THEN 28
                WHEN caseTipification='Modificación de datos' THEN 40
                WHEN caseTipification='Modificar límite de ordenes diarias para despacho' THEN 28
                WHEN caseTipification='Modificar zona de entrega' THEN 48
                WHEN caseTipification='Motivo de inautorización de productos ' THEN 4
                WHEN caseTipification='Necesito apoyo en el proceso de ingreso de unidades' THEN 28
                WHEN caseTipification='Necesito el IMEI de una orden FBY para facturar al cliente' THEN 64
                WHEN caseTipification='Necesito saber a que hacen referencia los conceptos cobrados' THEN 16
                WHEN caseTipification='No he recibido mi factura' THEN 28
                WHEN caseTipification='No he recibido mi producto por fallo de entrega' THEN 40
                WHEN caseTipification='No me aparece el monto disponible en FPAY' THEN 40
                WHEN caseTipification='No puede agregar/editar cuenta bancaria' THEN 40
                WHEN caseTipification='No puedo editar mis datos bancarios en FPAY' THEN 40 -- 28 (regional)
                WHEN caseTipification='No puedo generar la guía de mi orden para despacho' THEN 40 -- 28 (regional)
                WHEN caseTipification='No puedo retirar mi dinero' THEN 40
                WHEN caseTipification='No recolectaron mi paquete. Necesito una nueva recolección' THEN 4
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='No recolectaron mis envíos pendientes' THEN 40
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='No recolectaron mis envíos pendientes' THEN 28
                ---
                WHEN caseTipification='Notificación de creación de marca' THEN 24
                WHEN caseTipification='Otros errores con mis productos' THEN 40
                WHEN caseTipification='Otros Fpay' THEN 36
                WHEN caseTipification='Pagos pendientes' THEN 28
                WHEN caseTipification='Pricing y recomendaciones' THEN 40
                WHEN caseTipification='Problema con acceso/contraseña de la cuenta' THEN 28          
                WHEN caseTipification='No se han pagado unidades en la Liquidación mensual FBy' THEN 4
                WHEN caseTipification='Problema con la contraseña de Portal Fpay' THEN 40
                WHEN caseTipification='Problemas al agendar la cita en almacen' THEN 28
                WHEN caseTipification='Problemas con devoluciones/retiros de la bodega' THEN 36
                WHEN caseTipification='Problemas con el etiquetado de productos' THEN 36
                WHEN caseTipification='Problemas con Fulfillment by Falabella' THEN 24
                WHEN caseTipification='Problemas con la app del conductor' THEN 48
                WHEN caseTipification='Problemas en cambio de estado' THEN 48
                WHEN caseTipification='Problemas en carga de template' THEN 16
                WHEN caseTipification='Problemas en la descarga de etiqueta bulto' THEN 16
                WHEN caseTipification='Problemas en la recepción de paquetes' THEN 48
                WHEN caseTipification='Productos patrocinados' THEN 40
                WHEN caseTipification='Productos promocionados - Servicio de publicidad' THEN 40
                WHEN caseTipification='Programar pagos que no han tenido cambio de estado en Falabella Seller Center' THEN 40
                WHEN caseTipification='Publicación de productos' THEN 40
                WHEN caseTipification='Quiero activar / inactivar mi cuenta ' THEN 40
                ---
                WHEN caseCreatedDate < '2023-08-25'
                  AND caseTipification='Quiero cancelar una orden debido a que no actualice mi stock en el sistema' THEN 28
                WHEN caseCreatedDate >= '2023-08-25' 
                  AND caseTipification='Quiero cancelar una orden debido a que no actualice mi stock en el sistema' THEN 16
                ---
                WHEN caseTipification='Quiero eliminar reseñas negativa de mi producto' THEN 40
                WHEN caseTipification='Quiero formalizar reclamo por un producto dañado' THEN 40
                WHEN caseTipification='Quiero formalizar reclamo por una entrega que no reconozco' THEN 40
                WHEN caseTipification='Quiero modificar los datos de mi cuenta en Falabella Seller Center' THEN 28
                WHEN caseTipification='Quiero reactivar mi cuenta de F.com' THEN 52
                WHEN caseTipification='Quiero rechazar una devolución' THEN 40
                WHEN caseTipification='Quiero saber el motivo de la cancelación de una orden' THEN 4
                WHEN caseTipification='Reagendar envío/retiro de fulfillment by falabella por incumplimiento' THEN 4
                WHEN caseTipification='Recibi un producto por garantía dañado' THEN 40  
                WHEN caseTipification='Recibí una caja vacía o mi producto fue cambiado' THEN 40     
                WHEN caseTipification='Reclamos, desconocimiento transacciones' THEN 72
                WHEN caseTipification='Requiero apoyo ya que despaché una orden cancelada' THEN 28
                WHEN caseTipification='Requiero cambio de estatus ya que mi guía no posee movimiento' THEN 40
                WHEN caseTipification='Requiero cambio de la paquetería que uso para el envío de mis productos' THEN 24
                WHEN caseTipification='Requiero de su apoyo para gestionar solicitudes de los clientes' THEN 28 -- dif
                WHEN caseTipification='Requiero el comprobante de pago' THEN 28
                WHEN caseTipification='Requiero etiquetas adicionales para el despacho de mis órdenes' THEN 28
                WHEN caseTipification='Requiero información sobre retención en la fuente' THEN 12 -- Caso random
                WHEN caseTipification='Requiero la constancia de detracción' THEN 40
                WHEN caseTipification='Requiero soporte de retenciones' THEN 28
                WHEN caseTipification='Retiro total/parcial de producto almacenado' THEN 28
                WHEN caseTipification='Solicitud de cierre de cuenta comercio' THEN 36
                WHEN caseTipification='Solicitud de documentos, modificaciones, prepago, cierre productos' THEN 72
                WHEN caseTipification='Solicitud de IMEI-N° Serie' THEN 88
                WHEN caseTipification='Solicitudes adicionales de despacho' THEN 28
                WHEN caseTipification='Temas legales y de cumplimiento' THEN 40
                WHEN caseTipification='Tengo cobros errado por retrasos en envíos' THEN 28
                WHEN caseTipification='Tengo cobros errados en la plataforma de productos promocionados' THEN 40
                WHEN caseTipification='Tengo cobros errados por cancelaciones' THEN 28
                WHEN caseTipification='Tengo cobros por reetiquetado' THEN 4
                WHEN caseTipification='Tengo diferencias de stock en F.com' THEN 100
                WHEN caseTipification='Tengo dudas de cobros de productos patrocinados' THEN 4
                WHEN caseTipification='Tengo dudas sobre pagos recibidos' THEN 40
                WHEN caseTipification='Tengo error en cobros de productos patrocinados' THEN 36
                WHEN caseTipification='Tengo pagos con error en Falabella Seller Center' THEN 28
                WHEN caseTipification='Tengo productos pendientes de aprobación y activación en Falabella Seller Center' THEN 28
                WHEN caseTipification='Tengo un cobro errado de guÍas por devolución' THEN 28
                WHEN caseTipification='Tengo un cobro errado de guÍas por envío gratis' THEN 28
                WHEN caseTipification='Tengo un cobro errado de penalidades' THEN 28
                WHEN caseTipification='Tengo un descuadre entre la factura y el corte de pago' THEN 40
                WHEN caseTipification='Tengo un error en cálculo de comisión' THEN 28
                WHEN caseTipification='Tengo un error en la plataforma de productos promocionados' THEN 4
                WHEN caseTipification='Tengo un error en los cobros del programa f.plus+' THEN 28
                WHEN caseTipification='Tengo un error por FBy' THEN 28
                WHEN caseTipification='Términos y condiciones' THEN 40
                WHEN caseTipification='Validación de documentos por parte de Legal' THEN 40 
                WHEN caseTipification='Validación QC GSC' THEN 24
                WHEN caseTipification='Venta de marca o producto restringido' THEN 4        
              ELSE SAFE_CAST(caseSLA AS INT64) END AS New_SLA,
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
              WHEN FC_Commerce_Name__c IN ('F.com','GSC','Falabella') THEN 'F.com'
              WHEN FC_Commerce_Name__c NOT IN ('F.com','GSC','Falabella') AND REGEXP_CONTAINS(FC_SellerId__c,'^SC') OR REGEXP_CONTAINS(FC_SellerId__c,'^Falabella') THEN 'F.com'
              WHEN FC_Commerce_Name__c NOT IN ('F.com','GSC','Falabella') AND REGEXP_CONTAINS(FC_ExternalIDOrderSeller__c,'^2') THEN 'F.com'
              ELSE FC_Commerce_Name__c END AS commerce,
            CaseNumber AS caseNumber,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_BusinessUnit__c) AS caseBU,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Level_1__c) AS caseLevel1,
            `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs`.FIX_STRING(FC_Level_2__c) AS caseLevel2,
            COALESCE(FC_TipificationName__c, Subject) AS caseTipification,
            Origin,
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
                b.value,
                b.ingreso,
                b.egreso,
                b.pago,
                b.descuento,
                b.ajuste
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
                  UNION ALL
                  SELECT 
                    CaseNumber,
                    uniqueorderNumber
                  FROM `tc-sc-bi-bigdata-cons-fcom-prd.svw_tc_sc_bi_bigdata_dtl_fcom_prd_trf_corp_reg_cust_csmg.svw_agg_salesforce_case`,
                  UNNEST(REGEXP_EXTRACT_ALL(Description, r'\b2[0-9]{1,9}\b')) AS uniqueorderNumber
                )
              ) a
              LEFT JOIN `bi-fcom-drmb-local-pe-sbx.Dragonite_SX_KPIs.reporte_cambio_estado` b ON a.uniqueorderNumber=b.deliveryOrderNumber
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
    )
    WHERE country='PE'
    AND commerce IN ('F.com','GSC','Falabella')
    --AND sellerId NOT IN ('FALABELLA_PERU','SODIMAC_PERU','TOTTUS_PERU','FALABELLA PERU','SODIMAC PERU', 'TOTTUS PERU')
    --AND sellerName NOT IN ('FALABELLA','SODIMAC','TOTTUS')
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
