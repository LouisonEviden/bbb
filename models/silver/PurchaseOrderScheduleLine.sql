WITH PurchaseOrderScheduleLine AS (
    SELECT
        PurchaseOrders.Client_MANDT,
        PurchaseOrders.DocumentNumber_EBELN,
        PurchaseOrders.Item_EBELP,
        PurchaseOrders.DeliveryCompletedFlag_ELIKZ,
        PurchaseOrders.PurchasingDocumentDate_BEDAT,
        PurchaseOrders.NetOrderValueinPOCurrency_NETWR,
        PurchaseOrders.CurrencyKey_WAERS,
        PurchaseOrders.POQuantity_MENGE,
        PurchaseOrders.UoM_MEINS,
        PurchaseOrders.NetPrice_NETPR,
        PurchaseOrders.CreatedOn_AEDAT,
        PurchaseOrders.Status_STATU,
        PurchaseOrders.MaterialNumber_MATNR,
        PurchaseOrders.MaterialType_MTART,
        PurchaseOrders.MaterialGroup_MATKL,
        PurchaseOrders.PurchasingOrganization_EKORG,
        PurchaseOrders.PurchasingGroup_EKGRP,
        PurchaseOrders.VendorAccountNumber_LIFNR,
        PurchaseOrders.Company_BUKRS,
        PurchaseOrders.Plant_WERKS,
        PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO,
        PurchaseOrders.OverdeliveryToleranceLimit_UEBTO,
        POScheduleLine.ItemDeliveryDate_EINDT,
        POScheduleLine.OrderDateOfScheduleLine_BEDAT,
        PurchaseOrders.YearOfPurchasingDocumentDate_BEDAT,
        PurchaseOrders.MonthOfPurchasingDocumentDate_BEDAT,
        PurchaseOrders.WeekOfPurchasingDocumentDate_BEDAT,
        COALESCE(
            (PurchaseOrders.UnderdeliveryToleranceLimit_UNTTO * PurchaseOrders.POQuantity_MENGE) / 100,
            0
        ) AS UnderdeliveryToleranceLimit,
        COALESCE(
            (PurchaseOrders.OverdeliveryToleranceLimit_UEBTO * PurchaseOrders.POQuantity_MENGE) / 100,
            0
        ) AS OverdeliveryToleranceLimit
    FROM
        {{ ref("PurchaseDocuments") }} AS PurchaseOrders
    LEFT JOIN (
        SELECT
            Client_MANDT,
            PurchasingDocumentNumber_EBELN,
            ItemNumberOfPurchasingDocument_EBELP,
            MAX(ItemDeliveryDate_EINDT) AS ItemDeliveryDate_EINDT,
            MAX(OrderDateOfScheduleLine_BEDAT) AS OrderDateOfScheduleLine_BEDAT
        FROM {{ ref("POSchedule") }}
        GROUP BY
            Client_MANDT,
            PurchasingDocumentNumber_EBELN,
            ItemNumberOfPurchasingDocument_EBELP
    ) AS POScheduleLine
    ON
        PurchaseOrders.Client_MANDT = POScheduleLine.Client_MANDT
        AND PurchaseOrders.DocumentNumber_EBELN = POScheduleLine.PurchasingDocumentNumber_EBELN
        AND PurchaseOrders.Item_EBELP = POScheduleLine.ItemNumberOfPurchasingDocument_EBELP
    WHERE
        PurchaseOrders.DocumentType_BSART IN ('NB', 'ENB')
        AND PurchaseOrders.ItemCategoryinPurchasingDocument_PSTYP != '2'
)

SELECT * FROM PurchaseOrderScheduleLine