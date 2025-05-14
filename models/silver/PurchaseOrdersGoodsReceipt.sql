WITH PurchaseOrdersGoodsReceipt AS (
    SELECT
      PurchaseOrderScheduleLine.Client_MANDT,
      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
      PurchaseOrderScheduleLine.Item_EBELP,
      PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ,
      PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.NetOrderValueinPOCurrency_NETWR,
      PurchaseOrderScheduleLine.CurrencyKey_WAERS,
      PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT,
      PurchaseOrderScheduleLine.OrderDateOfScheduleLine_BEDAT,
      PurchaseOrderScheduleLine.POQuantity_MENGE,
      PurchaseOrderScheduleLine.UoM_MEINS,
      PurchaseOrderScheduleLine.NetPrice_NETPR,
      PurchaseOrderScheduleLine.CreatedOn_AEDAT,
      PurchaseOrderScheduleLine.Status_STATU,
      PurchaseOrderScheduleLine.MaterialNumber_MATNR,
      PurchaseOrderScheduleLine.MaterialType_MTART,
      PurchaseOrderScheduleLine.MaterialGroup_MATKL,
      PurchaseOrderScheduleLine.PurchasingOrganization_EKORG,
      PurchaseOrderScheduleLine.PurchasingGroup_EKGRP,
      PurchaseOrderScheduleLine.Company_BUKRS,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO,
      PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.OverdeliveryToleranceLimit,
      PurchaseOrderScheduleLine.VendorAccountNumber_LIFNR,
      PurchaseOrderScheduleLine.Plant_WERKS,
      PurchaseOrderScheduleLine.YearOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.MonthOfPurchasingDocumentDate_BEDAT,
      PurchaseOrderScheduleLine.WeekOfPurchasingDocumentDate_BEDAT,
      POOrderHistory.AmountInLocalCurrency_DMBTR,
      POOrderHistory.CurrencyKey_WAERS AS POOrderHistoryCurrencyKey_WAERS,
      -- Actual Reciept Date

      CASE 
        WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' 
        THEN POOrderHistory.PostingDateInTheDocument_BUDAT
        ELSE NULL
      END AS PostingDateInTheDocument_BUDAT,

      -- DeliveryStatus
      -- TRUE stands for Delivered Orders and FALSE stands for NotDelivered Orders
      CASE 
        WHEN PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ IS NULL 
        THEN FALSE 
        ELSE TRUE 
      END AS IsDelivered,

      -- Vendor Cycle Time in Days

      CASE 
        WHEN PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X' THEN 
            COALESCE(
                DATEDIFF(
                    'DAY',  -- Intervalle de différence
                    PurchaseOrderScheduleLine.PurchasingDocumentDate_BEDAT,  -- Date de début
                    CASE
                        WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN
                            MAX(POOrderHistory.PostingDateInTheDocument_BUDAT) OVER (
                                PARTITION BY
                                    PurchaseOrderScheduleLine.Client_MANDT,
                                    PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                    PurchaseOrderScheduleLine.Item_EBELP
                            )
                        ELSE NULL
                    END  -- Date de fin
                ),
                0
            )
        ELSE NULL
      END AS VendorCycleTimeInDays,


















      -- Vendor Quality (Rejection)
      -- TRUE stands for Rejected Orders and FALSE stands for NotRejected Orders
      CASE 
        WHEN POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161') THEN TRUE
        ELSE FALSE
      END AS IsRejected,

      -- Rejected Quantity
      CASE 
        WHEN POOrderHistory.MovementType__inventoryManagement___BWART IN ('122', '161') THEN POOrderHistory.Quantity_MENGE
        ELSE 0
      END AS RejectedQuantity,

      -- Vendor On Time Delivery
      -- TRUE stands for NotDelayed Orders and FALSE for Delayed Orders
      CASE 
          WHEN PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X' THEN 
              CASE 
                  WHEN (
                      CASE 
                          WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN 
                              POOrderHistory.PostingDateInTheDocument_BUDAT
                          ELSE NULL
                      END
                  ) <= PurchaseOrderScheduleLine.ItemDeliveryDate_EINDT THEN TRUE
                  ELSE FALSE
              END
          ELSE NULL
      END AS IsDeliveredOnTime,


      -- Vendor InFull Delivery
      -- TRUE stands for DeliveredInFull Orders and FALSE stands for NotDeliveredInFull Orders
      CASE 
          WHEN PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X' THEN 
              CASE 
                  WHEN PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL 
                      AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL THEN 
                      CASE 
                          WHEN SUM(
                              CASE 
                                  WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN POOrderHistory.Quantity_MENGE
                                  ELSE POOrderHistory.Quantity_MENGE * -1
                              END
                          ) OVER (
                              PARTITION BY 
                                  PurchaseOrderScheduleLine.Client_MANDT,
                                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                  PurchaseOrderScheduleLine.Item_EBELP
                          ) >= PurchaseOrderScheduleLine.POQuantity_MENGE THEN TRUE
                          ELSE FALSE
                      END
                  ELSE 
                      CASE 
                          WHEN SUM(
                              CASE 
                                  WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN POOrderHistory.Quantity_MENGE
                                  ELSE POOrderHistory.Quantity_MENGE * -1
                              END
                          ) OVER (
                              PARTITION BY 
                                  PurchaseOrderScheduleLine.Client_MANDT,
                                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                  PurchaseOrderScheduleLine.Item_EBELP
                          ) >= PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit THEN TRUE
                          ELSE FALSE
                      END
                      OR
                      CASE 
                          WHEN SUM(
                              CASE 
                                  WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN POOrderHistory.Quantity_MENGE
                                  ELSE POOrderHistory.Quantity_MENGE * -1
                              END
                          ) OVER (
                              PARTITION BY 
                                  PurchaseOrderScheduleLine.Client_MANDT,
                                  PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                  PurchaseOrderScheduleLine.Item_EBELP
                          ) <= PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit THEN TRUE
                          ELSE FALSE
                      END
              END
          ELSE NULL
      END AS IsDeliveredInFull,

      -- Vendor Invoice Accuracy
      -- TRUE stands for Accurate Invoices and FALSE stands for Inaccurate Invoices
      CASE 
          WHEN PurchaseOrderScheduleLine.DeliveryCompletedFlag_ELIKZ = 'X' THEN 
              CASE 
                  WHEN PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit_UNTTO IS NULL 
                      AND PurchaseOrderScheduleLine.OverdeliveryToleranceLimit_UEBTO IS NULL THEN 
                      CASE 
                          WHEN PurchaseOrderScheduleLine.POQuantity_MENGE = 
                              SUM(
                                  CASE 
                                      WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN POOrderHistory.Quantity_MENGE
                                      ELSE POOrderHistory.Quantity_MENGE * -1
                                  END
                              ) OVER (
                                  PARTITION BY 
                                      PurchaseOrderScheduleLine.Client_MANDT,
                                      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                      PurchaseOrderScheduleLine.Item_EBELP
                              ) THEN TRUE
                          ELSE FALSE
                      END
                  ELSE 
                      CASE 
                          WHEN SUM(
                                  CASE 
                                      WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN POOrderHistory.Quantity_MENGE
                                      ELSE POOrderHistory.Quantity_MENGE * -1
                                  END
                              ) OVER (
                                  PARTITION BY 
                                      PurchaseOrderScheduleLine.Client_MANDT,
                                      PurchaseOrderScheduleLine.DocumentNumber_EBELN,
                                      PurchaseOrderScheduleLine.Item_EBELP
                              ) 
                              BETWEEN 
                                  PurchaseOrderScheduleLine.POQuantity_MENGE - PurchaseOrderScheduleLine.UnderdeliveryToleranceLimit
                                  AND 
                                  PurchaseOrderScheduleLine.POQuantity_MENGE + PurchaseOrderScheduleLine.OverdeliveryToleranceLimit THEN TRUE
                          ELSE FALSE
                      END
              END
          ELSE NULL
      END AS IsGoodsReceiptAccurate,

      -- Vendor Spend Analysis In Source Currency
      -- Goods Receipt Amount In Source Currency

      CASE 
          WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' THEN 
              POOrderHistory.AmountInLocalCurrency_DMBTR
          ELSE 
              POOrderHistory.AmountInLocalCurrency_DMBTR * -1
      END AS GoodsReceiptAmountInSourceCurrency,

      -- Goods Receipt Quantity
      CASE 
        WHEN POOrderHistory.MovementType__inventoryManagement___BWART = '101' 
        THEN POOrderHistory.Quantity_MENGE
        ELSE POOrderHistory.Quantity_MENGE * -1
      END AS GoodsReceiptQuantity




    FROM
      {{ ref("PurchaseOrderScheduleLine") }} 
    LEFT JOIN (
      SELECT
          Client_MANDT,
          PurchasingDocumentNumber_EBELN,
          ItemNumberOfPurchasingDocument_EBELP,
          MovementType__inventoryManagement___BWART,
          AmountInLocalCurrency_DMBTR,
          CurrencyKey_WAERS,
          PostingDateInTheDocument_BUDAT,
          Quantity_MENGE
      FROM {{ ref("PurchaseDocumentsHistory") }}  
      WHERE TransactioneventType_VGABE = '1'  
        AND MovementType__inventoryManagement___BWART IN ('101', '102', '161', '122')  
      ) AS POOrderHistory
          ON PurchaseOrderScheduleLine.Client_MANDT = POOrderHistory.Client_MANDT
          AND PurchaseOrderScheduleLine.DocumentNumber_EBELN = POOrderHistory.PurchasingDocumentNumber_EBELN
          AND PurchaseOrderScheduleLine.Item_EBELP = POOrderHistory.ItemNumberOfPurchasingDocument_EBELP
)

SELECT * FROM PurchaseOrdersGoodsReceipt