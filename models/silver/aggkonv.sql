WITH aggkonv AS (
  SELECT
    KONV.KNUMV,
    KONV.KPOSN,
    KONV.MANDT,
    SUM(
      IFF(KONV.KRECH = 'C' AND KONV.KOAID = 'B' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)
    ) AS ListPrice,
    SUM(
      IFF(KONV.KRECH = 'C' AND KONV.KOAID = 'B' AND KONV.KSCHL = 'PB00', KONV.KWERT, NULL)
    ) AS AdjustedPrice,
    SUM(IFF(KONV.KOAID = 'A' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)) AS Discount,
    SUM(
      IFF(KONV.KFKIV = 'X' AND KONV.KOAID = 'B' AND KONV.KINAK IS NULL, KONV.KWERT, NULL)
    ) AS InterCompanyPrice,
    SUM(IFF((KONV.koaid = 'C' AND KONV.KINAK IS NULL), KONV.kwert, NULL)) AS Rebate
  FROM
    {{ source("source_db", "konv") }} AS KONV
  GROUP BY KNUMV, KPOSN, MANDT
)


select * from aggkonv



