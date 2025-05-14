WITH tcurr_data AS (
    SELECT
        mandt AS client_value,
        kurst,
        fcurr,
        tcurr,
        ukurs,
        TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT))) AS start_date,
        CASE 
            WHEN LEAD(TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)))) 
                 OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC) IS NULL 
            THEN DATEADD(YEAR, 1000, TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT))))
            ELSE DATEADD(DAY, -1, LEAD(TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)))) 
                 OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC))
        END AS end_date
    FROM {{ source('source_db', 'tcurr') }}
),
filtered_tcurr AS (
    SELECT *
    FROM tcurr_data
    WHERE client_value = {{ var('client_value') }}
      AND kurst = '{{ var('kurst') }}'  -- Les guillemets simples corrigent l'erreur
      AND fcurr = '{{ var('fcurr') }}'
      AND tcurr = '{{ var('tcurr') }}'
      AND '{{ var('conv_date') }}' BETWEEN start_date AND end_date
)
SELECT
    CASE
        WHEN ukurs < 0 THEN (1 / ABS(ukurs)) * {{ var('ip_amount') }}
        ELSE ukurs * {{ var('ip_amount') }}
    END AS converted_amount
FROM filtered_tcurr

