{% macro currency_conversion_macro(mandt, kurst, fcurr, tcurr, conv_date, ip_amount) %}
    (
        SELECT
            CASE
                WHEN ukurs < 0 THEN (1 / ABS(ukurs)) * {{ ip_amount }}
                ELSE ukurs * {{ ip_amount }}
            END AS converted_amount
        FROM (
            SELECT
                mandt,
                kurst,
                fcurr,
                tcurr,
                ukurs,
                TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)), 'YYYY-MM-DD') AS start_date,
                CASE 
                    WHEN LEAD(TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)), 'YYYY-MM-DD')) 
                         OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC) IS NULL 
                    THEN DATEADD(YEAR, 1000, TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)), 'YYYY-MM-DD'))
                    ELSE DATEADD(DAY, -1, LEAD(TO_DATE(TO_CHAR(99999999 - CAST(gdatu AS INT)), 'YYYY-MM-DD')) 
                         OVER (PARTITION BY mandt, kurst, fcurr, tcurr ORDER BY gdatu DESC))
                END AS end_date
            FROM {{ source('source_db', 'tcurr') }}
        ) AS tcurr_sub
        WHERE
            mandt = '{{ mandt }}'
            AND kurst = '{{ kurst }}'
            AND fcurr = '{{ fcurr }}'
            AND tcurr = '{{ tcurr }}'
            AND TO_DATE('{{ conv_date }}', 'YYYY-MM-DD') BETWEEN start_date AND end_date
        LIMIT 1
    )
{% endmacro %}

