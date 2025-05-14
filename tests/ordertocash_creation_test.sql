SELECT *
FROM {{ ref("ordertocash") }}
WHERE client_mandt IS NULL