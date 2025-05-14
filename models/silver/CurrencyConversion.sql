WITH CurrencyConversion AS (
    SELECT
        '{{ var("client_value") }}' AS Client_MANDT,
        '{{ var("exchange_rate_type") }}' AS ExchangeRateType_KURST,
        '{{ var("from_currency") }}' AS FromCurrency_FCURR,
        '{{ var("to_currency") }}' AS ToCurrency_TCURR,
        NULL AS ExchangeRate_UKURS,
        NULL AS StartDate,
        NULL AS EndDate,
        '{{ var("conversion_date") }}' AS ConvDate,
        {{ currency_conversion_macro(
            mandt=var("client_value"),
            kurst=var("exchange_rate_type"),
            fcurr=var("from_currency"),
            tcurr=var("to_currency"),
            conv_date=var("conversion_date"),
            ip_amount=var("amount_to_convert")
        ) }} AS ConvertedAmount
)

SELECT * FROM CurrencyConversion