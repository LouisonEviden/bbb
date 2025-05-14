{% macro is_leap_year(year) %}
(
    (MOD({{ year }}, 4) = 0 AND MOD({{ year }}, 100) != 0)
    OR
    MOD({{ year }}, 400) = 0
)
{% endmacro %}

