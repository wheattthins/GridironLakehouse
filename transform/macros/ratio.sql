{#
    Floating-point division that returns NULL instead of failing when the
    denominator is zero (Databricks SQL raises DIVIDE_BY_ZERO otherwise).
#}

{% macro ratio(numerator, denominator) -%}
    cast({{ numerator }} as double) / nullif({{ denominator }}, 0)
{%- endmacro %}
