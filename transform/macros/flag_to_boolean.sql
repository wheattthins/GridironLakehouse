{#
    nflverse stores indicators as 0/1 doubles that are NULL on rows that are not
    plays (timeouts, end of quarter, ...). Convert them to booleans that are
    never NULL, so downstream filters do not need three-valued logic.
#}

{% macro flag_to_boolean(column_name) -%}
    coalesce({{ column_name }} = 1, false)
{%- endmacro %}
