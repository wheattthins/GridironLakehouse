{#
    Decides which schema a model is built in.

    prod target   -> the layer schema as configured: staging, silver, gold
    other targets -> prefixed with the target schema: dev_staging, dev_silver, dev_gold

    Development runs therefore never write to the schemas that production reads.
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema | trim }}
    {%- elif target.name == 'prod' -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ target.schema | trim }}_{{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
