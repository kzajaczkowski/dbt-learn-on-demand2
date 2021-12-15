{% macro standardise_string(column_name) -%}
TRIM({{ column_name }})
{%- endmacro %}