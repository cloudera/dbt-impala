{% macro impala__listagg(measure, delimiter_text, order_by_clause, limit_num) -%}

  {# see https://issues.apache.org/jira/browse/IMPALA-5545 #}
  {% if order_by_clause %}
    {{ exceptions.warn("order_by_clause is not supported for listagg on Impala") }}
  {% endif %}
  {% if limit_num %}
    {{ exceptions.warn("limit_num is not supported for listagg on Impala") }}
  {% endif %}

  {% set collect_list %} group_concat({{ measure }}, {{ delimiter_text }}) {% endset %}

  {% do return(collect_list) %}

{%- endmacro %}
