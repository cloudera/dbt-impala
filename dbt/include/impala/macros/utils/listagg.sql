{#
# Copyright 2022 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#}

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
