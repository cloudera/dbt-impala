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

{#-- Assume grants copy over --#}
{% macro impala__copy_grants() %}
    {{ return(True) }}
{% endmacro %}

{%- macro impala__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on table {{ relation }} to user {{ adapter.quote(grantees[0]) }}
{%- endmacro %}

{%- macro impala__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on table {{ relation }} from user {{ adapter.quote(grantees[0]) }}
{%- endmacro %}

{#-- Impala does not support multiple grantees per dcl statement --#}
{%- macro impala__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}

{% macro impala__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
        {% call statement('grant_or_revoke') %}
            {{ dcl_statement }}
        {% endcall %}
    {% endfor %}
{% endmacro %}

{% macro impala__get_show_grant_sql(relation) %}
    show grant user current_user on table {{ relation }}
{% endmacro %}
