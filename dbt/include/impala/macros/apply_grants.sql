{#-- Assume grants copy over --#}
{% macro impala__copy_grants() %}
    {{ return(True) }}
{% endmacro %}


{%- macro impala__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on {{ relation }} to {{ adapter.quote(grantees[0]) }}
{%- endmacro %}


{%- macro impala__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation }} from {{ adapter.quote(grantees[0]) }}
{%- endmacro %}

{#-- Impala does not support multile grantees per dcl statement --#}
{%- macro impala__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(False) }}
{%- endmacro -%}


{% macro imapal__call_dcl_statements(dcl_statement_list) %}
    {% for dcl_statement in dcl_statement_list %}
        {% call statement('grant_or_revoke') %}
            {{ dcl_statement }}
        {% endcall %}
    {% endfor %}
{% endmacro %}
