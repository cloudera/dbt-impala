
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

{% macro assert_not_null(function, arg) %}
    coalesce(cast({{function}}({{arg}}) as bigint), nvl2({{function}}({{arg}}), istrue({{function}}({{arg}}) is not null), null))
{% endmacro %}

{% macro assert_not_null2(function, arg1, arg2) %}
    coalesce(cast({{function}}({{arg1}}, {{arg2}}) as bigint), nvl2({{function}}({{arg1}}, {{arg2}}), istrue({{function}}({{arg1}}, {{arg2}}) is not null), null))
{% endmacro %}

{% macro impala__datediff(first_date, second_date, datepart) %}

    {# make sure the dates are real, otherwise raise an error asap #}
    {% set org_first_date = first_date %}
    {% set org_second_date = second_date %}
    {% set first_date = assert_not_null2('date_part', '"EPOCH"', first_date) %}
    {% set second_date = assert_not_null2('date_part', '"EPOCH"', second_date) %}

    {%- if datepart == 'day' -%}

        datediff(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp))

    {%- elif datepart == 'week' -%}

        case when {{first_date}} < {{second_date}}
            then floor(datediff(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp))/7)
            else ceil(datediff(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp))/7)
            end

        -- did we cross a week boundary (Sunday)?
        + case
            when {{first_date}} < {{second_date}} and dayofweek(cast({{second_date}} as timestamp)) < dayofweek(cast({{first_date}} as timestamp)) then 1
            when {{first_date}} > {{second_date}} and dayofweek(cast({{second_date}} as timestamp)) > dayofweek(cast({{first_date}} as timestamp)) then -1
            else 0 end

    {%- elif datepart == 'month' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp)))
            else ceil(months_between(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp)))
            end

        -- did we cross a month boundary?
        + case
            when {{first_date}} < {{second_date}} and dayofmonth(cast({{second_date}} as timestamp)) < dayofmonth(cast({{first_date}} as timestamp)) then 1
            when {{first_date}} > {{second_date}} and dayofmonth(cast({{second_date}} as timestamp)) > dayofmonth(cast({{first_date}} as timestamp)) then -1
            else 0 end

    {%- elif datepart == 'quarter' -%}

        case when {{first_date}} < {{second_date}}
            then floor(months_between(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp))/3)
            else ceil(months_between(cast({{second_date}} as timestamp), cast({{first_date}} as timestamp))/3)
            end

        -- did we cross a quarter boundary?
        + case
            when {{first_date}} < {{second_date}} and (
                (dayofyear(cast({{second_date}} as timestamp)) - (quarter(cast({{second_date}} as timestamp)) * 365/4))
                < (dayofyear(cast({{first_date}} as timestamp)) - (quarter(cast({{first_date}} as timestamp)) * 365/4))
            ) then 1
            when {{first_date}} > {{second_date}} and (
                (dayofyear(cast({{second_date}} as timestamp)) - (quarter(cast({{second_date}} as timestamp)) * 365/4))
                > (dayofyear(cast({{first_date}} as timestamp)) - (quarter(cast({{first_date}} as timestamp)) * 365/4))
            ) then -1
            else 0 end

    {%- elif datepart == 'year' -%}

        year(cast({{second_date}} as timestamp)) - year(cast({{first_date}} as timestamp))

    {%- elif datepart in ('hour', 'minute', 'second', 'millisecond', 'microsecond') -%}

        {%- set divisor -%}
            {%- if datepart == 'hour' -%} 3600
            {%- elif datepart == 'minute' -%} 60
            {%- elif datepart == 'second' -%} 1
            {%- elif datepart == 'millisecond' -%} (1/1000)
            {%- elif datepart == 'microsecond' -%} (1/1000000)
            {%- endif -%}
        {%- endset -%}

        case when {{first_date}} < {{second_date}}
            then ceil((
                {{second_date}} - {{first_date}}
            ) / {{divisor}})
            else floor((
                {{second_date}} - {{first_date}}
            ) / {{divisor}})
            end

            {% if datepart == 'millisecond' %}
                + millisecond({{org_second_date}})
                - millisecond({{org_first_date}})
            {% endif %}

            {% if datepart == 'microsecond' %}
                {% set capture_str = '[0-9]{4}-[0-9]{2}-[0-9]{2}.[0-9]{2}:[0-9]{2}:[0-9]{2}.([0-9]{6})' %}
                -- Impala doesn't really support microseconds extration from timestamp, eventhough there is microsecond_add/sub
                -- So this is a massive hack!
                -- It will only work if the timestamp-string is of the format
                -- 'yyyy-MM-dd-HH mm.ss.SSSSSS'
                + cast(regexp_extract(cast(cast({{second_date}} as timestamp) as string), '{{capture_str}}', 1) as bigint)
                - cast(regexp_extract(cast(cast({{first_date}} as timestamp) as string), '{{capture_str}}', 1) as bigint)
            {% endif %}

    {%- else -%}

        {{ exceptions.raise_compiler_error("macro datediff not implemented for datepart ~ '" ~ datepart ~ "' ~ on Impala") }}

    {%- endif -%}

{% endmacro %}
