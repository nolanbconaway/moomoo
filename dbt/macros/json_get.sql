
{%- macro json_get(column, path, as_json=false) -%}
  {%- if as_json -%}
    {{ _get(column, path) }}
  {%- else -%}
    nullif(trim({{ _get(column, path[:-1]) }} ->> '{{ path[-1] }}'), '')
  {%- endif -%}
{%- endmacro -%}

{% macro _get(column, path) -%}
  {{ column -}}
  {%- for item in path %} ->
    {%- if item is number %} {{ item }} {%- else %} '{{ item }}' {%- endif -%}
  {%- endfor -%}
{% endmacro -%}