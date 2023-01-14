
{%- macro json_get(column, path) -%}
  nullif(trim({{ column -}}
      {%- for item in path -%}
        {%- if loop.last %} ->> '{{ item }}'
        {%- else %} -> '{{ item }}'
        {%- endif -%}
      {%- endfor -%}
    ), '')
{%- endmacro -%}