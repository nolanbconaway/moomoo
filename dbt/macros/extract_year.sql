{%- macro extract_year(date_str_col) -%}
{#- Extract the year from a date string column -#}

case
    when substring({{ date_str_col }} from 1 for 4) ~ '^\d+(\.\d+)?$'
    then substring({{ date_str_col }} from 1 for 4)::int
end
{%- endmacro -%}