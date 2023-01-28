{%- macro try_cast_uuid(string_arg) -%}
  {#- Tries to cast an arg to a uuid. if not returns null -#}
  {%- set regex='^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$' -%}
  case
    when ({{ string_arg }})::varchar ~ '{{ regex }}'
    then ({{ string_arg }})::uuid 
  end
{%- endmacro -%}