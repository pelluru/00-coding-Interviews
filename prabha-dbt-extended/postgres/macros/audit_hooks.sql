{% macro ensure_audit_table() %}
  {# Create a lightweight audit table in the target schema if it doesn't exist #}
  {% set sql %}
  create table if not exists {{ target.schema }}.dbt_audit (
    run_id string,
    event string,
    at timestamp
  )
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}

{% macro log_run_start() %}
  {% do ensure_audit_table() %}
  {% set sql %}
  insert into {{ target.schema }}.dbt_audit (run_id, event, at)
  values ('{{ invocation_id }}', 'start', {{ current_timestamp() }})
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}

{% macro log_run_end() %}
  {% do ensure_audit_table() %}
  {% set sql %}
  insert into {{ target.schema }}.dbt_audit (run_id, event, at)
  values ('{{ invocation_id }}', 'end', {{ current_timestamp() }})
  {% endset %}
  {% do run_query(sql) %}
{% endmacro %}
