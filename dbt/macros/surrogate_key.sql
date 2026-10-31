{#
  Deterministic surrogate key from one or more natural-key columns.

  Hand-rolled rather than pulled from dbt_utils so the project has no package
  dependency and the generated SQL is readable in `dbt compile` output. Nulls
  are coalesced to a sentinel so two rows differing only in a null do not
  collide into the same key.
#}

{% macro surrogate_key(columns) %}
    md5(
        {%- for column in columns %}
        coalesce(cast({{ column }} as varchar), '_null_')
        {%- if not loop.last %} || '|' || {% endif %}
        {%- endfor %}
    )
{% endmacro %}
