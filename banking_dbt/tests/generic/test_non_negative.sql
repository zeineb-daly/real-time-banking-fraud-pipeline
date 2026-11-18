{% test non_negative(model, column_name) %}
-- Fails for every row where the column is negative (NULLs are allowed).
select *
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
