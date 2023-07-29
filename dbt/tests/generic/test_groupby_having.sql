{% test groupby_having(model, group, having_condition) %}

{# 
    Assert that a condition is met for every group.
#}

select {{ group }}
from {{ model }}
group by {{ group }}
having not ({{ having_condition }})

{% endtest %}