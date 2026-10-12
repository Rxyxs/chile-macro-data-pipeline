-- A week without a dollar value means a missing extract, not a holiday.
select fecha, prev_fecha
from (select fecha, lag(fecha) over (order by fecha) as prev_fecha from {{ ref('mart_mercados_diario') }})
where prev_fecha is not null and fecha - prev_fecha > 7
