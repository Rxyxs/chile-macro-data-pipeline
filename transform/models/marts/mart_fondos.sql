-- Real (UF) index of each pension fund, its 12-month real return and its drawdown from
-- the previous peak. The 12-month return uses the last index on or before the same date a
-- year earlier, since funds are only valued on business days.
with f as (select * from {{ ref('stg_fondos') }})
select
    f.fecha, f.fondo, f.indice_real,
    (f.indice_real / prev.indice_real - 1) * 100 as retorno_real_12m,
    (f.indice_real / max(f.indice_real) over (partition by f.fondo order by f.fecha) - 1) * 100
        as caida_desde_maximo
from f
asof left join f as prev
    on prev.fondo = f.fondo and prev.fecha <= f.fecha - interval 365 day
order by f.fondo, f.fecha
