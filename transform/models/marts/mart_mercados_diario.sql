-- One row per trading day (the days the dollar is published). The UF is daily on calendar
-- days, so it always joins; copper is missing on London holidays and stays null there.
select
    d.fecha,
    u.uf,
    d.dolar,
    c.cobre                               as cobre_usd_lb,
    c.cobre * d.dolar                     as cobre_clp_lb,
    c.cobre * d.dolar / u.uf              as cobre_uf_lb
from {{ ref('stg_dolar') }} d
join {{ ref('stg_uf') }} u using (fecha)
left join {{ ref('stg_cobre') }} c using (fecha)
