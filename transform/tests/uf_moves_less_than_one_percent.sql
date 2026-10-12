-- The UF follows last month's CPI spread over the month; it never moves 1% in a day.
-- datoschile already removes the known bad days (the dollar leaking into the series in
-- December 2014); this test fails if a new one appears.
select fecha, uf, prev_uf
from (select fecha, uf, lag(uf) over (order by fecha) as prev_uf from {{ ref('stg_uf') }})
where prev_uf is not null and abs(uf / prev_uf - 1) >= 0.01
