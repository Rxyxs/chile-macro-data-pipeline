-- Bounds wide enough to never trip on real data and narrow enough to catch a unit change
-- (dollar in cents, copper per kilo, a rate published as a fraction).
select 'dolar' as serie, fecha::varchar as clave from {{ ref('stg_dolar') }} where dolar not between 300 and 2000
union all select 'cobre', fecha::varchar from {{ ref('stg_cobre') }} where cobre not between 0.5 and 10
union all select 'tpm', fecha::varchar from {{ ref('stg_tpm') }} where tpm not between 0 and 20
union all select 'ipc', fecha::varchar from {{ ref('stg_ipc') }} where abs(ipc) > 5
union all select 'desempleo', fecha::varchar from {{ ref('stg_desempleo') }} where desempleo not between 2 and 20
union all select 'morosidad', mes::varchar from {{ ref('stg_morosidad') }} where total not between 0 and 20
