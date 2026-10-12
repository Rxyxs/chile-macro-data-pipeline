-- Health of each source as of the latest run: did it load, how old is its data, how many
-- of its past values have been revised since the pipeline started watching. A source is
-- stale when its newest value is older than its publication rhythm allows: a week for
-- daily series, about two and a half months for monthly ones (the CPI comes out on the
-- 8th of the next month), four months for the CMF files.
with limite(fuente, dias_max) as (
    values ('uf', 7), ('dolar', 7), ('cobre', 7), ('fondos', 7), ('tpm', 75), ('ipc', 75),
           ('imacec', 75), ('desempleo', 105), ('morosidad', 125)
), ultima as (
    select * from {{ ref('stg_loads') }}
    qualify row_number() over (partition by fuente order by cargado_en desc) = 1
), ultima_ok as (
    select fuente, max(cargado_en) as ultima_carga_ok, arg_max(ultima_fecha, cargado_en) as ultimo_dato,
           arg_max(filas, cargado_en) as filas
    from {{ ref('stg_loads') }} where estado = 'ok' group by 1
), rev as (
    select fuente, count(*) as revisiones_totales, max(detectado_en) as ultima_revision
    from {{ ref('stg_revisions') }} group by 1
)
select u.fuente, u.cargado_en as ultima_ejecucion, u.estado as estado_ultima_ejecucion, u.error,
       o.ultima_carga_ok, o.ultimo_dato, o.filas,
       date_diff('day', o.ultimo_dato, cast(u.cargado_en as date)) as dias_desde_ultimo_dato,
       coalesce(date_diff('day', o.ultimo_dato, cast(u.cargado_en as date)) > l.dias_max, true)
           as desactualizada,
       coalesce(r.revisiones_totales, 0) as revisiones_totales, r.ultima_revision
from ultima u
left join limite l using (fuente)
left join ultima_ok o using (fuente)
left join rev r using (fuente)
order by u.fuente
