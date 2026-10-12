-- mindicador publishes the CPI as a monthly % change and the IMACEC as a 12-month % change;
-- the policy rate (TPM) is a level, taken at the last observation of each month.
with ipc as (
    select date_trunc('month', fecha) as mes, ipc as ipc_mensual
    from {{ ref('stg_ipc') }}
), ipc12 as (
    select mes, ipc_mensual,
           case when count(*) over w = 12
                then (exp(sum(ln(1 + ipc_mensual / 100)) over w) - 1) * 100 end as ipc_12m
    from ipc
    window w as (order by mes rows between 11 preceding and current row)
), tpm as (
    select date_trunc('month', fecha) as mes, arg_max(tpm, fecha) as tpm
    from {{ ref('stg_tpm') }} group by 1
), imacec as (
    select date_trunc('month', fecha) as mes, imacec as imacec_12m from {{ ref('stg_imacec') }}
), desempleo as (
    select date_trunc('month', fecha) as mes, desempleo from {{ ref('stg_desempleo') }}
)
select cast(i.mes as date) as mes, i.ipc_mensual, i.ipc_12m, t.tpm,
       t.tpm - i.ipc_12m as tpm_real_ex_post, m.imacec_12m, d.desempleo
from ipc12 i
left join tpm t using (mes)
left join imacec m using (mes)
left join desempleo d using (mes)
order by mes
