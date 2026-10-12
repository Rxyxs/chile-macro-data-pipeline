-- The 2023-07 system row is published as '---' by the CMF; it stays null, not filled.
select cast(fecha as date) as mes, total, comercial, personas, consumo, vivienda, monto_moroso_mm
from {{ source('raw', 'morosidad') }}
