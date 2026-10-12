select cast(fecha as date) as fecha, cast(valor as double) as uf
from {{ source('raw', 'uf') }}
where valor is not null
