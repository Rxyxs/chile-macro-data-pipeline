select cast(fecha as date) as fecha, cast(valor as double) as cobre
from {{ source('raw', 'cobre') }}
where valor is not null
