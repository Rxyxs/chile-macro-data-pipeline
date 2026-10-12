select cast(fecha as date) as fecha, cast(valor as double) as dolar
from {{ source('raw', 'dolar') }}
where valor is not null
