select cast(fecha as date) as fecha, cast(valor as double) as desempleo
from {{ source('raw', 'desempleo') }}
where valor is not null
