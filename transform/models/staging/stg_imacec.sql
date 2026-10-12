select cast(fecha as date) as fecha, cast(valor as double) as imacec
from {{ source('raw', 'imacec') }}
where valor is not null
