select cast(fecha as date) as fecha, fondo, cast(indice as double) as indice_real
from {{ source('raw', 'fondos') }}
where indice is not null
