select cast(fecha as date) as fecha, cast(valor as double) as ipc
from {{ source('raw', 'ipc') }}
where valor is not null
