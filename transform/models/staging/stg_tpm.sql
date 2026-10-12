select cast(fecha as date) as fecha, cast(valor as double) as tpm
from {{ source('raw', 'tpm') }}
where valor is not null
