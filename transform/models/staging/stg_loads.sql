select fuente, cast(cargado_en as timestamp) as cargado_en, estado, filas,
       cast(ultima_fecha as date) as ultima_fecha, revisiones, error
from {{ source('raw', '_loads') }}
