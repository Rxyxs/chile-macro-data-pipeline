select fuente, clave, antes, ahora, cast(detectado_en as timestamp) as detectado_en
from {{ source('raw', '_revisions') }}
