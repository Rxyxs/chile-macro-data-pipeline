select fuente, clave, antes, ahora, ahora - antes as cambio, detectado_en
from {{ ref('stg_revisions') }}
order by detectado_en desc, fuente, clave
