
import json
import uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

# Conexión con Kafka
producer = KafkaProducer(
    bootstrap_servers=['localhost:29092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    key_serializer=lambda k: k.encode('utf-8'),
    acks='all'
)

# Cliente que vamos a simular
cliente_id = '17850'

# Identificador único de la sesión
sesion_id = str(uuid.uuid4())

# Actividades simuladas del cliente
eventos = [
    ('VISITA_PRODUCTO', '22423'),
    ('BUSQUEDA', '85099B'),
    ('AGREGAR_CARRITO', '47566')
]

for tipo, producto in eventos:

    evento = {
        'evento_id': str(uuid.uuid4()),
        'cliente_id': cliente_id,
        'sesion_id': sesion_id,
        'tipo_evento': tipo,
        'producto_id': producto,
        'fecha_evento': datetime.now(timezone.utc).isoformat()
    }

    # Publicar evento en Kafka
    producer.send(
        'navegacion-retail',
        key=cliente_id,
        value=evento
    ).get(timeout=30)

    print('Publicado:', evento)

producer.flush()
producer.close()
