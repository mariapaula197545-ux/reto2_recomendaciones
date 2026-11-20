
import json
from uuid import UUID
from datetime import datetime

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

# Conectar con Cassandra
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('reto2_retail')

# Preparar inserción de navegación por cliente
por_cliente = session.prepare("""
INSERT INTO navegacion_por_cliente
(cliente_id, fecha_evento, evento_id,
 sesion_id, tipo_evento, producto_id)
VALUES (?, ?, ?, ?, ?, ?)
""")

# Preparar inserción de eventos por sesión
por_sesion = session.prepare("""
INSERT INTO eventos_por_sesion
(sesion_id, fecha_evento, evento_id,
 cliente_id, tipo_evento, producto_id)
VALUES (?, ?, ?, ?, ?, ?)
""")

# Crear sesión si todavía no existe
sesion_actual = session.prepare("""
INSERT INTO sesiones_por_id
(sesion_id, cliente_id, inicio,
 ultima_actividad, estado)
VALUES (?, ?, ?, ?, ?)
IF NOT EXISTS
""")

# Actualizar la última actividad
actualizar = session.prepare("""
UPDATE sesiones_por_id
SET ultima_actividad = ?, estado = ?
WHERE sesion_id = ?
""")

# Escuchar los eventos de Kafka
consumer = KafkaConsumer(
    'navegacion-retail',
    bootstrap_servers=['localhost:29092'],
    group_id='reto2-navegacion-v1',
    auto_offset_reset='earliest',
    enable_auto_commit=False,
    value_deserializer=lambda b: json.loads(
        b.decode('utf-8')
    )
)

print('Esperando eventos de navegación...')

try:
    for msg in consumer:
        evento = msg.value

        fecha = datetime.fromisoformat(
            evento['fecha_evento']
        )

        evento_id = UUID(evento['evento_id'])

        # Guardar actividad del cliente
        session.execute(por_cliente, (
            evento['cliente_id'],
            fecha,
            evento_id,
            evento['sesion_id'],
            evento['tipo_evento'],
            evento['producto_id']
        ))

        # Guardar actividad de la sesión
        session.execute(por_sesion, (
            evento['sesion_id'],
            fecha,
            evento_id,
            evento['cliente_id'],
            evento['tipo_evento'],
            evento['producto_id']
        ))

        # Registrar sesión
        session.execute(sesion_actual, (
            evento['sesion_id'],
            evento['cliente_id'],
            fecha,
            fecha,
            'ACTIVA'
        ))

        # Actualizar última actividad
        session.execute(actualizar, (
            fecha,
            'ACTIVA',
            evento['sesion_id']
        ))

        # Confirmar que se procesó el mensaje
        consumer.commit()

        print(
            'Guardado:',
            evento['tipo_evento'],
            evento['cliente_id']
        )

except KeyboardInterrupt:
    print('Consumidor detenido')

finally:
    consumer.close()
    cluster.shutdown()
