
import json
import uuid
from datetime import datetime
from kafka import KafkaConsumer
from cassandra.cluster import Cluster

print("Conectando con Cassandra...")

cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("reto2_retail")

insertar_compra = session.prepare("""
    INSERT INTO compras_detalle_por_cliente (
        cliente_id,
        fecha_compra,
        evento_id,
        factura,
        producto_id,
        producto,
        cantidad,
        precio_unitario,
        total
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
""")

insertar_evento = session.prepare("""
    INSERT INTO eventos_por_cliente (
        cliente_id,
        fecha_evento,
        evento_id,
        tipo_evento,
        producto_id
    ) VALUES (?, ?, ?, ?, ?)
""")

consumer = KafkaConsumer(
    "compras-retail",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    group_id="reto2-cassandra-v1",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(
        m.decode("utf-8")
    )
)

print("Kafka conectado.")
print("Esperando compras...\n")

contador = 0

try:
    for mensaje in consumer:
        evento = mensaje.value

        # Identificador estable para cada mensaje Kafka.
        # Evita duplicados al volver a procesar el mismo offset.
        id_estable = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"compras-retail:{mensaje.partition}:{mensaje.offset}"
        )

        fecha = datetime.fromisoformat(
            evento["fecha_compra"]
        )

        session.execute(insertar_compra, (
            str(evento["cliente_id"]),
            fecha,
            str(id_estable),
            str(evento["factura"]),
            str(evento["producto_id"]),
            str(evento["producto"]),
            int(evento["cantidad"]),
            float(evento["precio_unitario"]),
            float(evento["total"])
        ))

        session.execute(insertar_evento, (
            str(evento["cliente_id"]),
            fecha,
            id_estable,
            str(evento["tipo_evento"]),
            str(evento["producto_id"])
        ))

        # Confirmar el mensaje solo después de guardar
        # correctamente las dos filas.
        consumer.commit()

        contador += 1

        if contador <= 10 or contador % 1000 == 0:
            print(
                f"Compra guardada #{contador} | "
                f"Cliente: {evento['cliente_id']} | "
                f"Producto: {evento['producto_id']}"
            )

except KeyboardInterrupt:
    print("\nConsumidor detenido.")

finally:
    consumer.close()
    cluster.shutdown()
    print("Compras procesadas:", contador)
