
import json
import uuid
from datetime import datetime

from kafka import KafkaConsumer
from cassandra.cluster import Cluster

print("Conectando con Cassandra...")

cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("reto2_retail")

# Tabla 1: detalle de compras
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

# Tabla 2: eventos de compras
insertar_evento = session.prepare("""
    INSERT INTO eventos_por_cliente (
        cliente_id,
        fecha_evento,
        evento_id,
        tipo_evento,
        producto_id
    ) VALUES (?, ?, ?, ?, ?)
""")

# Tabla 3: compras agrupadas por cliente
insertar_compra_cliente = session.prepare("""
    INSERT INTO compras_por_cliente (
        cliente_id,
        fecha_compra,
        factura,
        producto_id,
        producto,
        cantidad,
        precio_unitario,
        total
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
""")

consumer = KafkaConsumer(
    "compras-retail",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    group_id="reto2-cassandra-v2",
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

        id_estable = uuid.uuid5(
            uuid.NAMESPACE_URL,
            f"compras-retail:{mensaje.partition}:{mensaje.offset}"
        )

        fecha = datetime.fromisoformat(
            evento["fecha_compra"]
        )

        cliente = str(evento["cliente_id"])
        factura = str(evento["factura"])
        producto_id = str(evento["producto_id"])
        producto = str(evento["producto"])
        cantidad = int(evento["cantidad"])
        precio = float(evento["precio_unitario"])
        total = float(evento["total"])

        # 1. Guardar detalle de compra
        session.execute(insertar_compra, (
            cliente,
            fecha,
            str(id_estable),
            factura,
            producto_id,
            producto,
            cantidad,
            precio,
            total
        ))

        # 2. Guardar evento
        session.execute(insertar_evento, (
            cliente,
            fecha,
            id_estable,
            str(evento["tipo_evento"]),
            producto_id
        ))

        # 3. Guardar compra por cliente
        session.execute(insertar_compra_cliente, (
            cliente,
            fecha,
            factura,
            producto_id,
            producto,
            cantidad,
            precio,
            total
        ))

        # Confirmar el mensaje después de
        # completar las tres escrituras.
        consumer.commit()

        contador += 1

        if contador <= 10 or contador % 1000 == 0:
            print(
                f"Compra guardada #{contador} | "
                f"Cliente: {cliente} | "
                f"Producto: {producto_id}"
            )

except KeyboardInterrupt:
    print("\nConsumidor detenido.")

finally:
    consumer.close()
    cluster.shutdown()
    print("Compras procesadas:", contador)
