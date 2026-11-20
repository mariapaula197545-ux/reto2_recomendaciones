
import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    "compras-retail",
    bootstrap_servers=["localhost:29092"],
    auto_offset_reset="earliest",
    group_id="reto2-demo-consumidor",
    enable_auto_commit=False,
    value_deserializer=lambda m: json.loads(
        m.decode("utf-8")
    )
)

print("Escuchando compras de Kafka...")
print("Presiona Ctrl+C para detener.\n")

try:
    for mensaje in consumer:
        evento = mensaje.value

        print("-----------------------------")
        print("Particion:", mensaje.partition)
        print("Offset:", mensaje.offset)
        print("Cliente:", evento["cliente_id"])
        print("Producto:", evento["producto"])
        print("Cantidad:", evento["cantidad"])
        print("Precio:", evento["precio_unitario"])
        print("Total:", evento["total"])

except KeyboardInterrupt:
    print("\nConsumidor detenido.")

finally:
    consumer.close()
