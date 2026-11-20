
import csv
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"

parser = argparse.ArgumentParser()
parser.add_argument("--limite", type=int, default=100)
parser.add_argument("--pausa", type=float, default=0.1)
args = parser.parse_args()

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False
    ).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5
)

enviados = 0
inicio = time.perf_counter()

try:
    with open(ARCHIVO, encoding="utf-8", newline="") as f:
        lector = csv.DictReader(f)

        for fila in lector:
            evento = {
                "tipo_evento": "COMPRA",
                "factura": fila["InvoiceNo"],
                "cliente_id": fila["CustomerID"],
                "producto_id": fila["StockCode"],
                "producto": fila["Description"],
                "cantidad": int(float(fila["Quantity"])),
                "precio_unitario": float(fila["UnitPrice"]),
                "total": float(fila["Total"]),
                "fecha_compra": fila["InvoiceDate"],
                "pais": fila["Country"],
                "fecha_publicacion": datetime.now(
                    timezone.utc
                ).isoformat()
            }

            producer.send(
                "compras-retail",
                key=evento["cliente_id"],
                value=evento
            )

            enviados += 1

            if enviados <= 10 or enviados % 1000 == 0:
                print(
                    f"Enviado {enviados}: "
                    f"Cliente={evento['cliente_id']} "
                    f"Producto={evento['producto_id']}"
                )

            if args.pausa > 0:
                time.sleep(args.pausa)

            if args.limite > 0 and enviados >= args.limite:
                break

    producer.flush(timeout=120)

finally:
    producer.close()

duracion = time.perf_counter() - inicio

print("\nENVIO FINALIZADO")
print("Eventos enviados:", enviados)
print("Tiempo:", round(duracion, 2), "segundos")
