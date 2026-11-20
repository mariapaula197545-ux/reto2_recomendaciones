
import csv
import json
import time
import argparse
from pathlib import Path
from datetime import datetime, timezone
from kafka import KafkaProducer

# ======================================
# CONFIGURACION DEL PROYECTO
# ======================================

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"

# ======================================
# ARGUMENTOS
# ======================================

parser = argparse.ArgumentParser()

parser.add_argument(
    "--limite",
    type=int,
    default=1000,
    help="Cantidad de compras a enviar. 0 = todas"
)

parser.add_argument(
    "--pausa",
    type=float,
    default=0.01,
    help="Tiempo de espera entre mensajes"
)

args = parser.parse_args()

if args.limite < 0:
    parser.error("--limite no puede ser negativo")

if args.pausa < 0:
    parser.error("--pausa no puede ser negativa")

print("================================")
print(" PRODUCTOR DE COMPRAS - KAFKA")
print("================================")
print("Archivo:", ARCHIVO)
print("Limite:", args.limite if args.limite else "Sin limite")
print("Pausa:", args.pausa, "segundos")

# ======================================
# VERIFICAR ARCHIVO
# ======================================

if not ARCHIVO.exists():
    parser.error(
        f"No se encontro el archivo: {ARCHIVO}"
    )

# ======================================
# CONEXION A KAFKA
# ======================================

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    value_serializer=lambda v: json.dumps(
        v, ensure_ascii=False
    ).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8"),
    acks="all",
    retries=5
)

# ======================================
# ENVIO DE COMPRAS
# ======================================

enviados = 0
inicio = time.perf_counter()

try:
    with open(
        ARCHIVO,
        encoding="utf-8",
        newline=""
    ) as f:

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

            # Enviar evento y esperar confirmacion
            producer.send(
                "compras-retail",
                key=evento["cliente_id"],
                value=evento
            ).get(timeout=30)

            # Contar cada compra una sola vez
            enviados += 1

            print(
                f"[{enviados}] COMPRA ENVIADA | "
                f"Cliente: {evento['cliente_id']} | "
                f"Producto: {evento['producto_id']} | "
                f"Total: {evento['total']:.2f}"
            )

            if args.pausa > 0:
                time.sleep(args.pausa)

            if args.limite > 0 and enviados >= args.limite:
                break

    # Confirmar envios pendientes
    producer.flush(timeout=120)

except KeyboardInterrupt:
    print("\nEnvio detenido por el usuario.")

except Exception as e:
    print("\nERROR DURANTE EL ENVIO:", e)

finally:
    producer.close()

# ======================================
# RESUMEN FINAL
# ======================================

duracion = time.perf_counter() - inicio

print("\n================================")
print(" RESUMEN DEL PRODUCTOR")
print("================================")
print("Compras enviadas:", enviados)
print("Tiempo:", round(duracion, 2), "segundos")
print("================================")
