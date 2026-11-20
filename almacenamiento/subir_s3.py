
from pathlib import Path
import boto3
from botocore.exceptions import BotoCoreError, ClientError

# Carpeta principal del proyecto
BASE = Path(__file__).resolve().parent.parent

# Configuración de Amazon S3
BUCKET = "reto2-retail-maria-2026-168197327138-us-east-1-an"

sesion = boto3.Session(
    profile_name="reto2",
    region_name="us-east-1"
)

s3 = sesion.client("s3")

# Archivo local -> ubicación en Amazon S3
archivos = {
    "datos/compras_limpias.csv":
        "datos/compras_limpias.csv",

    "spark/resultados/recomendaciones.csv":
        "spark/recomendaciones.csv",

    "neo4j/resultados/recomendaciones_neo4j.csv":
        "neo4j/recomendaciones_neo4j.csv",

    "comparacion/resultados/comparacion_modelos.csv":
        "comparacion/comparacion_modelos.csv",

    "comparacion/resultados/resumen_comparacion.txt":
        "comparacion/resumen_comparacion.txt"
}

print("\n======================================")
print(" RETO 2 - SUBIDA A AMAZON S3")
print("======================================")

correctos = 0
errores = 0

for ruta_local, ruta_s3 in archivos.items():

    archivo = BASE / ruta_local

    if not archivo.is_file():
        print(f"Archivo no encontrado: {archivo}")
        errores += 1
        continue

    try:
        print(f"\nSubiendo: {ruta_local}")

        s3.upload_file(
            str(archivo),
            BUCKET,
            ruta_s3
        )

        print(f"OK: s3://{BUCKET}/{ruta_s3}")
        correctos += 1

    except (ClientError, BotoCoreError, OSError) as error:
        print(f"ERROR: {error}")
        errores += 1

print("\n======================================")
print(" RESUMEN DE LA CARGA")
print("======================================")
print(f"Archivos subidos: {correctos}")
print(f"Errores: {errores}")
print(f"Bucket: {BUCKET}")
