
import csv
import statistics
from pathlib import Path
from collections import defaultdict

BASE = Path(__file__).resolve().parent.parent

ARCHIVO_SPARK = BASE / "spark/resultados/recomendaciones.csv"
ARCHIVO_NEO4J = BASE / "neo4j/resultados/recomendaciones_100_clientes.csv"

SALIDA = BASE / "comparacion/resultados"
SALIDA.mkdir(parents=True, exist_ok=True)

ARCHIVO_RESULTADOS = SALIDA / "comparacion_100_clientes.csv"
ARCHIVO_RESUMEN = SALIDA / "resumen_100_clientes.txt"


def cargar_recomendaciones(ruta):
    recomendaciones = defaultdict(list)

    with open(ruta, newline="", encoding="utf-8-sig") as archivo:
        lector = csv.DictReader(archivo)

        for fila in lector:
            cliente = fila["cliente"].strip()
            producto = fila["recomendado"].strip()
            posicion = int(fila["posicion"])

            if cliente and producto and posicion <= 5:
                recomendaciones[cliente].append((posicion, producto))

    resultado = {}

    for cliente, productos in recomendaciones.items():
        ordenados = sorted(productos)

        # Evitar productos duplicados
        unicos = list(dict.fromkeys(
            producto for _, producto in ordenados
        ))

        resultado[cliente] = unicos[:5]

    return resultado


spark = cargar_recomendaciones(ARCHIVO_SPARK)
neo4j = cargar_recomendaciones(ARCHIVO_NEO4J)

clientes_comunes = sorted(
    set(spark.keys()) & set(neo4j.keys())
)

resultados = []

for cliente in clientes_comunes:
    productos_spark = set(spark[cliente])
    productos_neo4j = set(neo4j[cliente])

    # Evaluar solo listas completas de 5 productos
    if len(productos_spark) != 5 or len(productos_neo4j) != 5:
        continue

    comunes = productos_spark & productos_neo4j
    union = productos_spark | productos_neo4j

    coincidencias = len(comunes)
    porcentaje = (coincidencias / 5) * 100
    jaccard = len(comunes) / len(union)

    resultados.append({
        "cliente": cliente,
        "recomendaciones_spark": len(productos_spark),
        "recomendaciones_neo4j": len(productos_neo4j),
        "productos_comunes": coincidencias,
        "coincidencia_porcentaje": round(porcentaje, 2),
        "jaccard": round(jaccard, 4),
        "productos_coincidentes": ", ".join(sorted(comunes))
    })


columnas = [
    "cliente",
    "recomendaciones_spark",
    "recomendaciones_neo4j",
    "productos_comunes",
    "coincidencia_porcentaje",
    "jaccard",
    "productos_coincidentes"
]

with open(ARCHIVO_RESULTADOS, "w", newline="", encoding="utf-8") as archivo:
    escritor = csv.DictWriter(archivo, fieldnames=columnas)
    escritor.writeheader()
    escritor.writerows(resultados)

if resultados:
    promedio_coincidencia = statistics.mean(
        fila["coincidencia_porcentaje"] for fila in resultados
    )

    promedio_jaccard = statistics.mean(
        fila["jaccard"] for fila in resultados
    )

    clientes_con_coincidencias = sum(
        fila["productos_comunes"] > 0 for fila in resultados
    )
else:
    promedio_coincidencia = 0
    promedio_jaccard = 0
    clientes_con_coincidencias = 0

resumen = f"""
========================================
 RETO 2 - COMPARACION DE 100 CLIENTES
========================================

Clientes disponibles en Spark: {len(spark)}
Clientes disponibles en Neo4j: {len(neo4j)}
Clientes con Top 5 completo en ambos: {len(resultados)}

Clientes con al menos una coincidencia: {clientes_con_coincidencias}

PROMEDIO COINCIDENCIA TOP 5: {promedio_coincidencia:.2f}%
PROMEDIO JACCARD: {promedio_jaccard:.4f}

Archivo de resultados: {ARCHIVO_RESULTADOS}
"""

print(resumen)

with open(ARCHIVO_RESUMEN, "w", encoding="utf-8") as archivo:
    archivo.write(resumen)

print("Comparacion finalizada correctamente.")
