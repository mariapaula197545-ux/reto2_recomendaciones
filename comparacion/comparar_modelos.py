
import csv
from pathlib import Path

# ==========================================
# RUTAS DEL PROYECTO
# ==========================================

BASE = Path(__file__).resolve().parent.parent

ARCHIVO_SPARK = (
    BASE / "spark" / "resultados" /
    "recomendaciones.csv"
)

ARCHIVO_NEO4J = (
    BASE / "neo4j" / "resultados" /
    "recomendaciones_neo4j.csv"
)

SALIDA = BASE / "comparacion" / "resultados"
SALIDA.mkdir(parents=True, exist_ok=True)

ARCHIVO_COMPARACION = (
    SALIDA / "comparacion_modelos.csv"
)

CLIENTE = "17850"


# ==========================================
# 1. LEER RECOMENDACIONES DE SPARK
# ==========================================

def leer_recomendaciones(ruta, cliente):

    recomendaciones = []

    with open(
        ruta,
        "r",
        encoding="utf-8-sig",
        newline=""
    ) as archivo:

        lector = csv.DictReader(archivo)

        for fila in lector:

            if fila["cliente"] == cliente:
                recomendaciones.append({
                    "producto": fila["recomendado"],
                    "puntaje": float(fila["puntaje"]),
                    "posicion": int(fila["posicion"])
                })

    return sorted(
        recomendaciones,
        key=lambda x: x["posicion"]
    )


print("====================================")
print(" COMPARACIÓN SPARK VS NEO4J")
print("====================================")

spark = leer_recomendaciones(
    ARCHIVO_SPARK,
    CLIENTE
)

neo4j = leer_recomendaciones(
    ARCHIVO_NEO4J,
    CLIENTE
)

print("\nCliente analizado:", CLIENTE)

print("\nRECOMENDACIONES SPARK")
for fila in spark:
    print(
        fila["posicion"],
        fila["producto"],
        fila["puntaje"]
    )

print("\nRECOMENDACIONES NEO4J")
for fila in neo4j:
    print(
        fila["posicion"],
        fila["producto"],
        fila["puntaje"]
    )


# ==========================================
# 2. CALCULAR COINCIDENCIA
# ==========================================

productos_spark = {
    fila["producto"] for fila in spark
}

productos_neo4j = {
    fila["producto"] for fila in neo4j
}

coincidencias = (
    productos_spark & productos_neo4j
)

union = (
    productos_spark | productos_neo4j
)

total_coincidencias = len(coincidencias)

k = 5

coincidencia_top5 = (
    total_coincidencias / k * 100
)

jaccard = (
    len(coincidencias) / len(union)
    if union else 0
)

print("\n====================================")
print(" RESULTADOS DE LA COMPARACIÓN")
print("====================================")

print(
    "Productos en común:",
    total_coincidencias
)

print(
    "Coincidencia Top 5:",
    f"{coincidencia_top5:.2f}%"
)

print(
    "Similitud de Jaccard:",
    f"{jaccard:.4f}"
)

print(
    "Productos compartidos:",
    ", ".join(sorted(coincidencias))
    if coincidencias else "Ninguno"
)


# ==========================================
# 3. GUARDAR COMPARACIÓN EN CSV
# ==========================================

with open(
    ARCHIVO_COMPARACION,
    "w",
    newline="",
    encoding="utf-8"
) as archivo:

    escritor = csv.writer(archivo)

    escritor.writerow([
        "cliente",
        "algoritmo",
        "producto",
        "puntaje",
        "posicion",
        "coincide_con_otro_modelo"
    ])

    for fila in spark:
        escritor.writerow([
            CLIENTE,
            "Spark",
            fila["producto"],
            fila["puntaje"],
            fila["posicion"],
            fila["producto"] in coincidencias
        ])

    for fila in neo4j:
        escritor.writerow([
            CLIENTE,
            "Neo4j",
            fila["producto"],
            fila["puntaje"],
            fila["posicion"],
            fila["producto"] in coincidencias
        ])


# ==========================================
# 4. GUARDAR RESUMEN
# ==========================================

RUTA_RESUMEN = (
    SALIDA / "resumen_comparacion.txt"
)

with open(
    RUTA_RESUMEN,
    "w",
    encoding="utf-8"
) as archivo:

    archivo.write(
        "COMPARACIÓN DE RECOMENDACIONES\n"
    )

    archivo.write(
        f"Cliente: {CLIENTE}\n"
    )

    archivo.write(
        f"Recomendaciones Spark: {len(spark)}\n"
    )

    archivo.write(
        f"Recomendaciones Neo4j: {len(neo4j)}\n"
    )

    archivo.write(
        f"Productos coincidentes: "
        f"{total_coincidencias}\n"
    )

    archivo.write(
        f"Coincidencia Top 5: "
        f"{coincidencia_top5:.2f}%\n"
    )

    archivo.write(
        f"Similitud Jaccard: "
        f"{jaccard:.4f}\n"
    )

    archivo.write(
        "Nota: la coincidencia no mide "
        "precision ni relevancia real.\n"
    )

print("\nARCHIVOS GENERADOS:")
print(ARCHIVO_COMPARACION)
print(RUTA_RESUMEN)

print("\nCOMPARACIÓN FINALIZADA CORRECTAMENTE")
