
import os
import csv
import time
from pathlib import Path
from neo4j import GraphDatabase

BASE = Path(__file__).resolve().parent.parent

ARCHIVO_SPARK = BASE / "spark" / "resultados" / "recomendaciones.csv"
CARPETA_SALIDA = BASE / "neo4j" / "resultados"
CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

ARCHIVO_SALIDA = CARPETA_SALIDA / "recomendaciones_100_clientes.csv"

URI = "bolt://localhost:7687"
USUARIO = "neo4j"
PASSWORD = os.getenv("NEO4J_PASSWORD", "Reto2Clave2026")

CANTIDAD_CLIENTES = 100

# Utilizamos la misma consulta del programa original
CONSULTA = """
MATCH (c:Cliente {id: $cliente})-[:COMPRO]->(p:Producto)
MATCH (otro:Cliente)-[:COMPRO]->(p)
WHERE otro <> c

WITH c, otro, count(DISTINCT p) AS productos_comunes
ORDER BY productos_comunes DESC
LIMIT 50

MATCH (otro)-[:COMPRO]->(recomendado:Producto)
WHERE NOT EXISTS {
    MATCH (c)-[:COMPRO]->(recomendado)
}

RETURN
    recomendado.id AS producto,
    recomendado.nombre AS nombre,
    count(DISTINCT otro) AS clientes_similares,
    sum(productos_comunes) AS puntaje
ORDER BY puntaje DESC, producto ASC
LIMIT 5
"""

# Leer clientes de Spark, sin repetirlos
clientes = []
vistos = set()

with open(ARCHIVO_SPARK, newline="", encoding="utf-8-sig") as archivo:
    lector = csv.DictReader(archivo)

    for fila in lector:
        cliente = fila["cliente"].strip()

        if cliente and cliente not in vistos:
            vistos.add(cliente)
            clientes.append(cliente)

# Conservar el cliente de la evaluación anterior
if "17850" in vistos:
    clientes.remove("17850")
    clientes.insert(0, "17850")

print("========================================")
print(" RETO 2 - EVALUACION DE 100 CLIENTES")
print("========================================")

print("Clientes disponibles en Spark:", len(clientes))

driver = GraphDatabase.driver(
    URI,
    auth=(USUARIO, PASSWORD)
)

clientes_evaluados = 0
clientes_con_recomendaciones = 0
total_recomendaciones = 0

inicio = time.perf_counter()

try:
    driver.verify_connectivity()
    print("Conectado a Neo4j\n")

    with open(
        ARCHIVO_SALIDA,
        "w",
        newline="",
        encoding="utf-8"
    ) as archivo:

        escritor = csv.writer(archivo)

        escritor.writerow([
            "cliente",
            "recomendado",
            "puntaje",
            "posicion",
            "nombre",
            "clientes_similares"
        ])

        with driver.session() as session:

            # Se revisan clientes hasta obtener 100
            # con recomendaciones, si es posible.
            for cliente in clientes:

                resultado = session.run(
                    CONSULTA,
                    cliente=cliente
                )

                recomendaciones = [
                    dict(registro)
                    for registro in resultado
                ]

                clientes_evaluados += 1

                if not recomendaciones:
                    print(
                        f"Cliente {cliente}: "
                        "sin recomendaciones"
                    )
                    continue

                clientes_con_recomendaciones += 1

                for posicion, fila in enumerate(
                    recomendaciones,
                    start=1
                ):
                    escritor.writerow([
                        cliente,
                        fila["producto"],
                        fila["puntaje"],
                        posicion,
                        fila["nombre"],
                        fila["clientes_similares"]
                    ])

                    total_recomendaciones += 1

                print(
                    f"Cliente {cliente}: "
                    f"{len(recomendaciones)} recomendaciones "
                    f"({clientes_con_recomendaciones}/100)"
                )

                if clientes_con_recomendaciones >= CANTIDAD_CLIENTES:
                    break

finally:
    driver.close()

duracion = time.perf_counter() - inicio

print("\n========================================")
print(" RESULTADOS")
print("========================================")
print("Clientes consultados:", clientes_evaluados)
print("Clientes con recomendaciones:", clientes_con_recomendaciones)
print("Total de recomendaciones:", total_recomendaciones)
print(f"Tiempo: {duracion:.2f} segundos")
print("Archivo generado:", ARCHIVO_SALIDA)
