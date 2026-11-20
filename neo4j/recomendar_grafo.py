
import os
import csv
import time
from pathlib import Path
from neo4j import GraphDatabase

# ==========================================
# CONFIGURACIÓN
# ==========================================

BASE = Path(__file__).resolve().parent.parent

CARPETA_SALIDA = BASE / "neo4j" / "resultados"
CARPETA_SALIDA.mkdir(parents=True, exist_ok=True)

ARCHIVO_SALIDA = (
    CARPETA_SALIDA / "recomendaciones_neo4j.csv"
)

URI = "bolt://localhost:7687"
USUARIO = "neo4j"
PASSWORD = os.getenv(
    "NEO4J_PASSWORD",
    "Reto2Clave2026"
)

CLIENTE = "17850"

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

print("====================================")
print(" RETO 2 - RECOMENDACIONES NEO4J")
print("====================================")

driver = GraphDatabase.driver(
    URI,
    auth=(USUARIO, PASSWORD)
)

try:
    driver.verify_connectivity()
    print("\nConectado a Neo4j")

    inicio = time.perf_counter()

    with driver.session() as session:
        resultado = session.run(
            CONSULTA,
            cliente=CLIENTE
        )

        recomendaciones = [
            dict(registro)
            for registro in resultado
        ]

    duracion = time.perf_counter() - inicio

    print("\nRECOMENDACIONES CLIENTE", CLIENTE)

    if recomendaciones:
        for posicion, fila in enumerate(
            recomendaciones,
            start=1
        ):
            print(
                f"{posicion}. "
                f"Producto: {fila['producto']} | "
                f"Nombre: {fila['nombre']} | "
                f"Puntaje: {fila['puntaje']}"
            )
    else:
        print("No se encontraron recomendaciones.")

    # Guardar CSV
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

        for posicion, fila in enumerate(
            recomendaciones,
            start=1
        ):
            escritor.writerow([
                CLIENTE,
                fila["producto"],
                fila["puntaje"],
                posicion,
                fila["nombre"],
                fila["clientes_similares"]
            ])

    print("\nRESULTADOS GUARDADOS")
    print("Archivo:", ARCHIVO_SALIDA)
    print(f"Tiempo de consulta: {duracion:.3f} segundos")

finally:
    driver.close()
    print("\nConexión cerrada.")
