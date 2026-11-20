
import os
import pandas as pd
from pathlib import Path
from neo4j import GraphDatabase

# =====================================
# CONFIGURACIÓN
# =====================================

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"

URI = "bolt://localhost:7687"
USUARIO = "neo4j"
PASSWORD = os.getenv(
    "NEO4J_PASSWORD",
    "Reto2Clave2026"
)

TAMANO_LOTE = 1000

print("====================================")
print(" RETO 2 - CARGA DEL GRAFO EN NEO4J")
print("====================================")

# =====================================
# 1. LEER DATOS
# =====================================

print("\nLeyendo compras...")

df = pd.read_csv(
    ARCHIVO,
    dtype={
        "CustomerID": str,
        "StockCode": str
    }
)

print("Registros leídos:", len(df))

# Una relación por cliente-producto.
# Se acumula la cantidad de artículos.
relaciones = (
    df.groupby(
        ["CustomerID", "StockCode"],
        as_index=False
    )
    .agg(
        cantidad=("Quantity", "sum"),
        veces=("InvoiceNo", "nunique"),
        total_gastado=("Total", "sum"),
        producto=("Description", "first")
    )
)

print("Relaciones únicas:", len(relaciones))

# =====================================
# 2. CONECTAR NEO4J
# =====================================

driver = GraphDatabase.driver(
    URI,
    auth=(USUARIO, PASSWORD)
)

driver.verify_connectivity()

print("Conectado correctamente a Neo4j")

# =====================================
# 3. CREAR ÍNDICES Y RESTRICCIONES
# =====================================

with driver.session() as session:

    session.run("""
        CREATE CONSTRAINT cliente_id_unico
        IF NOT EXISTS
        FOR (c:Cliente)
        REQUIRE c.id IS UNIQUE
    """).consume()

    session.run("""
        CREATE CONSTRAINT producto_id_unico
        IF NOT EXISTS
        FOR (p:Producto)
        REQUIRE p.id IS UNIQUE
    """).consume()

print("Restricciones creadas")

# =====================================
# 4. CARGAR RELACIONES POR LOTES
# =====================================

def cargar_lote(tx, lote):
    tx.run("""
        UNWIND $filas AS fila

        MERGE (c:Cliente {id: fila.cliente})

        MERGE (p:Producto {id: fila.producto_id})
        SET p.nombre = fila.nombre

        MERGE (c)-[r:COMPRO]->(p)
        SET
            r.cantidad = fila.cantidad,
            r.veces = fila.veces,
            r.total_gastado = fila.total_gastado
    """, filas=lote).consume()


total = len(relaciones)

try:
    with driver.session() as session:

        for inicio in range(0, total, TAMANO_LOTE):

            parte = relaciones.iloc[
                inicio:inicio + TAMANO_LOTE
            ]

            lote = []

            for fila in parte.itertuples(index=False):

                lote.append({
                    "cliente": str(fila.CustomerID),
                    "producto_id": str(fila.StockCode),
                    "nombre": str(fila.producto),
                    "cantidad": int(fila.cantidad),
                    "veces": int(fila.veces),
                    "total_gastado": float(
                        fila.total_gastado
                    )
                })

            session.execute_write(
                cargar_lote,
                lote
            )

            procesados = min(
                inicio + TAMANO_LOTE,
                total
            )

            print(
                f"Relaciones cargadas: "
                f"{procesados}/{total}"
            )

    print("\n====================================")
    print(" GRAFO CARGADO CORRECTAMENTE")
    print("====================================")

finally:
    driver.close()
