
import pandas as pd
from pathlib import Path
from cassandra.cluster import Cluster

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"

print("Leyendo compras...")
df = pd.read_csv(
    ARCHIVO,
    dtype={
        "CustomerID": str,
        "StockCode": str,
        "InvoiceNo": str
    }
)

# Agrupar compras por cliente
perfiles = df.groupby("CustomerID").agg(
    pais=("Country", "first"),
    total_compras=("InvoiceNo", "nunique"),
    total_gastado=("Total", "sum")
).reset_index()

print("Perfiles encontrados:", len(perfiles))

# Conectar con Cassandra
cluster = Cluster(["127.0.0.1"], port=9042)
session = cluster.connect("reto2_retail")

consulta = session.prepare("""
    INSERT INTO perfiles_clientes (
        cliente_id,
        pais,
        total_compras,
        total_gastado
    ) VALUES (?, ?, ?, ?)
""")

contador = 0

try:
    for fila in perfiles.itertuples(index=False):
        session.execute(consulta, (
            str(fila.CustomerID),
            str(fila.pais),
            int(fila.total_compras),
            float(fila.total_gastado)
        ))

        contador += 1

        if contador % 500 == 0:
            print(f"Perfiles guardados: {contador}")

finally:
    cluster.shutdown()

print("\nCARGA FINALIZADA")
print("Total de perfiles:", contador)
