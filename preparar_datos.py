
import pandas as pd
import zipfile
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATOS = BASE / "datos"

ZIP = DATOS / "online+retail.zip"
EXCEL = DATOS / "Online Retail.xlsx"

CSV_COMPLETO = DATOS / "compras_completas.csv"
CSV_LIMPIO = DATOS / "compras_limpias.csv"

# ======================================
# 1. EXTRAER ARCHIVO
# ======================================

if not EXCEL.exists():
    with zipfile.ZipFile(ZIP, "r") as archivo:
        archivo.extract("Online Retail.xlsx", DATOS)

# ======================================
# 2. LEER DATASET ORIGINAL
# ======================================

print("Leyendo base de datos...")

df = pd.read_excel(EXCEL, engine="openpyxl")

print("Registros originales:", len(df))
print("Columnas:", df.columns.tolist())

# ======================================
# 3. DATASET COMPLETO
# ======================================

# Conservar todas las transacciones originales.
# Incluye registros sin cliente identificado,
# devoluciones y posibles duplicados.

completo = df.copy()

completo["StockCode"] = (
    completo["StockCode"].astype("string")
)

completo["InvoiceNo"] = (
    completo["InvoiceNo"].astype("string")
)

completo["Total"] = (
    completo["Quantity"] *
    completo["UnitPrice"]
)

completo.to_csv(
    CSV_COMPLETO,
    index=False,
    encoding="utf-8"
)

print("\nDATASET COMPLETO")
print("Registros:", len(completo))
print("Archivo:", CSV_COMPLETO)

# ======================================
# 4. DATASET PARA RECOMENDACIONES
# ======================================

limpio = df.dropna(
    subset=["CustomerID"]
).copy()

limpio = limpio[
    (limpio["Quantity"] > 0) &
    (limpio["UnitPrice"] > 0)
].copy()

limpio = limpio[
    ~limpio["InvoiceNo"]
    .astype(str)
    .str.startswith("C")
].copy()

limpio = limpio.drop_duplicates()

limpio["CustomerID"] = (
    limpio["CustomerID"]
    .astype(int)
    .astype(str)
)

limpio["StockCode"] = (
    limpio["StockCode"].astype(str)
)

limpio["InvoiceNo"] = (
    limpio["InvoiceNo"].astype(str)
)

limpio["Total"] = (
    limpio["Quantity"] *
    limpio["UnitPrice"]
)

limpio.to_csv(
    CSV_LIMPIO,
    index=False,
    encoding="utf-8"
)

# ======================================
# 5. RESULTADOS
# ======================================

print("\n======================================")
print(" RETO 2 - PREPARACION DE DATOS")
print("======================================")

print("Registros originales:", len(df))
print("Registros completos:", len(completo))
print("Registros para recomendaciones:", len(limpio))

print(
    "Clientes únicos:",
    limpio["CustomerID"].nunique()
)

print(
    "Productos únicos:",
    limpio["StockCode"].nunique()
)

print("\nArchivos generados:")
print(CSV_COMPLETO)
print(CSV_LIMPIO)

print("\nPREPARACION FINALIZADA")
