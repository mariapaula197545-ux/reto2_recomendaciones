
import pandas as pd
import zipfile
from pathlib import Path

BASE = Path(__file__).resolve().parent
DATOS = BASE / "datos"

ZIP = DATOS / "online+retail.zip"
EXCEL = DATOS / "Online Retail.xlsx"
CSV = DATOS / "compras_limpias.csv"

# Extraer Excel
if not EXCEL.exists():
    with zipfile.ZipFile(ZIP, "r") as archivo:
        archivo.extract("Online Retail.xlsx", DATOS)

# Leer registros
print("Leyendo base de datos...")
df = pd.read_excel(EXCEL, engine="openpyxl")

print("Registros originales:", len(df))
print("Columnas:", df.columns.tolist())

# Limpiar compras
df = df.dropna(subset=["CustomerID"]).copy()
df = df[
    (df["Quantity"] > 0) &
    (df["UnitPrice"] > 0)
].copy()

df = df[
    ~df["InvoiceNo"].astype(str).str.startswith("C")
].copy()

df = df.drop_duplicates()

# Preparar identificadores
df["CustomerID"] = df["CustomerID"].astype(int).astype(str)
df["StockCode"] = df["StockCode"].astype(str)
df["InvoiceNo"] = df["InvoiceNo"].astype(str)

# Calcular total
df["Total"] = df["Quantity"] * df["UnitPrice"]

# Guardar CSV
df.to_csv(CSV, index=False, encoding="utf-8")

print("\nBASE DE DATOS PREPARADA")
print("Registros limpios:", len(df))
print("Clientes únicos:", df["CustomerID"].nunique())
print("Productos únicos:", df["StockCode"].nunique())
print("Archivo generado:", CSV)
