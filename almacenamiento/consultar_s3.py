
import boto3
import pandas as pd
from io import BytesIO

# Conectarse a Amazon S3
sesion = boto3.Session(
    profile_name="reto2",
    region_name="us-east-1"
)

s3 = sesion.client("s3")

# Bucket donde está nuestra base de datos
BUCKET = "reto2-retail-maria-2026-168197327138-us-east-1-an"
ARCHIVO = "datos/compras_limpias.csv"

print("Conectando con Amazon S3...")

# Leer el archivo directamente desde S3
respuesta = s3.get_object(
    Bucket=BUCKET,
    Key=ARCHIVO
)

# Convertir los datos en una tabla de Pandas
df = pd.read_csv(BytesIO(respuesta["Body"].read()))

print("\nBASE DE DATOS CARGADA DESDE AMAZON S3")
print("Total de registros:", len(df))

# CONSULTA 1: Mostrar los primeros registros
print("\n1. PRIMERAS CINCO COMPRAS")
print(df.head().to_string(index=False))

# CONSULTA 2: Compras del cliente 17850
print("\n2. COMPRAS DEL CLIENTE 17850")

clientes = df["CustomerID"].astype(str).str.replace(
    r"\.0$", "", regex=True
)

compras_cliente = df[clientes == "17850"]

print(compras_cliente.head(10).to_string(index=False))
print("Total de registros:", len(compras_cliente))

# CONSULTA 3: Productos más vendidos
print("\n3. CINCO PRODUCTOS MÁS VENDIDOS")

productos = (
    df.groupby("StockCode")["Quantity"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
)

print(productos)

# CONSULTA 4: Ventas por país
print("\n4. VENTAS POR PAÍS")

df["TotalVenta"] = df["Quantity"] * df["UnitPrice"]

ventas_pais = (
    df.groupby("Country")["TotalVenta"]
    .sum()
    .sort_values(ascending=False)
    .head(5)
)

print(ventas_pais)

print("\nCONSULTAS FINALIZADAS CORRECTAMENTE")
