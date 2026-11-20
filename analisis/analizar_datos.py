
from pathlib import Path
import pandas as pd

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"
SALIDA = BASE / "analisis" / "resultados"

SALIDA.mkdir(parents=True, exist_ok=True)

print("======================================")
print(" RETO 2 - ANALISIS DE DATOS")
print("======================================")

df = pd.read_csv(ARCHIVO)

# Identificar columnas sin depender de mayúsculas
columnas = {c.lower().strip(): c for c in df.columns}

def columna(nombre):
    if nombre.lower() not in columnas:
        raise ValueError(
            f"No existe la columna '{nombre}'. "
            f"Columnas disponibles: {list(df.columns)}"
        )
    return columnas[nombre.lower()]

producto = columna("StockCode")
descripcion = columna("Description")
cantidad = columna("Quantity")
precio = columna("UnitPrice")
cliente = columna("CustomerID")
pais = columna("Country")
fecha = columna("InvoiceDate")

df[cantidad] = pd.to_numeric(df[cantidad], errors="coerce")
df[precio] = pd.to_numeric(df[precio], errors="coerce")
df[fecha] = pd.to_datetime(
    df[fecha],
    errors="coerce"
)

df["TotalVenta"] = df[cantidad] * df[precio]

# ======================================
# ANALISIS 1: PRODUCTOS MAS VENDIDOS
# ======================================

productos = (
    df.groupby([producto, descripcion], dropna=False)[cantidad]
    .sum()
    .reset_index()
    .sort_values(cantidad, ascending=False)
    .head(10)
)

productos.to_csv(
    SALIDA / "productos_mas_vendidos.csv",
    index=False
)

print("\n1. PRODUCTOS MAS VENDIDOS")
print(productos.to_string(index=False))

# ======================================
# ANALISIS 2: CLIENTES CON MAYOR GASTO
# ======================================

clientes = (
    df.groupby(cliente)["TotalVenta"]
    .sum()
    .reset_index()
    .sort_values("TotalVenta", ascending=False)
    .head(10)
)

clientes.to_csv(
    SALIDA / "clientes_mayor_gasto.csv",
    index=False
)

print("\n2. CLIENTES CON MAYOR GASTO")
print(clientes.to_string(index=False))

# ======================================
# ANALISIS 3: VENTAS POR PAIS
# ======================================

paises = (
    df.groupby(pais)["TotalVenta"]
    .sum()
    .reset_index()
    .sort_values("TotalVenta", ascending=False)
    .head(10)
)

paises.to_csv(
    SALIDA / "ventas_por_pais.csv",
    index=False
)

print("\n3. VENTAS POR PAIS")
print(paises.to_string(index=False))

# ======================================
# ANALISIS 4: VENTAS MENSUALES
# ======================================

df["Mes"] = df[fecha].dt.strftime("%Y-%m")

ventas_mes = (
    df.dropna(subset=["Mes"])
    .groupby("Mes")["TotalVenta"]
    .sum()
    .reset_index()
    .sort_values("Mes")
)

ventas_mes.to_csv(
    SALIDA / "ventas_mensuales.csv",
    index=False
)

print("\n4. VENTAS MENSUALES")
print(ventas_mes.to_string(index=False))

# ======================================
# ANALISIS 5: SPARK VS NEO4J
# ======================================

spark_path = (
    BASE / "spark" / "resultados" /
    "recomendaciones.csv"
)

neo4j_path = (
    BASE / "neo4j" / "resultados" /
    "recomendaciones_neo4j.csv"
)

spark = pd.read_csv(spark_path)
neo4j = pd.read_csv(neo4j_path)

print("\n5. COMPARACION SPARK VS NEO4J")
print("Recomendaciones Spark:", len(spark))
print("Recomendaciones Neo4j:", len(neo4j))

# Reutilizar la comparación calculada anteriormente
resumen = (
    BASE / "comparacion" / "resultados" /
    "resumen_comparacion.txt"
)

if resumen.exists():
    print("\nResumen de la comparación:")
    print(resumen.read_text(encoding="utf-8"))

print("\n======================================")
print(" ANALISIS FINALIZADOS")
print("======================================")
print("Resultados guardados en:", SALIDA)
