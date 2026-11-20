
import os
import sys
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark import StorageLevel

# ==========================================
# CONFIGURACIÓN DEL PROYECTO
# ==========================================

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE = Path(__file__).resolve().parent.parent
ARCHIVO = BASE / "datos" / "compras_limpias.csv"

SALIDA = BASE / "spark" / "resultados"
SALIDA.mkdir(parents=True, exist_ok=True)

RUTA_CSV = SALIDA / "recomendaciones.csv"

print("======================================")
print(" RETO 2 - MOTOR DE RECOMENDACIONES")
print("======================================")
print("Iniciando Apache Spark...")

# ==========================================
# INICIAR SPARK
# ==========================================

spark = (
    SparkSession.builder
    .appName("Reto2RecomendacionesOptimizado")
    .master("local[2]")
    .config("spark.driver.memory", "2g")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

try:

    # ==========================================
    # 1. CARGAR LOS DATOS
    # ==========================================

    print("\n1. Cargando compras...")

    df = (
        spark.read
        .option("header", True)
        .csv(str(ARCHIVO))
    )

    total_registros = df.count()

    print("Registros procesados:", total_registros)

    # ==========================================
    # 2. RELACIONES CLIENTE - PRODUCTO
    # ==========================================

    print("\n2. Identificando compras por cliente...")

    compras = (
        df.select(
            F.col("CustomerID").alias("cliente"),
            F.col("StockCode").alias("producto")
        )
        .dropna()
        .distinct()
        .repartition(16, "cliente")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_relaciones = compras.count()

    print("Relaciones cliente-producto:", total_relaciones)

    # ==========================================
    # 3. PRODUCTOS MÁS POPULARES
    # ==========================================

    print("\n3. Identificando productos populares...")

    populares = (
        compras.groupBy("producto")
        .agg(
            F.count("*").alias("clientes")
        )
        .orderBy(
            F.desc("clientes"),
            F.asc("producto")
        )
        .limit(200)
        .select("producto")
    )

    # ==========================================
    # 4. FILTRAR PRODUCTOS POPULARES
    # ==========================================

    compras_populares = (
        compras.join(
            F.broadcast(populares),
            on="producto",
            how="inner"
        )
        .persist(StorageLevel.MEMORY_AND_DISK)
    )

    total_populares = compras_populares.count()

    print(
        "Relaciones de productos populares:",
        total_populares
    )

    # ==========================================
    # 5. CREAR PARES DE PRODUCTOS
    # ==========================================

    print("\n4. Calculando productos relacionados...")

    a = compras_populares.alias("a")
    b = compras_populares.alias("b")

    pares = (
        a.join(
            b,
            (F.col("a.cliente") == F.col("b.cliente"))
            &
            (F.col("a.producto") < F.col("b.producto"))
        )
        .select(
            F.col("a.producto").alias("producto_a"),
            F.col("b.producto").alias("producto_b")
        )
        .groupBy(
            "producto_a",
            "producto_b"
        )
        .agg(
            F.count("*").alias("clientes_comunes")
        )
        .filter(
            F.col("clientes_comunes") >= 3
        )
    )

    # ==========================================
    # 6. RELACIONES EN AMBOS SENTIDOS
    # ==========================================

    directas = pares.select(
        F.col("producto_a").alias("producto"),
        F.col("producto_b").alias("recomendado"),
        "clientes_comunes"
    )

    inversas = pares.select(
        F.col("producto_b").alias("producto"),
        F.col("producto_a").alias("recomendado"),
        "clientes_comunes"
    )

    relaciones = directas.unionByName(inversas)

    # ==========================================
    # 7. CALCULAR RECOMENDACIONES
    # ==========================================

    print("\n5. Generando recomendaciones...")

    candidatos = (
        compras_populares.join(
            relaciones,
            on="producto",
            how="inner"
        )
        .groupBy(
            "cliente",
            "recomendado"
        )
        .agg(
            F.sum("clientes_comunes").alias("puntaje")
        )
    )

    # ==========================================
    # 8. EXCLUIR PRODUCTOS YA COMPRADOS
    # ==========================================

    ya_comprados = compras.select(
        "cliente",
        F.col("producto").alias("recomendado")
    )

    candidatos = candidatos.join(
        ya_comprados,
        on=["cliente", "recomendado"],
        how="left_anti"
    )

    # ==========================================
    # 9. OBTENER TOP 5 POR CLIENTE
    # ==========================================

    ventana = (
        Window.partitionBy("cliente")
        .orderBy(
            F.desc("puntaje"),
            F.asc("recomendado")
        )
    )

    top5 = (
        candidatos
        .withColumn(
            "posicion",
            F.row_number().over(ventana)
        )
        .filter(
            F.col("posicion") <= 5
        )
        .select(
            "cliente",
            "recomendado",
            "puntaje",
            "posicion"
        )
    )

    # ==========================================
    # 10. GUARDAR RESULTADOS CON PYTHON
    # ==========================================

    print("\n6. Guardando recomendaciones...")
    print("Archivo:", RUTA_CSV)

    contador = 0
    ejemplo_17850 = []

    # Spark calcula los resultados y Python
    # escribe el CSV sin usar el escritor
    # de archivos Hadoop de Spark.

    with open(
        RUTA_CSV,
        "w",
        newline="",
        encoding="utf-8"
    ) as archivo:

        escritor = csv.writer(archivo)

        escritor.writerow([
            "cliente",
            "recomendado",
            "puntaje",
            "posicion"
        ])

        for fila in top5.toLocalIterator():

            cliente = str(fila["cliente"])
            producto = str(fila["recomendado"])
            puntaje = int(fila["puntaje"])
            posicion = int(fila["posicion"])

            escritor.writerow([
                cliente,
                producto,
                puntaje,
                posicion
            ])

            contador += 1

            # Guardar ejemplo mientras se procesa,
            # sin ejecutar nuevamente todo Spark.
            if cliente == "17850":
                ejemplo_17850.append((
                    posicion,
                    producto,
                    puntaje
                ))

            if contador % 5000 == 0:
                print(
                    "Recomendaciones guardadas:",
                    contador
                )

    print("\n======================================")
    print(" RECOMENDACIONES GUARDADAS")
    print("======================================")

    print("Total de recomendaciones:", contador)
    print("Archivo generado:", RUTA_CSV)

    # ==========================================
    # 11. MOSTRAR RECOMENDACIONES DE EJEMPLO
    # ==========================================

    print("\nRECOMENDACIONES PARA CLIENTE 17850")

    if ejemplo_17850:

        print("Producto | Puntaje | Posición")

        for posicion, producto, puntaje in sorted(
            ejemplo_17850
        ):
            print(
                producto,
                "|",
                puntaje,
                "|",
                posicion
            )

    else:
        print(
            "No se encontraron recomendaciones "
            "para este cliente."
        )

    print("\nPROCESAMIENTO FINALIZADO CORRECTAMENTE")

finally:

    print("\nCerrando Apache Spark...")
    spark.stop()
    print("Spark detenido.")
