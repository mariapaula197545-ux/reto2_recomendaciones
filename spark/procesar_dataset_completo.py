
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, sum as spark_sum, when
)

BASE = Path(__file__).resolve().parent.parent

ARCHIVO = BASE / "datos" / "compras_completas.csv"
SALIDA = BASE / "spark" / "resultados"
SALIDA.mkdir(parents=True, exist_ok=True)

spark = SparkSession.builder \
    .appName("Reto2_Procesamiento_500Mil_Registros") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

try:
    print("\n======================================")
    print(" RETO 2 - PROCESAMIENTO BIG DATA")
    print("======================================")

    df = spark.read.csv(
        str(ARCHIVO),
        header=True,
        inferSchema=True,
        multiLine=True,
        escape='"'
    ).cache()

    total = df.count()

    print("\nREGISTROS PROCESADOS:", total)

    # Clasificar los registros
    analisis = df.select(
        "*",
        when(
            col("CustomerID").isNull(),
            "SIN_CLIENTE"
        ).when(
            (col("Quantity") <= 0) |
            (col("UnitPrice") <= 0) |
            col("InvoiceNo").cast("string").startswith("C"),
            "DEVOLUCION_O_NO_VALIDA"
        ).otherwise(
            "COMPRA_POTENCIALMENTE_VALIDA"
        ).alias("TipoRegistro")
    )

    resumen = analisis.groupBy(
        "TipoRegistro"
    ).agg(
        count("*").alias("Cantidad")
    )

    print("\nCLASIFICACION DE REGISTROS")
    resumen.show(truncate=False)

    # Calcular operaciones comerciales
    operaciones = analisis.withColumn(
        "TotalOperacion",
        col("Quantity") * col("UnitPrice")
    )

    estadisticas = operaciones.agg(
        count("*").alias("TotalRegistros"),
        spark_sum("TotalOperacion").alias("ValorNetoOperaciones")
    )

    print("\nESTADISTICAS GENERALES")
    estadisticas.show(truncate=False)

    # Guardar evidencia
    with open(
        SALIDA / "evidencia_500mil.txt",
        "w",
        encoding="utf-8"
    ) as archivo:
        archivo.write(
            "RETO 2 - PROCESAMIENTO CON APACHE SPARK\n"
        )
        archivo.write(
            f"Registros procesados: {total}\n"
        )
        archivo.write(
            "Motor: Apache Spark\n"
        )
        archivo.write(
            "Archivo: compras_completas.csv\n"
        )

    print("\nEvidencia guardada correctamente.")

finally:
    spark.stop()