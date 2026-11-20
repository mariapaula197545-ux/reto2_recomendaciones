# Reto 2 – Motor de Recomendaciones para Comercio Electrónico

## 1. Descripción del proyecto

Este proyecto implementa un sistema de procesamiento y análisis de datos de comercio electrónico mediante tecnologías Big Data. Su objetivo es analizar el comportamiento de compra de los clientes, identificar patrones de consumo y generar recomendaciones personalizadas de productos.

Se utilizan Apache Kafka, Apache Cassandra, Apache Spark, Neo4j y Amazon S3 para integrar la transmisión de eventos, el almacenamiento de datos, el análisis de relaciones y la persistencia de resultados en la nube.

## 2. Objetivos

**Objetivo general**

Desarrollar un motor de recomendaciones para comercio electrónico utilizando tecnologías Big Data, con el propósito de analizar patrones de compra y recomendar productos a los clientes.

**Objetivos específicos**

- Procesar eventos de compras mediante Apache Kafka.
- Almacenar información de compras y clientes utilizando Apache Cassandra.
- Generar recomendaciones de productos mediante Apache Spark.
- Analizar relaciones entre clientes y productos utilizando Neo4j.
- Comparar las recomendaciones obtenidas con Spark y Neo4j.
- Realizar cinco análisis de datos sobre el comportamiento comercial.
- Almacenar los datos y resultados en Amazon S3.

## 3. Tecnologías utilizadas

| Tecnología | Función |
|---|---|
| Python | Desarrollo de scripts y procesamiento de datos |
| Apache Kafka | Producción y consumo de eventos de compra |
| Apache Cassandra | Almacenamiento NoSQL de información comercial |
| Apache Spark | Procesamiento de datos y recomendaciones por coocurrencia |
| Neo4j | Modelado y análisis de relaciones mediante grafos |
| Amazon S3 | Almacenamiento de archivos y resultados en la nube |
| Docker | Ejecución de servicios mediante contenedores |
| Pandas | Análisis estadístico y generación de archivos CSV |
| Boto3 | Integración de Python con Amazon S3 |

## 4. Conjunto de datos

Se utiliza el conjunto de datos *Online Retail*, que contiene transacciones de comercio electrónico.

El conjunto original contiene 541.909 registros.

Después de la limpieza se obtuvieron:

- 392.692 registros válidos.
- 4.338 clientes.
- 3.665 productos.

La preparación se realiza mediante `preparar_datos.py`, que genera `datos/compras_limpias.csv`.

## 5. Arquitectura del sistema

El proyecto se organiza en varias etapas:

1. **Preparación:** Python limpia el conjunto de datos.
2. **Transmisión:** Kafka publica y consume eventos de compra.
3. **Almacenamiento NoSQL:** Cassandra almacena eventos y perfiles de clientes.
4. **Recomendaciones con Spark:** se identifican relaciones entre productos comprados por clientes.
5. **Recomendaciones con Neo4j:** se representan clientes y productos como un grafo y se identifican recomendaciones mediante relaciones.
6. **Comparación:** se evalúa la coincidencia entre las listas de recomendaciones.
7. **Análisis:** se calculan indicadores de productos, clientes, países y ventas mensuales.
8. **Almacenamiento en la nube:** Amazon S3 almacena los archivos procesados y los resultados.

Los componentes se implementan mediante scripts especializados; no todos corresponden a un único flujo de procesamiento automático.

## 6. Estructura del proyecto

```text
reto2_recomendaciones/
│
├── almacenamiento/
│   └── subir_s3.py
│
├── analisis/
│   ├── analizar_datos.py
│   └── resultados/
│
├── cassandra/
│   ├── cargar_perfiles.py
│   └── consumidor_cassandra.py
│
├── comparacion/
│   ├── comparar_modelos.py
│   └── resultados/
│
├── datos/
│   ├── Online Retail.xlsx
│   └── compras_limpias.csv
│
├── kafka/
│   ├── consumidor_compras.py
│   └── productor_compras.py
│
├── neo4j/
│   ├── cargar_grafo.py
│   ├── recomendar_grafo.py
│   └── resultados/
│
├── spark/
│   ├── recomendar_productos.py
│   └── resultados/
│
├── docker-compose.yml
├── preparar_datos.py
└── README.md
```

## 7. Ejecución del proyecto

**Requisitos:** Python, Docker Desktop, Java y un entorno compatible con PySpark 3.5.1.

Instalar las dependencias de Python utilizadas en las diferentes etapas:

```bash
python -m pip install pandas openpyxl kafka-python cassandra-driver neo4j pyspark==3.5.1 boto3
```

Iniciar los servicios:

```bash
docker compose up -d
```

Preparar el conjunto de datos:

```bash
python preparar_datos.py
```

Ejecutar el productor de compras:

```bash
python kafka/productor_compras.py
```

Ejecutar el consumidor de Cassandra en otra terminal:

```bash
python cassandra/consumidor_cassandra.py
```

Cargar los perfiles:

```bash
python cassandra/cargar_perfiles.py
```

Generar las recomendaciones con Spark:

```bash
python spark/recomendar_productos.py
```

Cargar los datos en Neo4j:

```bash
python neo4j/cargar_grafo.py
```

Generar recomendaciones mediante grafos:

```bash
python neo4j/recomendar_grafo.py
```

Comparar los modelos:

```bash
python comparacion/comparar_modelos.py
```

Ejecutar los análisis:

```bash
python analisis/analizar_datos.py
```

Los servicios deben estar disponibles antes de ejecutar los scripts que dependen de ellos. Algunas etapas requieren terminales independientes.

## 8. Resultados de los análisis

### Análisis 1. Productos más vendidos

El producto con mayor cantidad vendida fue **PAPER CRAFT, LITTLE BIRDIE**, con 80.995 unidades.

### Análisis 2. Clientes con mayor gasto

El cliente **14646** registró el mayor gasto acumulado, con 280.206,02 unidades monetarias.

### Análisis 3. Ventas por país

**Reino Unido** presentó los mayores ingresos, con 7.285.024,64 unidades monetarias.

### Análisis 4. Ventas mensuales

El mes con mayores ingresos fue **noviembre de 2011**, con 1.156.205,61 unidades monetarias.

### Análisis 5. Comparación Spark y Neo4j

Apache Spark generó 20.550 recomendaciones en el archivo de resultados.

Para el cliente 17850 se compararon las cinco recomendaciones principales de Spark y Neo4j:

- Productos coincidentes: 2.
- Coincidencia Top 5: 40 %.
- Similitud Jaccard: 0,25.

La coincidencia representa el porcentaje de productos compartidos entre las listas, no una medida de precisión predictiva.

## 9. Integración con Amazon S3

Los archivos del proyecto se almacenan en un bucket privado de Amazon S3 ubicado en la región `us-east-1`.

El script `almacenamiento/subir_s3.py` utiliza Boto3 y un perfil local de AWS CLI denominado `reto2`.

Para configurar las credenciales, se debe utilizar un usuario IAM con permisos limitados al bucket. No se deben incluir claves de acceso en el repositorio.

Ejecutar:

```bash
python almacenamiento/subir_s3.py
```

La ejecución verificada permitió subir cinco archivos correctamente, sin errores.

También se almacenaron cuatro archivos CSV con los resultados de los análisis.

## 10. Conclusiones

El proyecto demuestra la integración de diferentes tecnologías Big Data para procesar información de comercio electrónico.

Apache Kafka permite transmitir eventos de compra, mientras que Cassandra proporciona almacenamiento NoSQL.

Apache Spark permite procesar grandes conjuntos de datos y generar recomendaciones a partir de patrones de compra. Neo4j ofrece una alternativa basada en relaciones entre clientes y productos.

La comparación realizada demuestra que los métodos pueden producir listas de recomendaciones diferentes para un mismo cliente.

Finalmente, Amazon S3 permite conservar los datos procesados y los resultados analíticos en infraestructura de nube.

## 11. Consideraciones de seguridad

- No publicar claves de acceso de AWS.
- No utilizar credenciales root en scripts.
- Mantener el bucket S3 privado.
- Utilizar permisos IAM limitados.
- Evitar subir archivos `.env`, credenciales o claves privadas al repositorio.
- Revisar los costos de los recursos de AWS al finalizar las pruebas.