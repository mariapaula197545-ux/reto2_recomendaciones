# Reto 2 – Motor de Recomendaciones para Comercio Electrónico

## 1. Descripción del proyecto

Este proyecto implementa un sistema de procesamiento y análisis de datos de comercio electrónico mediante tecnologías Big Data. Su objetivo es analizar el comportamiento de compra de los clientes, identificar patrones de consumo y generar recomendaciones personalizadas de productos.

Se utilizan Apache Kafka, Apache Cassandra, Apache Spark, Neo4j y Amazon S3 para integrar la transmisión de eventos, el almacenamiento de datos, el análisis de relaciones y la persistencia de resultados en la nube.

El proyecto incluye una evaluación comparativa de **100 clientes**, utilizando dos enfoques de recomendación: asociaciones de productos mediante Spark y relaciones entre clientes y productos mediante Neo4j.

## 2. Objetivos

**Objetivo general**

Desarrollar un motor de recomendaciones para comercio electrónico utilizando tecnologías Big Data, con el propósito de analizar patrones de compra y recomendar productos a los clientes.

**Objetivos específicos**

- Procesar eventos de compras mediante Apache Kafka.
- Almacenar información de compras y clientes utilizando Apache Cassandra.
- Generar recomendaciones de productos mediante Apache Spark.
- Analizar relaciones entre clientes y productos utilizando Neo4j.
- Comparar las recomendaciones obtenidas con Spark y Neo4j para 100 clientes.
- Realizar cinco análisis de datos sobre el comportamiento comercial.
- Almacenar los datos y resultados en Amazon S3.
- Evaluar la coincidencia entre los motores mediante Top 5 e índice Jaccard.

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

El conjunto original contiene **541.909 registros**.

Después de la limpieza se obtuvieron:

- 392.692 registros válidos.
- 4.338 clientes.
- 3.665 productos.

La preparación se realiza mediante `preparar_datos.py`, que genera `datos/compras_limpias.csv`.

Adicionalmente, Spark procesó el conjunto original y clasificó:

| Clasificación | Registros |
|---|---:|
| Compras potencialmente válidas | 397.884 |
| Registros sin identificación del cliente | 135.080 |
| Devoluciones u operaciones no válidas | 8.945 |
| **Total procesado** | **541.909** |

Los resultados corresponden a las etapas de procesamiento y limpieza implementadas en el proyecto.

## 5. Arquitectura del sistema

El proyecto se organiza en varias etapas:

1. **Preparación:** Python limpia el conjunto de datos.
2. **Transmisión:** Kafka publica y consume eventos de compra.
3. **Almacenamiento NoSQL:** Cassandra almacena eventos y perfiles de clientes.
4. **Recomendaciones con Spark:** se identifican asociaciones entre productos comprados por los clientes.
5. **Recomendaciones con Neo4j:** se representan clientes y productos como nodos conectados mediante relaciones de compra.
6. **Comparación:** se evalúa la coincidencia de las recomendaciones generadas por ambos motores.
7. **Análisis:** se calculan indicadores de productos, clientes, países y ventas mensuales.
8. **Almacenamiento en la nube:** Amazon S3 almacena archivos procesados y resultados.

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
│   ├── comparar_100_clientes.py
│   └── resultados/
│       ├── comparacion_100_clientes.csv
│       └── resumen_100_clientes.txt
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
│   ├── recomendar_100_clientes.py
│   └── resultados/
│       └── recomendaciones_100_clientes.csv
│
├── spark/
│   ├── procesar_dataset_completo.py
│   ├── recomendar_productos.py
│   └── resultados/
│       └── recomendaciones.csv
│
├── docker-compose.yml
├── preparar_datos.py
└── README.md
```

## 7. Ejecución del proyecto

**Requisitos:** Python, Docker Desktop, Java y un entorno compatible con PySpark 3.5.1.

Instalar las dependencias de Python:

```bash
python -m pip install pandas openpyxl kafka-python cassandra-driver neo4j pyspark==3.5.1 boto3
```

**Paso 1. Iniciar los servicios**

```bash
docker compose up -d
```

**Paso 2. Preparar el conjunto de datos**

```bash
python preparar_datos.py
```

**Paso 3. Ejecutar el productor de compras**

```bash
python kafka/productor_compras.py --limite 1000 --pausa 0.01
```

El productor envía eventos al tópico `compras-retail` y espera confirmación de Kafka antes de contabilizar cada mensaje.

**Paso 4. Ejecutar el consumidor de Cassandra**

En otra terminal:

```bash
python cassandra/consumidor_cassandra.py
```

Para consumir los eventos durante una prueba, el consumidor debe estar activo según la configuración de lectura utilizada.

**Paso 5. Cargar los perfiles**

```bash
python cassandra/cargar_perfiles.py
```

**Paso 6. Procesar el conjunto completo con Spark**

```bash
python spark/procesar_dataset_completo.py
```

**Paso 7. Generar recomendaciones con Spark**

```bash
python spark/recomendar_productos.py
```

**Paso 8. Cargar los datos en Neo4j**

```bash
python neo4j/cargar_grafo.py
```

**Paso 9. Generar recomendaciones con Neo4j**

Para la evaluación individual:

```bash
python neo4j/recomendar_grafo.py
```

Para evaluar 100 clientes:

```bash
python neo4j/recomendar_100_clientes.py
```

**Paso 10. Comparar los motores**

Para la comparación inicial:

```bash
python comparacion/comparar_modelos.py
```

Para la evaluación de 100 clientes:

```bash
python comparacion/comparar_100_clientes.py
```

**Paso 11. Ejecutar los análisis comerciales**

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

Se implementaron dos motores de recomendaciones para analizar los historiales de compra.

**Resultados de Apache Spark**

- Clientes con recomendaciones: 4.110.
- Recomendaciones generadas: 20.550.
- Recomendaciones por cliente: 5.
- Archivo: `spark/resultados/recomendaciones.csv`.

**Resultados de Neo4j**

- Clientes evaluados: 100.
- Recomendaciones por cliente: 5.
- Total de recomendaciones: 500.
- Tiempo de ejecución: 42,87 segundos.
- Archivo: `neo4j/resultados/recomendaciones_100_clientes.csv`.

**Comparación inicial de un cliente**

Para el cliente 17850 se compararon las cinco recomendaciones principales de Spark y Neo4j:

- Productos coincidentes: 2.
- Coincidencia Top 5: 40 %.
- Similitud Jaccard: 0,25.

**Comparación ampliada de 100 clientes**

Se evaluaron 100 clientes con cinco recomendaciones disponibles en ambos motores.

| Indicador | Resultado |
|---|---:|
| Clientes comparados | 100 |
| Recomendaciones por cliente y motor | 5 |
| Clientes con al menos una coincidencia | 96 |
| Clientes sin coincidencias | 4 |
| Coincidencia Top 5 promedio | 48,20 % |
| Índice Jaccard promedio | 0,3422 |
| Productos coincidentes promedio | 2,41 de 5 |

La coincidencia Top 5 se calcula mediante:

**Coincidencia Top 5 (%) = (Productos compartidos / 5) × 100**

El índice Jaccard se calcula mediante:

**Jaccard = Productos compartidos / Total de productos distintos entre ambas listas**

Los resultados indican que Spark y Neo4j presentan coincidencias parciales, pero también generan recomendaciones diferentes debido a sus enfoques de análisis.

El 48,20 % representa similitud entre los motores, **no precisión predictiva ni porcentaje de compras acertadas**.

Los resultados se almacenan en:

- `comparacion/resultados/comparacion_100_clientes.csv`
- `comparacion/resultados/resumen_100_clientes.txt`

## 9. Integración con Amazon S3

Los archivos del proyecto se almacenan en un bucket privado de Amazon S3 ubicado en la región `us-east-1`.

El script `almacenamiento/subir_s3.py` utiliza Boto3 y un perfil local de AWS CLI denominado `reto2`.

Para configurar las credenciales, se debe utilizar un usuario IAM con permisos limitados al bucket. No se deben incluir claves de acceso en el repositorio.

Ejecutar la carga inicial:

```bash
python almacenamiento/subir_s3.py
```

La ejecución inicial verificada permitió subir cinco archivos correctamente, sin errores.

También se almacenaron cuatro archivos CSV con los resultados de los análisis.

**Actualización: resultados de 100 clientes**

Se incorporaron tres archivos adicionales a Amazon S3:

- `neo4j/resultados/recomendaciones_100_clientes.csv`
- `comparacion/resultados/comparacion_100_clientes.csv`
- `comparacion/resultados/resumen_100_clientes.txt`

Estos archivos contienen las recomendaciones generadas por Neo4j y los resultados comparativos de los 100 clientes evaluados.

Para verificar los archivos en Amazon S3:

```bash
aws s3 ls s3://reto2-retail-maria-2026-168197327138-us-east-1-an/ --recursive --profile reto2
```

## 10. Conclusiones

El proyecto demuestra la integración de diferentes tecnologías Big Data para procesar información de comercio electrónico y generar recomendaciones personalizadas.

Apache Kafka permite transmitir eventos de compra, mientras que Cassandra proporciona almacenamiento NoSQL.

Apache Spark permitió procesar 541.909 registros originales y generar 20.550 recomendaciones para 4.110 clientes.

Neo4j permitió representar las relaciones entre clientes y productos mediante grafos y generar 500 recomendaciones para 100 clientes en 42,87 segundos.

La comparación ampliada mostró una coincidencia Top 5 promedio del 48,20 % y una similitud Jaccard promedio de 0,3422. Además, 96 de los 100 clientes evaluados presentaron al menos una recomendación compartida entre ambos motores.

Estos resultados demuestran que los métodos pueden identificar productos relacionados, aunque sus recomendaciones no siempre coinciden.

Finalmente, Amazon S3 permitió conservar los datos procesados y los resultados analíticos en infraestructura de nube.

Como trabajo futuro, se propone evaluar la calidad predictiva de las recomendaciones utilizando compras posteriores, automatizar las etapas de procesamiento y realizar pruebas de escalabilidad.

## 11. Consideraciones de seguridad

- No publicar claves de acceso de AWS.
- No utilizar credenciales root en scripts.
- Mantener el bucket S3 privado.
- Utilizar permisos IAM limitados.
- Evitar subir archivos `.env`, credenciales o claves privadas al repositorio.
- No publicar datasets grandes innecesariamente en GitHub.
- Revisar los costos de los recursos de AWS al finalizar las pruebas.
