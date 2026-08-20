# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7258fd2d-1f78-420e-a9cc-d14506147f77",
# META       "default_lakehouse_name": "lh_bonceCombustible",
# META       "default_lakehouse_workspace_id": "e8fae30f-df4b-42a1-be74-fd9b5313fd2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "7258fd2d-1f78-420e-a9cc-d14506147f77"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### GUÍA DE EDA (Exploratory Data Analysis) — PLANTILLA REUTILIZABLE
# ##### Diosvely Perez Artega

# CELL ********************

# ================================================================
# GUÍA DE EDA (Exploratory Data Analysis) — PLANTILLA REUTILIZABLE
# ----------------------------------------------------------------
# Objetivo: ante CUALQUIER dataset, ejecutar estos bloques en orden
# para entenderlo y decidir qué limpiar/convertir ANTES de transformar.
#
# CÓMO USARLA: ejecuta bloque por bloque, mira el resultado, y decide.
# El EDA solo MIRA, no transforma nada.
#
# QUÉ CAMBIAR EN OTRO PROYECTO:
#   - TABLA          -> nombre de tu tabla
#   - COLS_PRECIO    -> tus columnas numéricas a perfilar
#   - COL_CATEGORICA -> una columna categórica de ejemplo (ej. Provincia)
# ================================================================
 
from pyspark.sql.functions import (
    col, count, when, countDistinct, mean, stddev, min, max,
    regexp_replace
)
from pyspark.sql.types import DoubleType
 
# ---- PARÁMETROS DEL PROYECTO (cambiar en otro dataset) ----
TABLA = "bronze_estaciones"
COL_CATEGORICA = "Provincia"        # una columna de texto para ver distribución
COLS_PRECIO = [                     # columnas numéricas (llegan como texto con coma)
    "Precio_Gasoleo_A", "Precio_Gasolina_95_E5", "Precio_Gasoleo_Premium",
    "Precio_Gasolina_98_E5", "Precio_Gasoleo_B",
    "Precio_Gases_licuados_del_petróleo", "Precio_Gasolina_95_E5_Premium"
]
 
df = spark.read.table(TABLA)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 1 — ESTRUCTURA GENERAL

# CELL ********************


# ================================================================
# BLOQUE 1 — ESTRUCTURA GENERAL
# Pregunta: "¿Qué forma y tipos tiene mi dataset?"
# Decisión que habilita: dimensionar el trabajo + ver qué hay que CONVERTIR
# ================================================================
print("=" * 55)
print(f"FILAS:    {df.count():,}")
print(f"COLUMNAS: {len(df.columns)}")
print("=" * 55)
 
print("\nESQUEMA (nombre + tipo de cada columna):")
df.printSchema()
# ? Si TODO sale 'string', hay que convertir tipos en Silver
#   (precios y coordenadas -> double; fechas -> date)
 
print("\nNOMBRES DE COLUMNA:")
for c in df.columns:
    print(f"  - {c}")
 
print("\nPRIMERAS FILAS (ver formato real de los datos):")
display(df.limit(10))
# ? Aquí ves la coma decimal, los vacíos '', el formato de fecha, etc.

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 2 — CALIDAD DE COLUMNA

# CELL ********************


# ================================================================
# BLOQUE 2 — CALIDAD DE COLUMNA (Column Quality)
# Pregunta: "¿CUÁNTOS datos tiene cada columna? ¿Cuántos vacíos?"
# Decisión que habilita: ELIMINAR columnas casi vacías (poco % válido)
# Equivale al panel "Valid / Empty %" de Power Query
# ================================================================
print("\n" + "=" * 55)
print("BLOQUE 2 — CALIDAD (% vacíos / % válidos por columna)")
print("=" * 55)
 
total = df.count()
for c in df.columns:
    # Cuenta nulos Y strings vacíos '' (típico de datos crudos)
    nulos = df.filter(col(c).isNull() | (col(c) == "")).count()
    pct_vacio = round(nulos / total * 100, 1)
    pct_valido = round((total - nulos) / total * 100, 1)
    print(f"{c}: {nulos} vacíos ({pct_vacio}%) | válidos: {pct_valido}%")
 
# CÓMO DECIDIR: columnas con <2-5% válido -> candidatas a ELIMINAR.
# OJO: el % alto NO garantiza que sea útil (ver Bloque 4: puede ser 100%
#      válido pero siempre el mismo valor "0,0" -> inútil).
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 3 — DISTRIBUCIÓN (Column Distribution)

# CELL ********************


# ================================================================
# BLOQUE 3 — DISTRIBUCIÓN (Column Distribution)
# Pregunta: "¿Cuántos valores DISTINTOS tiene cada columna? (cardinalidad)"
# Decisión que habilita: identificar claves, dimensiones, o columnas
#                        con un único valor dominante
# Equivale al "Distinct / Unique" de Power Query
# ================================================================
print("\n" + "=" * 55)
print("BLOQUE 3 — DISTRIBUCIÓN (valores distintos por columna)")
print("=" * 55)
 
for c in df.columns:
    distintos = df.select(countDistinct(col(c))).collect()[0][0]
    print(f"{c}: {distintos} valores distintos")
 
# CÓMO INTERPRETAR:
#   - 1 valor distinto        -> columna constante (inútil, eliminar)
#   - pocos valores           -> categórica / dimensión (ej. Provincia ~52)
#   - casi tantos como filas  -> identificador único (ej. IDEESS, Dirección)
 
# Distribución de UNA columna categórica (cuántas filas por valor):
print(f"\nDistribución de '{COL_CATEGORICA}' (top valores):")
df.groupBy(COL_CATEGORICA).count().orderBy(col("count").desc()).show(10)
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 4 — PERFILADO NUMÉRICO (Column Profile)

# CELL ********************


# ================================================================
# BLOQUE 4 — PERFILADO NUMÉRICO (Column Profile)
# Pregunta: "¿QUÉ valores tiene una columna? (min, max, media, rango)"
# Decisión que habilita: detectar columnas siempre-cero, outliers,
#                        rangos imposibles (ej. precio negativo)
# Equivale al panel de estadísticas de Power Query
# ----------------------------------------------------------------
# IMPORTANTE: los precios son TEXTO con coma ("1,499"). Para perfilar
# números de verdad, convertimos la coma a punto AL VUELO (solo para
# mirar; no modifica la tabla). Esto resuelve el caso "BioEtanol=0,0".
# ================================================================
print("\n" + "=" * 55)
print("BLOQUE 4 — PERFILADO NUMÉRICO (min/max/media de precios)")
print("=" * 55)
 
for c in COLS_PRECIO:
    # Convertir coma->punto y castear SOLO para explorar
    col_num = regexp_replace(col(c), ",", ".").cast(DoubleType())
    stats = df.select(
        count(when(col_num.isNotNull(), True)).alias("con_dato"),
        mean(col_num).alias("media"),
        stddev(col_num).alias("desv_std"),
        min(col_num).alias("minimo"),
        max(col_num).alias("maximo")
    ).collect()[0]
    print(f"\n{c}:")
    print(f"   con dato: {stats['con_dato']:,} | media: {stats['media']} "
          f"| min: {stats['minimo']} | max: {stats['maximo']} | std: {stats['desv_std']}")
 
# CÓMO DECIDIR con esto:
#   - min = max = 0        -> columna siempre-cero (ej. BioEtanol), ELIMINAR
#   - min negativo o raro  -> datos sucios / errores a investigar
#   - max absurdo          -> outlier o error de captura
#   - rango coherente       -> columna sana (ej. gasolina entre 1.2 y 2.1 €)
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 5 — DUPLICADOS

# CELL ********************


# ================================================================
# BLOQUE 5 — DUPLICADOS
# Pregunta: "¿Tengo filas repetidas?"
# Decisión que habilita: saber si hay que deduplicar en Silver
# ================================================================
print("\n" + "=" * 55)
print("BLOQUE 5 — DUPLICADOS")
print("=" * 55)
total_filas = df.count()
filas_unicas = df.dropDuplicates().count()
print(f"Filas totales: {total_filas:,}")
print(f"Filas únicas:  {filas_unicas:,}")
print(f"Duplicados:    {total_filas - filas_unicas:,}")
 
# En datos con histórico, revisa duplicados por CLAVE + FECHA, no solo fila entera:
# df.groupBy("IDEESS", "FechaDato").count().filter(col("count") > 1).show()
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

# ¿Hay gasolineras repetidas en la misma FechaDato?
dups = df.groupBy("IDEESS", "FechaDato").count().filter(col("count") > 1)
print("Combinaciones IDEESS + FechaDato duplicadas:", dups.count())
dups.show(10)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
