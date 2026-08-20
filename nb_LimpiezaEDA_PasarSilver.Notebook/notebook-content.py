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
# META         },
# META         {
# META           "id": "54ce9ba1-4b65-48ce-a25c-8998621bd226"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "known_warehouses": []
# META     }
# META   }
# META }

# MARKDOWN ********************

# #### NOTEBOOK DE LIMPIEZA — SILVER (Proyecto Combustible España)

# CELL ********************

# ================================================================
# NOTEBOOK DE LIMPIEZA — SILVER (Proyecto Combustible España)
# ----------------------------------------------------------------
# Transforma bronze_estaciones -> silver_estaciones
# Decisiones tomadas y validadas en el EDA (5 bloques).
#
# ORDEN (buenas prácticas: reduce -> limpia -> tipa -> guarda):
#   1. Leer Bronze
#   2. Seleccionar columnas útiles (reduce primero)
#   3. Vacíos '' -> null
#   4. Convertir tipos (coma decimal, coordenadas, fecha)
#   5. Verificar
#   6. Guardar en Silver
# ================================================================

from pyspark.sql.functions import col, when, regexp_replace, to_date
from pyspark.sql.types import DoubleType


# ================================================================
# PASO 1 — Leer Bronze
# ================================================================
df = spark.read.table("bronze_estaciones")
print(f"Bronze leído: {df.count():,} filas, {len(df.columns)} columnas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PASO 2 — Seleccionar columnas útiles (REDUCE primero)

# CELL ********************


# ================================================================
# PASO 2 — Seleccionar columnas útiles (REDUCE primero)
# Mejor 'select' (qué conservar) que 'drop' (qué quitar): más robusto.
# ================================================================
columnas_silver = [
    # --- Claves ---
    "IDEESS", "IDCCAA", "IDProvincia", "IDMunicipio",
    # --- Geografía ---
    "Provincia", "Municipio", "Localidad", "C_P",
    "Latitud", "Longitud_WGS84",
    # --- Descriptivas de la estación ---
    "Dirección", "Rótulo", "Margen", "Tipo_Venta", "Remisión",
    # --- Combustibles (los 7 elegidos y validados) ---
    "Precio_Gasoleo_A",
    "Precio_Gasolina_95_E5",
    "Precio_Gasoleo_Premium",
    "Precio_Gasolina_98_E5",
    "Precio_Gasoleo_B",
    "Precio_Gases_licuados_del_petróleo",   # GLP
    "Precio_Gasolina_95_E5_Premium",        # Gasolina 95 Premium
    # --- Fechas ---
    "FechaDato", "FechaCarga"
]

df = df.select(*columnas_silver)
print(f"Tras seleccionar: {len(df.columns)} columnas")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PASO 3 — Vacíos '' -> null

# CELL ********************


# ================================================================
# PASO 3 — Vacíos '' -> null
# En los precios, '' significa "no vende ese combustible".
# Lo convertimos a null (semánticamente correcto: ausencia de dato).
# Se hace ANTES de castear: '' no castea a número, null sí lo maneja.
# ================================================================
columnas_precio = [
    "Precio_Gasoleo_A", "Precio_Gasolina_95_E5", "Precio_Gasoleo_Premium",
    "Precio_Gasolina_98_E5", "Precio_Gasoleo_B",
    "Precio_Gases_licuados_del_petróleo", "Precio_Gasolina_95_E5_Premium"
]
columnas_coordenadas = ["Latitud", "Longitud_WGS84"]

# Vacíos -> null en precios y coordenadas
for c in columnas_precio + columnas_coordenadas:
    df = df.withColumn(c, when(col(c) == "", None).otherwise(col(c)))



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PASO 4 — Convertir tipos (con locale: coma decimal española)

# CELL ********************


# ================================================================
# PASO 4 — Convertir tipos (con locale: coma decimal española)
# ================================================================

# 4a. PRECIOS: "1,499" -> "1.499" -> Double
#     regexp_replace cambia la coma por punto; cast convierte a número.
for c in columnas_precio:
    df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast(DoubleType()))

# 4b. COORDENADAS: "39,211417" -> Double (misma lógica de coma)
for c in columnas_coordenadas:
    df = df.withColumn(c, regexp_replace(col(c), ",", ".").cast(DoubleType()))

# 4c. FECHA: FechaDato string "DD-MM-YYYY" -> DateType
#     Ahora sí se podrá ordenar/comparar/filtrar cronológicamente.
df = df.withColumn("FechaDato", to_date(col("FechaDato"), "dd-MM-yyyy"))

# Nota: los IDs (IDEESS, IDProvincia...) se dejan como STRING.
#       Son claves, no se hacen cálculos con ellos, y así casan en los joins.
# Nota: FechaCarga se deja como está (trazabilidad técnica).



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PASO 5 — Verificar (que los tipos quedaron bien)

# CELL ********************


# ================================================================
# PASO 5 — Verificar (que los tipos quedaron bien)
# ================================================================
print("\nEsquema tras la conversión (ya NO debe ser todo string):")
df.printSchema()

print("\nMuestra de datos limpios:")
df.select("IDEESS", "Provincia", "Precio_Gasolina_95_E5",
          "Latitud", "FechaDato").show(10)

# Verificación rápida: rango de fechas REAL (ya como fecha, no alfabético)
from pyspark.sql.functions import min as fmin, max as fmax
df.select(fmin("FechaDato"), fmax("FechaDato")).show()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### PASO 6 — Guardar en SILVER

# CELL ********************


# ================================================================
# PASO 6 — Guardar en SILVER
# ================================================================
# overwrite: Silver se reconstruye desde Bronze cuando reprocesas.
#            (idempotente: puedes re-ejecutar sin duplicar)
df.write.mode("overwrite").format("delta").saveAsTable("lh_Silver.dbo.silver_estaciones")

print(f"\n? Silver guardado: {df.count():,} filas, {len(df.columns)} columnas")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
