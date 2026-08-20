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

# #### Paso 1 — Ver el estado ANTES (diagnóstico)

# CELL ********************

# Estado actual: nº de archivos, tamaño, particiones
spark.sql("DESCRIBE DETAIL bronze_estaciones").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Paso 2 — OPTIMIZE (compactar archivos pequeños)

# CELL ********************

# Compactar los archivos pequeños en archivos grandes y eficientes
spark.sql("OPTIMIZE bronze_estaciones")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Paso 3 — Ver el estado DESPUÉS (comparar)

# CELL ********************

# Volver a mirar: numFiles debería haber BAJADO
spark.sql("DESCRIBE DETAIL bronze_estaciones").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### Paso 4 — VACUUM (limpiar obsoletos) — con DRY RUN primero

# CELL ********************

# DRY RUN: muestra qué borraría SIN borrar nada (mirar antes de actuar)
spark.sql("VACUUM bronze_estaciones RETAIN 168 HOURS DRY RUN").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

# Si el DRY RUN se ve bien, ejecutar de verdad (7 días de retención)
spark.sql("VACUUM bronze_estaciones RETAIN 168 HOURS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
