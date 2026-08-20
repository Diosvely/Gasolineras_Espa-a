# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "54ce9ba1-4b65-48ce-a25c-8998621bd226",
# META       "default_lakehouse_name": "lh_Silver",
# META       "default_lakehouse_workspace_id": "e8fae30f-df4b-42a1-be74-fd9b5313fd2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "54ce9ba1-4b65-48ce-a25c-8998621bd226"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ####  NOTEBOOK OPTIMIZACIÓN — SILVER

# CELL ********************

# ============================================
# NOTEBOOK OPTIMIZACIÓN — SILVER
# (asociar este notebook a lh_silver)
# Diferencia con Bronze: aquí SÍ aplicamos V-Order
# ============================================

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 1 — Estado ANTES (diagnóstico)

# CELL ********************

# Ver nº de archivos y tamaño actual de Silver
spark.sql("DESCRIBE DETAIL silver_estaciones").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 2 — Activar V-Order en la sesión

# CELL ********************

# Activar V-Order para esta sesión (reordena para lectura óptima en Power BI/Direct Lake)
spark.conf.set("spark.sql.parquet.vorder.enabled", "true")
print("V-Order activado para la sesión")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 3 — OPTIMIZE con V-Order

# CELL ********************

# Compactar archivos + aplicar V-Order
spark.sql("OPTIMIZE silver_estaciones VORDER").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 4 — Estado DESPUÉS (comparar)

# CELL ********************

# Ver el resultado: numFiles debería bajar, y los datos quedan V-Ordered
spark.sql("DESCRIBE DETAIL silver_estaciones").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### BLOQUE 5 — VACUUM (limpiar obsoletos) — DRY RUN primero

# CELL ********************

# DRY RUN: ver qué borraría sin borrar
spark.sql("VACUUM silver_estaciones RETAIN 168 HOURS DRY RUN").show(truncate=False)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Ejecutar de verdad (168 horas = 7 días de retención)
spark.sql("VACUUM silver_estaciones RETAIN 168 HOURS")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
