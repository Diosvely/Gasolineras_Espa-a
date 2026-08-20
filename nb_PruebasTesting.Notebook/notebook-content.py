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

# CELL ********************

# Aqui verificamos todo lo que teniamos cargado antes de actualizar para ver si funciona 

from pyspark.sql.functions import to_date, col

df = spark.read.table("bronze_estaciones")

# Contar filas totales y fechas distintas ANTES
print("Filas ANTES:", df.count())
print("Fechas distintas ANTES:", df.select("FechaDato").distinct().count())

# Ver las últimas fechas cargadas
(df.withColumn("FechaReal", to_date(col("FechaDato"), "dd-MM-yyyy"))
   .select("FechaReal").distinct().orderBy(col("FechaReal").desc()).show(5))
   

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


   
   # Borrar la semana 2026-08-15 (formato DD-MM-YYYY como está en FechaDato)
spark.sql("DELETE FROM bronze_estaciones WHERE FechaDato = '15-08-2026'")

# Verificar que se borró
df = spark.read.table("bronze_estaciones")
print("Filas DESPUÉS de borrar:", df.count())
print("Fechas distintas DESPUÉS de borrar:", df.select("FechaDato").distinct().count())

# Confirmar cuál es ahora la última fecha (debería ser 2026-08-08)
from pyspark.sql.functions import to_date, col
(df.withColumn("FechaReal", to_date(col("FechaDato"), "dd-MM-yyyy"))
   .select("FechaReal").distinct().orderBy(col("FechaReal").desc()).show(3))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
