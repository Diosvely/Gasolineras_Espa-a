# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "03c80666-ed89-43d3-b081-d04406dd3527",
# META       "default_lakehouse_name": "lh_Gold",
# META       "default_lakehouse_workspace_id": "e8fae30f-df4b-42a1-be74-fd9b5313fd2c",
# META       "known_lakehouses": [
# META         {
# META           "id": "03c80666-ed89-43d3-b081-d04406dd3527"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import (
    col, explode, sequence, to_date, lit,
    year, month, dayofmonth, quarter, dayofweek, date_format
)

# Generar TODAS las fechas del periodo (continuo, sin huecos)
# Desde 01/01/2024 hasta 31/12/2026
df_rango = spark.sql("""
    SELECT explode(sequence(
        to_date('2024-01-01'),
        to_date('2026-12-31'),
        interval 1 day
    )) as Fecha
""")

# Enriquecer con los atributos de calendario
dim_fecha = (df_rango
    .withColumn("Anio",      year("Fecha"))
    .withColumn("Mes",       month("Fecha"))
    .withColumn("Dia",       dayofmonth("Fecha"))
    .withColumn("Trimestre", quarter("Fecha"))
    .withColumn("DiaSemana", dayofweek("Fecha"))
    .withColumn("NombreMes", date_format("Fecha", "MMMM"))
    .withColumn("AnioMes",   date_format("Fecha", "yyyy-MM")))

print(f"dim_fecha: {dim_fecha.count():,} fechas (debería ser ~1096 días = 3 años)")
display(dim_fecha.orderBy("Fecha").limit(10))

# Guardar
dim_fecha.write.mode("overwrite").format("delta").saveAsTable("lh_Gold.dbo.dim_calendario")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
