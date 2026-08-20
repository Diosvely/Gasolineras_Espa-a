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

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("silver_estaciones")

# Renombrar las columnas con tilde
df = df.withColumnRenamed("Precio_Gases_licuados_del_petróleo", "Precio_GLP")
df = df.withColumnRenamed("Dirección", "Direccion")
df = df.withColumnRenamed("Rótulo", "Rotulo")
df = df.withColumnRenamed("Remisión", "Remision")

print("Nombres nuevos:", df.columns)

# Guardar con overwriteSchema para reemplazar el esquema viejo
df.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("silver_estaciones")

print("✓ Silver actualizado con nombres limpios")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
