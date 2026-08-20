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
# META         },
# META         {
# META           "id": "54ce9ba1-4b65-48ce-a25c-8998621bd226"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# dim_combustible con ATRIBUTOS (aquí está el valor real)
from pyspark.sql import Row

combustibles = [
    # (id, nombre, categoria, es_premium, es_renovable)
    Row(IDCombustible=1, TipoCombustible="Gasoleo_A",           Categoria="Diesel",   EsPremium=False, EsRenovable=False),
    Row(IDCombustible=2, TipoCombustible="Gasolina_95_E5",      Categoria="Gasolina", EsPremium=False, EsRenovable=False),
    Row(IDCombustible=3, TipoCombustible="Gasoleo_Premium",     Categoria="Diesel",   EsPremium=True,  EsRenovable=False),
    Row(IDCombustible=4, TipoCombustible="Gasolina_98_E5",      Categoria="Gasolina", EsPremium=True,  EsRenovable=False),
    Row(IDCombustible=5, TipoCombustible="Gasoleo_B",           Categoria="Diesel",   EsPremium=False, EsRenovable=False),
    Row(IDCombustible=6, TipoCombustible="GLP",                 Categoria="Gas",      EsPremium=False, EsRenovable=False),
    Row(IDCombustible=7, TipoCombustible="Gasolina_95_Premium", Categoria="Gasolina", EsPremium=True,  EsRenovable=False),
]
dim_combustible = spark.createDataFrame(combustibles)
dim_combustible.write.mode("overwrite").format("delta").saveAsTable("dim_combustible")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ============================================
# Actualizar fact_precios: reemplazar el string TipoCombustible por IDCombustible
# ============================================

# 1. Leer la tabla de hechos desde Gold (ya no está en memoria)
fact = spark.read.table("fact_precios")           # si lh_Gold es el default
# (o "lh_Gold.dbo.fact_precios" según cómo la guardaste)

# 2. Leer la dim_combustible que creaste
dim_combustible = spark.read.table("dim_combustible")

# 3. Unir para traer el IDCombustible, y quitar el string
fact = fact.join(
    dim_combustible.select("IDCombustible", "TipoCombustible"),
    on="TipoCombustible",
    how="left"
).drop("TipoCombustible")   # quitar el string, dejar solo el ID entero

# 4. Verificar antes de guardar
print("Columnas de fact ahora:", fact.columns)
display(fact.limit(10))

# 5. Volver a guardar la tabla de hechos (sobrescribir con la versión nueva)
fact.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("fact_precios")

print("✓ fact_precios actualizada: ahora usa IDCombustible (entero) en vez del string")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_Gold.dbo.dim_geografia LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM lh_Gold.dbo.dim_estacion LIMIT 10")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Leer los hechos actuales
fact = spark.read.table("fact_precios")

# Quedarte solo con las claves necesarias + métricas
# Claves de relación: IDEESS (estación), IDMunicipio (geografía), FechaDato (fecha), IDCombustible
fact = fact.select(
    "IDEESS",           # → dim_estacion
    "IDMunicipio",      # → dim_geografia (cubre provincia y CCAA)
    "FechaDato",        # → dim_fecha
    "IDCombustible",    # → dim_combustible
    "Precio"            # la métrica
)

# Guardar (con overwriteSchema porque quitas columnas)
fact.write.mode("overwrite") \
    .option("overwriteSchema", "true") \
    .format("delta") \
    .saveAsTable("fact_precios")

print("✓ Hechos limpios:", fact.columns)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
