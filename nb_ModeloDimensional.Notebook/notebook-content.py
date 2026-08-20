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
# META           "id": "54ce9ba1-4b65-48ce-a25c-8998621bd226"
# META         },
# META         {
# META           "id": "03c80666-ed89-43d3-b081-d04406dd3527"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# ================================================================
# NOTEBOOK GOLD — Star Schema (Proyecto Combustible)
# ----------------------------------------------------------------
# De silver_estaciones (ANCHO, nombres limpios) -> modelo dimensional:
#   - gold_fact_precios   (hechos, formato LARGO)
#   - gold_dim_estacion   (dimensión gasolinera, Tipo 1)
#   - gold_dim_geografia  (dimensión geográfica)
#   - gold_dim_fecha      (dimensión calendario)
#
# Asociar a lh_silver (leer) y lh_gold (escribir).
# ================================================================
 
from pyspark.sql.functions import (
    col, expr, year, month, dayofmonth, quarter, dayofweek,
    date_format, trim
)
 
# ================================================================
# PASO 1 — Leer Silver
# ================================================================
df = spark.read.table("lh_silver.silver_estaciones")
 
# Limpieza menor: quitar espacios de relleno en textos (ej. "PINTO      ")
for c in ["Localidad", "Municipio", "Provincia"]:
    df = df.withColumn(c, trim(col(c)))
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ================================================================
# PASO 2 — TABLA DE HECHOS (formato LARGO con stack/unpivot)
# ================================================================
fact = df.select(
    "IDEESS", "IDProvincia", "IDMunicipio", "IDCCAA",
    "FechaDato",
    expr("""
        stack(7,
            'Gasoleo_A',           Precio_Gasoleo_A,
            'Gasolina_95_E5',      Precio_Gasolina_95_E5,
            'Gasoleo_Premium',     Precio_Gasoleo_Premium,
            'Gasolina_98_E5',      Precio_Gasolina_98_E5,
            'Gasoleo_B',           Precio_Gasoleo_B,
            'GLP',                 Precio_GLP,
            'Gasolina_95_Premium', Precio_Gasolina_95_E5_Premium
        ) as (TipoCombustible, Precio)
    """)
)
fact = fact.filter(col("Precio").isNotNull())
print(f"Hechos (largo): {fact.count():,} filas")
display(fact.limit(20))
 
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# PASO 3 — DIMENSIÓN ESTACIÓN (Tipo 1)
# ================================================================
dim_estacion = (df.select(
        "IDEESS", "Rotulo", "Direccion", "Margen",
        "Tipo_Venta", "Remision", "Latitud", "Longitud_WGS84"
    ).dropDuplicates(["IDEESS"]))
print(f"dim_estacion: {dim_estacion.count():,} gasolineras")
 

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# PASO 4 — DIMENSIÓN GEOGRAFÍA
# ================================================================
dim_geografia = (df.select(
        "IDMunicipio", "Municipio", "Localidad", "C_P",
        "IDProvincia", "Provincia", "IDCCAA"
    ).dropDuplicates(["IDMunicipio"]))
print(f"dim_geografia: {dim_geografia.count():,} municipios")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# ================================================================
# PASO 6 — # Guardar en Gold (dos partes: lakehouse.tabla, SIN dbo)
# ================================================================
fact.write.mode("overwrite").format("delta").saveAsTable("fact_precios")
dim_estacion.write.mode("overwrite").format("delta").saveAsTable("dim_estacion")
dim_geografia.write.mode("overwrite").format("delta").saveAsTable("dim_geografia")



print("\n? Gold guardado: 1 tabla de hechos + 2 dimensiones")
print(f"  fact_precios:  {fact.count():,} filas")
print(f"  dim_estacion:  {dim_estacion.count():,}")
print(f"  dim_geografia: {dim_geografia.count():,}")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
