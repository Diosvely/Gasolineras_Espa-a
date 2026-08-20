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

# #### NOTEBOOK: INGESTA INCREMENTAL (watermark)

# CELL ********************

# ============================================
# NOTEBOOK: INGESTA INCREMENTAL (watermark)
# Se programa cada 7 días. Trae desde la última fecha cargada.
# ============================================
import requests
import re
import time
from datetime import date, timedelta
from pyspark.sql.functions import lit, to_date, col, max as spark_max

# --- CONFIGURACIÓN ---
FRECUENCIA_DIAS = 7
URL_BASE = "https://energia.serviciosmin.gob.es/ServiciosRestCarburantes/PreciosCarburantes/EstacionesTerrestresHist"
TABLA_DESTINO = "bronze_estaciones"
PAUSA_SEGUNDOS = 2

# --- PASO 1: Encontrar el WATERMARK (última fecha ya cargada) ---
df_bronze = spark.read.table(TABLA_DESTINO)
ultima = (df_bronze
          .withColumn("FechaReal", to_date(col("FechaDato"), "dd-MM-yyyy"))
          .agg(spark_max("FechaReal").alias("max_fecha"))
          .collect()[0]["max_fecha"])

if ultima is None:
    print("⚠️ Bronze vacío. Arrancando desde hace 7 días.")
    fecha_desde = date.today() - timedelta(days=FRECUENCIA_DIAS)
else:
    fecha_desde = ultima + timedelta(days=FRECUENCIA_DIAS)
    print(f"Watermark (última fecha cargada): {ultima}")

fecha_hasta = date.today()
print(f"Cargando desde {fecha_desde} hasta {fecha_hasta}")

# --- FUNCIÓN: cargar una fecha (idéntica al backfill, sin eliminar columnas) ---
def cargar_fecha(fecha_obj):
    fecha_str = fecha_obj.strftime("%d-%m-%Y")
    url = f"{URL_BASE}/{fecha_str}"
    try:
        response = requests.get(url, timeout=30)
    except Exception as e:
        print(f"  ⚠️ {fecha_str}: error de conexión ({e})")
        return None
    if response.status_code != 200:
        print(f"  ⚠️ {fecha_str}: status {response.status_code}")
        return None
    try:
        data = response.json()
    except Exception:
        print(f"  ⚠️ {fecha_str}: JSON inválido")
        return None
    lista = data.get("ListaEESSPrecio", [])
    if not lista:
        print(f"  ⚠️ {fecha_str}: lista vacía")
        return None

    df = spark.createDataFrame(lista)
    df = df.withColumn("FechaDato", lit(fecha_str))
    df = df.withColumn("FechaCarga", lit(str(date.today())))
    nuevas = [re.sub(r'[ .,;{}()\n\t=%]+', '_', c).strip('_') for c in df.columns]
    df = df.toDF(*nuevas)
    df.write.mode("append").format("delta").saveAsTable(TABLA_DESTINO)
    return df.count()

# --- PASO 2: Cargar solo lo nuevo ---
if fecha_desde > fecha_hasta:
    print("✓ Ya está todo al día. Nada nuevo que cargar.")
else:
    fecha = fecha_desde
    cargas = 0
    while fecha <= fecha_hasta:
        filas = cargar_fecha(fecha)
        if filas is not None:
            print(f"  ✓ {fecha.strftime('%d-%m-%Y')}: {filas} filas")
            cargas += 1
        time.sleep(PAUSA_SEGUNDOS)
        fecha += timedelta(days=FRECUENCIA_DIAS)
    print(f"Incremental completo. {cargas} nuevas cargas.")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
