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

# #### BACKFILL HISTÓRICO — Bronze (crudo completo)

# CELL ********************

# ============================================
# BACKFILL HISTÓRICO — Bronze (crudo completo)
# Solo corrige nombres (tildes/puntos). NO elimina columnas.
# ============================================
import requests
import re
import time
from datetime import date, timedelta
from pyspark.sql.functions import lit, current_timestamp

# --- CONFIGURACIÓN ---
FECHA_INICIO = date(2024, 8, 17)
FECHA_FIN    = date(2026, 8, 17)
FRECUENCIA_DIAS = 7
URL_BASE = "https://energia.serviciosmin.gob.es/ServiciosRestCarburantes/PreciosCarburantes/EstacionesTerrestresHist"
TABLA_DESTINO = "bronze_estaciones"
PAUSA_SEGUNDOS = 2

# --- ARRANQUE LIMPIO: borrar tabla previa de prueba (ejecutar solo la 1ª vez) ---
spark.sql(f"DROP TABLE IF EXISTS {TABLA_DESTINO}")
print("Tabla anterior borrada. Empezando histórico de cero.")

# --- FUNCIÓN: cargar una fecha ---
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
    fecha_api = data.get("Fecha", fecha_str)

    df = spark.createDataFrame(lista)
    # Las dos fechas (ambas STRING para consistencia)
    df = df.withColumn("FechaDato", lit(fecha_str))
    df = df.withColumn("FechaCarga", lit(str(date.today())))   # STRING, no timestamp
    # Limpiar SOLO nombres (tildes, puntos, espacios, símbolos)
    nuevas = [re.sub(r'[ .,;{}()\n\t=%]+', '_', c).strip('_') for c in df.columns]
    df = df.toDF(*nuevas)
    # Guardar crudo completo (sin eliminar columnas)
    df.write.mode("append").format("delta").saveAsTable(TABLA_DESTINO)
    return df.count()

# --- BUCLE PRINCIPAL ---
fecha = FECHA_INICIO
cargas, fallos, filas_total = 0, 0, 0
print(f"Backfill: {FECHA_INICIO} → {FECHA_FIN} (cada {FRECUENCIA_DIAS} días)")
print("=" * 60)

while fecha <= FECHA_FIN:
    filas = cargar_fecha(fecha)
    if filas is not None:
        print(f"  ✓ {fecha.strftime('%d-%m-%Y')}: {filas} filas")
        cargas += 1
        filas_total += filas
    else:
        fallos += 1
    time.sleep(PAUSA_SEGUNDOS)
    fecha += timedelta(days=FRECUENCIA_DIAS)

print("=" * 60)
print(f"COMPLETO. Cargas: {cargas} | Fallos: {fallos} | Filas: {filas_total}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# ¿Cuántas filas totales tengo ahora?
df = spark.read.table("bronze_estaciones")
print("Total filas:", df.count())

# ¿Cuántas fechas distintas cargué? (deberían ser ~104 si no hubo fallos)
df.select("FechaDato").distinct().orderBy("FechaDato").show(110, truncate=False)

# ¿Rango de fechas? (la primera y la última)
from pyspark.sql.functions import min, max
df.select(min("FechaDato"), max("FechaDato")).show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import to_date, col, min, max

df = spark.read.table("bronze_estaciones")

# Convertir el string "DD-MM-YYYY" a fecha real (solo para verificar)
df_check = df.withColumn("FechaReal", to_date(col("FechaDato"), "dd-MM-yyyy"))

# Ahora sí, min/max cronológicos correctos:
df_check.select(min("FechaReal"), max("FechaReal")).show()

# Fechas ordenadas cronológicamente de verdad:
df_check.select("FechaReal").distinct().orderBy("FechaReal").show(110)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
