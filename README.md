⛽ CombustibleSpain — Análisis de precios de carburante en España

Solución analítica de extremo a extremo sobre Microsoft Fabric, construida como proyecto de preparación para la certificación DP-600 (Implementing Analytics Solutions Using Microsoft Fabric).

Analiza los precios oficiales de los carburantes en las estaciones de servicio de España: evolución temporal, comparativa geográfica (con Canarias como caso de estudio), rankings de provincias y marcas, y diferencias entre tipos de combustible.

📊 Arquitectura

Arquitectura medallion (Bronze → Silver → Gold) sobre Lakehouses de Fabric, con un modelo semántico en Direct Lake encima de la capa Gold.

Fuente oficial de precios
        │
        ▼
   lh_Bronze  ──►  lh_Silver  ──►  lh_Gold  ──►  Modelo semántico  ──►  Informe Power BI
  (datos crudos)   (limpieza)    (modelado)      (ms_Combustible)     (rpt_CombustibleSpain)
Capa	Elemento	Rol
Bronze	lh_bonceCombustible	Ingesta de datos crudos
Silver	lh_Silver	Limpieza y estandarización
Gold	lh_Gold	Modelo dimensional listo para consumo
Semántico	ms_Combustible	Modelo Direct Lake sobre lh_Gold
Informe	rpt_CombustibleSpain_Analisis	4 páginas de análisis

Orquestación mediante un task flow de Fabric que refleja el flujo Bronze → Silver → Gold → Visualize.

⭐ Modelo semántico
<img width="615" height="331" alt="image" src="https://github.com/user-attachments/assets/665e8540-f9bc-458f-bd15-8cdbbc55eeef" />

Esquema en estrella con fact_precios en el centro y cuatro dimensiones. Todas las tablas de datos en modo Direct Lake; la tabla de medidas es calculada.

Relaciones (todas activas, Many→One, filtro unidireccional):

Desde (Many)	Hacia (One)
fact_precios[IDEESS]	dim_estacion[IDEESS]
fact_precios[IDCombustible]	dim_combustible[IDCombustible]
fact_precios[FechaDato]	dim_calendario[Fecha]
fact_precios[IDMunicipio]	dim_geografia[IDMunicipio]

34 medidas DAX organizadas en carpetas de visualización:

01_Precios Base — precio medio, mín, máx, desviación, rango
02_Conteo y Cobertura — nº gasolineras, registros, combustibles
03_Precio por Combustible — Gasolina 95, Gasóleo A, premium, diferencial
04_Inteligencia de Tiempo — YTD, año anterior, mes anterior, media móvil 30d, variación YoY (abs y %)
05_Análisis Geográfico — Canarias, Península, nacional, diferencia vs nacional
06_Rankings — provincia cara/barata, marca cara (RANKX)
07_KPIs Formato — precio actual, semáforo, texto dinámico, ALLSELECTED, ALLEXCEPT
08_Geolocalizacion — latitud/longitud media por contexto (ver decisión #3)

Documentación completa del modelo y las medidas en docs/ms_Combustible_modelo.md.

📈 Informe

rpt_CombustibleSpain_Analisis — thin report conectado en vivo al modelo:

Resumen Ejecutivo — KPIs (precio actual, variación YoY, nº gasolineras, última fecha), tendencia con media móvil, precio por provincia, slicer de combustible.
Análisis Temporal — precio medio vs año anterior, YTD, variación YoY % por mes.
Análisis Geográfico — Canarias vs Península, ahorro, diferencia vs nacional por provincia, tabla con ranking y semáforo.
Marcas y Combustibles — ranking de marcas, precio por tipo de combustible, diferencial premium.

Cifras clave: precio actual 1,78 € · media histórica 1,51 € · ahorro Canarias 0,228 € · diferencial premium 0,115 € · 11.758 gasolineras.

🧩 Decisiones técnicas
1. Direct Lake en lugar de Import

El modelo consulta directamente los Parquet de lh_Gold sin importar datos. Consultas rápidas sobre volúmenes grandes y sin duplicar el almacenamiento. Contrapartida: el modelo depende de que el Lakehouse exista (ver Limitaciones).

2. Corrección del desajuste calendario vs datos

dim_calendario llega hasta el 31/12/2026, pero fact_precios solo tiene datos hasta el 15/08/2026. Dos medidas (Precio Actual y Ultima Fecha) usaban MAX(dim_calendario[Fecha]) y devolvían una fecha sin datos (BLANK / 31-dic). Se corrigieron tomando el máximo del hecho y liberando el filtro de calendario:

dax
CALCULATE ( MAX ( fact_precios[FechaDato] ), ALL ( dim_calendario ) )
3. Geolocalización cruzando dimensiones (CROSSFILTER)

Al construir el mapa por provincia, todas las provincias devolvían la misma coordenada (el centro de España). Causa: dim_geografia y dim_estacion cuelgan ambas de fact_precios con relaciones unidireccionales, así que el filtro de provincia no llega a la tabla de estaciones (donde están las coordenadas). Solución sin tocar el diseño del modelo — filtrado bidireccional local en la medida:

dax
Latitud Provincia =
CALCULATE (
    AVERAGE ( dim_estacion[Latitud] ),
    CROSSFILTER ( fact_precios[IDEESS], dim_estacion[IDEESS], BOTH )
)
4. Barras por provincia en vez de mapa (fallback)

Los visuales de mapa estaban deshabilitados a nivel de tenant durante gran parte del desarrollo. El análisis geográfico se resolvió con barras + tabla; el modelo quedó preparado (dataCategories de Latitud/Longitud/Provincia/Municipio/C_P) para migrar a mapa cuando se habilite.

5. Edición del modelo con TMDL y ref table

Las medidas se modificaron desde la vista TMDL usando ref table _Medidas (referencia a la tabla), no createOrReplace table — que habría reemplazado la tabla entera y borrado el resto de medidas.

🔁 Control de versiones (Git)

Workspace conectado a este repositorio mediante la integración Git de Fabric.

Qué se versiona: definición del modelo semántico (TMDL), definición del informe (páginas y visuales), y metadatos de los Lakehouses (esquema y nombres de tabla).

Qué NO se versiona: los datos físicos de los Lakehouses (los Parquet de Bronze/Silver/Gold). Git guarda la definición del proyecto, no los datos.

Nota de configuración inicial: Fabric no puede conectar contra un repositorio completamente vacío. Si el primer commit falla, inicializa el repo con una rama main que tenga al menos este README, y vuelve a conectar el workspace apuntando a esa rama.

⚠️ Limitaciones y estado
Entorno de trial. Construido sobre una capacidad de prueba de Fabric. Al expirar el trial, lh_Gold deja de estar disponible y, por ser Direct Lake, el modelo dejaría de funcionar. Este repositorio conserva la definición del proyecto, no los datos.
Ventana de datos: hasta el 15/08/2026.
Mapa geográfico: pendiente de conectar las medidas Latitud/Longitud Provincia al visual (habilitar antes "Upgrade map" → mapa de Azure).
📚 Relación con el DP-600

Este proyecto cubre elementos de las cuatro áreas del examen:

Área del examen	Cubierto en el proyecto
Preparar y servir datos	Arquitectura medallion, Lakehouses, Direct Lake
Implementar y gestionar modelos	Star schema, relaciones, 34 medidas DAX, TMDL
Explorar y analizar datos	Consultas DAX con funciones INFO, análisis del modelo
Mantener la solución	Integración Git, versionado, ciclo de vida (ALM)

Conceptos DAX aplicados: contexto de filtro, CALCULATE, ALL/ALLSELECTED/ALLEXCEPT, CROSSFILTER, inteligencia de tiempo (TOTALYTD, SAMEPERIODLASTYEAR, DATESINPERIOD), RANKX, SWITCH, DIVIDE.

📂 Estructura del repositorio
/
├── README.md
├── ms_Combustible.SemanticModel/     # TMDL: tablas, relaciones, medidas
├── rpt_CombustibleSpain_Analisis.Report/   # Definición del informe
└── docs/
    ├── ms_Combustible_modelo.md      # Documentación del modelo y medidas
    └── guia_git_fabric.md            # Guía de integración Git en Fabric

Proyecto de preparación DP-600 · Microsoft Fabric · Agosto 2026
