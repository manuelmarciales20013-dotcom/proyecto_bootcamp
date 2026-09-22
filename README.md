#  TikTok Data Engineering Pipeline — Medallion Lakehouse

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-FF3621?logo=databricks&logoColor=white)](https://databricks.com/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-ACID_Storage-00ADD8?logo=apachespark&logoColor=white)](https://delta.io/)
[![Apache Spark](https://img.shields.io/badge/Apache_Spark-PySpark-E25A1C?logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Unity Catalog](https://img.shields.io/badge/Unity_Catalog-Governance-0275d8)](https://docs.databricks.com/data-governance/unity-catalog/)
[![SQL](https://img.shields.io/badge/Databricks_SQL-Semantics-4B8BBE?logo=mysql&logoColor=white)](https://databricks.com/product/databricks-sql)

---

## 1. ¿Qué hace?
> **Pipeline de ingeniería de datos end-to-end que extrae, transforma y modela contenido de TikTok en el nicho de Data Engineering para descubrir a los creadores líderes, la distribución geográfica de producción y los hashtags de mayor impacto y viralidad.**

---

## 2. ¿Cómo funciona?

El flujo implementa una **Arquitectura Medallion** respaldada por Unity Catalog, garantizando idempotencia en cada etapa mediante operaciones `MERGE` de Delta Lake:

```mermaid
flowchart TD
    API["🌐 API Scraper (Apify TikTok)"] -->|JSON Raw| VOL["📦 Volume Landing<br/><code>archivos/tiktok_raw_*.json</code>"]
    
    subgraph BRONZE_LAYER ["🥉 Capa Bronze"]
        VOL -->|Ingesta Raw + MERGE| BRONZE["Delta Table: <code>bronze.tiktok_bronze</code><br/>(Esquema crudo, tipos STRING, auditoría)"]
    end
    
    subgraph SILVER_LAYER ["🥈 Capa Silver"]
        BRONZE -->|Tipado + Limpieza + EXPLODE hashtags + MERGE| SILVER["Delta Table: <code>silver.silver_tiktok</code><br/>(Tipos nativos, normalización, 1 fila por video x hashtag)"]
    end
    
    subgraph GOLD_LAYER ["🥇 Capa Gold (Modelo Dimensional Estrella)"]
        SILVER -->|SCD Tipo 1| D_AUTOR["<code>gold.dim_autor</code><br/>(Autores únicos)"]
        SILVER -->|SCD Tipo 0| D_FECHA["<code>gold.dim_fecha</code><br/>(Dimensión temporal)"]
        SILVER -->|SCD Tipo 1| D_HASH["<code>gold.dim_hashtag</code><br/>(Catálogo hashtags)"]
        SILVER -->|M:N Bridge| B_HASH["<code>gold.bridge_video_hashtag</code><br/>(video_id ↔ hashtag_id)"]
        SILVER -->|Surrogate Key SHA-256| F_METRICAS["<code>gold.fct_video_metricas</code><br/>(Hechos: plays, likes, comments, shares)"]
    end
    
    subgraph SEMANTICA_LAYER ["🧠 Capa Semántica"]
        D_AUTOR & D_FECHA & D_HASH & B_HASH & F_METRICAS --> VISTAS["Vistas Analíticas (<code>semantica.v_*</code>)<br/>• v_kpi_metricas_generales<br/>• v_top5_autores_likes<br/>• v_videos_por_region<br/>• v_nube_palabras_hashtags"]
    end
    
    subgraph BI_LAYER ["📊 Capa de Consumo"]
        VISTAS --> DASHBOARD["Databricks AI/BI Lakeview<br/>(Dashboard Ejecutivo Horizontal Panorámico)"]
    end
```

---

## 3. ¿Qué tecnologías usa?

- **Plataforma Core:** Databricks Lakehouse Platform con Unity Catalog (`tiktok_data_eng`).
- **Motor de Cómputo:** Apache Spark (PySpark) y Databricks SQL Warehouse.
- **Almacenamiento y Rendimiento:** Delta Lake con transacciones ACID, compactación controlada (`OPTIMIZE`) para consolidación de archivos Parquet y aprovechamiento de Delta Cache en memoria (evitando sobre-ingeniería de particionado en bajo volumen).
- **Orquestación:** Databricks Workflows / Jobs (DAG con tareas concurrentes para dimensiones y secuencial para la tabla de hechos).
- **Modelado Dimensional:** Modelo Estrella (Kimball) con claves sustitutas hash determinísticas (`row_hash` con SHA-256).
- **Plataforma Core:** Databricks Lakehouse Platform con Unity Catalog (`tiktok_data_eng`).
- **Motor de Cómputo:** Apache Spark (PySpark) y Databricks Serverless SQL Warehouse.
- **Almacenamiento y Rendimiento:** Delta Lake con transacciones ACID, compactación periódica (`OPTIMIZE`) para consolidación de binarios Parquet y aprovechamiento de Delta Cache en memoria.
- **Orquestación:** Databricks Workflows / Jobs (DAG de 6 tareas con carga concurrente de dimensiones y resolución secuencial de la tabla de hechos).
- **Modelado Dimensional:** Modelo Estrella (Kimball) con claves sustitutas hash determinísticas (`row_hash` con SHA-256) y clave subrogada de fecha entera (`YYYYMMDD`).
- **Visualización:** Databricks AI/BI Dashboards (Lakeview) optimizado para visualización panorámica horizontal sin scroll vertical.

---

## 4. ¿Cómo lo corro?

### Prerrequisitos
1. Workspace de Databricks con Unity Catalog activo.
2. Catálogo `tiktok_data_eng` con esquemas: `landing`, `bronze`, `silver`, `gold`, `semantica`.
3. Serverless SQL Warehouse o clúster interactivo configurado.

### Paso a Paso

1. **Creación de Infraestructura y Tablas (DDL):**
   Ejecutar en orden los notebooks de `src/DDL/`:
   ```bash
   src/DDL/Bronze/DDL_bronze.ipynb
   src/DDL/Silver/DDL_silver.ipynb
   src/DDL/Gold/DDL_dim_autor.ipynb
   src/DDL/Gold/DDL_dim_fecha.ipynb
   src/DDL/Gold/DDL_dim_hashtag.ipynb
   src/DDL/Gold/DDL_bridge_video_hashtag.ipynb
   src/DDL/Gold/DDL_fct_video_metricas.ipynb
   ```

2. **Carga y Transformación de Datos (ETL):**
   Ejecutar los notebooks de `src/ETL/` o disparar el Job orquestado:
   - `src/ETL/Bronze/ETL_bronze.ipynb`
   - `src/ETL/Silver/ETL_silver.ipynb`
   - `src/ETL/Gold/ETL_dim_autor.ipynb`
   - `src/ETL/Gold/ETL_dim_fecha.ipynb`
   - `src/ETL/Gold/ETL_dim_hashtag.ipynb`
   - `src/ETL/Gold/ETL_bridge_video_hashtag.ipynb`
   - `src/ETL/Gold/ETL_fct_video_metricas.ipynb`

3. **Capa Semántica y Vistas Analíticas:**
   Ejecutar los notebooks en `src/Views/` para instanciar las vistas `v_*` en el esquema `tiktok_data_eng.semantica`:
   - `v_kpi_metricas_generales`
   - `v_top5_autores_likes`
   - `v_videos_por_region`
   - `v_nube_palabras_hashtags`

4. **Visualización y Consumo (AI/BI Lakeview):**
   Los artefactos del dashboard se encuentran versionados en `src/Dashboards/`:
   - `src/Dashboards/dashboard_tiktok_lakeview_v2.json` (Definición activa horizontal).
   - `src/Dashboards/dashboard_tiktok_lakeview.json` (Definición histórica original).
   - `src/Dashboards/TikTok Analytics - Dashboard Ejecutivo 2026-09-22 03_31.pdf` (Reporte exportado en PDF).

5. **Orquestación Automática (Databricks Workflow):**
   Pipeline definido en `src/Jobs/job_tiktok.yml`:
   ```mermaid
   graph LR
       T_Silver["ETL_Silver"] --> T_DimAutor["ETL_Dim_Autor"]
       T_Silver --> T_DimFecha["ETL_Dim_Fecha"]
       T_Silver --> T_DimHash["ETL_Dim_Hashtag"]
       T_Silver --> T_Bridge["ETL_Bridge_Hash"]
       T_DimAutor & T_DimFecha & T_DimHash & T_Bridge --> T_Fact["ETL_Fct_Metricas"]
   ```

---

## 5. ¿Qué resultados tiene?

### 📈 Métricas Clave del Dataset (Limpio y Filtrado — Nicho Ingeniería de Datos)
> *Se aplicó un doble criterio de calidad de datos en la capa Silver: (1) **Media Recortada (Trimmed Mean)** eliminando colas P05-P95 para mitigar outliers extremos, y (2) **Filtro de Relevancia Temática Positiva** enfocándose exclusivamente en búsquedas especializadas de ingeniería de datos para eliminar el sesgo de homonimia y ruido de entretenimiento.*

| Métrica | Valor Obtenido |
|---|---|
| **Total de Videos Analizados (Rango Central 90%)** | `775` videos únicos |
| **Total de Reproducciones Acumuladas** | `13,180,396` (~13.2 Millones) |
| **Total de Likes Acumulados** | `841,051` (~841 Mil) |
| **Media Recortada de Vistas por Video** | `17,007` reproducciones *(vs. 19.7K sesgada)* |
| **Media Recortada de Likes por Video** | `1,085` likes |

### 🛡️ Calidad de Datos: Eliminación del Sesgo de Homonimia (`#data`)
Durante la ingesta inicial en Bronze, la búsqueda general `#data` introdujo severo sesgo debido a su homonimia con el álbum musical *"DATA"* de Tainy y Bad Bunny (canciones virales como *Mojabi Ghost*), así como contenido genérico de noticias y crímenes que diluían el propósito del análisis.
- **Solución arquitectónica en Silver:** Se restringió la ingesta en Silver exclusivamente a los 5 términos especializados del nicho (`ingenieriadedatos`, `databricks`, `dataengineering`, `pyspark`, `dataengineer`). Esta regla positiva erradicó la necesidad de listas negras (`NOT IN`), garantizando un dataset 100% puro del ecosistema de Ingeniería de Datos.

### 🏆 Hallazgos Principales (Insights)
- **Top Creadores por Likes:** Liderado por figuras del ecosistema como `@masana.xx` (188K likes), `@nataliefratto` (95K likes), `@itssimplyjordan` (40K likes), la cuenta oficial de `@airbytehq` (37K likes, herramienta líder de integración de datos) y `@sergi.data` (35K likes, referente hispanohablante).
- **Concentración Regional:** Perú (`PE` con 137 videos), México (`MX` con 114), Estados Unidos (`US` con 73), España (`ES` con 72) y Reino Unido (`GB` con 62) concentran la producción de contenido de ingeniería de datos.
- **Hashtags de Mayor Tracción:** `#dataengineer` (207 videos), `#databricks` (146 videos), `#dataengineering` (210 videos), `#datascience` (233 videos), `#sql` (131 videos) y `#dataanalytics` (115 videos) dominan la conversación comunitaria.

### 🖥️ Dashboard Ejecutivo (Layout Horizontal Panorámico)
Diseñado en **Databricks AI/BI Lakeview** para visualización fluida de una sola pantalla sin scroll vertical:
- **Nivel 1 (Cabecera Superior):** 4 Scorecards ejecutivos (`Total Videos: 775`, `Total Vistas: 13.2M`, `Avg Likes: 1.1K`, `Avg Vistas: 17.0K`).
- **Nivel 2 (Banda Central):** Gráfico de barras de ancho completo con el `Top 5 Autores con Más Likes Acumulados`.
- **Nivel 3 (Banda Inferior Dividida 50/50):**
  - **Izquierda:** Gráfico de Torta (Pie Chart) de `Top 5 Regiones con Mayor Reproducción de Videos` (`PE`, `MX`, `US`, `ES`, `GB`).
  - **Derecha:** Gráfico de barras de `Frecuencia de Aparición de Hashtags (Nube Analítica)` con los términos más representativos.

🔗 **Dashboard en Vivo:** [Abrir en Databricks AI/BI Lakeview](https://dbc-8ad652f9-dac0.cloud.databricks.com/sql/dashboardsv3/01f1b55d263416f8bb8a035313d6e60c)

