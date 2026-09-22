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
        D_AUTOR & D_FECHA & D_HASH & B_HASH & F_METRICAS --> VISTAS["Vistas Analíticas (<code>semantica.v_*</code>)<br/>• v_kpi_metricas_generales<br/>• v_videos_por_region<br/>• v_top5_autores_likes<br/>• v_hashtags_mas_vistos<br/>• v_hashtags_mas_likeados<br/>• v_nube_palabras_hashtags"]
    end
    
    subgraph BI_LAYER ["📊 Capa de Consumo"]
        VISTAS --> DASHBOARD["Databricks AI/BI Lakeview<br/>(Dashboard Ejecutivo - Patrón F)"]
    end
```

---

## 3. ¿Qué tecnologías usa?

- **Plataforma Core:** Databricks Lakehouse Platform con Unity Catalog (`tiktok_data_eng`).
- **Motor de Cómputo:** Apache Spark (PySpark) y Databricks SQL Warehouse.
- **Almacenamiento y Rendimiento:** Delta Lake con transacciones ACID, compactación controlada (`OPTIMIZE`) para consolidación de archivos Parquet y aprovechamiento de Delta Cache en memoria (evitando sobre-ingeniería de particionado en bajo volumen).
- **Orquestación:** Databricks Workflows / Jobs (DAG con tareas concurrentes para dimensiones y secuencial para la tabla de hechos).
- **Modelado Dimensional:** Modelo Estrella (Kimball) con claves sustitutas hash determinísticas (`row_hash` con SHA-256).
- **Visualización:** Databricks AI/BI Dashboards (Lakeview) optimizado con Patrón F.

---

## 4. ¿Cómo lo corro?

### Prerrequisitos
1. Workspace de Databricks con Unity Catalog activo.
2. Catálogo `tiktok_data_eng` con esquemas: `landing`, `bronze`, `silver`, `gold`, `semantica`.
3. SQL Warehouse o clúster interactivo configurado.

### Paso a Paso

1. **Creación de Infraestructura y Tablas (DDL):**
   Ejecutar en orden los notebooks de `src/DDL/`:
   ```bash
   src/DDL/Bronze/DDL_bronze.ipynb
   src/DDL/Silver/DDL_silver.ipynb
   src/DDL/Gold/DDL_gold_dims.ipynb
   src/DDL/Gold/DDL_gold_bridge.ipynb
   src/DDL/Gold/DDL_gold_fact.ipynb
   ```

2. **Carga y Transformación de Datos (ETL):**
   Ejecutar los notebooks de `src/ETL/` o disparar el Job orquestado:
   - `src/ETL/Bronze/ETL_bronze.ipynb`
   - `src/ETL/Silver/ETL_silver.ipynb`
   - `src/ETL/Gold/` (ETL de dimensiones en paralelo + hecho fct_video_metricas)

3. **Capa Semántica y Vistas:**
   Ejecutar los notebooks en `src/VIEWS/` para instanciar las vistas `v_*` en el esquema `tiktok_data_eng.semantica`.

4. **Orquestación Automática (Databricks Workflow):**
   Ejecutar el Job `Pipeline_TikTok_Medallion_ETL`:
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
> *Se aplicó un doble criterio de calidad de datos en la capa Silver: (1) **Media Recortada (Trimmed Mean)** eliminando colas P05-P95 para mitigar outliers extremos, y (2) **Filtro Semántico Regex** para desambiguar el término homónimo `#data` y eliminar ruido musical (álbum "DATA" de Tainy/Bad Bunny y virales ajenos).*

| Métrica | Valor Obtenido |
|---|---|
| **Total de Videos Analizados (Ingeniería de Datos)** | `898` videos únicos |
| **Total de Reproducciones Acumuladas** | `162,455,344` (~162.5 Millones) |
| **Total de Likes Acumulados** | `12,822,114` (~12.8 Millones) |
| **Media Recortada de Vistas por Video** | `180,908` reproducciones |
| **Media Recortada de Likes por Video** | `14,279` likes |

### 🛡️ Calidad de Datos: Desambiguación Semántica del Término `#data`
Durante la extracción en Bronze con la búsqueda `#data`, se descubrió que TikTok asociaba el hashtag al álbum musical de reggaetón *"DATA"* del productor Tainy y Bad Bunny (canciones como *Mojabi Ghost*), trayendo cientos de videos musicales con millones de reproducciones irrelevantes.
- **Solución implementada en Silver:** Regla de negocio en SQL mediante expresiones regulares (`RLIKE`) que valida que la descripción contenga vocabulario técnico del ecosistema de datos (`engineer`, `sql`, `python`, `etl`, `pipeline`, `spark`, `databricks`, `bi`, etc.) y excluye términos de música y entretenimiento (`badbunny`, `tainy`, `mojabi`, `lyrics`, `reggaeton`).

### 🏆 Hallazgos Principales (Insights)
- **Top Creadores por Likes:** Liderado legítimamente por educadores y profesionales del rubro como `@pablo.maxmaxdata` (947K likes), `@chartosaur` (546K likes), `@imelijahbutler` (363K likes), `@datachictiktok` (357K likes) y `@marytheanalyst` (294K likes).
- **Concentración Regional:** Perú (`PE` con 138 videos), México (`MX` con 116), Estados Unidos (`US` con 116), España (`ES` con 83) y Reino Unido (`GB` con 81) concentran la producción de contenido técnico.
- **Hashtags de Mayor Tracción:** `#ingenieriadedatos` (467 videos), `#data` técnico (273 videos), `#datascience` (258 videos), `#dataengineering` (212 videos) y `#dataengineer` (207 videos) dominan el ecosistema.

### 🖥️ Dashboard Ejecutivo (Patrón F)
- **Cabecera Horizontal Superior:** 4 Scorecards de KPIs clave (Totales y Promedios limpios).
- **Banda Intermedia:** Gráfico de barras del Top 5 de Regiones Geográficas (`PE`, `MX`, `US`, `ES`, `GB`).
- **Bloque Vertical Inferior:** Rankings de Autores, Hashtags con más vistas, Hashtags récord de likes y Nube de términos.
