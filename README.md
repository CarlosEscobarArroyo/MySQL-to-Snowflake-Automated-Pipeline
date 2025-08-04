# Proyecto de Automatización ETL – MySQL → Snowflake

Este repositorio contiene el código y la configuración necesaria para construir un flujo de integración de datos (ETL) que extrae información desde una base de datos transaccional **MySQL**, realiza limpieza y transformación con **Python**, modela los datos en un esquema de estrella y los carga en **Snowflake**.  La orquestación de las tareas se realiza con **Apache Airflow** dentro de un entorno reproducible en **Docker**.

## Objetivo

El objetivo del proyecto es demostrar cómo automatizar un flujo de datos end‑to‑end partiendo de tablas transaccionales de un ERP hasta un almacén de datos analítico.  Se implementa un proceso semanal que:

1. **Extrae** tablas crudas de MySQL (`persona`, `contenido`, `campana`, `documento` y `documentodetalle`) mediante el conector `pymysql`:contentReference[oaicite:0]{index=0}.  La función `extraer_datos_mysql` devuelve un diccionario de `DataFrame` con estas tablas:contentReference[oaicite:1]{index=1}.
2. **Limpia y transforma** los datos para crear dimensiones (campaña, producto, ubicación, fecha, vendedor y coordinadora) y tablas de staging (`STG_DOCUMENTO` y `STG_DOCUMENTODETALLE`).  La función `limpiar_tablas` elimina columnas irrelevantes, renombra campos, clasifica productos en categorías/subcategorías y convierte fechas:contentReference[oaicite:2]{index=2}:contentReference[oaicite:3]{index=3}.  Se utilizan archivos auxiliares `tabla_cruzada.xlsx` y `dimdate.xlsx` para enriquecer con datos de ubicación y calendario:contentReference[oaicite:4]{index=4}.
3. **Carga** los `DataFrame` a Snowflake utilizando `write_pandas`; se establece la conexión mediante las variables del archivo `.env` y se sobrescriben las tablas de destino:contentReference[oaicite:5]{index=5}:contentReference[oaicite:6]{index=6}.
4. **Construye tablas de hechos** con SQL.  Las vistas de staging se unen con las dimensiones para crear `FACT_PEDIDOS`:contentReference[oaicite:7]{index=7} y `FACT_PEDIDOS_DETALLE`:contentReference[oaicite:8]{index=8}.  El script también ordena numéricamente las campañas y limpia tablas temporales:contentReference[oaicite:9]{index=9}.
5. **Orquesta** todo el flujo con un **DAG de Airflow**, programado semanalmente.  El DAG ejecuta la función ETL, luego las sentencias SQL de creación de tablas, y se ejecuta sin arrastres de períodos previos:contentReference[oaicite:10]{index=10}:contentReference[oaicite:11]{index=11}.

## Tabla de contenidos

1. [Dataset y fuentes de datos](#dataset-y-fuentes-de-datos)
2. [Tecnologías utilizadas](#tecnologias-utilizadas)
3. [Arquitectura del pipeline](#arquitectura-del-pipeline)
4. [Modelado de datos](#modelado-de-datos)
5. [Descripción del ETL](#descripcion-del-etl)
6. [Orquestación con Airflow](#orquestacion-con-airflow)
7. [Configuración del entorno](#configuracion-del-entorno)
8. [Ejecución y uso](#ejecucion-y-uso)

## Dataset y fuentes de datos

El flujo utiliza como origen una base de datos MySQL que contiene la información de ventas de una empresa de comercialización.  Las tablas extraídas son:

| Tabla             | Descripción breve                             |
|-------------------|-----------------------------------------------|
| **persona**       | Datos personales de vendedores y coordinadoras |
| **contenido**     | Catálogo de productos con atributos diversos  |
| **campana**       | Información de campañas comerciales           |
| **documento**     | Pedidos de venta (cabecera)                   |
| **documentodetalle** | Detalles de pedidos (líneas de producto)    |

Para enriquecer los datos se requieren dos archivos Excel locales: `tabla_cruzada.xlsx` (relaciona códigos de ubigeo con descripciones de ubicación) y `dimdate.xlsx` (calendario de fechas):contentReference[oaicite:12]{index=12}.

## Tecnologías utilizadas

El proyecto se construye con las siguientes herramientas y lenguajes:

| Categoría        | Herramientas / Librerías                               |
|------------------|-------------------------------------------------------|
| **Lenguajes**    | Python 3.10, SQL                                      |
| **Extracción**   | `pymysql` para conectar a MySQL:contentReference[oaicite:13]{index=13} |
| **Transformación** | `pandas`, `numpy`, `openpyxl`                         |
| **Carga**        | `snowflake-connector-python` y `pandas_tools.write_pandas`:contentReference[oaicite:14]{index=14} |
| **Orquestación** | Apache Airflow 2.5.1 (Python Operator y Snowflake Operator):contentReference[oaicite:15]{index=15} |
| **Contenedores** | Docker y Docker Compose (con Postgres y Redis para Airflow):contentReference[oaicite:16]{index=16}:contentReference[oaicite:17]{index=17} |
| **Almacén de datos** | Snowflake (dimensionamiento y tablas de hechos) |

## Arquitectura del pipeline

La arquitectura general comprende tres capas principales: extracción, procesamiento y carga/orquestación.  A continuación se describe cada etapa.

### 1. Extracción de MySQL

La función `extraer_datos_mysql` abre una conexión a la base de datos MySQL usando las credenciales definidas en `.env`.  Extrae las tablas `persona`, `contenido`, `campana`, `documento` y `documentodetalle` en forma de `DataFrame`:contentReference[oaicite:18]{index=18}.

### 2. Transformación y limpieza

La función `limpiar_tablas` aplica múltiples transformaciones:contentReference[oaicite:19]{index=19}:

- **Eliminación y renombrado de columnas:** se descartan campos no utilizados y se estandarizan nombres:contentReference[oaicite:20]{index=20}.
- **Clasificación de productos:** se define un diccionario de palabras clave para asignar categorías y subcategorías a cada producto:contentReference[oaicite:21]{index=21}.
- **Dimensiones:** se construyen tablas de dimensiones (campaña, producto, ubicación, fecha, vendedor y coordinadora) y tablas de staging para pedidos y detalles.  Esto facilita un modelo de estrella y la carga posterior en Snowflake:contentReference[oaicite:22]{index=22}.

### 3. Carga a Snowflake

La función `cargar_tablas_en_snowflake` itera sobre los `DataFrame` generados y utiliza `write_pandas` para escribir cada tabla en Snowflake:contentReference[oaicite:23]{index=23}:contentReference[oaicite:24]{index=24}.  La conexión se configura con las variables `SNOWFLAKE_USER`, `SNOWFLAKE_PASSWORD`, `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE`, `SNOWFLAKE_DATABASE` y `SNOWFLAKE_SCHEMA` definidas en el archivo `.env`:contentReference[oaicite:25]{index=25}:contentReference[oaicite:26]{index=26}.

### 4. Construcción de tablas de hechos

Tras cargar las dimensiones y tablas de staging, se ejecutan sentencias SQL para materializar las tablas de hechos:

- **FACT_PEDIDOS**: une `STG_DOCUMENTO` con las dimensiones fecha, vendedor y ubicación para calcular métricas de pedidos (monto total, monto pagado, indicador de nueva vendedora):contentReference[oaicite:27]{index=27}.
- **FACT_PEDIDOS_DETALLE**: combina `STG_DOCUMENTODETALLE` con `FACT_PEDIDOS` y `DIM_PRODUCTO` para obtener cantidades e importes por línea de pedido:contentReference[oaicite:28]{index=28}.  También se numeran las campañas y se eliminan las tablas staging:contentReference[oaicite:29]{index=29}.

> **Figura de arquitectura general del pipeline aquí**

## Modelado de datos

Los datos se estructuran en un **modelo estrella** para optimizar consultas analíticas.  Se utilizan seis tablas de dimensión, dos tablas de staging y dos tablas de hechos.

| Tabla                | Tipo    | Descripción breve                                                     |
|----------------------|---------|-----------------------------------------------------------------------|
| DIM_CAMPANA          | Dimensión | Información de campañas (nombre, estado, fechas)                      |
| DIM_PRODUCTO         | Dimensión | Catálogo de productos con categorías y subcategorías                  |
| DIM_UBICACION        | Dimensión | Datos geográficos y códigos de ubigeo                                 |
| DIM_FECHA            | Dimensión | Calendario con columnas derivadas de fechas                           |
| DIM_VENDEDOR         | Dimensión | Datos de vendedores (identificador, coordinadora, fecha de ingreso)   |
| DIM_COORDINADORA     | Dimensión | Datos de coordinadoras                                                |
| STG_DOCUMENTO        | Staging  | Pedidos crudos transformados                                          |
| STG_DOCUMENTODETALLE | Staging  | Detalle de pedidos crudo transformado                                |
| FACT_PEDIDOS         | Hecho    | Resumen de pedidos con métricas clave                                 |
| FACT_PEDIDOS_DETALLE | Hecho    | Nivel de detalle de productos por pedido                              |

> **Figura del modelo estrella aquí**

## Descripción del ETL

La siguiente lista resume cada función principal del script `etl/etl_script.py`:

1. **extraer_datos_mysql()** – Conecta a MySQL y devuelve un diccionario de `DataFrame` con las tablas base:contentReference[oaicite:30]{index=30}:contentReference[oaicite:31]{index=31}.
2. **limpiar_tablas()** – Recibe los `DataFrame` de origen, aplica limpieza y transformaciones (categorías, subcategorías, fechas, cálculo de edad) y devuelve un diccionario con dimensiones y tablas staging:contentReference[oaicite:32]{index=32}:contentReference[oaicite:33]{index=33}.
3. **cargar_tablas_en_snowflake()** – Carga cada `DataFrame` transformado en su respectiva tabla de Snowflake, sobrescribiéndola si existe:contentReference[oaicite:34]{index=34}:contentReference[oaicite:35]{index=35}.
4. **ejecutar_etl()** – Orquesta el flujo completo llamando a las funciones anteriores en secuencia:contentReference[oaicite:36]{index=36}.  Esta función se utiliza en el DAG de Airflow.

## Orquestación con Airflow

El DAG `dags/glamour_dag.py` define la orquestación del proceso.  Las características clave incluyen:

- **Programación semanal**: el DAG inicia el 1 de enero de 2024 y se ejecuta cada semana:contentReference[oaicite:37]{index=37}.
- **Tareas**: consta de tres tareas encadenadas – una `PythonOperator` que ejecuta el script ETL (`run_etl_script`) y dos `SnowflakeOperator` que ejecutan los SQL de creación de hechos:contentReference[oaicite:38]{index=38}.
- **Sin catch‑up**: el parámetro `catchup=False` evita que se ejecuten períodos atrasados:contentReference[oaicite:39]{index=39}.
- **Plantillas SQL**: las sentencias SQL se almacenan en la carpeta `sql` y se referencian mediante `template_searchpath`:contentReference[oaicite:40]{index=40}.

> **Figura de DAG de Airflow aquí**

## Configuración del entorno

Siga estos pasos para preparar el entorno local usando Docker:

1. **Clonar el repositorio**

   ```bash
   git clone https://github.com/CarlosEscobarArroyo/MySQL-to-Snowflake-Automated-Pipeline.git
   cd MySQL-to-Snowflake-Automated-Pipeline
