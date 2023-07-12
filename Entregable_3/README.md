# coderhouse_de
Proyecto de Data Engineering de Coderhouse

## Entregable N°3
### PENDIENTES
Habilitar el ETL utilizando Spark.
Por el momento está entregado con una función dummy que se utilizó para probar el envío de parámetros de configuración a Spark.

### Objetivo
Se hace uso de Spark para recuperar datos desde una API y un CSV, Pandas para organizar, limpiar y Transformar los datos obtenidos, y Spark nuevamente para la carga de los datos en RedShift.

Además se agrega el uso de Airflow para poder ejecutar las tareas en forma individual.

### Pasos para la ejecución
1. Hacer pull de toda la carpeta Entregable_3
2. Crear las carpetas necesarias:
```bash
mkdir -p dags,logs,plugins,postgres_data,scripts
```
3. Generar las imágenes de Airflow y Spark. Los dockerfiles están ubicados en:`docker_images/`. Es necesario generarlas debido a que están personalizadas, se incluyen las bibliotecas necesarias para la ejecución (nba_api, rsa).
```bash
docker build -t my_airflow_2_6_2 .
docker build -t my_spark .
```
4. Ejecutar el docker-compose para crear los contenedores configurados con: La variable de entorno .env - El alta de directorios scripts y archivos (y copia de los archivos que contiene)
```bash
docker-compose up --build
```
5. Acceder al Airflow (airflow/airflow).
6. Configurar la conexión con Spark. En la pestaña `Admin -> Connections` crear una nueva conexión con los siguientes datos:
    * Conn Id: `spark_default`
    * Conn Type: `Spark`
    * Host: `spark://spark`
    * Port: `7077`

7. Lanzar el DAG `etl_awards`
    * Observación_1: En caso de querer completar la fáctica desde el CSV, ajustar el parámetro `REGENERAR_FACTICA` en `archivos/config.json` a 1. Dropea la tabla y la re-crea.
    * Observación_2: Se puede ir ajustando el parámetro `PLAYER_ID_UPD` en `archivos/config.json` para simular distintas novedades. Valores posibles: 76003 | 1628389 | 200746 | 335

### Resumen del proceso
1. Carga de la configuración para la app en un diccionario. Se carga desde un JSON cifrado con RSA y con las variables de entorno. Función: cargar_configuracion()
2. Ejecución del DAG `etl_awards`:
    2.1. Task `create_table_task`: Crea la tabla fáctica si no existe, si el parámetro `REGENERAR_FACTICA == 1` (config.json), la elimina y la re-crea.
    2.2. Task `clean_task`: Elimina los registros asociados al id del parámetro `PLAYER_ID_UPD` (config.json). Es para simular la carga de una novedad.
    2.3. Task `spark_etl_task`: Delega la ejecución del ETL al nodo de Spark.

### Detalle del ETL (`scripts/etl_awards_process.py`)
1. Crear la sesión de Spark.
2. Ejecutar el proceso EXTRACT de Equipos, Jugadores y Premios. Se utiliza Spark para esta tarea.  [EXTRACT]
3. Se cargan los datos obtenidos en DataFrames de Pandas.
4. Analizar la información de los DFs y realizar limpieza (de nulos, duplicados, corrección de formato). Se utiliza Pandas.  [TRANSFORM]
5. Se hace el merge de los 3 DFs en uno solo (factica_df) que se utilizará para la carga en la BD. 
6. Analizar la información del DF de la fáctica y realizar limpieza.
7. Delegar a Spark la carga en Redshift del DataFrame asociado a la factica. [LOAD]

### Problemas detectados y su solución
- Lentitud y desconexión constante del Airflow (se caía el webserver, no se podía conectar a la base postgre, etc). Resultó ser un tema de recursos, se ajustó el .wsl del Docker para que pueda utilizar más memoria.

- Al ejecutar el docker-compose, a veces, tira error al querer intentar levantar el Airflow porque indica que la bd postgresql no está saludable (unhealthy). No implica un problema, si bien el docker-compose se cancela, se puede lanzar nuevamente sin problemas.