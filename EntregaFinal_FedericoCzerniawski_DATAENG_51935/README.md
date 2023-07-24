# coderhouse_de
Proyecto de Data Engineering de Coderhouse

## Entregable N°4 - Final
### Objetivo
Se hace uso de Spark para recuperar datos desde una API y un CSV, Pandas para organizar, limpiar y Transformar los datos obtenidos, y Spark nuevamente para la carga de los datos en RedShift.

Además se agrega el uso de Airflow para poder ejecutar las tareas en forma individual.

### Novedades
- Se agrega el envío de alertas por mail.
- Se agrega el uso de Variables de Airflow (para la parametrización de las alertas).
- Se agrega columna fh_proceso a la fáctica. Para poder tener un control de auditoría de cuando se modifican los datos.
- En la carpeta archivos se agregan print de patalla con los ejemplos de mails de alerta.

### Pasos para la ejecución
1. Hacer pull de toda la carpeta Entregable_4
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

7. Parametrizar las Alertas. En la pestaña `Admin -> Variables` dar de alta las siguientes variables:
    * `SMTP_EMAIL_FROM`: Dirección de correo desde donde se enviarán las alertas (si no se utiliza GMAIL, es necesario configurar el SMTP server en el config.json)
    * `SMTP_PASSWORD`: Clave de aplicación que se obtiene desde la administración de cuentas de GMAIL.
    * `SMTP_EMAIL_TO`: Dirección de correo destino, donde se enviarán las alertas. Puede ser una lista como la siguiente: `xx@gmail.com yy@yahoo.com.ar`
    * `ALERTS_LIST`: Lista de alertas (separadas por espacios en blanco) que se desea recibir. Por ejemplo: `ERROR TIMEOUT AVISO`
    * `ALERT_MAX_TIMEOUT`: Máximo timeout admitido, si se supera se envía un alerta. Recomendado: `0.5`

8. Lanzar el DAG `etl_awards`
    * Observación_1: En caso de querer completar la fáctica desde el CSV, ajustar el parámetro `REGENERAR_FACTICA` en `archivos/config.json` a 1. Dropea la tabla y la re-crea.
    * Observación_2: Se puede ir ajustando el parámetro `PLAYER_ID_UPD` en `archivos/config.json` para simular distintas novedades. Valores posibles: 76003 | 1628389 | 200746 | 335
    * Observación_3: En los "print", se agrega la palabra clave "LOG:" para poder hacer el seguimiento del proceso en el log de la tarea.

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

- Al querer establecer la conexión con Redshift en el ETL sale error de posgre.
    ```Error: Suppressed: org.postgresql.util.PSQLException: FATAL: no PostgreSQL user name specified in startup packet```
    Se había un problema con la variable de entorno, no estaba completa.