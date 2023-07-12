# coderhouse_de
Proyecto de Data Engineering de Coderhouse

## Entregable N°3
### Objetivo
Se hace uso de Spark para recuperar datos desde una API y un CSV, Pandas para organizar, limpiar y Transformar los datos obtenidos, y Spark nuevamente para la carga de los datos en RedShift.

Además se agrega el uso de Airflow para poder ejecutar las tareas en forma individual.

### Requisitos previos
- Instalar la bibliotecas:
    - nba_api               > pip install nba_api
    - rsa                   > pip install rsa
- Tener la carpeta Archivos con el archivo de configuración, la private key y premios.csv.

### Resumen del proceso
1. Crear la imagen de Airflow y Spark utilizando los dockerfiles de la carpeta docker_images.

1. Cargar la configuración desde un JSON cifrado con RSA: cargar_configuracion()
2. Ejecutar el proceso EXTRACT de Equipos, Jugadores y Premios. Se cargan los datos obtenidos en DataFrames de Pandas.  [EXTRACT]
3. Analizar la información de los DFs y realizar limpieza (de nulos, duplicados, corrección de formato).  [TRANSFORM]
4. Se hace el merge de los 3 DFs en uno solo (factica_df) que se utilizará para la carga en la BD. 
5. Analizar la información del DF de la fáctica y realizar limpieza.
6. Conectar a la BD Redshift, crear la sesión y la tabla (por parámetro se puede seleccionar solo actualizar la tabla).
7. Iterar la factica_df y cargar los registros en la BD. [LOAD]