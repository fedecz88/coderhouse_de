# coderhouse_de
Proyecto de Data Engineering de Coderhouse

## Entregable N°2
### Objetivo
Se hace uso de Pandas para organizar y limpiar los datos obtenidos de la API para, luego de la transformación y análisis, hacer la carga en la BD.

Se hace la entrega en formato Jupyter para poder hacerle mejor seguimiento de acuerdo a lo solicitado.

**Observación:** A modo de observación de esta entrega, se cambia la forma de obtener los datos del endpoint de Awards debido a que, por ser una API gratiuita, no tiene la mejor performance. Lo que se hizo fue exportar esos datos a un archivo CSV (premios.csv) que se adjunta en la sección Archivos para poder trabajar mejor.

### Requisitos previos
- Instalar la bibliotecas:
    - nba_api               > pip install nba_api
    - sqlalchemy-redshift   > pip install sqlalchemy-redshift
    - redshift_connector    > pip install sqlalchemy-redshift_connector
    - rsa                   > pip install rsa
- Tener la carpeta Archivos con el archivo de configuración, la private key y premios.csv.

### Resumen del proceso
1. Cargar la configuración desde un JSON cifrado con RSA: cargar_configuracion()
2. Ejecutar el proceso EXTRACT de Equipos, Jugadores y Premios. Se cargan los datos obtenidos en DataFrames de Pandas.  [EXTRACT]
3. Analizar la información de los DFs y realizar limpieza (de nulos, duplicados, corrección de formato).  [TRANSFORM]
4. Se hace el merge de los 3 DFs en uno solo (factica_df) que se utilizará para la carga en la BD. 
5. Analizar la información del DF de la fáctica y realizar limpieza.
6. Conectar a la BD Redshift, crear la sesión y la tabla.
7. Iterar la factica_df y cargar los registros en la BD. [LOAD]


## Entregable N°1
### Objetivo
Utilizar una API gratuita de NBA (free-nba) para obtener información de los Equipos y Jugadores (activos e inactivos) de la liga de basquet.

Luego establecer una conexión con AWS RedShift para crear la fáctica que contendrá los datos de los Premios obtenidos por cada Jugador, por Equipo y Temporada.

La interacción con la BD se realiza vía: sqlalchemy

### Requisitos previos
- Instalar la bibliotecas:
    - nba_api               > pip install nba_api
    - sqlalchemy-redshift   > pip install sqlalchemy-redshift
    - redshift_connector    > pip install sqlalchemy-redshift_connector
- Tener la carpeta Archivos con el archivo de configuración y la private key.

### Resumen del proceso
1. Cargar la configuración desde un JSON cifrado con RSA: cargar_configuracion()
2. Establecer la conexión con RedShift y se crea una Session.
3. Crear la tabla fáctica: redshift_crear_factica(table_name, redshift_engine).
4. Ejecutar el proceso EXTRACT de Equipos y Jugadores.
5. Ejecutar el proceso EXTRACT - LOAD de Premios (por cada Jugador se recuperan los premios y se cargan en la fáctica). 

**Observación:** La API gratuita a veces falla por timeout, el error está capturado y lo que se hace es omitir el registro y seguir procesando.

### Información asociada
- Manipulación de AWS: https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/
- API: https://rapidapi.com/theapiguy/api/free-nba/details