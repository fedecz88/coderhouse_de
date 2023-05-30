# coderhouse_de
Proyecto de Data Engineering de Coderhouse

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

### Resumen del proceso
1. Cargar la configuración desde un JSON cifrado con RSA: cargar_configuracion()
2. Establecer la conexión con RedShift y se crea una Session.
3. Crear la tabla fáctica: redshift_crear_factica(table_name, redshift_engine).
4. Ejecutar el proceso EXTRACT de Equipos y Jugadores.
5. Ejecutar el proceso EXTRACT - LOAD de Premios (por cada Jugador se recuperan los premios y se cargan en la fáctica). 


### Información asociada
- Manipulación de AWS: https://aws.amazon.com/blogs/big-data/use-the-amazon-redshift-sqlalchemy-dialect-to-interact-with-amazon-redshift/
- API: https://rapidapi.com/theapiguy/api/free-nba/details
