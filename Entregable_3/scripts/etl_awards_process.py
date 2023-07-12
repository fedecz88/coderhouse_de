#!pip install nba_api

from nba_api.stats.static import players, teams
from nba_api.stats.endpoints import playerawards

import pandas as pa

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType

from datetime import datetime as dt

import sys, os

config_dic = {}

def cargar_configuracion(lst):
    config_dic['URL_REDSHIFT'] = os.getenv('REDSHIFT_URL')
    config_dic['DATABASE_REDSHIFT'] = os.getenv('REDSHIFT_DB')
    config_dic['SCHEMA_REDSHIFT'] = os.getenv('REDSHIFT_SCHEMA')
    config_dic['HOST_REDSHIFT'] = os.getenv('REDSHIFT_HOST')
    config_dic['PORT_REDSHIFT'] = os.getenv('REDSHIFT_PORT')
    config_dic['DRIVER_PATH'] = os.getenv('DRIVER_PATH')
    config_dic['JDBC_DRIVER_REDSHIFT'] = os.getenv('JDBC_DRIVER_REDSHIFT')

    config_dic['USER_REDSHIFT'] = lst[1]
    config_dic['PASS_REDSHIFT'] = lst[2]
    config_dic['FACT_TABLE_NAME'] = lst[3]
    config_dic['REGENERAR_FACTICA'] = int(lst[4])
    config_dic['REQUEST_TIMEOUT'] = int(lst[5])
    config_dic['PLAYER_ID_UPD'] = int(lst[6])
    config_dic['CSV_PREMIOS'] = lst[7]

##  ***************************** EXTRACT ***************************** 
"""
    El objetivo es recuperar los datos de Equipos, Jugadores y Premios desde la API (o CSV) utilizando Spark para distribuir la carga.
    Finalmente los DF de Spark se convierten a Pandas para poder hacer la manipulación de los datos.
"""
def extact_premios(spark_s):
    try:
        schema = StructType([
                    StructField("PERSON_ID", StringType(), True),
                    StructField("FIRST_NAME", StringType(), True),
                    StructField("LAST_NAME", StringType(), True),
                    StructField("TEAM", StringType(), True),
                    StructField("DESCRIPTION", StringType(), True),
                    StructField("ALL_NBA_TEAM_NUMBER", StringType(), True),
                    StructField("SEASON", StringType(), True),
                    StructField("MONTH", StringType(), True),
                    StructField("WEEK", StringType(), True),
                    StructField("CONFERENCE", StringType(), True),
                    StructField("TYPE", StringType(), True),
                    StructField("SUBTYPE1", StringType(), True),
                    StructField("SUBTYPE2", StringType(), True),
                    StructField("SUBTYPE3", StringType(), True),
                ])
        
        if config_dic["REGENERAR_FACTICA"] == 1:
            print("\t>>> Inicialización de la fáctica.")
            premios_df = spark_s.read.options(header=True, schema=schema).csv(config_dic['CSV_PREMIOS'])

            print(f"\t\tJugadores recuperados desde el CSV: {premios_df.select('PERSON_ID').distinct().count()}")
        else:
            result = playerawards.PlayerAwards(player_id=config_dic['PLAYER_ID_UPD'], timeout=config_dic['REQUEST_TIMEOUT']).get_normalized_dict()
            premios_df = spark_s.createDataFrame(data=result['PlayerAwards'], schema=schema)
        
        premios_df = premios_df.toPandas()

    except Exception as error:
        print(f"\t\tError al leer de la API: {error}.")
        return None
        
    else:
        #Corrijo posibles errores
        print("\t\tCorrigiendo DF de Premios.")
        #ALL_NBA_TEAM_NUMBER debe ser un INT
        premios_df['ALL_NBA_TEAM_NUMBER'] = premios_df['ALL_NBA_TEAM_NUMBER'].str.replace('.0', '') #Eliminar formato de float
        premios_df['ALL_NBA_TEAM_NUMBER'] = premios_df['ALL_NBA_TEAM_NUMBER'].str.replace(r"\(.*\)","0", regex=True) #Eliminar valores que no son número
        premios_df['ALL_NBA_TEAM_NUMBER'] = premios_df['ALL_NBA_TEAM_NUMBER'].str.replace('', '0') #Eliminar valores que no son número
        premios_df['ALL_NBA_TEAM_NUMBER'].fillna('0',inplace=True) #Completar los campos en NULL con 0
        premios_df['ALL_NBA_TEAM_NUMBER'] = premios_df['ALL_NBA_TEAM_NUMBER'].astype('int32')

        #MONTH y WEEK deben ser date
        premios_df['MONTH'] = premios_df['MONTH'].astype('datetime64[ns]')
        premios_df['WEEK'] = premios_df['WEEK'].astype('datetime64[ns]')

        return premios_df
    
def etl_extract(spark_s):
    print(">>> Proceso de extracción")

    print("\tObteniendo información de los equipos.")
    teams_rdd = spark_s.sparkContext.parallelize(teams.get_teams()) #Recuperar los equipos (guardar resultados de la API como RDD)
    teams_df = spark_s.createDataFrame(teams_rdd)
    teams_df = teams_df.toPandas()  #Convierto el DataFrame a Pandas
    
    teams_df = pa.concat([teams_df, teams_df], ignore_index=True) #Simulo que hay elementos duplicados (a efectos de la práctica)

    print("\tObteniendo información de los jugadores.")
    players_rdd = spark_s.sparkContext.parallelize(players.get_players())
    players_df = spark_s.createDataFrame(players_rdd)
    players_df = players_df.toPandas()

    print("\tObteniendo información de los premios.")

    premios_df = extact_premios(spark_s)

    print(">>> Fin del proceso de extracción")
    
    return teams_df, players_df, premios_df


##  ***************************** TRANSFORM ***************************** 
"""
    El objetivo es ajustar algunos datos con los DF y finalmente hacer merge en un DF con la misma estructura que la fáctica de BD.
    Se utiliza Pandas para la transformación.
"""
def df_drop_duplicates(df, nombre):
    print("\t>>> Eliminando duplicados del DF.")
    
    dup_count = len(df[df.duplicated()])
    
    if dup_count != 0:
        print(f"\t\t(Se detectaron {dup_count} duplicados en {nombre})")
        df = df.drop_duplicates()
        df.reset_index(drop=True, inplace=True)
    else:
        print(f"\t\t(No se detectaron duplicados en {nombre})")
    return df

def etl_transform(teams_df, players_df, premios_df):
    print(">>> Proceso de transformación")
    
    try:
        len(teams_df.index)
        len(players_df.index)
        len(premios_df.index)
    except:
        print("\tERROR: Hay DFs erróneos.")

    ##TRANSFORM: PREMIOS_DF
    premios_df = df_drop_duplicates(premios_df, 'DF Premios')
    
    ##TRANSFORM: PLAYERS_DF
    """Ajustar players_df: Se agrega columna PERSON_ID para poder hacer el join con premios_df"""
    players_df['PERSON_ID'] = players_df['id']
    premios_df['PERSON_ID'] = premios_df['PERSON_ID'].astype('int64')
    players_df = df_drop_duplicates(players_df, 'DF Players')

    """Merge de premios_df con players_df"""
    #factica_df = premios_df[['PERSON_ID','TEAM','DESCRIPTION','ALL_NBA_TEAM_NUMBER','SEASON','MONTH','WEEK','CONFERENCE','TYPE','SUBTYPE1','SUBTYPE2','SUBTYPE3']].merge(players_df[['first_name','last_name','is_active','PERSON_ID']], on='PERSON_ID', how='left')
    factica_df = premios_df[['PERSON_ID','TEAM','DESCRIPTION','ALL_NBA_TEAM_NUMBER','SEASON','TYPE','SUBTYPE1','SUBTYPE2']].merge(players_df[['first_name','last_name','is_active','PERSON_ID']], on='PERSON_ID', how='left')
    
    """Renombrar columnas de jugadores"""
    factica_df['PLAYER_ID'] = factica_df['PERSON_ID']
    factica_df['PLAYER_FST_NAME'] = factica_df['first_name']
    factica_df['PLAYER_LST_NAME'] = factica_df['last_name']
    factica_df['PLAYER_IS_ACTIVE'] = factica_df['is_active']

    ##TRANSFORM: TEAMS_DF
    """Se agrega columna TEAM_ID y se convierte a mayusculas FULL_NAME para poder hacer el join con premios_df utilizando el atributo TEAM"""
    teams_df['TEAM'] = teams_df['full_name'].str.upper() #Agregar el nombre del equipo en mayúsculas como TEAM
    teams_df['TEAM_ID'] = teams_df['id'].astype('object') #Se agrega el team_id como object, para evitar problemas de conversión
    teams_df = df_drop_duplicates(teams_df, 'DF Teams')

    """Ajustar factica_df: Se convierte a mayuscula el atributo TEAM para poder hacer el join"""
    factica_df['TEAM'] = factica_df['TEAM'].str.upper()

    """Merge de factica_df con teams_df"""
    factica_df = factica_df[['PLAYER_ID','PLAYER_FST_NAME','PLAYER_LST_NAME','PLAYER_IS_ACTIVE','TEAM','DESCRIPTION','ALL_NBA_TEAM_NUMBER','SEASON','TYPE','SUBTYPE1','SUBTYPE2']].merge(teams_df[['TEAM_ID','abbreviation','city','state','year_founded','TEAM']], on='TEAM', how='left')

    factica_df['TEAM_ABB'] = factica_df['abbreviation']
    factica_df['TEAM_CITY'] = factica_df['city']
    factica_df['TEAM_STATE'] = factica_df['state']
    factica_df['TEAM_YEAR_FOUNDED'] = factica_df['year_founded']

    ##TRANSFORM: FACTICA_DF
    #Corregir el tipo de dato del TEAM_ID y tambien TEAM_YEAR_FOUNDED
    """Primero debo eliminar valores nulos"""
    factica_df['TEAM_ID'].fillna('0',inplace=True)
    factica_df['TEAM_YEAR_FOUNDED'].fillna('0',inplace=True)
    """Finalmente los casteo"""
    factica_df['TEAM_ID'] = factica_df['TEAM_ID'].astype('int64')
    factica_df['TEAM_YEAR_FOUNDED'] = factica_df['TEAM_YEAR_FOUNDED'].astype('int32')
    
    #Se corrigen atributos de los premios
    factica_df['AWARD_TYPE'] = factica_df['TYPE']
    factica_df['AWARD_SUBTYPE1'] = factica_df['SUBTYPE1']
    factica_df['AWARD_SUBTYPE2'] = factica_df['SUBTYPE2']
    
    #Eliminar columnas redundantes y no utilizadas
    #factica_df.drop(['first_name','last_name','is_active','PERSON_ID','abbreviation','city','state','year_founded','MONTH', 'WEEK', 'CONFERENCE','TYPE','SUBTYPE1','SUBTYPE2','SUBTYPE3'], axis=1, errors='ignore', inplace=True)
    factica_df.drop(['first_name','last_name','is_active','PERSON_ID','abbreviation','city','state','year_founded','TYPE','SUBTYPE1','SUBTYPE2'], axis=1, errors='ignore', inplace=True)
    
    #Se eliminan registros que NO tienen equipo asociado
    print("\t>>> Eliminando registros de Premios sin equipo asociado.")
    elim_count = len(factica_df.index)
    factica_df = factica_df[factica_df['TEAM_ID']!=0]
    elim_count -= len(factica_df.index)
    print(f"\t\t(Se eliminaron {elim_count} registros)")
    
    #Se eliminan duplicados
    factica_df = df_drop_duplicates(factica_df, 'DF Factica')
    
    #Se agrega el ID considerando el max(id) de la tabla
    idx_from = config_dic['MAX_ROWID'] + 1
    idx_to = idx_from + len(factica_df.index)

    factica_df['ID'] = [x for x in range(idx_from, idx_to)]
    
    print(">>> Fin del proceso de transformación")
    
    return factica_df

##  ***************************** LOAD ***************************** 
"""Se utiliza el Dataframe de Spark para grabar en la base de datos."""
def etl_load(spark_s, factica_df):
    print(">>> Proceso de carga")
    
    df = spark_s.createDataFrame(factica_df)
    
    try:
        date_ini = dt.now()
        #if not config_dic['REGENERAR_FACTICA'] == 1: #Si no se regeneró la fáctica, es porque se van a procesar novedades.
        #    redshift_eliminar_registros(column_name='PLAYER_ID', value=config_dic['PLAYER_ID_UPD']) #Se eliminan los registros viejos
            
        df.write.format("jdbc") \
            .option("url", config_dic['URL_REDSHIFT']) \
            .option("dbtable", f"{config_dic['SCHEMA_REDSHIFT']}.{config_dic['FACT_TABLE_NAME']}") \
            .option("user", config_dic['USER_REDSHIFT']) \
            .option("password", config_dic['PASS_REDSHIFT']) \
            .option("driver", config_dic['JDBC_DRIVER_REDSHIFT']) \
            .mode("append") \
            .save()
        date_end = dt.now()
        
        print(f"\t(Tiempo transcurrido: {round((date_end-date_ini).total_seconds(),2)} segs)")
    except Exception as error:
        print(f"Error al cargar la fáctica en la BD: {error}")


if __name__ == '__main__':
    #https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3
    
    cargar_configuracion(sys.argv)

    print(f">>>>>>>>> TEST_SPARK: {config_dic}")

    #Crear sesión de Spark
    spark_s = SparkSession.builder.master("local[1]").appName("Test_Spark").config("spark.jars", config_dic['DRIVER_PATH']).config("spark.executor.extraClassPath", config_dic['DRIVER_PATH']).getOrCreate() 

    #EXTRACT
    teams_df, players_df, premios_df = etl_extract(spark_s)

    #TRANSFORM
    #factica_df = etl_transform(teams_df, players_df, premios_df)

    #LOAD
    #etl_load(spark_s, factica_df)

    print(f" >>>>>>>>>>>>>> TEST_SPARK: IDs de jugadores con premios {premios_df['PERSON_ID'].unique()}")