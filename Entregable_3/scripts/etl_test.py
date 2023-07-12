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


if __name__ == '__main__':
#https://medium.com/codex/executing-spark-jobs-with-apache-airflow-3596717bbbe3
    #config_dic.update(dict(sys.argv[1]))

    cargar_configuracion(sys.argv)

    print(f">>>>>>>>> TEST_SPARK: {config_dic}")