from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

from etl_utils import cargar_configuracion
from etl_connections import redshift_crear_factica, redshift_eliminar_registros
from etl_extract import etl_extract_test

#Cargar la configuración
args = {
    "owner": "Fede Czerniawski",
    "start_date": datetime(2023, 7, 11),
    "retries": 0,
    "retry_delay": timedelta(seconds=5),
}
args.update(cargar_configuracion())

spark_args_lst = [str(args['USER_REDSHIFT']),
                  str(args['PASS_REDSHIFT']),
                  str(args['FACT_TABLE_NAME']),
                  str(args['REGENERAR_FACTICA']),
                  str(args['REQUEST_TIMEOUT']),
                  str(args['PLAYER_ID_UPD']),
                  str(args['CSV_PREMIOS'])]    #Son parámetros que no se almacenan en la variable de entorno.

with DAG(
    dag_id="etl_awards",
    default_args=args,
    description="ETL de la tabla de premios",
    schedule_interval="@daily",
    catchup=False
) as dag:
    
    create_table_task = PythonOperator(
        task_id="create_table_task",
        python_callable=redshift_crear_factica,
        provide_context=True,
        dag=dag
    )

    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=redshift_eliminar_registros,
        provide_context=True,
        dag=dag
    )

    spark_etl_task = SparkSubmitOperator(   ##Temporal, falta habilitar el ETL.
        task_id="spark_etl_task",
        application=f"{args['SCRIPTS_PATH']}/etl_test.py",
        conn_id="spark_default",
        dag=dag,
        driver_class_path=args['DRIVER_PATH'],
        application_args=spark_args_lst
    )

    create_table_task >> clean_task >> spark_etl_task