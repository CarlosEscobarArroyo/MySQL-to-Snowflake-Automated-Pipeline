from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from datetime import datetime
import sys
import os

sys.path.append('/opt/airflow/etl')
from etl_script import ejecutar_etl

default_args = {
    'owner': 'carlos',
    'depends_on_past': False,
    'email': ['carlos.escobar.arroyo@gmail.coms'],
    'retries': 1,}

with DAG(
    dag_id='etl_glamour',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['glamour', 'etl', 'snowflake'],
    template_searchpath=['/opt/airflow/sql']
) as dag:

    etl_task = PythonOperator(
        task_id='run_etl_script',
        python_callable=ejecutar_etl
    )

    create_fact_pedidos_task = SnowflakeOperator(
        task_id='create_fact_pedidos',
        sql='create_fact_pedidos.sql',
        snowflake_conn_id='snowflake_conn'
    )

    create_fact_pedidos_detalle_task = SnowflakeOperator(
        task_id='create_fact_pedidos_detalle',
        sql='create_fact_ventas.sql',
        snowflake_conn_id='snowflake_conn'
    )

    etl_task >> create_fact_pedidos_task >> create_fact_pedidos_detalle_task
