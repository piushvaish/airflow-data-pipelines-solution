import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import LoadDimensionOperator

import logging

def create_table_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        create_sql_stmt, 
        insert_sql_stmt,
        table_exists,
        *args, **kwargs):    
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )
    
      
    # Create dimension table : users
    create_table = PostgresOperator(
        task_id=f"create_{table}",
        dag=dag,
        postgres_conn_id=redshift_conn_id,
        sql=create_sql_stmt
    )

    load_dimension_table = LoadDimensionOperator(
        task_id=f'Load_{table}_dim_table',
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=f"public.{table}",
        table_exit=table_exists,
        query=insert_sql_stmt
    )
    
    create_table >> load_dimension_table

    
    return dag
