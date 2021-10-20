import csv
import datetime
import os
from typing import List
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator

args = {
    'owner': 'devolegf',
}


def get_tables() -> List:
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    records = postgres_hook.get_records(
        sql="SELECT table_name FROM information_schema.tables WHERE table_schema = 'public';")
    return [record[0] for record in records]


def dump_data(**kwargs):
    ti = kwargs['ti']
    tables = ti.xcom_pull(task_ids='get_tables')
    full_path = f'{os.path.dirname(os.path.abspath(__file__))}/tables_dump'
    for table in tables:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        records = postgres_hook.get_records(
            sql=f"SELECT *  FROM {table}")
        if len(records) > 0:
            folder_path = os.path.join(full_path, table)
            os.makedirs(folder_path,
                        exist_ok=True)
            with open(os.path.join(folder_path, 'data.csv'), 'w') as f:
                write = csv.writer(f)
                write.writerows(records)


with DAG(
        dag_id="dump_dshop",
        start_date=datetime.datetime(2020, 2, 2),
        schedule_interval="@once",
        catchup=False,
) as dag:
    t1 = PythonOperator(
        task_id="get_tables",
        python_callable=get_tables,
    )
    t2 = PythonOperator(
        task_id="dump_data",
        python_callable=dump_data,
    )
    t1 >> t2
