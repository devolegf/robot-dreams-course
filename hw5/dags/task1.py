import csv
import datetime
import os
from typing import List
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from hdfs import InsecureClient

args = {
    'owner': 'devolegf',
}

tables_names = ['products']


def dump_data(**kwargs):
    client = InsecureClient(f'http://192.168.0.134:50070/', user='user')
    full_path = f'{os.path.dirname(os.path.abspath(__file__))}/tables_dump'
    table_name = kwargs['table']
    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    root_dir = '/tables_dump'
    records = postgres_hook.get_records(
        sql=f"SELECT *  FROM {table_name}")
    if len(records) > 0:
        folder_path = f'{root_dir}/{table_name}'
        client.makedirs(folder_path)
        folder_path_local = os.path.join(full_path, table_name)
        os.makedirs(folder_path_local,
                    exist_ok=True)
        with open(os.path.join(folder_path_local, 'data.csv'), 'w') as f:
            write = csv.writer(f)
            write.writerows(records)
        client.upload(f'/tables_dump/products', f'{folder_path_local}', cleanup=True)


with DAG(
        dag_id="dump_dshop_to_hdfs",
        start_date=datetime.datetime(2020, 2, 2),
        schedule_interval="@daily",
        catchup=False,
) as dag:

    start = DummyOperator(
        task_id='Starting...',
        dag=dag)
    complete = DummyOperator(
        task_id='job_completed',
        dag=dag)

    dump_operators = [PythonOperator(
        task_id=f'dump_data_{_table}',
        python_callable=dump_data,
        op_kwargs={'table': _table}
    ) for _table in tables_names]

    start >> dump_operators >> complete
