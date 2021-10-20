import json
import os
import sys

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from requests import HTTPError
sys.path.append('./')
from config import Config

args = {
    'owner': 'devolegf',
}


def load_config(**kwargs):
    file_dir = os.path.dirname(os.path.abspath(__file__))
    config = Config(os.path.join(file_dir, 'config.yaml'))
    return config.get_config()


def auth_on_server(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='load_config')
    url = f"{config['api']['base_url']}{config['api']['auth']['endpoint']}"
    headers = {'content-type': config['api']['auth']['output type']}
    res = requests.post(url,
                        headers=headers,
                        data=json.dumps(config['api']['auth']['payload']),
                        timeout=5.0)
    if res.status_code != 200:
        raise HTTPError
    config['api'].setdefault('token', res.json()['access_token'])
    return config


def sort_out_of_stock(**kwargs):
    ti = kwargs['ti']
    config = ti.xcom_pull(task_ids='auth_on_server')
    file_dir = os.path.dirname(os.path.abspath(__file__))
    for process_date in config['app']['date']:
        response = fetch_out_of_stock(config['api'], process_date)
        full_path = f"{file_dir}{config['app']['export_dir']}"
        os.makedirs(os.path.join(full_path, process_date),
                    exist_ok=True)
        with open(os.path.join(full_path,
                               process_date,
                               'products.json'), 'w') as json_file:
            json.dump(response, json_file, indent=4)


def fetch_out_of_stock(config, check_date):
    url = f"{config['base_url']}{config['out_of_stock']['endpoint']}"
    headers = {'content-type': config['out_of_stock']['output type'],
               'Authorization': 'JWT ' + config['token']}
    res = requests.get(url,
                       headers=headers,
                       data=json.dumps({'date': check_date}))
    return res.json()


dag = DAG('test_case',
          description='First task from hm',
          default_args=args,
          schedule_interval=None,
          start_date=days_ago(2),
          tags=['hw'])

load_config_task = PythonOperator(task_id='load_config',
                                  python_callable=load_config,
                                  dag=dag,
                                  )

auth_on_server_task = PythonOperator(task_id='auth_on_server',
                                     python_callable=auth_on_server,
                                     dag=dag,
                                     )
sort_out_of_stock_task = PythonOperator(task_id='sort_out',
                                        python_callable=sort_out_of_stock,
                                        dag=dag,
                                        )

load_config_task >> auth_on_server_task >> sort_out_of_stock_task
