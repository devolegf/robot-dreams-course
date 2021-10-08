import json
import os

import requests

from hw1.config import Config


def auth_on_server(config):
    url = f"{config['base_url']}{config['auth']['endpoint']}"
    headers = {'content-type': config['auth']['output type']}
    res = requests.post(url,
                        headers=headers,
                        data=json.dumps(config['auth']['payload']))
    return res.json()['access_token']


def fetch_out_of_stock(config, check_date):
    url = f"{config['base_url']}{config['out_of_stock']['endpoint']}"
    headers = {'content-type': config['out_of_stock']['output type'],
               'Authorization': 'JWT ' + config['token']}
    res = requests.get(url,
                       headers=headers,
                       data=json.dumps({'date': check_date}))
    return res.json()


def main():
    config = Config(os.path.join('.', 'config.yaml'))
    api_config = config.get_config('api')
    api_config.setdefault('token', auth_on_server(config=api_config))

    app_config = config.get_config('app')

    for process_date in app_config['date']:
        response = fetch_out_of_stock(config=api_config,
                                      check_date=process_date)
        os.makedirs(os.path.join(app_config['export_dir'], process_date),
                    exist_ok=True)
        with open(os.path.join(app_config['export_dir'],
                               process_date,
                               'products.json'), 'w') as json_file:
            json.dump(response, json_file, indent=4)


if __name__ == '__main__':
    main()
