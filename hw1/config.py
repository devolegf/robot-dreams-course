import yaml


class Config:
    def __init__(self, path):
        with open(path, 'r') as file:
            self._config = yaml.safe_load(file)

    def get_config(self, app):
        return self._config.get(app)
