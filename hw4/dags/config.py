import yaml


class Config:
    def __init__(self, path):
        with open(path, 'r') as file:
            self._config = yaml.safe_load(file)

    def get_config(self, app=None):
        if app is None:
            return self._config
        return self._config.get(app)
