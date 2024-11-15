import yaml

class ConfigLoader:
    _instance = None

    def __new__(cls, config_file='config.yaml'):
        if cls._instance is None:
            cls._instance = super(ConfigLoader, cls).__new__(cls)
            cls._instance.config = cls._load_config(config_file)
        return cls._instance

    @staticmethod
    def _load_config(config_file):
        with open(config_file, 'r') as file:
            return yaml.safe_load(file)

    def get(self, key, default=None):
        return self.config.get(key, default)