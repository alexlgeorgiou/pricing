import yaml
from yaml.loader import SafeLoader

class Config:
    def __init__(self, config_file:str = 'config.yaml'):
        self.config = self.load_config(config_file)
    
    @staticmethod
    def load_config(config_file: str):
        with open(config_file, 'r') as f:
            config = yaml.load(f, Loader=SafeLoader)

        return config

    @property
    def volatility_topic(self):
        return self.config['volatility']

    @property
    def prices_topic(self):
        return self.config['prices_topic']

    @property
    def bootstrap_server(self):
        return self.config['bootstrap_server']

    @property
    def delay(self):
        if self.config['delay_for_dev'] == 'false':
            return False
        elif self.config['delay_for_dev'] == 'true':
            return True