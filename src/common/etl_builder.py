import yaml
import os
from src.common.ea_etl_exception import EnergyAustraliaETLException
from src.common.logger import get_logger

log = get_logger(__name__)


class ETLBuilder:
    def __init__(self, env, config_path):
        self.config_path = config_path
        self.config = None
        self.env = env
        self.root_dir = os.path.abspath(os.curdir)

    def __call__(self):
        self.read_config()
        self.run_etl()

    def read_config(self):
        try:
            if self.env == 'dev':
                self.config = yaml.safe_load(open(self.config_path))
            else:
                self.config = yaml.safe_load(open(self.config_path))
        except FileNotFoundError as e:
            raise EnergyAustraliaETLException(f'''Config file {self.config_path} not exists''')
        except yaml.YAMLError as e:
            raise EnergyAustraliaETLException(
                f'''{self.config_path} is not valid. Refer the Template under /src/common/config''')

    def run_etl(self):
        pass
