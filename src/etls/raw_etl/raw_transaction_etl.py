import pandas as pd

from src.common.ea_etl_exception import EnergyAustraliaETLException
from src.common.logger import get_logger

from src.common.etl_builder import ETLBuilder
from src.etls.raw_etl.raw_schema_config import raw_schema

log = get_logger(__name__)

'''
This ETL 

1. Reads data from Raw CSV
2. Validate the CSV with defined schema at raw_schema_config.py
3. Filter invalid data and write in the given path
4. Filter valid data and write in the given path

'''


class RawTransactionETl(ETLBuilder):
    def __init__(self, env, config_path=None):
        self.config_path = 'config/raw_etl_config.yaml' if env == 'dev' else config_path
        self.env = env
        ETLBuilder.__init__(self, self.env, self.config_path)

    def run_etl(self):
        raw_input_path = self.config['raw_input_path']
        error_data_path = self.config['error_data_path']
        valid_data_path = self.config['valid_data_path']
        try:
            log.info(f'''Reads Raw data from {raw_input_path}''')
            original_df = pd.read_csv(raw_input_path)
            original_df.columns = original_df.columns.str.replace(' ', '')
            print(original_df.columns)
            original_df = original_df.loc[:, ~original_df.columns.str.contains('^Unnamed')]

            errors = raw_schema.validate(original_df)
            errors_index_rows = [e.row for e in errors]
            data_clean = original_df.drop(index=errors_index_rows)
            pd.DataFrame({'col': errors}).to_csv(error_data_path, index=False)
            data_clean[['Account_ID']] = data_clean[['Account_ID']].astype(int)
            data_clean.to_csv(valid_data_path, index=False)
            log.info(f'''Valid Data has been written at  {valid_data_path}''')
            log.info(f'''InValid Data has been written at  {error_data_path}''')

        except Exception as e:
            raise EnergyAustraliaETLException(e)
