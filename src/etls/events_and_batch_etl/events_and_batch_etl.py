import csv
import json
import time
from multiprocessing import Process

from src.common.ea_etl_exception import EnergyAustraliaETLException
from src.common.logger import get_logger

from src.common.etl_builder import ETLBuilder

log = get_logger(__name__)

'''
This ETL 

1. Reads data from valid CSV
2. Convert csv into json and emit one event at a time into a staging file
3. A file watcher emits a output json for every 1000 events
'''


class EventsAndBatchETl(ETLBuilder):
    def __init__(self, env, config_path=None):
        self.config_path = 'config/event_batch_etl_config.yaml' if env == 'dev' else config_path
        self.env = env
        self.staging_path = None
        self.transaction_input_path = None
        self.batch_json_path = None
        ETLBuilder.__init__(self, self.env, self.config_path)

    '''This method converts csv into json and write one json into staging file at time'''

    def read_valid_transaction_for_events(self):
        valid_raw_csv = open(self.transaction_input_path, 'r')
        reader = csv.DictReader(valid_raw_csv)
        for row in reader:
            json_file = open(self.staging_path, 'a')
            json.dump(row, json_file)
            json_file.write('\n')
            json_file.close()
        json_file = open(self.staging_path, 'a')
        json_file.write('//####EODFILE####')

    '''This method watches the staging file and publish {batch_number.json} for every 1000 events '''

    def publish_batch(self):
        incoming_data = []
        batch = 1
        chunk = 1000

        def reading_log_files(filename):
            with open(filename, "r") as f:
                data = f.read().splitlines()
            return data

        def log_generator(filename, period=1):
            data = reading_log_files(filename)
            if data:
                incoming_data.extend(data)
            execute_flag = True
            while execute_flag:
                time.sleep(period)
                new_data = reading_log_files(filename)
                yield new_data[len(data):]
                data = new_data
                if '//####EODFILE####' in data:
                    execute_flag = False

        def write_data(incoming_data, is_eod):
            nonlocal batch
            if len(incoming_data) >= chunk:
                for i in range(int(len(incoming_data) / chunk)):
                    with open(f'''{self.batch_json_path}\\{batch}.json''', "w") as file:
                        file.write('\n'.join(incoming_data[:chunk]))
                        incoming_data = incoming_data[chunk:]
                        batch = batch + 1
            if is_eod:
                with open(f'''{self.batch_json_path}\\{batch}.json''', "w") as file:
                    file.write('\n'.join(incoming_data))
            return batch, incoming_data

        x = log_generator(self.staging_path)
        for lines in x:
            incoming_data.extend(lines)
            batch, incoming_data = write_data(incoming_data, False)
        batch, incoming_data = write_data(incoming_data, True)
        a_file = open(f'''{self.batch_json_path}\\{batch}.json''', "r")
        lines = a_file.readlines()
        a_file.close()
        lines = lines[:-1]
        new_file = open(f'''{self.batch_json_path}\\{batch}.json''', "w")
        for line in lines:
            new_file.write(line)
        new_file.close()

    def run_etl(self):
        self.transaction_input_path = self.config['transaction_input_path']
        self.staging_path = self.config['staging_path']
        self.batch_json_path = self.config['batch_json_path']
        try:
            log.info(f'''Data Read from {self.transaction_input_path}''')
            event_process = Process(target=self.read_valid_transaction_for_events, args=())
            batch_process = Process(target=self.publish_batch, args=( ))
            event_process.start()
            batch_process.start()
            event_process.join()
            batch_process.join()
            log.info(f'''Output path {self.batch_json_path}''')
        except Exception as e:
            raise EnergyAustraliaETLException(e)
