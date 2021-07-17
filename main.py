import argparse

from src.etls.events_and_batch_etl.events_and_batch_etl import EventsAndBatchETl
from src.etls.raw_etl.raw_transaction_etl import RawTransactionETl
from src.etls.report_etl.reporting_etl import ReportingETl


def run_main():
    etl_list = [RawTransactionETl(args.env), EventsAndBatchETl(args.env), ReportingETl(args.env)]
    for etl in etl_list:
        etl()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='ELT Application')
    parser.add_argument('--env', help='Where the application Runs (dev/ prod)', default='dev')
    args, unknown = parser.parse_known_args()
    run_main()
