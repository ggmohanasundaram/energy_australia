import logging


def get_logger(log_name):
    logging.basicConfig(format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                        datefmt='%m-%d %H:%M')
    logging.Formatter('%(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger("EA-ETL  " + log_name)

    logger.setLevel(logging.DEBUG)
    return logger
