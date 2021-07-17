import datetime
from decimal import Decimal, InvalidOperation

from pandas import np
from pandas_schema.validation import CustomElementValidation


def check_decimal(dec):
    if dec:
        try:
            Decimal(dec)
            return True
        except InvalidOperation:
            return False
    return True


def check_int(num):
    try:
        if num % 1 > 0.00:
            return False
        else:
            return True
    except ValueError:
        return False
    except TypeError:
        return False


def check_positive(num):
    try:
        if num < 0.00:
            return False
        else:
            return True
    except ValueError:
        return False
    except TypeError:
        return False


def check_datetime(date_time):
    try:
        datetime.datetime.strptime(date_time, '%d/%m/%Y %H:%M')
    except ValueError:
        return False
    except TypeError:
        return False
    return True


decimal_validation = [CustomElementValidation(lambda d: check_decimal(d), 'is not decimal')]
int_validation = [CustomElementValidation(lambda i: check_int(i), 'is not integer')]
date_validation = [CustomElementValidation(lambda i: check_datetime(i), 'is not DateTime')]
positive_validation = [CustomElementValidation(lambda i: check_positive(i), 'is not Positive')]
null_validation = [CustomElementValidation(lambda d: d is not np.nan, 'this field cannot be null')]
no_validation = [CustomElementValidation(lambda d: True, 'No Validation')]
