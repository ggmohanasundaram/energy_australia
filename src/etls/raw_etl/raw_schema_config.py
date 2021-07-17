import pandas_schema
from pandas_schema import Column

from src.common.filed_datatype_validation import int_validation, null_validation, date_validation, no_validation, \
    positive_validation

raw_schema = pandas_schema.Schema([
    Column('Account_ID', int_validation + null_validation),
    Column('CODE', int_validation + null_validation),
    Column('ImplementedDate', date_validation + null_validation),
    Column('ActiveIndicator', no_validation),
    Column('AccountType', null_validation),
    Column('Service', null_validation),
    Column('BU', null_validation),
    Column('RequestDate', date_validation + null_validation),
    Column('Accountstatus', null_validation),
    Column('StatusCode', int_validation + null_validation),
    Column('$Amount', positive_validation + null_validation),
    Column('Version', null_validation),
    Column('AgentID', no_validation),
    Column('FIBRE', no_validation),
    Column('lastUpdatedDate', no_validation),
    Column('PropertyTYPE', no_validation),
    Column('PostCode', no_validation),

])
