#Energy Australia Code Assignment

##  ETL Details
1. RawTransactionETl - energy_australia-main/src/etls/raw_etl/raw_transaction_etl.py
        
        1. Read raw transaction csv
        2. Validate the csv against schema
        3. Segregate valid and invalid data
        
        Assumption Made:
            1.  The schema is configured in energy_australia-main/src/etls/raw_etl/raw_schema_config.py
                based on assumption
        File Path for Dev Mode:
            1. Raw Trasction CSV- energy_australia-main/data/raw/original/Transaction.csv
            2. Valid CSV - energy_australia-main/data/raw/processed/valid_raw/valid_transaction.csv
            3. Invalid CSV - energy_australia-main/data/raw/processed/invalid_raw/error.csv
            
2. EventsAndBatchETl - energy_australia-main/src/etls/events_and_batch_etl/events_and_batch_etl.py
        
        1. Process 1 :
            a. A process reads  valid_transaction.csv(output of first etl) 
            b. Convert them into json
            c. Write one json every time into stagging file- energy_australia-main/data/processed/staging/transaction_events.json
        2. Process 2: 
            a. Watches the stagging file
            b. Write a json file into energy_australia-main/data/processed/transactions/ for every 1000 events of Process 1

3. ReportingETl - energy_australia-main/src/etls/report_etl/reporting_etl.py
    
        1. Reads json from energy_australia-main/data/processed/transactions/
        2. Generate reports into energy_australia-main/data/reports/
        
 Note : The above paths are for dev mode. The paths can be configured.
 
 ##  How To Run
 
 1. pip install -r requirements.txt
 2. python main.py
 
