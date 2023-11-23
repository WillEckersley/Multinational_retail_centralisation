import pandas as pd
import data_extraction as dex
import database_utils as dbu

from datetime import datetime


class DataCleaner:

    engine = dbu.DatabaseConnector().init_db_engine()

    def clean_user_data(self):
        legacy = dex.DataExtractor().read_rds_tables(DataCleaner.engine, "legacy_users")
        legacy["join_date"] = pd.to_datetime(legacy["join_date"], errors="coerce", format="%Y-%m-%d")
        legacy = legacy.dropna() # Drop 59 NaT columns in the 'join_date' column 
        legacy.info()
        print(legacy.head())

x = DataCleaner()
x.clean_user_data()