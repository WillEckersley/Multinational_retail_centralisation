import pandas as pd
import database_utils as dbu
import sqlalchemy

from sqlalchemy import text

class DataExtractor:


    def list_db_tables(self):
        engine_obj = dbu.DatabaseConnector()
        engine = engine_obj.init_db_engine()
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            table_names_data = connection.execute(text("""SELECT table_name 
                                                        FROM information_schema.tables
                                                        WHERE table_schema = 'public'"""))
            table_name_lst = [table_name for table_name in table_names_data]
            return table_name_lst
                

    def read_rds_tables(self, table_name):
        engine_obj = dbu.DatabaseConnector()
        engine = engine_obj.init_db_engine()
        table_names = self.list_db_tables()
        for table in table_names:
            result = pd.read_sql_table(table_name, engine)
        print(result.head())

        
x = DataExtractor()
x.list_db_tables()
x.read_rds_tables("legacy_users")
x.read_rds_tables("orders_table")
x.read_rds_tables("legacy_store_details")