import pandas as pd
import database_utils as dbu
import sqlalchemy
import tabula

from sqlalchemy import text

class DataExtractor:


    engine = dbu.DatabaseConnector().init_db_engine()


    def list_db_tables(self, engine):
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            table_names_data = connection.execute(text("""SELECT table_name 
                                                        FROM information_schema.tables
                                                        WHERE table_schema = 'public'"""))
            return [table_name for table_name in table_names_data]
                
    def read_rds_tables(self, engine, table_name):
        table_names = self.list_db_tables(engine)
        for _ in table_names:
            result = pd.read_sql_table(table_name, engine)
        return result
    
    def retrieve_pdf_data(self, link):
        pdf_data_read = tabula.read_pdf(link, pages="all")
        return pdf_data_read
