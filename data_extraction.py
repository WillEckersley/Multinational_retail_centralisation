import pandas as pd
import database_utils as dbu
import sqlalchemy
import tabula
import requests
import json

from sqlalchemy import text

store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details"
json_keys = "/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/api_key.json"
number_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores/"

class DataExtractor:

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

    def list_number_of_stores(self, endpoint, header={"x-api-key" : "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"}):
            response = requests.get(endpoint, headers=header)
            print(response.json())

x = DataExtractor()
x.list_number_of_stores(number_endpoint)