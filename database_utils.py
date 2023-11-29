import yaml
import sqlalchemy
import psycopg2
import pandas as pd


from sqlalchemy import create_engine, text 


class DatabaseConnector:


    def read_db_creds(self):
        with open("/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/db_creds.yaml", "r") as f:
            credentials = yaml.safe_load(f)
            return credentials
    
    def init_db_engine(self):
        creds_dict = self.read_db_creds()
        host = creds_dict["RDS_HOST"]
        user = creds_dict["RDS_USER"]
        password = creds_dict["RDS_PASSWORD"]
        db = creds_dict["RDS_DATABASE"]
        port = creds_dict["RDS_PORT"]
        type = creds_dict["RDS_TYPE"]
        api = creds_dict["RDS_API"]
        with psycopg2.connect(host=host, user=user, password=password, database=db, port=port) as psy_connection:
            engine = sqlalchemy.create_engine(f"{type}+{api}://{user}:{password}@{host}:{port}/{db}")
        return engine

    def list_db_tables(self):
        connection = self.init_db_engine()

    def upload_to_db(self, df, name):
        creds_dict = self.read_db_creds()
        host = creds_dict["SALES_HOST"]
        user = creds_dict["SALES_USER"]
        password = creds_dict["SALES_PASSWORD"]
        db = creds_dict["SALES_DATABASE"]
        port = creds_dict["SALES_PORT"]
        type = creds_dict["SALES_TYPE"]
        api = creds_dict["SALES_API"]
        with psycopg2.connect(host=host, user=user, password=password, database=db, port=port) as psy_connection:
            engine = sqlalchemy.create_engine(f"{type}+{api}://{user}:{password}@{host}:{port}/{db}")
            database_uploader = df.to_sql(name, engine, if_exists="replace")
        return database_uploader