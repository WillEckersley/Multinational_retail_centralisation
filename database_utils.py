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



connector = DatabaseConnector()
connector.init_db_engine()