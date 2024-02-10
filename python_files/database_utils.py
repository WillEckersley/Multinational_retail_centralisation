import pandas as pd
import psycopg2
import sqlalchemy
import yaml


class DatabaseConnector:

    def create_database_connection(self, yaml_location):
        """Reads in database credentials and stores them in a dictionary.
        
        Args:
            None.
        
        Returns:
            A dictionary where the keys are labels for database credentials; the values strings or ints representing those credentials. 
        """
        with open(yaml_location, "r") as f: 
            creds = yaml.safe_load(f)
            with psycopg2.connect(host=creds["HOST"], user=creds["USER"], password=creds["PASSWORD"], database=creds["DB"], port=creds["PORT"]) as connection:
                engine = sqlalchemy.create_engine(f"{creds['TYPE']}+{creds['API']}://{creds['USER']}:{creds['PASSWORD']}@{creds['HOST']}:{creds['PORT']}/{creds['DB']}")
        
        return engine

    def upload_to_db(self, df, name):
        """Uploads data from Pandas dataframes to a specific SQL database.

        Args:
            df (Pandas DataFrame): the dataframe one wants to upload as a table to the SQL RDS.
            name (str): the name the table will take on in the database it is uploaded to.
        
        Returns:
            An SQL table in the target database. 
        """
        yaml_location = "/Users/willeckersley/projects/Repositories/Multinational_retail_centralisation/db_uploader_creds.yaml"
        engine = self.create_database_connection(yaml_location)
        database_uploader = df.to_sql(name, engine, if_exists="replace", index=False)
        
        return database_uploader
