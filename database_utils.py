import pandas as pd
import psycopg2
import sqlalchemy
import yaml


class DatabaseConnector:


    def read_db_creds(self):
        """Reads in database credentials and stores them in a dictionary.
        
        Args:
            None.
        
        Returns:
            A dictionary where the keys are labels for database credentials; the values strings or ints representing those credentials. 
        """
        with open("/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/db_creds.yaml", "r") as f: 
            credentials = yaml.safe_load(f)
            
            return credentials
    

    def init_db_engine(self):
        """Creates an SQLAlchemy engine for retrieving data from a specific AWS RDS.
        
        Args:
            None.
        
        Returns:
            An SQLAlchemy engine. 
        """
        # Read in the database credentials.
        creds_dict = self.read_db_creds()

        # Access source DB creds.
        host = creds_dict["RDS_HOST"]
        user = creds_dict["RDS_USER"]
        password = creds_dict["RDS_PASSWORD"]
        db = creds_dict["RDS_DATABASE"]
        port = creds_dict["RDS_PORT"]
        type = creds_dict["RDS_TYPE"]
        api = creds_dict["RDS_API"]
        
        # Create an engine. 
        with psycopg2.connect(host=host, user=user, password=password, database=db, port=port) as psy_connection:
            engine = sqlalchemy.create_engine(f"{type}+{api}://{user}:{password}@{host}:{port}/{db}")
        
        return engine


    def upload_to_db(self, df, name):
        """Uploads data from Pandas dataframes to a specific SQL database.

        Args:
            df (Pandas DataFrame): the dataframe one wants to upload as a table to the SQL RDS.
            name (str): the name the table will take on in the database it is uploaded to.
        
        Returns:
            An SQL table in the target database. 
        """
        # Read in the database credentials.
        creds_dict = self.read_db_creds()

        #Access the target DB creds. 
        host = creds_dict["SALES_HOST"]
        user = creds_dict["SALES_USER"]
        password = creds_dict["SALES_PASSWORD"]
        db = creds_dict["SALES_DATABASE"]
        port = creds_dict["SALES_PORT"]
        type = creds_dict["SALES_TYPE"]
        api = creds_dict["SALES_API"]
        
        # Create an engine to send the DataFrame to the target location. 
        with psycopg2.connect(host=host, user=user, password=password, database=db, port=port) as psy_connection:
            engine = sqlalchemy.create_engine(f"{type}+{api}://{user}:{password}@{host}:{port}/{db}")
            database_uploader = df.to_sql(name, engine, if_exists="replace")
        
        return database_uploader
    