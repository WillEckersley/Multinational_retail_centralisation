from sqlalchemy import text
import boto3
import json
import pandas as pd
import requests
import sqlalchemy
import tabula


class DataExtractor:

    @staticmethod
    def list_db_tables(engine):
        """Prints a list of table names from an AWS RDS.
        
        Args:
            engine (SQLAlchemy engine): a database engine.
        
        Returns:
            None.
        """
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            table_names_data = connection.execute(text("""SELECT table_name 
                                                        FROM information_schema.tables
                                                        WHERE table_schema = 'public'"""))
            table_names_list = [table_name for table_name in table_names_data]

            print(table_names_list)
        
    def read_rds_tables(self, engine, table_name):
        """Retrieves a table from an AWS RDS and converts it into a Pandas dataframe.
        
        Args:
            engine (SQLAlchemy engine): a database engine.
            table_name (str): the name of a table in the database one wants to retrieve
                              data from.
        
        Returns:
            A Pandas dataframe containing the data from the table in the database with
            the same name as passed as an argument to the 'table_name' parameter.
        """
        result = pd.read_sql_table(table_name, engine)
        
        return result
    
    def retrieve_pdf_data(self, link):
        """Retrieves data from a .pdf file containing tabula data.
        
        Args:
            link (str): the url link or filepath to the pdf document.
        
        Returns:
            A data structure that can be read directly into a Pandas dataframe.
        """
        pdf_data_read = tabula.read_pdf(link, pages="all")
        
        return pdf_data_read

    def list_number_of_stores(self, endpoint, header):
        """Prints a list of the number of stores stored at an API endpoint. This method is to be used only
        in conjunction with the method directly below it in the current codebase (retrieve_store_data). 
        This is because the number of stores must first be determined (450) in order to work out the range
        used in the list comprehension returned by that function. In the comprehension in theretrieve_stores_data 
        function the range represents the number of each store. The stop value of the range thus equals the total 
        number of stores printed by this function.  
        
        Args:
            endpoint (str): the url of the API endpoint.
            header (dict): a header dictionary containing relevant metadata and/or API keys, 
                           request tokens etc.
        
        Returns:
            None.
        """
        try:
            with open(header, "r") as f:
                api = json.load(f)
                response = requests.get(endpoint, headers=api)
                print(response.json())
        except requests.RequestException as e:
            print(f"An error occured: {e}")

    def retrieve_stores_data(self, endpoint, header):
        """Retrieves data about stores from an API endpoint.
        
        Args:
            endpoint (str): the url of the API endpoint.
            header (dict): a header dictionary containing relevant metadata and/or API keys, 
                           request tokens etc.
        
        Returns:
            A list containing data about stores in dict format.
        """
        with open(header, "r") as f:
            try:
                api = json.load(f)
                response_list = [requests.get(endpoint + str(num), headers=api).json() for num in range(0, 451)]
                return response_list
            except requests.RequestException as e:
                print(f"An error occurredL {e}")
       
    def extract_from_s3(self, endpoint, filename, location):
        """Downloads a file from S3 to a local address.
        
        Args:
            endpoint (str): AWS S3 bucket name.
            filename (str): The name of the file to download.
            location (str): the local directory to download the data to.
        
        Returns:
            A .csv file in a prespecified local address.
        """
        client = boto3.client("s3")
        download = client.download_file(endpoint, filename, location)
        
        return download