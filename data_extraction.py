from sqlalchemy import text


import boto3
import database_utils as dbu
import json
import pandas as pd
import requests
import sqlalchemy
import tabula


class DataExtractor:


    def list_db_tables(self, engine):
        """Retrieves table names from an AWS RDS.
        
        Args:
            engine (SQLAlchemy engine): a database engine.
        
        Returns:
            A list containing the names of tables in a database.
        """
        with engine.execution_options(isolation_level="AUTOCOMMIT").connect() as connection:
            table_names_data = connection.execute(text("""SELECT table_name 
                                                        FROM information_schema.tables
                                                        WHERE table_schema = 'public'"""))
            return [table_name for table_name in table_names_data]

                
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
        table_names = self.list_db_tables(engine)
        for _ in table_names:
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
        """Lists the number of stores listed at an API endpoint. This method is to be used only
        in conjunction with the method directly below it in the current codebase (retrieve_store_data). 
        This is because, the number of stores must first be determined in order to work out the range
        used in the list comprehension returned by that function. Viz. there, the range represents the 
        number of each store, stop value of the range thus equals the total number of stores extracted
        here.  
        
        Args:
            endpoint (str): the url of the API endpoint.
            header (dict): a header dictionary containing relevant metadata and/or API keys, 
                           request tokens etc.
        
        Returns:
            A dict-like object where the value represents the number of stores.
        """
        with open(header, "r") as f:
            api = json.load(f)
            response = requests.get(endpoint, headers=api)
            print(response.json())


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
            api = json.load(f)
            response_list = [requests.get(endpoint + str(num), headers=api).json() for num in range(0, 451)]
            return response_list

       
    def extract_from_s3(self, endpoint):
        """Downloads a specific .csv file to a specific local address and names it 'productscsv'.
        
        Args:
            endpoint (str): AWS S3 bucket name.
        
        Returns:
            A .csv file in a prespecified local address.
        """
        client = boto3.client("s3")
        download = client.download_file(endpoint, "products.csv", "/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/productscsv.csv")
        return download


    def extract_json_from_s3(self, endpoint):
        """Downloads a specific .json file to a specific local address and names it 'datedetails'.
        
        Args:
            endpoint (str): AWS S3 bucket name.
        
        Returns:
            A .json file in a prespecified local address.
        """
        client = boto3.client("s3")
        download = client.download_file(endpoint, "date_details.json", "/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/datedetails.json")
        return download