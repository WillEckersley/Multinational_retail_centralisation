from dateutil.parser import parse
import data_extraction as dex
import database_utils as dbu
import numpy as np
import pandas as pd
import re


class DataCleaner:


    def clean_orders_table(self):
        """Cleans a dataframe of order data extracted from an AWS RDS.

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.
        """
        # Extract orders table. 
        yaml_location = "/Users/willeckersley/projects/Repositories/Multinational_retail_centralisation/db_creds.yaml"
        orders = dex.DataExtractor().read_rds_tables(dbu.DatabaseConnector().create_database_connection(yaml_location), "orders_table")

        # Drop unneeded columns and set 'card_number' to object dtype. 
        orders.drop(columns=["level_0", "index", "first_name", "last_name", "1"], inplace=True)
        
        return orders

    def clean_user_data(self):
        """Cleans data extracted from a database stored in AWS RDS. 

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.    
        """
        # Read the dataframes into the program. Here, the orders dataframe is used to perform a
        # quick inner join with the uuid in the users table. This is a simple way to ensure 1:1 
        # matching of pkey/fkey in the db upload. While this may not always be a desirable approach,
        # due to duplication the view taken here is that the tradeoff is worth it to achieve integrity
        # throughout a large and highly corrupt dataset. The merge is a temporary method to ensure 
        # quick and accurate setup of the relationships to be ultimately generated int he SQL database.   
        orders = self.clean_orders_table()
        yaml_location = "/Users/willeckersley/projects/Repositories/Multinational_retail_centralisation/db_creds.yaml"
        legacy = dex.DataExtractor().read_rds_tables(dbu.DatabaseConnector().create_database_connection(yaml_location), "legacy_users")

        # Drop redundant country and index columns and rename country_code to country.
        # Drop phone_number column - data corruption extremely high. 
        legacy.drop(columns=["country", "index", "phone_number"], axis="columns", inplace=True)                                                      
        legacy.rename(columns={"country_code": "country"}, inplace=True)

        # Replace erroneous instances of 'GGB' in country column. 
        legacy["country"] = legacy["country"].replace("GGB", "GB")
        
        #Replace '\n' as seperators for lines in address column. Replace with commas.                                            
        legacy["address"] = legacy["address"].str.split("\\n")                                                  
        legacy["address"] = legacy["address"].apply(lambda x: ", ".join(x))

        # Replace mistaken instances of '@@' with '@' in email_address column.                                                                           
        legacy["email_address"] = legacy["email_address"].str.replace("@@", "@") 
    
        # Create a column with distinct entries in the orders table using drop_duplicates(). 
        # Use the resultant merge col to perform an inner join to ensure 1:1 matching of pkey/
        # fkey in SQL tables.
        merge_col = orders["user_uuid"].drop_duplicates()
        legacy = pd.merge(legacy, merge_col, on="user_uuid", how="inner")

        # Reformat date_of_birth and join_date columns to datetime64 to catch mis-matched formatting.     
        legacy["date_of_birth"] = legacy["date_of_birth"].apply(parse)  
        legacy["join_date"] = legacy["join_date"].apply(parse)  

        return legacy   
    
    def clean_card_data(self):
        """Cleans credit card data stored in a pdf document in an AWS S3 bucket.

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.
        """
        # Extract data from remote pdf document and concaternate into one dataframe.
        orders = self.clean_orders_table()
        card_dfs = dex.DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        card_data = pd.concat(card_dfs)

        # Reset index of dataframe.
        card_data.reset_index(drop=True, inplace=True)

        # Clean card_number column: removing any strings with non-numeric values.
        card_data["card_number"] = card_data["card_number"].apply(lambda x: str(x))
        card_data["card_number"] = card_data["card_number"].apply(lambda x: x.replace("?", ""))
        card_data["card_number"] = card_data.loc[card_data["card_number"].apply(lambda x: x.isnumeric()), "card_number"]
    
        # Merge with orders table to ensure matching in pkey and fkey during db upload phase. 
        orders["card_number"] = orders["card_number"].astype("str")
        merge_col = orders["card_number"].drop_duplicates()
        card_data = pd.merge(card_data, merge_col, how="inner", on="card_number")

        # Convert date_payment_confirmed to datetime64 format to catch mismatching formatting.
        card_data["date_payment_confirmed"] = card_data["date_payment_confirmed"].apply(parse) 

        return card_data
    
    def clean_store_data(self):
        """Cleans store card data stored in an AWS API.

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.
        """    
        # Import store data into function and store in a dataframe.
        store_data = dex.DataExtractor().retrieve_stores_data("https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/", "/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/api_key.json")
        store_data = pd.DataFrame(store_data)

        # Drop redundant columns. 
        # 'lat' is largely NaN values and repeats any data that is already stored in 'latitude'.
        # 'latitude' and 'longitude' are redundant when address and storecode identifiers are already present. 
        # Second, latitude and longitude data are unnecessarilly precise for this dataset's purposes.
        # 'continent' may have been useful for aggregations but given that only three countries accross 
        # two continents are represented, it makes more sense to aggregate over these in future analyses.
        store_data.drop(columns=["continent", "latitude", "lat", "longitude"], inplace=True)

        # Replace '\n' seperators in address column with commas.
        store_data["address"] = store_data["address"].astype("str")
        store_data["address"] = store_data["address"].str.split("\\n")
        store_data["address"] = store_data["address"].apply(lambda x: ", ".join(x))

        # Rename country code to country and remove erroneous values.
        store_data.loc[~store_data["country_code"].isin(["GB", "DE", "US"]), "country_code"] = np.nan
        store_data.dropna(inplace=True)
        store_data.rename(columns={"country_code": "country"}, inplace=True)

        # Remove erroneous alphabetical characters from staff number values.
        store_data["staff_numbers"] = store_data["staff_numbers"].apply(lambda x: x if x.isnumeric() else x.replace("".join(filter(str.isalpha, x)), ""))

        # Apply formatting to opening date column.
        store_data["opening_date"] = store_data["opening_date"].apply(parse)

        # Handle the address and locality entries for the online store individually. 
        store_data.loc[0, "address"] = "Online"
        store_data.loc[0, "locality"] = "Online"

        # Drop 'index' column
        store_data.drop(columns=["index"], inplace=True)

        return store_data

    def convert_product_weights(self):
        """Handles the conversion of the weights of products originally stored in a .csv file in an AWS bucket.

        Args:
            None.

        Returns:
            A partially cleaned dataframe. See code comments for details of process. This dataframe is specifically 
            designed to be imported and further cleaned in the function below this one in the current codebase 
            (clean_products_data). 
        """
        # Read .csv into dataframe
        csv_read = pd.read_csv("/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/productscsv.csv")
        products = pd.DataFrame(csv_read)

        # Convert dtype of 'weight' column to perform manipulations.
        products["weight"] = products["weight"].astype("str")

        # Create seperate 'kg', 'non_kg' and 'x' columns and handle formatting of entries. 
        products["kg"] = products.loc[products["weight"].apply(lambda x: x.endswith("kg")), "weight"]
        products["kg"] = products["kg"].astype("str")
        products["kg"] = products["kg"].str.replace("kg", "")
        products["kg"] = products["kg"].str.replace("nan", "")
    
        products["non_kg"] = products.loc[products["weight"].apply(lambda x: not x.endswith("kg") and not "x" in x and x.endswith("g") or x.endswith("l")), "weight"]
        products["non_kg"] = products["non_kg"].str.replace("g", "")
        products["non_kg"] = products["non_kg"].str.replace("ml", "")
        products["non_kg"] = products["non_kg"].apply(lambda x: float(x) / 1000)
        products["non_kg"] = products["non_kg"].astype("str")
        products["non_kg"] = products["non_kg"].str.replace("nan", "")
        
        products["x"] = products.loc[products["weight"].apply(lambda x: "x" in x), "weight"]
        products["x"] = products["x"].astype("str")
        products["x"] = products["x"].apply(lambda x: float(re.findall(r"\d+", x)[0]) * float(re.findall(r"\d+", x)[1]) /1000 if "x" in x else x)
        products["x"] = products["x"].astype("str")
        products["x"] = products["x"].replace("nan", "")

        # Concaternate filtered columns and convert empty cells to NaN.         
        products["weight"] = products["x"] + products["non_kg"] + products["kg"]
        products.drop(columns=["x", "kg", "non_kg"], inplace=True)
        products.loc[products["weight"].isin([""]), "weight"] = np.nan
        
        # Handle two values with corupt and alternative formatting. 
        products.loc[1779, "weight"] = 0.077
        products.loc[1841, "weight"] = 0.453

        # Drop NaN rows and perform final formatting steps. 
        products.dropna(inplace=True)
        products["weight"] = products["weight"].apply(lambda x: round(float(x), 4))
        products.rename(columns={"weight": "product_weight"}, inplace=True)

        return products
    
    def clean_products_data(self):
        """Cleans the remaining data in the dataframe partially handled in the convert_product_weights function.

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.
        """
        # Import dataframe with cleaned weight column 
        products = DataCleaner().convert_product_weights()

        # Convert column to boolean for readability and error reduction.
        # Lower down: reverse naming for comprehension/readability.
        boolean_map = {"Still_avaliable": True, "Removed": False}
        products["removed"] = products["removed"].map(boolean_map)
        
        # Parse dates stated in erroneious format and convert dtype to datetime64 to catch inconsistent formatting.
        products["date_added"] = products["date_added"].apply(parse)

        # Remove '£' symbol from prices and convert dtype to float to allow arithmetical manipulations.
        products["product_price"] = products["product_price"].apply(lambda x: x.replace("£", "") if x.startswith("£") else x)
        products["product_price"] = products["product_price"].astype("float")
        
        # Remove unnecessary '-' separators from values in 'category' column.
        products["category"] = products["category"].str.replace("-", " ")

        # Rename/drop columns for clarity.
        products.rename(columns={"removed": "available", "category": "product_category", "EAN": "ean"}, inplace=True)
        products.drop(columns=["Unnamed: 0"], inplace=True)
        
        # Use regex to remove weights from product names. 
        products["product_name"] = products["product_name"].str.replace(r"\b\d+g\b", "", regex=True)
        products["product_name"] = products["product_name"].str.replace(r"\b\d+kg\b", "", regex=True)
        products["product_name"] = products["product_name"].str.replace(r"\b\d+ml\b", "", regex=True)
        products["product_name"] = products["product_name"].str.replace(r"\b\d+L\b", "", regex=True)
        products["product_name"] = products["product_name"].str.replace(r"\b\d+oz\b", "", regex=True)

        return products
    
    def clean_purchase_dates(self):
        """Cleans a dataframe of purchase times extracted from a .json file stored in AWS S3 bucket.

        Args:
            None.

        Returns:
            A cleaned dataframe. See code comments for details of process.
        """
        dates_json = pd.read_json("/Users/willeckersley/projects/repositories/Multinational_retail_centralisation/datedetails.json")
        dates = pd.DataFrame(dates_json)
        
        # Remove erroneous values from columns.
        dates.loc[~dates["day"].apply(lambda x: x.isnumeric()), "day"] = np.nan
        dates.dropna(inplace=True)
        
        # Apply formatting to day and month columns and concaternate with timestsamp column in new datetime column.
        dates["day"] = dates["day"].apply(lambda x: str(x) + "-")
        dates["month"] = dates["month"].apply(lambda x: str(x) + "-")
        dates["purchase_date"] = dates["day"] + dates["month"] + dates["year"]

        # Drop original columns, rename timestamp to purchase time and rearange order of columns.
        dates.rename(columns={"timestamp": "purchase_time"}, inplace=True) 
        dates.drop(columns=["time_period", "month", "day", "year"], inplace=True)
        dates = dates.iloc[:, [1, 2, 0]]

        return dates   
