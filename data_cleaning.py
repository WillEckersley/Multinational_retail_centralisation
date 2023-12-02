import pandas as pd
import data_extraction as dex
import database_utils as dbu

from datetime import datetime


class DataCleaner:

    engine = dbu.DatabaseConnector().init_db_engine()

    def clean_user_data(self):
        # Read the dataframe into the program:
        legacy = dex.DataExtractor().read_rds_tables(DataCleaner.engine, "legacy_users")

        # Reformat join_date column to datetime64 and drop nan values. 
        legacy["join_date"] = pd.to_datetime(legacy["join_date"], errors="coerce", format="%Y-%m-%d")
        legacy = legacy.dropna()  

        # Replace mistaken instances of 'GGB' for 'GB' in country_code column.                                                                         
        legacy["country_code"] = legacy["country_code"].replace("GGB", "GB")

        # Drop 'country' in favour of 'country code' as columns repeat information with code column less prone to errors in data entry.
        # Rename 'country_code' to 'country' for simplicity.                                 
        legacy = legacy.drop(["country"], axis="columns")                                                      
        legacy = legacy.rename(columns={"country_code": "country"})  

        #Replace '\n' as seperators for lines in address column. Replace with commas.                                            
        legacy["address"] = legacy["address"].str.split("\\n")                                                  
        legacy["address"] = legacy["address"].apply(lambda x: ", ".join(x))

        # Reformat date_of_birth column to datetime64 and drop nan values.                              
        legacy["date_of_birth"] = pd.to_datetime(legacy["date_of_birth"], errors="coerce", format="%Y-%m-%d")   
        legacy = legacy.dropna()     

        # Replace mistaken instances of '@@' with '@' in email_address column.                                                                           
        legacy["email_address"] = legacy["email_address"].str.replace("@@", "@", regex=False) 
        
        # Phone data cleaning:
        # Create seperate column to clean only UK phone numbers.
        gb_mask = legacy["country"].isin(["GB"])
        legacy_gb = legacy[gb_mask]
        legacy["gb_nums"] = legacy_gb["phone_number"]
        
        # Reformat UK phone numbers. 
        legacy["gb_nums"] = legacy["gb_nums"].replace({r"\(0": "", r"\)": "", r"\ ": ""}, regex=True)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: str(x))
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x.replace("0", "+44", 1) if x.startswith("0", 0, 1) else x)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])

        # Remove entries of incorrect length
        legacy = legacy[legacy["gb_nums"].str.len() <= 15]
                                                            
        # Create sepearte column to clean only German phone numbers. 
        de_mask = legacy["country"].isin(["DE"])
        legacy_de = legacy[de_mask]
        legacy["de_nums"] = legacy_de["phone_number"]

        # Reformat German phone numbers.
        legacy["de_nums"] = legacy["de_nums"].replace({r"\)": "", r"\(0": "", r"\ ": ""}, regex=True)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: str(x))
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x.replace("0", "+49", 1) if x.startswith("0", 0, 1) else x)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])

        # Remove entries of incorrect length
        legacy = legacy[legacy["de_nums"].str.len() <= 14]

        #Create seperate column to clean only US phone numbers.
        us_mask = legacy["country"].isin(["US"])
        legacy_us = legacy[us_mask]
        legacy["us_nums"] = legacy_us["phone_number"]

        # Reformat US phone numbers.
        legacy["us_nums"] = legacy["us_nums"].replace({r"\-": " ", r"\.": " ", r"\(": "", r"\)": ""}, regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: str(x))
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: "+1" + x if not x.startswith("+1", 0, 2) else x)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x[0:2] + " " + x[2:5] + " " + x[5:])
        legacy["us_nums"] = legacy["us_nums"].replace("x", " ", regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x.replace("+1 nan", ""))
        
        # Remove entries of incorrect length
        legacy = legacy[legacy["us_nums"].str.len() <= 15]
        
        # Concaternate and clean phone number column.
        legacy["phone_number"] = legacy["gb_nums"] + legacy["de_nums"] + legacy["us_nums"]
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.lstrip("nan"))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.lstrip("nan nan"))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.replace("nan", ""))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.rstrip())
        legacy = legacy[legacy["phone_number"].str.startswith("+")]

        # Drop phone number columns used in formatting stages.
        legacy = legacy.drop(["gb_nums", "de_nums", "us_nums"], axis="columns")

        # Reindex resultant dataframe.
        legacy = legacy.reset_index(drop=True)
        legacy = legacy.drop(["index"], axis="columns")
        return legacy   
    
    def clean_card_data(self):
        # Extract data from remote pdf document and concaternate into one dataframe.
        card_dfs = dex.DataExtractor().retrieve_pdf_data("https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf")
        card_data = pd.concat(card_dfs)

        # Reset index of dataframe.
        card_data = card_data.reset_index(drop=True)
        
        # Group by provider to filter card numbers of incorrect length by provider
        groups = card_data.groupby("card_provider")

        # Filter each group by target length
        thirteen = groups.get_group("VISA 13 digit") 
        filtered = thirteen.loc[thirteen["card_number"].apply(lambda x: len(str(x)) == 13), "card_number"]
        thirteen["card_number"] = filtered
    
        fourteen = pd.concat([groups.get_group("Diners Club / Carte Blanche"), groups.get_group("Maestro")])
        filtered = fourteen.loc[fourteen["card_number"].apply(lambda x: len(str(x)) == 14), "card_number"]
        fourteen["card_number"] = filtered

        fifteen = pd.concat([groups.get_group("American Express"), groups.get_group("JCB 15 digit")]) 
        filtered = fifteen.loc[fifteen["card_number"].apply(lambda x: len(str(x)) == 15), "card_number"]
        fifteen["card_number"] = filtered
        
        sixteen = pd.concat([groups.get_group("Discover"), groups.get_group("JCB 16 digit"), groups.get_group("Mastercard"), groups.get_group("VISA 16 digit")])
        filtered = sixteen.loc[sixteen["card_number"].apply(lambda x: len(str(x)) == 16), "card_number"]
        sixteen["card_number"] = filtered 
        
        nineteen = groups.get_group("VISA 19 digit") 
        filtered = nineteen.loc[nineteen["card_number"].apply(lambda x: len(str(x)) == 19), "card_number"]
        nineteen["card_number"] = filtered

        # Concaternate filtered dataframes and reset index .
        concaternated = pd.concat([thirteen, fourteen, fifteen, sixteen, nineteen])
        card_data = concaternated
        card_data = card_data.reset_index(drop=True)

        # Clean card_number column: removing any strings with non-numeric values.
        card_data["card_number"] = card_data["card_number"].apply(lambda x: str(x))
        card_data["card_number"] = card_data.loc[card_data["card_number"].apply(lambda x: x.isnumeric()), "card_number"]
        
        # Convert date_payment_confirmed to datetime64 format.
        card_data["date_payment_confirmed"] = pd.to_datetime(card_data["date_payment_confirmed"], errors="coerce", format="%Y-%m-%d")
        
        #Drop 2000 or so null values
        card_data = card_data.dropna()
    
        return card_data
        

x = DataCleaner()
x.clean_card_data()