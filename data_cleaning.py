import pandas as pd
import data_extraction as dex
import database_utils as dbu

from datetime import datetime


class DataCleaner:

    engine = dbu.DatabaseConnector().init_db_engine()

    def clean_user_data(self):
        legacy = dex.DataExtractor().read_rds_tables(DataCleaner.engine, "legacy_users")
        legacy["join_date"] = pd.to_datetime(legacy["join_date"], errors="coerce", format="%Y-%m-%d")
        legacy = legacy.dropna()                                                                                
        legacy["country_code"] = legacy["country_code"].replace("GGB", "GB")                                   
        legacy = legacy.drop(["country"], axis="columns")                                                      
        legacy = legacy.rename(columns={"country_code": "country"})                                             
        legacy["address"] = legacy["address"].str.split("\\n")                                                  
        legacy["address"] = legacy["address"].apply(lambda x: ", ".join(x))                                     
        legacy["date_of_birth"] = pd.to_datetime(legacy["date_of_birth"], errors="coerce", format="%Y-%m-%d")   
        legacy = legacy.dropna()                                                                                
        legacy["email_address"] = legacy["email_address"].str.replace("@@", "@", regex=False) 
        
        # UK phone number data cleaning 
        gb_mask = legacy["country"].isin(["GB"])
        legacy_gb = legacy[gb_mask]
        
        legacy["gb_nums"] = legacy_gb["phone_number"]
        legacy["gb_nums"] = legacy["gb_nums"].replace({r"\(0": "", r"\)": "", r"\ ": ""}, regex=True)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: str(x))
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x.replace("0", "+44", 1) if x.startswith("0", 0, 1) else x)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])
                                                            
         # DE number data cleaning
        de_mask = legacy["country"].isin(["DE"])
        legacy_de = legacy[de_mask]

        legacy["de_nums"] = legacy_de["phone_number"]
        legacy["de_nums"] = legacy["de_nums"].replace({r"\)": "", r"\(0": "", r"\ ": ""}, regex=True)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: str(x))
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x.replace("0", "+49", 1) if x.startswith("0", 0, 1) else x)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])

        #US phone data cleaning
        us_mask = legacy["country"].isin(["US"])
        legacy_us = legacy[us_mask]

        legacy["us_nums"] = legacy_us["phone_number"]
        legacy["us_nums"] = legacy["us_nums"].replace({r"\-": " ", r"\.": " ", r"\(": "", r"\)": ""}, regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: str(x))
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: "+1" + x if not x.startswith("+1", 0, 2) else x)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x[0:2] + " " + x[2:5] + " " + x[5:])
        legacy["us_nums"] = legacy["us_nums"].replace("x", " ", regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x.replace("+1 nan", ""))

        legacy["phone_number"] = legacy["gb_nums"] + legacy["de_nums"] + legacy["us_nums"]
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.lstrip("nan"))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.lstrip("nan nan"))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.replace("nan", ""))
        legacy["phone_number"] = legacy["phone_number"].apply(lambda x: x.rstrip())
        legacy = legacy[legacy["phone_number"].str.startswith("+")]
        legacy = legacy[legacy["phone_number"].str.len() <= 15]

        print(legacy[["country", "phone_number"]].info())      


        
    

x = DataCleaner()
x.clean_user_data()