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
        email_regex = "^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$"
        legacy["email_address"] = legacy["email_address"].str.replace("@@", "@", regex=False) 
        
        # UK phone number non regex-matched data cleaning 
        gb_phone_regex = "^((\(?0\d{4}\)?\s?\d{3}\s?\d{3})|(\(?0\d{3}\)?\s?\d{3}\s?\d{4})|(\(?0\d{2}\)?\s?\d{4}\s?\d{4}))(\s?\#(\d{4}|\d{3}))?$"
        gb_mask = legacy["country"].isin(["GB"])
        legacy_gb = legacy[gb_mask]
        legacy["gb_nums"] = legacy_gb["phone_number"].loc[~legacy_gb["phone_number"].str.match(gb_phone_regex)]
        legacy["gb_nums"] = legacy["gb_nums"].replace({r"\(0": "", r"\)": "", r"\ ": ""}, regex=True)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: str(x))
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x.replace("0", "+44", 1) if x.startswith("0", 0, 1) else x)
        legacy["gb_nums"] = legacy["gb_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])
        

        # UK phone number regex-matched data cleaning 
        legacy["gb_nums_reg"] = legacy_gb["phone_number"].loc[legacy_gb["phone_number"].str.match(gb_phone_regex)]
        legacy["gb_nums_reg"] = legacy["gb_nums_reg"].replace({r"\(0": "+44", r"\)": "", r"\ ": ""}, regex=True)
        legacy["gb_nums_reg"] = legacy["gb_nums_reg"].apply(lambda x: str(x))
        legacy["gb_nums_reg"] = legacy["gb_nums_reg"].apply(lambda x: x.replace("0", "+44", 1) if x.startswith("0", 0, 1) else x)
        legacy["gb_nums_reg"] = legacy["gb_nums_reg"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])
        
        # DE number non regex-matched data cleaning
        de_phone_regex = "^(\+49|0)(\d{2,4})?(\s?\d{3,4}){1,3}$"
        de_mask = legacy["country"].isin(["DE"])
        legacy_de = legacy[de_mask]
        legacy["de_nums"] = legacy_de["phone_number"].loc[~legacy_de["phone_number"].str.match(de_phone_regex)]
        legacy["de_nums"] = legacy["de_nums"].replace({r"\)": "", r"\(0": "", r"\ ": ""}, regex=True)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: str(x))
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x.replace("0", "+49", 1) if x.startswith("0", 0, 1) else x)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])
        

        # DE phone number regex-matched data cleaning
        legacy["de_nums_reg"] = legacy_de["phone_number"].loc[legacy_de["phone_number"].str.match(de_phone_regex)]
        legacy["de_nums_reg"] = legacy["de_nums_reg"].apply(lambda x: str(x))
        legacy["de_nums_reg"] = legacy["de_nums_reg"].apply(lambda x: x.replace("0", "+49", 1) if x.startswith("0", 0, 1) else x)
        legacy["de_nums_reg"] = legacy["de_nums_reg"].apply(lambda x: x[0:3] + " " + x[3:7] + " " + x[7:])
        legacy["de_nums_reg"] = legacy["de_nums_reg"].replace("  ", " ", regex=True)

        #US phone number non regex-matched data cleaning
        us_phone_regex = "^\(?(\d{3})\)?[-\. ]?(\d{3})[-\. ]?(\d{4})( x\d{4})?$"
        us_mask = legacy["country"].isin(["US"])
        legacy_us = legacy[us_mask]

        legacy["us_nums"] = legacy_us["phone_number"].loc[~legacy_us["phone_number"].str.match(us_phone_regex)]
        legacy["us_nums"] = legacy["us_nums"].replace({r"\-": " ", r"\.": " ", r"\(": "", r"\)": ""}, regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: str(x))
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: "+1" + x if not x.startswith("+1", 0, 2) else x)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x[0:2] + " " + x[2:6] + " " + x[6:])
        legacy["us_nums"] = legacy["us_nums"].replace("x", " ", regex=True)
        legacy["us_nums"] = legacy["us_nums"].apply(lambda x: x.replace("+1 nan", ""))

        #US phone number regex-matched data cleaning
        legacy["us_nums_reg"] = legacy_us["phone_number"].loc[legacy_us["phone_number"].str.match(us_phone_regex)]
        legacy["us_nums_reg"] = legacy["us_nums_reg"].replace({r"\-": " ", r"\.": " ", r"\(": "", r"\)": ""}, regex=True)
        legacy["us_nums_reg"] = legacy["us_nums_reg"].apply(lambda x: str(x))
        legacy["us_nums_reg"] = legacy["us_nums_reg"].apply(lambda x: "+1" + x if not x.startswith("+1", 0, 1) else x)
        legacy["us_nums_reg"] = legacy["us_nums_reg"].apply(lambda x: x[0:2] + " " + x[2:6] + " " + x[6:])
        legacy["us_nums_reg"] = legacy["us_nums_reg"].replace("  ", " ", regex=True)
        legacy["us_nums_reg"] = legacy["us_nums_reg"].apply(lambda x: x.replace("+1 nan", ""))
       
        
        
        legacy["phone_number"] = legacy["gb_nums"] + legacy["gb_nums_reg"] + legacy["de_nums"] + legacy["de_nums_reg"] + legacy["us_nums"] + legacy["us_nums_reg"]
        legacy["phone_number"] = legacy["phone_number"].replace("nan", "", regex=True).str.rstrip()
        display(legacy[["country", "phone_number"]])      
  

       



        
        
        
       
x = DataCleaner()
x.clean_user_data()