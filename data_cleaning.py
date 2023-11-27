import pandas as pd
import data_extraction as dex
import database_utils as dbu

from datetime import datetime


class DataCleaner:

    engine = dbu.DatabaseConnector().init_db_engine()

    def clean_user_data(self):
        legacy = dex.DataExtractor().read_rds_tables(DataCleaner.engine, "legacy_users")
        legacy["join_date"] = pd.to_datetime(legacy["join_date"], errors="coerce", format="%Y-%m-%d")
        legacy = legacy.dropna()                                                                                # Drop 59 NaT columns in the 'join_date' column. 
        legacy["country_code"] = legacy["country_code"].replace("GGB", "GB")                                    # replace 6 eroneous instances of 'GGB' in country_code column with 'GB'.
        legacy = legacy.drop(["country"], axis="columns")                                                       # drop 'country' column in favour of country_code as less prone to error and to prevent needless redundancy.
        legacy = legacy.rename(columns={"country_code": "country"})                                             # rename 'country_code' column to 'country' for simplicity.
        legacy["address"] = legacy["address"].str.split("\\n")                                                  # split the corupted addresses at the point '\n'.
        legacy["address"] = legacy["address"].apply(lambda x: ", ".join(x))                                     # join the lists from previous line into strings by applying a join to the lists. 
        legacy["date_of_birth"] = pd.to_datetime(legacy["date_of_birth"], errors="coerce", format="%Y-%m-%d")   # set 'date_of_birth' column to datetime64 format.
        legacy = legacy.dropna()                                                                                # drop the resulant NaT values.
        email_regex = "^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$"
        legacy["email_address"] = legacy["email_address"].str.replace("@@", "@", regex=False)                   # replace instances of '@@' with '@'                
        legacy["phone_number"] = legacy["phone_number"].replace({r"\+44": "", r"\(0": "0", r"\)": "", r"\ ": ""}, regex=True)
        
        # some manipulations with phone data: closer than before
        de_phone_regex = "^(\+49|0)(\d{2,4})?(\s?\d{3,4}){1,3}$"
        de_mask = legacy["country"].isin(["DE"])
        legacy_de = legacy[de_mask]
        legacy["de_nums"] = legacy_de["phone_number"].loc[~legacy_de["phone_number"].str.match(de_phone_regex)]
        legacy["de_nums"] = legacy["de_nums"].replace({r"\+49": "", r"\)": "", r"\(0": "", r"\ ": ""}, regex=True)
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: str(x))
        legacy["de_nums"] = legacy["de_nums"].apply(lambda x: x[0:5] + " " + x[5:11])
        legacy.loc[legacy["country"] == "DE", "phone_number"] = legacy["de_nums"]
        legacy["phone_number"] = legacy["phone_number"].replace({r"\+44": "", r"\(0": "0", r"\)": "", r"\ ": ""}, regex=True)
        display(legacy["de_nums"])
        display(legacy["phone_number"])
        

x = DataCleaner()
x.clean_user_data()