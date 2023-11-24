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
        regex = "^([a-zA-Z0-9_\-\.]+)@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.)|(([a-zA-Z0-9\-]+\.)+))([a-zA-Z]{2,4}|[0-9]{1,3})(\]?)$"
        legacy["email_address"] = legacy["email_address"].astype("str")                                         # set email_address column to str type to allow for application of regex.
        wrong_emails = legacy["email_address"].loc[~legacy["email_address"].str.match(regex)].str.split("@@")   # apply regex to email address column and strip emails with repeated @ symbol which fail regex test.
        wrong_emails.apply(lambda x: "@".join(x))                                                               # replace split emails with singluar @ symbol to allow for inclusion in the dataframe.                                                                              # drop the resulant NaT values 
        legacy.info()
        
        print(legacy["address"].head(30))

x = DataCleaner()
x.clean_user_data()