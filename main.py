import data_cleaning as dcl
import database_utils as dbu


if __name__ == "__main__":
    database_connector = dbu.DatabaseConnector()
    data_cleaner = dcl.DataCleaner()
    database_connector.upload_to_db(data_cleaner.clean_user_data(), "dim_users")
    database_connector.upload_to_db(data_cleaner.clean_card_data(), "dim_card_details")
    database_connector.upload_to_db(data_cleaner.clean_store_data(), "dim_store_details")
    database_connector.upload_to_db(data_cleaner.clean_products_data(), "dim_products")
    database_connector.upload_to_db(data_cleaner.clean_orders_table(), "orders_table")
    database_connector.upload_to_db(data_cleaner.clean_purchase_dates(), "dim_date_times")
    print("Upload successful")