import data_cleaning as dcl
import database_utils as dbu


if __name__ == "__main__":
    database_connector = dbu.DatabaseConnector()
    
    database_connector.upload_to_db(dcl.DataCleaner().clean_store_data(), "dim_store_details")
    database_connector.upload_to_db(dcl.DataCleaner().clean_products_data(), "dim_products")
    database_connector.upload_to_db(dcl.DataCleaner().clean_orders_table(), "orders_table")
    database_connector.upload_to_db(dcl.DataCleaner().clean_purchase_dates(), "dim_date_times")