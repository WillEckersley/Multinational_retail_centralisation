--- Update dtype of orders table.
ALTER TABLE orders_table
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid,
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN store_code TYPE VARCHAR(15),
ALTER COLUMN product_code TYPE VARCHAR(15),
ALTER COLUMN product_quantity TYPE SMALLINT;

--- Update dtype of users table.
ALTER TABLE dim_users
ALTER COLUMN first_name TYPE VARCHAR(225),
ALTER COLUMN last_name TYPE VARCHAR(225),
ALTER COLUMN date_of_birth TYPE DATE,
ALTER COLUMN country TYPE VARCHAR(2),
ALTER COLUMN user_uuid TYPE UUID
USING user_uuid::uuid,
ALTER COLUMN join_date TYPE DATE

--- Update dtype of store details table.
ALTER TABLE dim_store_details
ALTER COLUMN store_code TYPE VARCHAR(15),
ALTER COLUMN staff_numbers TYPE SMALLINT,
ALTER COLUMN opening_date TYPE DATE,
ALTER COLUMN store_type TYPE VARCHAR(225),
ALTER COLUMN country TYPE VARCHAR(2);

--- Update dtype of products table.
ALTER TABLE dim_products
ALTER COLUMN product_price TYPE FLOAT, 
ALTER COLUMN product_weight TYPE FLOAT, 
ALTER COLUMN ean TYPE VARCHAR(20), 
ALTER COLUMN product_code TYPE VARCHAR(15),
ALTER COLUMN date_added TYPE DATE,
ALTER COLUMN uuid TYPE UUID
USING uuid::uuid, 
ALTER COLUMN available TYPE BOOL;

--- Update dtype of order times table.
ALTER TABLE dim_date_times
ALTER COLUMN purchase_datetime TYPE TIMESTAMP,
ALTER COLUMN date_uuid TYPE UUID
USING date_uuid::uuid;

--- Update dtype of card details table.
ALTER TABLE dim_card_details
ALTER COLUMN card_number TYPE VARCHAR(19),
ALTER COLUMN expiry_date TYPE VARCHAR(5),
ALTER COLUMN date_payment_confirmed TYPE DATE;

--- Add primary keys to users, date_times, products and store tables. 
ALTER TABLE dim_users
ADD PRIMARY KEY (user_uuid);

ALTER TABLE dim_date_times
ADD PRIMARY KEY (date_uuid);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_code);

ALTER TABLE dim_store_details
ADD PRIMARY KEY (store_code);

ALTER TABLE dim_card_details
ADD PRIMARY KEY (card_number);

--- Add foreign keys. 
ALTER TABLE orders_table
ADD FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid),
ADD FOREIGN KEY (date_uuid) REFERENCES dim_date_times(date_uuid),
ADD FOREIGN KEY (store_code) REFERENCES dim_store_details(store_code),
ADD FOREIGN KEY (product_code) REFERENCES dim_products(product_code),
ADD FOREIGN KEY (card_number) REFERENCES dim_card_details(card_number);

--- Remove imported index columns. 
ALTER TABLE dim_card_details
DROP COLUMN index;

ALTER TABLE dim_date_times
DROP COLUMN index;

ALTER TABLE dim_products
DROP COLUMN index;

ALTER TABLE dim_store_details
DROP COLUMN index;

ALTER TABLE dim_users
DROP COLUMN index;