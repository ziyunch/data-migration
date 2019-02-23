# Data Migration

## Problem statement

My task is to create an ETL (Extract, Transform, Load) so that API data is consumable by business analysts. The Extract step is taken over by my co-worker and I has already been provided with a .zip file containing retail order data in the raw JSON format. The task my project manager put me on is to support these business analysts so that they can query that data using SQL from a PSQL database. I also need to create a user table that would contain summary metrics that the business analysts would find useful.

The **ETL would run daily** so the newly created tables have to be sanely structured and those steps should be reproducible

## Pipeline

unzip -> read json file -> transform and load in table in PSQL database

## Schema design
`order_table` is used to keep track of each order.
```sql
  (
    id INT
    user_id INT
    location_id INT
    order_number INT
    created_at TIMESTAMP
    closed_at TIMESTAMP
    processed_at TIMESTAMP
    updated_at TIMESTAMP
    cancelled_at TIMESTAMP
    total_price_usd MONEY
    total_price MONEY
    total_line_items_price MONEY
    subtotal_price MONEY
    total_discounts MONEY
    total_tax MONEY
    taxes_included BOOLEAN
    currency CHAR
    total_weight SMALLINT
    confirmed BOOLEAN
    processing_method CHAR
    checkout_id INT
    financial_status CHAR
    fulfillment_status CHAR
    cancel_reason CHAR
    customer_locale CHAR
    contact_email CHAR
    order_status_url CHAR
  )
```

`user_table` is used to store information of each user.
```sql
  (
    user_id INT
    id INT
    name CHAR
    email CHAR
    phone CHAR
    buyer_accepts_marketing BOOLEAN
    updated_at TIMESTAMP
  )
```

`line_item_table` is used to keep track of each line item, e.g. return
```sql
  (
    line_item_id INT
    variant_id INT
    quantity INT
    product_id INT
  )
```

`product_table` is used to store information of each product.
```sql
  (
    product_id INT
    id INT
  )
```

`referral_table` is used to keep record of `referring_site`, `landing_site`, `landing_site_ref` to help develop marketing strategies.
```sql
  (
    referring_site CHAR
    landing_site CHAR
    landing_site_ref CHAR
    id INT
    total_price_usd MONEY
  )
```