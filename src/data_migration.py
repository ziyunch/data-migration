import os
import sys
import zipfile
import json
import psycopg2
import psycopg2.extras

def UnZip(zip_file, zip_dir):
    """
    Unzip given data `zip_file` into given directory `zip_dir`
    Package used: `zipfile`
    """
    zip_ref = zipfile.ZipFile(zip_file, 'r')
    zip_ref.extractall(zip_dir)
    zip_ref.close()

def ETLProcess(input_dir):
    """
    Main ETL Process
    Package used: `json`
    """
    file_list = os.listdir(input_dir)
    for file_name in file_list:
        if file_name.endswith('.json'):
            input_file = input_dir + file_name
            print('Begin processing '+ input_file)
            with open(input_file, 'r') as json_file:
                json_content = json.load(json_file)
                TransformDict(json_content['orders'])
            print('Finish processing '+ input_file)

def TransformDict(d):
    """
    Parse the dictionary
    """
    # Use a dictionary to define the keys to be saved in each table
    table_list = {
        'orders': [
            'id', 'user_id', 'location_id',
            'order_number', 'created_at', 'closed_at',
            'processed_at', 'updated_at', 'cancelled_at',
            'total_price_usd', 'total_price', 'total_line_items_price',
            'subtotal_price', 'total_discounts', 'total_tax',
            'taxes_included', 'currency', 'total_weight',
            'confirmed', 'processing_method', 'checkout_id',
            'financial_status', 'fulfillment_status', 'cancel_reason',
            'customer_locale', 'contact_email', 'order_status_url'
        ],
        'users': [
            'user_id', 'id', 'name', 'email', 'phone',
            'buyer_accepts_marketing', 'updated_at'
        ],
        'referrals': [
                    'referring_site', 'landing_site', 'landing_site_ref',
                    'id', 'total_price_usd'
        ]
    }
    # Prepare the dictionary to store values for those three table
    order_values = {'orders':[], 'users':[], 'referrals':[]}
    # Prepare key, value list for `line_items` table
    line_keys = ['order_id'] + [key for key in d[0]['line_items'][0].keys()]
    line_values = []
    # Parson jason file
    for order in d:
        for table, keys in table_list.items():
            order_values[table].append(tuple([order[key] for key in keys]))
        for item in order['line_items']:
            line_values.append(tuple([order['id']] + [value for value in item.values()]))
    # Load values into table in PostgreSQL
    for table, keys in table_list.items():
        LoadinTable(table, keys, order_values[table])
    LoadinTable('line_items', line_keys, line_values)

def LoadinTable(table_name, key_list, value_list):
    """
    Load value in table in PostgreSQL
    """
    query = 'INSERT INTO ' + table_name+'(' + ','.join(key_list) + ') VALUES %s'
    psycopg2.extras.execute_values(cur, query, value_list)
    conn.commit()

def PrepareTable():
    """
    Prepare four table with designed schema in the database
    """
    queries = ("""
        CREATE TABLE IF NOT EXISTS orders (
            id BIGINT PRIMARY KEY,
            user_id BIGINT,
            location_id BIGINT,
            order_number BIGINT,
            created_at TIMESTAMP,
            closed_at TIMESTAMP,
            processed_at TIMESTAMP,
            updated_at TIMESTAMP,
            cancelled_at TIMESTAMP,
            total_price_usd MONEY,
            total_price MONEY,
            total_line_items_price MONEY,
            subtotal_price MONEY,
            total_discounts MONEY,
            total_tax MONEY,
            taxes_included BOOLEAN,
            currency TEXT,
            total_weight SMALLINT,
            confirmed BOOLEAN,
            processing_method TEXT,
            checkout_id BIGINT,
            financial_status TEXT,
            fulfillment_status TEXT,
            cancel_reason TEXT,
            customer_locale TEXT,
            contact_email TEXT,
            order_status_url TEXT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS users(
            user_id BIGINT,
            id BIGINT PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT,
            buyer_accepts_marketing BOOLEAN,
            updated_at TIMESTAMP
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS line_items(
            order_id BIGINT,
            id BIGINT PRIMARY KEY,
            variant_id BIGINT,
            quantity BIGINT,
            product_id BIGINT
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS referrals(
            referring_site TEXT,
            landing_site TEXT,
            landing_site_ref TEXT,
            id BIGINT PRIMARY KEY,
            total_price_usd MONEY
        )
        """
    )
    for query in queries:
        cur.execute(query)
        conn.commit()

if __name__ == '__main__':
    # Input validation
    if len(sys.argv) != 3:
        print("Two arguments are expected: [data.zip], [unzip folder]")
        exit(1)
    zip_file = sys.argv[1]
    zip_dir = sys.argv[2]
    # Connect to PostgreSQL
    user = os.getenv('POSTGRESQL_USER', 'postgres')
    pswd = os.getenv('POSTGRESQL_PASSWORD', 'psswrd')
    host = os.getenv('POSTGRESQL_HOST_IP', 'localhost')
    port = os.getenv('POSTGRESQL_PORT', '5432')
    dbname = 'postgres'
    conn = psycopg2.connect(dbname=dbname, user=user, host=host, password=pswd)
    print('Database Connected')
    cur = conn.cursor()
    # ETL Process
    PrepareTable()
    UnZip(zip_file, zip_dir)
    ETLProcess(zip_dir)
    # Close connection
    cur.close()
    conn.close()
    print('Database Closed')