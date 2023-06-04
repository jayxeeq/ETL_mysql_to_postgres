#import needed libraries
import os
import psycopg2
import mysql.connector
from configparser import ConfigParser
import json
from psycopg2.extensions import register_adapter, adapt

# This will adapt set type from mysql to be converted into list and fix the error from can't adapt type 'set'
def adapt_set(my_set):
    return adapt(list(my_set))
register_adapter(set, adapt_set)


# Load data type mapping from JSON file
with open('data_type_mapping.json') as file:
    data_type_mapping = json.load(file)


# Read MySQL configuration
mysql_config = ConfigParser()
mysql_config.read('mysql_config.ini')
mysql_host = mysql_config.get("mysql", 'host')
mysql_user = mysql_config.get("mysql", 'user')
mysql_password = mysql_config.get("mysql", 'password')
mysql_database = mysql_config.get("mysql", 'database')


# Read PostgreSQL configuration
postgres_config = ConfigParser()
postgres_config.read('postgres_config.ini')
postgres_host = postgres_config.get('postgres', 'host')
postgres_port = postgres_config.get('postgres', 'port')
postgres_user = postgres_config.get('postgres', 'user')
postgres_password = postgres_config.get('postgres', 'password')
postgres_database = postgres_config.get('postgres', 'database')



# Connect to MySQL
mysql_conn = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)

# Connect to PostgreSQL
postgres_conn = psycopg2.connect(
    host=postgres_host,
    user=postgres_user,
    password=postgres_password,
    database=postgres_database,
    port = postgres_port
)



# Connect to Mysql
try:
    mysql_conn = mysql.connector.connect(
        host=mysql_host,
        user=mysql_user,
        password=mysql_password,
        database=mysql_database
    )
    mysql_cursor = mysql_conn.cursor()

    #Get table names from MySQL
    mysql_cursor.execute("SHOW TABLES")
    tables = mysql_cursor.fetchall()


    #Connect to PostgreSQL
    postgres_conn = psycopg2.connect(
        host=postgres_host,
        user=postgres_user,
        password=postgres_password,
        database=postgres_database,
        port=postgres_port
    )
    postgres_cursor = postgres_conn.cursor()

    #Iterate through each table and transfer data
    for table in tables:
        table_name = table[0]
        query = f'SELECT * FROM {table_name}'

        # Fetch column names from MySQL
        mysql_cursor.execute(f"DESCRIBE {table_name}")
        mysql_columns = [f'"{column[0]}"' for column in mysql_cursor.fetchall()]


        # Fetch data from MySQL
        mysql_cursor.execute(query)
        rows = mysql_cursor.fetchall()

        # Drop table if exists in PostgreSQL
        drop_query = f'DROP TABLE IF EXISTS central_{table_name}'
        postgres_cursor.execute(drop_query)

        # Get column names and data types from MySQL
        desc_query = f'DESCRIBE {table_name}'
        mysql_cursor.execute(desc_query)
        columns = mysql_cursor.fetchall()

        # Create table in PostgreSQL
        create_query = f'CREATE TABLE central_{table_name} ('
        for column in columns:
            column_name = column[0]
            mysql_data_type = column[1].split('(')[0]
            postgres_data_type = data_type_mapping.get(mysql_data_type, 'text')
            #print(mysql_data_type)
            #print(postgres_data_type)

            create_query += f'"{column_name}" {postgres_data_type}, '
        create_query = create_query.rstrip(', ') + ')'
        postgres_cursor.execute(create_query)

        # Prepare the INSERT query with column names
        insert_query = f"INSERT INTO central_{table_name} ({', '.join(mysql_columns)}) VALUES %s"

        # Insert data into PostgreSQL
        for row in rows:
            try:
                postgres_cursor.execute(insert_query, (row,))
            except Exception as e:
                print(f"Error inserting data into PostgreSQL for table {table_name}: {str(e)}")
                break

        # Commit the changes for each table
        postgres_conn.commit()


    print("Data transfer complete.")

    
except Exception as e:
    print(f"Error transferring data: {str(e)}")


finally:
    # Close the connections
    if 'mysql_conn' in locals() and mysql_conn.is_connected():
        mysql_cursor.close()
        mysql_conn.close()

    if 'postgres_conn' in locals() and postgres_conn.closed == 0:
        postgres_cursor.close()
        postgres_conn.close()




