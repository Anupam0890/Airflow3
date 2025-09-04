import psycopg2
import os
# Replace with your actual connection URL
connection_url = os.getenv('AIRFLOW__DATABASE__SQL_ALCHEMY_CONN')

try:
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(connection_url)
    cur = conn.cursor()

    # Define the CREATE TABLE IF NOT EXISTS statement
    create_table_query = """
    CREATE TABLE IF NOT EXISTS mongo_backup_status (
        cluster VARCHAR(20) PRIMARY KEY,
        full_last_bkp_date VARCHAR(20) NOT NULL,
        full_last_bkp_status VARCHAR(20) NOT NULL,
        incr_bkp_last_ts BIGINT default 0000000000
        incr_bkp INTEGER default 0,
        updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """

    # Execute the query
    cur.execute(create_table_query)

    # Commit the changes to the database
    conn.commit()

    print("Table 'my_table' created successfully (if it did not exist).")

except psycopg2.Error as e:
    print(f"Error connecting to or interacting with the database: {e}")

finally:
    # Close the cursor and connection
    if cur:
        cur.close()
    if conn:
        conn.close()