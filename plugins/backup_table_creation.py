import psycopg2
import os

def create_table(conn):
    """
    Creates a sample table named 'mongo_backup_status' if it doesn't already exist.
    """
    try:
        with conn.cursor() as cur:
            create_table_query = """
            CREATE TABLE IF NOT EXISTS mongo_backup_status (
                    cluster VARCHAR(20) PRIMARY KEY,
                    full_last_bkp_date VARCHAR(20) NOT NULL,
                    full_last_bkp_status VARCHAR(20) NOT NULL,
                    incr_bkp_last_ts BIGINT default 0000000000,
                    incr_bkp INTEGER default 0,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """
            cur.execute(create_table_query)
            conn.commit()
            conn.close()
            print("Table 'mongo_backup_status' created successfully (if it didn't exist).")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")

if __name__ == "__main__":
    # Replace with your actual database connection details
    db_params = {
        "host": os.getenv('POSTGRES_HOST'),
        "port": os.getenv('POSTGRES_PORT'),
        "database": os.getenv('POSTGRES_PASSWORD'),
        "user": os.getenv('POSTGRES_USER'),
        "password": os.getenv('POSTGRES_PASSWORD')
    }

    try:
        with psycopg2.connect(**db_params) as conn:
            create_table(conn)
    except psycopg2.Error as e:
        print(f"Error connecting to database: {e}")