from sqlalchemy import create_engine
import urllib.parse

from dataclasses import dataclass, field
import pandas as pd

from sqlalchemy.engine import URL
import pyodbc

#CONNECTION_STRING = "SERVER={0},{1} ;DATABASE={2};UID={3};PWD={4};MARS_Connection=yes;"
connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=34.45.149.117;DATABASE=ny_taxi;UID=sqlserver;PWD=Server#Sql2017"
connection_url = URL.create("mssql+pyodbc", query={"odbc_connect": connection_string})

@dataclass
class MSSQLConnect:

    def __post_init__(self):
        self.cnxn = pyodbc.connect(connection_string)

    def create_table(self, table: str, dataframe: pd.DataFrame):
        #quoted_driver = urllib.parse.quote_plus(driver)
        # #connection_string = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver={quoted_driver}"
        #engine = create_engine(connection_string)
        engine = create_engine(connection_url)
        with engine.connect() as conn:
            dataframe.head(n=0).to_sql(name=table, schema="dbo", con=conn, if_exists='replace')

    def save_dataframe(self, table: str, dataframe: pd.DataFrame, schema="dbo", chunksize=1000, index=True,
                       if_exists="append"):
        #connection_string = CONNECTION_STRING.format(host, port, db_name, username, pwd)
        #params = "DRIVER={ODBC Driver 17 for SQL Server};" + connection_string
        engine = create_engine(connection_url, fast_executemany=True)

        with engine.connect() as conn:
            dataframe.to_sql(table, schema=schema, con=conn, chunksize=chunksize, index=index, if_exists=if_exists)

    def purge_data(self, conn):
        import random
        indx = random.randint(150, 250)
        sql_delete = f"DELETE FROM taxi_zone_lookup WHERE LocationID < {indx}"
        cursor = conn.cursor()
        cursor.execute(sql_delete)
        conn.commit()
        #cursor.close()
        #conn.close()
        print("Deletion Successful ...")

    def get_data(self):
        import random
        indx = random.randint(150, 250) 
        query = "SELECT * FROM taxi_zone_lookup WHERE LocationID = {indx}"
        cursor = self.cnxn.cursor()
        result = cursor.execute(query)
