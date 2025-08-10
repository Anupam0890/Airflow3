from sqlalchemy import create_engine
from dataclasses import dataclass, field
import pandas as pd

from sqlalchemy.engine import URL
import pyodbc

@dataclass
class MsSQLConnect:
    
    host: str
    port: str
    database: str
    user: str
    password: str
    cnxn: pyodbc.connect = field(init=False)


    def __post_init__(self):
        CONNECTION_STRING = f"SERVER={self.host};DATABASE={self.database};UID={self.user};PWD={self.password}"
        connection_string = "DRIVER={ODBC Driver 17 for SQL Server};" + CONNECTION_STRING
        pyodbc.pooling = False
        self.cnxn = pyodbc.connect(connection_string)

    def get_data(self):
        import random
        indx = random.randint(150, 250) 
        query = f"SELECT * FROM taxi_zone_lookup WHERE LocationID BETWEEN {indx - 5} AND {indx}"
        df = pd.read_sql(query, self.cnxn)
        print(df)

    def get_data_2(self):
        import random
        indx = random.randint(1, 250) 
        query1 = f"SELECT * FROM taxi_zone_lookup WHERE LocationID = {indx}"
        res = self.cnxn.cursor().execute(query1)
        for row in res:
            print(row[2])
