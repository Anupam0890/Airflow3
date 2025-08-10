from airflow.decorators import dag, task
from airflow.utils.edgemodifier import Label
import pendulum
from datetime import datetime, timedelta
import pandas as pd
from cloud_sql_connect import MSSQLConnect
from mssql_utils import MsSQLConnect
import pyodbc
from airflow.hooks.base import BaseHook
from airflow.models import Variable


@dag(
    schedule="*/2 * * * *",
    start_date=datetime.today() - timedelta(days=1),
    tags=["sql_idle_connections"])
def idle_sql_connection():
    connection_string = "DRIVER={ODBC Driver 17 for SQL Server};SERVER=;DATABASE=ny_taxi;UID=sqlserver;PWD="
    pyodbc.pooling = False
    cnxn = pyodbc.connect(connection_string)
    
    # @task
    # def create_table():
    #     #file_name = "D:\\airflow\\Airflow320\\dags\\taxi_zone_lookup.csv"
    #     table = "taxi_zone_lookup"
    #     df = pd.read_csv("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", nrows=1000)
    #     conn_obj = MSSQLConnect()
    #     conn_obj.create_table(table, df)

    # create_table()
    mssql_conn_hook = BaseHook.get_connection("mssql_connection_id")

    con_obj_1 = MsSQLConnect(
        host=mssql_conn_hook.host,
        port=mssql_conn_hook.port,
        database="ny_taxi",
        user=mssql_conn_hook.login,
        password=mssql_conn_hook.password
    )

    @task
    def save_dataframe():
        file_name = "/opt/airflow/dags/taxi_zone_lookup.csv"
        table = "taxi_zone_lookup"
        df = pd.read_csv(file_name, nrows=1000)
        conn_obj = MSSQLConnect()
        conn_obj.save_dataframe(table, df)

    @task
    def purge_data():
        conn_obj = MSSQLConnect()
        conn_obj.purge_data(cnxn)

    @task
    def get_data_1():
        con_obj_1.get_data()

    @task
    def get_data_2():
        con_obj_1.get_data_2()
        

    save_dataframe() >> purge_data() >> get_data_1() >> get_data_2()


idle_sql_connection()
