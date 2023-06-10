from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


import streamlit as st
import time
import numpy as np

@dag(
    dag_id="process-streamlit",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def start():
    data = None
    query ="SELECT * FROM stock"
    try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()
            # print("Data from stock table:")
            # for row in data:
            #     print(row)
            conn.close()
        except Exception as e:
            return 1
    
    @task
    def visualize():
        if data is not None:
            st.write(data) 
        else:
            print("Ra chi blan f psql")

    visualize()





dag = start()