import datetime
import pendulum
import os
import pandas as pd

import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(
    dag_id="process-apple-stock",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessAppleStock():
    create_stock_table = PostgresOperator(
        task_id="create_stock_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS stock (
                "Date" DATE,
                "Open" NUMERIC,
                "High" NUMERIC,
                "Low" NUMERIC,
                "Close" NUMERIC,
                "Volume" TEXT
            );""",
    )

    create_stock_temp_table = PostgresOperator(
        task_id="create_stock_temp_table",
        postgres_conn_id="tutorial_pg_conn",
        sql="""
            DROP TABLE IF EXISTS stock_temp;
            CREATE TABLE stock_temp (
                "Date" DATE,
                "Open" NUMERIC,
                "High" NUMERIC,
                "Low" NUMERIC,
                "Close" NUMERIC,
                "Volume" TEXT
            );""",
    )

    @task
    def get_data():
        # REMARQUE : configurez cela selon les besoins de votre environnement Airflow.
        data_path = "/opt/airflow/dags/files/stock.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        url = "https://raw.githubusercontent.com/asmaa-kplr/AAirflow/main/STOCK_AAPL.csv"

        response = requests.request("GET", url)

        with open(data_path, "w") as file:
            file.write(response.text)

        postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        with open(data_path, "r") as file:
            cur.copy_expert(
                "COPY stock_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                file,
            )
        conn.commit()

    @task
    def filter_data():
        input_path = "/opt/airflow/dags/files/stock.csv"
        output_path = "/opt/airflow/dags/files/filtered_stock.csv"

        df = pd.read_csv(input_path)

        filtered_df = df.drop_duplicates(subset=["Date"])

        filtered_df.to_csv(output_path, index=False)

    @task
    def merge_data():
        filtered_csv_path = "/opt/airflow/dags/files/filtered_stock.csv"

        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            with open(filtered_csv_path, "r") as file:
                cur.copy_expert(
                    "COPY stock FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()
        except Exception as e:
            return 1

    @task
    def print_data():
        query = """
            SELECT *
            FROM stock;
        """
        try:
            postgres_hook = PostgresHook(postgres_conn_id="tutorial_pg_conn")
            conn = postgres_hook.get_conn()
            cur = conn.cursor()
            cur.execute(query)
            data = cur.fetchall()
            print("Data from stock table:")
            for row in data:
                print(row)
            conn.close()
        except Exception as e:
            return 1
        
    [create_stock_table, create_stock_temp_table] >> get_data() >> filter_data() >> merge_data() >> print_data()


dag = ProcessAppleStock()
