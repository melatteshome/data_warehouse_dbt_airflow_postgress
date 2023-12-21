import os
import sys
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from scripts.database_connection import DatabaseConnection


default ={
    'owner': 'melat',
    'depends_on_past': False,
    'email':['mtadesse813@gmail.com'],
    'email_on_faliure': True,
    'email_on_retry': True,
    'retries':0,
    'start_date':datetime(2023,12, 21),
    'retry_delay':timedelta(minutes=2)
}

dag = DAG(
    dag_id='traffic',
    default_args=default,
    schedule_interval = '@once',
)

def Traffic():
    task_create_vehecle_table = PostgresOperator(
        task_id='create_vehecle_table',
        sql='scripts/schema/vehecle.sql',
        postgres_conn_id='pg_con',
        )
    task_create_trajectory_table = PostgresOperator(
        task_id='create_trajectory_table',
        sql='scripts\schema\trajectory.sql',
        postgres_conn_id='pg_con',
        )
    def DataReader():
        databaseConnection = DatabaseConnection()

        with databaseConnection as conn:
            path ='scripts/data/20181024_d1_0830_0900 (2).csv'
            with open(path,'r') as file:
                insert_query_vehicle = """
                    INSERT INTO vehicle ("track_id", "type", "traveled_distance", "avg_speed")
                    VALUES (%s, %s, %s, %s)
                """
                insert_query_trajectory = """
                    INSERT INTO trajectory ("track_id", "lat", "lon", "speed", "lon_acc", "lat_acc", "time")
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """

                vehicle_rows = []
                trajectory_rows = []

                for line in file:
                    row = line.strip().split(';')[:-1]

                    track_id = int(row[0].strip())
                    type = row[1].strip()
                    traveled_distance = float(row[2].strip())
                    avg_speed = float(row[3].strip())

                    vehicle_rows.append((track_id, type, traveled_distance, avg_speed))

                    len_row = len(row)
                    for i in range(4, len_row, 6):
                        lat = float(row[i].strip())
                        lon = float(row[i + 1].strip())
                        speed = float(row[i + 2].strip())
                        lon_acc = float(row[i + 3].strip())
                        lat_acc = float(row[i + 4].strip())
                        time = float(row[i + 5].strip())

                        trajectory_rows.append((track_id, lat, lon, speed, lon_acc, lat_acc, time))

                
                conn.execute(insert_query_vehicle, vehicle_rows)
                conn.execute(insert_query_trajectory, trajectory_rows)

    task_create_vehecle_table >> task_create_trajectory_table >> DataReader()


Dag = Traffic()