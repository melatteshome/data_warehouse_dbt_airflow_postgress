import os
import psycopg2
from dotenv import load_dotenv


class DatabaseConnection:
    def __init__(self):
        load_dotenv()

        self.host = os.getenv("DB_HOST")
        self.port = os.getenv("DB_PORT")
        self.database = os.getenv("DB_NAME")
        self.user = os.getenv("DB_USER")
        self.password = os.getenv("DB_PASSWORD")
        self.connection = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
            )
            print("Connected to the database successfully!")
        except psycopg2.Error as e:
            print(f"Failed to connect to the database: {e}")

    def close(self):
        if self.connection is not None:
            self.connection.close()
            print("Connection to the database closed.")

    def execute_query(self, query):
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            self.connection.commit()
            print("Query executed successfully!")
        except psycopg2.Error as e:
            print(f"Failed to execute the query: {e}")
        finally:
            cursor.close()


# Example usage
db = DatabaseConnection()
db.connect()

# Perform database operations here...

db.close()
