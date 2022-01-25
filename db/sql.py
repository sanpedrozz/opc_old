import psycopg2
from pprint import pprint
from psycopg2.extras import RealDictCursor


class DBConnector:
    def __init__(self, db_name: str, user: str, psw: str, host: str, port: str):
        self.connect = psycopg2.connect(database=db_name,
                                        user=user,
                                        password=psw,
                                        host=host,
                                        port=port,
                                        cursor_factory=RealDictCursor)
        self.cursor = self.connect.cursor()

    def fetchall_query(self, query):
        self.execute_query(query)
        return self.cursor.fetchall()

    def insert_query(self, query):
        self.cursor.execute(query)
        self.connect.commit()