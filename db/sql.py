import psycopg2
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

    def execute_query(self, query):
        return self.cursor.execute(query)

    def fetchall_query(self, query):
        self.execute_query(query)
        return [dict(row) for row in self.cursor.fetchall()][0]

    def insert_query(self, query):
        result = self.execute_query(query)
        self.connect.commit()
        return result
