import mysql.connector
from databases.mysql.queryresult import QueryResult


class MySQLServer:
    def __init__(self, url: str, user_name: str, password: str, database: str = None):
        self.connection = mysql.connector.connect(host=url, user=user_name, password=password, database=database)

    def fetchall(self, sql: str) -> QueryResult:
        c = self.connection.cursor()
        c.execute(sql)
        rows = c.fetchall()
        c.close()
        return QueryResult(c.column_names, rows)

    def select_database(self, database):
        self.connection.database = database

    def __del__(self):
        self.connection.close()


