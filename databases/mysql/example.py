from databases.mysql.server import MySQLServer
from databases.couchbase.server import CouchbaseServer

dbs = MySQLServer(url='localhost', user_name='devuser', password='password')
dbs.select_database('employees')
result = dbs.fetchall('SELECT * FROM employees LIMIT 2')

print(result.to_json({'documentType': 'employee'}))
