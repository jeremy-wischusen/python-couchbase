from databases.couchbase.server import CouchbaseServer

cb = CouchbaseServer(url='couchbase://localhost', user_name='devuser', password='password', bucket='main')
doc = cb.get('test_doc')
print(doc)