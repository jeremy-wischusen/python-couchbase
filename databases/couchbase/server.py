from couchbase.cluster import Cluster, PasswordAuthenticator, Bucket


class CouchbaseServer:
    """
    Simple wrapper class with some utility functions for interacting with
    a Couchbase database.
    """
    """
    A reference to the currently open Couchbase bucket.
    """
    bucket: Bucket
    """
    A reference to the currently connected Couchbase cluster.
    """
    cluster: Cluster

    def __init__(self, url: str, user_name: str, password: str, bucket: str = None):
        """
        Creates, connects and authenticates to the Couchbase cluster.
        If bucket is provided, that bucket will be opened.
        :param url: e.g, couchbase://localhost
        :param user_name: User name.
        :param password: Password
        :param bucket: Name of the bucket to initially open.
        """
        self.cluster = Cluster(url)
        auth = PasswordAuthenticator(user_name, password)
        self.cluster.authenticate(auth)
        if len(bucket) > 0:
            self.open_bucket(bucket)

    def open_bucket(self, name: str):
        """
        Opens a Couchbase bucket and stores a reference to it.
        :param name: Name of the bucket to open.
        """
        self.bucket = self.cluster.open_bucket(name)

    def get(self, id: str, full_doc: bool = False):
        """
        Gets a document by it's id. If full_doc is true, the full result of
        the bucket get will be returned which contains additional properties.
        Otherwise, just the value property is returned which contains the actual
        database document.
        :param id: Document ID
        :param full_doc: If true, return full result, otherwise just return the document.
        :return: ValueResult if full_doc is true. dict if full_value is false.
        """
        doc = self.bucket.get(id)
        if not full_doc:
            return doc.value
        return doc

    def upsert(self, doc_id: str, doc: dict):
        """
        Updates the document if it exists, otherwise creates it.
        :param doc_id: Document ID
        :param doc: Dictionary
        """
        return self.bucket.upsert(doc_id, doc)

    def update(self, doc_id: str, doc: dict):
        """
        Replaces the exiting document with the one passed in.
        :param doc_id: Document ID
        :param doc: Dictionary
        """
        return self.bucket.replace(doc_id, doc)

    def insert(self, doc_id: str, doc: dict):
        """
        Creates a new document.
        :param doc_id: Document ID
        :param doc: Dictionary
        """
        return self.bucket.insert(doc_id, doc)

    def delete(self, doc_id: str):
        """
        Deletes the document.
        :param doc_id: Document ID
        """
        return self.bucket.remove(doc_id)

    def select_query(self, sql: str) -> list:
        """
        Performs a query and loops through it to return a list.
        :param sql: The N1QL to execute.
        :return: list
        """
        rows = []
        for r in self.bucket.n1ql_query(sql):
            rows.append(r)
        return rows

    def select_query_single_result(self, sql: str):
        """
        Performs the N1QL query and returns a single result.
        :param sql: The N1QL to execute.
        :return: Mixed
        """
        return self.bucket.n1ql_query(sql).get_single_result()

    def query(self, sql: str):
        """
        Performs the N1QL query.
        :param sql: The N1QL to execute.
        :return: N1QLRequest
        """
        return self.bucket.n1ql_query(sql)

    def get_bucket(self) -> Bucket:
        """
        Returns the bucket object so you can perform direct
        operations on the bucket if needed.
        :return: Bucket
        """
        return self.bucket
