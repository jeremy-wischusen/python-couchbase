import datetime


class QueryResult:
    def __init__(self, columns, rows):
        self.columns = columns
        self.rows = rows
        self.number_of_rows = len(rows)

    def to_json(self, add_props=None):
        rows = []
        for row in self.rows:
            r = {}
            for col, val in enumerate(row):
                if isinstance(val, datetime.date):
                    val = self.__date_to_milliseconds(val)
                if add_props and isinstance(add_props, dict):
                    r.update(add_props)
                r[self.columns[col]] = val
            rows.append(r)
        return rows

    def __date_to_milliseconds(self, date: datetime.date):
        return int((datetime.datetime(date.year, date.month, date.day, 0, 0).timestamp() * 1000))
