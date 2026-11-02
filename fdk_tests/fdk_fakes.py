"""Offline stand-ins shared by the fdk tests: Supabase (psycopg2) and HTTP (requests)."""


class FakeCursor:
    """Answers COUNT queries with 1 (tables already seeded) and token lookups with no row."""

    def __init__(self):
        self.last_sql = ""
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, params=None):
        self.last_sql = sql
        self.executed.append((sql, params))

    def fetchone(self):
        if "COUNT" in self.last_sql.upper():
            return (1,)
        return None

    def fetchall(self):
        return []


class FakeConn:
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return FakeCursor()

    def commit(self):
        pass

    def close(self):
        pass


class FakeResponse:
    status_code = 200
    text = "ok"

    def __init__(self, payload=None):
        self._payload = payload if payload is not None else {"ok": True, "result": True}

    def json(self):
        return self._payload
