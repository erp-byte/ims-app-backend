"""A crashing endpoint must still answer with CORS headers.

If this fails, every backend 500 shows up in the browser as
"blocked by CORS policy / Failed to fetch" and the operator never sees the real error.
Guards the middleware ORDER in main.py: CORSMiddleware must stay outermost, the
catch-all inside it. Run: python test_cors_on_500.py
"""
from fastapi.testclient import TestClient

import main

ORIGIN = "https://candorims.netlify.app"


@main.app.get("/__boom_test")
def _boom():
    raise RuntimeError("kaboom")


# No `with` — that would run lifespan (startup migrations) against the real DB.
client = TestClient(main.app, raise_server_exceptions=False)


def test_500_carries_cors_headers():
    r = client.get("/__boom_test", headers={"Origin": ORIGIN})
    assert r.status_code == 500, r.status_code
    assert r.headers.get("access-control-allow-origin") == ORIGIN, dict(r.headers)
    assert "kaboom" in r.json()["detail"], r.text


def test_preflight_allowed():
    r = client.options(
        "/interunit/transfers",
        headers={
            "Origin": ORIGIN,
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "content-type",
        },
    )
    assert r.status_code == 200, r.status_code
    assert r.headers.get("access-control-allow-origin") == ORIGIN, dict(r.headers)


if __name__ == "__main__":
    test_500_carries_cors_headers()
    test_preflight_allowed()
    print("OK")
