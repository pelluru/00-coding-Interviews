from fastapi.testclient import TestClient
from app.main import app
from app.auth import create_token

client = TestClient(app)
TOKEN = create_token("tester")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def test_crud_flow():
    r = client.post("/customers", json={"first_name": "Ada", "last_name": "Lovelace", "email": "ada@example.com"}, headers=HEADERS)
    assert r.status_code == 201
    cust = r.json()

    r = client.get("/customers", headers=HEADERS)
    assert r.status_code == 200 and len(r.json()) == 1

    r = client.get(f"/customers/{cust['id']}", headers=HEADERS)
    assert r.status_code == 200

    r = client.put(
        f"/customers/{cust['id']}",
        json={"first_name": "Ada", "last_name": "Byron", "email": "ada@example.com"},
        headers={**HEADERS, "If-Match": cust["etag"]},
    )
    assert r.status_code == 200

    r = client.delete(f"/customers/{cust['id']}", headers=HEADERS)
    assert r.status_code == 204