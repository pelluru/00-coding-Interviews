from fastapi.testclient import TestClient
from app.main import app
from app.auth import create_token

client = TestClient(app)
TOKEN = create_token("tester")
HEADERS = {"Authorization": f"Bearer {TOKEN}"}

def test_crud_flow():
    r = client.post("/items", json={"name": "pen", "price": 1.5}, headers=HEADERS)
    assert r.status_code == 201
    item = r.json()

    r = client.get("/items", headers=HEADERS)
    assert r.status_code == 200 and len(r.json()) == 1

    r = client.get(f"/items/{item['id']}", headers=HEADERS)
    assert r.status_code == 200

    r = client.put(
        f"/items/{item['id']}",
        json={"name": "pen", "price": 2.0},
        headers={**HEADERS, "If-Match": item["etag"]},
    )
    assert r.status_code == 200

    r = client.delete(f"/items/{item['id']}", headers=HEADERS)
    assert r.status_code == 204