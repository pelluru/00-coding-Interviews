# FastAPI Customer API (Demo)

A minimal, production-leaning FastAPI REST service that demonstrates:
- Resource-oriented Customer CRUD
- JWT Bearer authentication
- ETag-based concurrency control (`If-Match` on PUT)
- Pagination parameters
- Auto-generated docs at `/docs`

## Quickstart

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export PYTHONPATH=.
uvicorn app.main:app --reload
```

### Generate a demo token (Python REPL)

```python
from app.auth import create_token
print(create_token("tester"))
```

### Try requests (replace TOKEN)

```bash
curl -H "Authorization: Bearer TOKEN" http://localhost:8000/customers
curl -X POST -H "Authorization: Bearer TOKEN" -H "Content-Type: application/json"      -d '{"first_name":"Ada","last_name":"Lovelace","email":"ada@example.com"}' http://localhost:8000/customers
```

## Tests

```bash
pytest -q
```

## OpenAPI & Postman

- `customer-api-openapi.yaml` provides the OpenAPI 3.0 spec.
- `customer-api-postman-collection.json` includes example requests with Bearer auth.