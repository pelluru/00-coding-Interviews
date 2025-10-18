from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from typing import Optional
from .db import CustomerRepo
from .models import Customer, CustomerIn
from .auth import verify_token

app = FastAPI(title="Customer API", version="1.0.0")
repo = CustomerRepo()
auth_scheme = HTTPBearer()

async def require_user(creds: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    try:
        return verify_token(creds.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.get("/customers", response_model=list[Customer])
async def list_customers(skip: int = 0, limit: int = 50, user: str = Depends(require_user)):
    return repo.list(skip, limit)

@app.post("/customers", response_model=Customer, status_code=201)
async def create_customer(payload: CustomerIn, user: str = Depends(require_user)):
    return repo.create(payload)

@app.get("/customers/{id}", response_model=Customer)
async def get_customer(id: int, user: str = Depends(require_user)):
    cust = repo.get(id)
    if not cust:
        raise HTTPException(404, "Customer not found")
    return cust

@app.put("/customers/{id}", response_model=Customer)
async def replace_customer(
    id: int,
    payload: CustomerIn,
    if_match: Optional[str] = Header(None, convert_underscores=False),
    user: str = Depends(require_user),
):
    current = repo.get(id)
    if not current:
        raise HTTPException(404, "Customer not found")
    if if_match and if_match != current.etag:
        raise HTTPException(412, "ETag precondition failed")
    updated = repo.replace(id, payload)
    return updated

@app.delete("/customers/{id}", status_code=204)
async def delete_customer(id: int, user: str = Depends(require_user)):
    if not repo.delete(id):
        raise HTTPException(404, "Customer not found")
    return None