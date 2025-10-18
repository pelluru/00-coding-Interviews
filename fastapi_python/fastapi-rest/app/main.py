from fastapi import FastAPI, Depends, HTTPException, Header
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from typing import Optional
from .db import ItemRepo
from .models import Item, ItemIn
from .auth import verify_token

app = FastAPI(title="Items API", version="1.0.0")
repo = ItemRepo()
auth_scheme = HTTPBearer()

async def require_user(creds: HTTPAuthorizationCredentials = Depends(auth_scheme)):
    try:
        return verify_token(creds.credentials)
    except Exception:
        raise HTTPException(status_code=401, detail="Invalid token")

@app.get("/items", response_model=list[Item])
async def list_items(skip: int = 0, limit: int = 50, user: str = Depends(require_user)):
    return repo.list(skip, limit)

@app.post("/items", response_model=Item, status_code=201)
async def create_item(payload: ItemIn, user: str = Depends(require_user)):
    return repo.create(payload)

@app.get("/items/{id}", response_model=Item)
async def get_item(id: int, user: str = Depends(require_user)):
    itm = repo.get(id)
    if not itm:
        raise HTTPException(404, "Item not found")
    return itm

@app.put("/items/{id}", response_model=Item)
async def replace_item(
    id: int,
    payload: ItemIn,
    if_match: Optional[str] = Header(None, convert_underscores=False),
    user: str = Depends(require_user),
):
    current = repo.get(id)
    if not current:
        raise HTTPException(404, "Item not found")
    if if_match and if_match != current.etag:
        raise HTTPException(412, "ETag precondition failed")
    updated = repo.replace(id, payload)
    return updated

@app.delete("/items/{id}", status_code=204)
async def delete_item(id: int, user: str = Depends(require_user)):
    if not repo.delete(id):
        raise HTTPException(404, "Item not found")
    return None