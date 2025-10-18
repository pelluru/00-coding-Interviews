from pydantic import BaseModel, Field
from typing import Optional

class ItemIn(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    price: float = Field(ge=0)
    description: Optional[str] = None

class Item(ItemIn):
    id: int
    etag: str