from pydantic import BaseModel, Field
from typing import Optional

class CustomerIn(BaseModel):
    first_name: str = Field(min_length=1, max_length=50)
    last_name: str = Field(min_length=1, max_length=50)
    email: str
    phone: Optional[str] = None

class Customer(CustomerIn):
    id: int
    etag: str