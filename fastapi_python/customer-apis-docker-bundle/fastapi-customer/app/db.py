import hashlib
from typing import Dict, List, Optional
from .models import Customer, CustomerIn

class CustomerRepo:
    def __init__(self):
        self._data: Dict[int, Customer] = {}
        self._seq = 0

    def _etag(self, cust: CustomerIn) -> str:
        raw = f"{cust.first_name}|{cust.last_name}|{cust.email}|{cust.phone}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def list(self, skip=0, limit=50) -> List[Customer]:
        return list(self._data.values())[skip: skip+limit]

    def get(self, id: int) -> Optional[Customer]:
        return self._data.get(id)

    def create(self, payload: CustomerIn) -> Customer:
        self._seq += 1
        cust = Customer(id=self._seq, etag=self._etag(payload), **payload.model_dump())
        self._data[cust.id] = cust
        return cust

    def replace(self, id: int, payload: CustomerIn) -> Optional[Customer]:
        if id not in self._data:
            return None
        cust = Customer(id=id, etag=self._etag(payload), **payload.model_dump())
        self._data[id] = cust
        return cust

    def delete(self, id: int) -> bool:
        return self._data.pop(id, None) is not None