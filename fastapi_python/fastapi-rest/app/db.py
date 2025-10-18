import hashlib
from typing import Dict, List, Optional
from .models import Item, ItemIn

class ItemRepo:
    def __init__(self):
        self._data: Dict[int, Item] = {}
        self._seq = 0

    def _etag(self, item: ItemIn) -> str:
        raw = f"{item.name}|{item.price}|{item.description}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def list(self, skip=0, limit=50) -> List[Item]:
        return list(self._data.values())[skip: skip+limit]

    def get(self, id: int) -> Optional[Item]:
        return self._data.get(id)

    def create(self, payload: ItemIn) -> Item:
        self._seq += 1
        itm = Item(id=self._seq, etag=self._etag(payload), **payload.model_dump())
        self._data[itm.id] = itm
        return itm

    def replace(self, id: int, payload: ItemIn) -> Optional[Item]:
        if id not in self._data:
            return None
        itm = Item(id=id, etag=self._etag(payload), **payload.model_dump())
        self._data[id] = itm
        return itm

    def delete(self, id: int) -> bool:
        return self._data.pop(id, None) is not None