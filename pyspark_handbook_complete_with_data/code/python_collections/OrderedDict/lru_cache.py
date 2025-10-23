"""
OrderedDict example 1: tiny LRU cache
Run: python lru_cache.py
"""
from collections import OrderedDict

class LRUCache:
    def __init__(self, capacity: int):
        self.cap = capacity
        self.od = OrderedDict()
    def get(self, key):
        if key not in self.od: return -1
        self.od.move_to_end(key)
        return self.od[key]
    def put(self, key, val):
        if key in self.od:
            self.od.move_to_end(key)
        self.od[key] = val
        if len(self.od) > self.cap:
            self.od.popitem(last=False)

def main():
    lru = LRUCache(2)
    lru.put(1,1); lru.put(2,2)
    print(lru.get(1))
    lru.put(3,3)
    print(lru.get(2))  # -1

if __name__ == "__main__":
    main()
