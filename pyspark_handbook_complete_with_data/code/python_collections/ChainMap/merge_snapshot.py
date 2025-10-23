"""
ChainMap example 3: flatten/merge snapshot
Run: python merge_snapshot.py
"""
from collections import ChainMap

def merge_snapshot(cm: ChainMap):
    out = {}
    for m in reversed(cm.maps):
        out.update(m)
    return out

def main():
    cm = ChainMap({"x":1}, {"x":0, "y":2})
    print(merge_snapshot(cm))

if __name__ == "__main__":
    main()
