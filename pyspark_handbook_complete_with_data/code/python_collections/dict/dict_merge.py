"""
Dict example 2: merge (Python 3.9+)
Run: python dict_merge.py
"""
def main():
    d1 = {"a":1}; d2 = {"b":2}
    merged = d1 | d2
    updated = d1 | {"a": 42}
    print("merged:", merged, "updated:", updated)

if __name__ == "__main__":
    main()
