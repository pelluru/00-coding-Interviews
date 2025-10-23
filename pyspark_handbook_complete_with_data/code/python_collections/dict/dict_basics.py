"""
Dict example 1: get, setdefault, items
Run: python dict_basics.py
"""
def main():
    d = {}
    d["a"] = d.get("a", 0) + 1
    d.setdefault("b", 0)
    for k, v in d.items():
        print(k, v)

if __name__ == "__main__":
    main()
