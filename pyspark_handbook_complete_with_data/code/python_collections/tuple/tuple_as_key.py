"""
Tuple example 2: using tuples as dict keys
Run: python tuple_as_key.py
"""
def main():
    grid = {(0,0): "S", (1,2): "X"}
    print(grid[(1,2)])

if __name__ == "__main__":
    main()
