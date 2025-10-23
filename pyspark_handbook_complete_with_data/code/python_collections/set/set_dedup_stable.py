"""
Set example 3: stable dedup via seen-set
Run: python set_dedup_stable.py
"""
def dedup_stable(iterable):
    seen = set()
    for x in iterable:
        if x not in seen:
            seen.add(x)
            yield x

def main():
    print(list(dedup_stable([1,2,2,3,1,4,3])))

if __name__ == "__main__":
    main()
