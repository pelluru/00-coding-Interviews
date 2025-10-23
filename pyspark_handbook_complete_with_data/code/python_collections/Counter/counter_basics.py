"""
Counter example 1: counts and most_common
Run: python counter_basics.py
"""
from collections import Counter

def main():
    c = Counter("banana")
    print(c, c.most_common(2))

if __name__ == "__main__":
    main()
