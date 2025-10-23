"""
namedtuple example 3: _replace and unpack
Run: python replace_and_unpack.py
"""
from collections import namedtuple

def main():
    P = namedtuple("P", "a b")
    p1 = P(1,2)
    p2 = p1._replace(b=42)
    a, b = p2
    print(a, b)

if __name__ == "__main__":
    main()
