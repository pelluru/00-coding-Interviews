"""
Counter example 3: arithmetic and subtraction
Run: python counter_arithmetic.py
"""
from collections import Counter

def main():
    a = Counter("aabccc")
    b = Counter("bccd")
    print("a+b:", a + b)
    print("a-b:", a - b)  # no negatives
    a.subtract(b)         # in-place, may have negatives
    print("a after subtract:", a)

if __name__ == "__main__":
    main()
