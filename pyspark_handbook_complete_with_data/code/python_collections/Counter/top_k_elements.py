"""
Counter example 2: top-k elements
Run: python top_k_elements.py
"""
from collections import Counter

def top_k(nums, k):
    return [x for x, _ in Counter(nums).most_common(k)]

def main():
    print(top_k([1,1,1,2,2,3], 2))

if __name__ == "__main__":
    main()
