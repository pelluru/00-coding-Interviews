"""
defaultdict example 1: group anagrams
Run: python group_anagrams.py
"""
from collections import defaultdict

def main():
    words = ["eat","tea","tan","ate","nat","bat"]
    buckets = defaultdict(list)
    for w in words:
        buckets["".join(sorted(w))].append(w)
    print(list(buckets.values()))

if __name__ == "__main__":
    main()
