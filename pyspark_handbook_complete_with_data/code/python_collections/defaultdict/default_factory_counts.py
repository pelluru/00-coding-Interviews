"""
defaultdict example 3: counting via default factory
Run: python default_factory_counts.py
"""
from collections import defaultdict

def main():
    counts = defaultdict(int)
    for ch in "mississippi":
        counts[ch] += 1
    print(dict(counts))

if __name__ == "__main__":
    main()
