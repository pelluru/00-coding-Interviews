#!/usr/bin/env python3
"""Example 3: First Unique Character (Hash Counting)"""
from collections import Counter
def first_unique_char(s: str) -> int:
    c = Counter(s)
    for i, ch in enumerate(s):
        if c[ch] == 1:
            return i
    return -1
if __name__ == "__main__":
    print("Example 3 — First Unique Char:", first_unique_char("leetcode"))
