#!/usr/bin/env python3
"""Example 2: Group Anagrams (Hash Map by Canonical Key)"""
from typing import List
from collections import defaultdict
def group_anagrams(strs: List[str]) -> List[List[str]]:
    buckets = defaultdict(list)
    for s in strs:
        key = tuple(sorted(s))
        buckets[key].append(s)
    return list(buckets.values())
if __name__ == "__main__":
    print("Example 2 — Group Anagrams:", group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"]))
