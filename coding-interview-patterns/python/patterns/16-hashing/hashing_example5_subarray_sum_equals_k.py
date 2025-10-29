#!/usr/bin/env python3
"""Example 5: Subarray Sum Equals K (Prefix Sum Hashing)"""
from collections import defaultdict
def subarray_sum(nums, k):
    count = 0
    pref = 0
    seen = defaultdict(int)
    seen[0] = 1
    for x in nums:
        pref += x
        count += seen[pref - k]
        seen[pref] += 1
    return count
if __name__ == "__main__":
    print("Example 5 — Subarray Sum Equals K:", subarray_sum([1, 1, 1], 2))
