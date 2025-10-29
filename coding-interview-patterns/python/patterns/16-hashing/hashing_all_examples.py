#!/usr/bin/env python3
"""Hashing — All Examples in One File"""
from collections import defaultdict, Counter

def two_sum(nums, target):
    seen = {}
    for i, x in enumerate(nums):
        y = target - x
        if y in seen:
            return (seen[y], i)
        seen[x] = i
    return None

def group_anagrams(strs):
    buckets = defaultdict(list)
    for s in strs:
        key = tuple(sorted(s))
        buckets[key].append(s)
    return list(buckets.values())

def first_unique_char(s):
    c = Counter(s)
    for i, ch in enumerate(s):
        if c[ch] == 1:
            return i
    return -1

def longest_consecutive(nums):
    s = set(nums)
    best = 0
    for x in s:
        if x - 1 not in s:
            y = x
            while y in s:
                y += 1
            best = max(best, y - x)
    return best

def subarray_sum(nums, k):
    seen = defaultdict(int)
    seen[0] = 1
    pref = 0
    count = 0
    for x in nums:
        pref += x
        count += seen[pref - k]
        seen[pref] += 1
    return count

def run_all():
    print("Example 1 — Two Sum:", two_sum([2,7,11,15], 9))
    print("Example 2 — Group Anagrams:", group_anagrams(["eat", "tea", "tan", "ate", "nat", "bat"]))
    print("Example 3 — First Unique Char:", first_unique_char("leetcode"))
    print("Example 4 — Longest Consecutive:", longest_consecutive([100,4,200,1,3,2]))
    print("Example 5 — Subarray Sum Equals K:", subarray_sum([1,1,1], 2))

if __name__ == "__main__":
    run_all()
