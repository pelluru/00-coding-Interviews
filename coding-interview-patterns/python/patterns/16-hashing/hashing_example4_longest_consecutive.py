#!/usr/bin/env python3
"""Example 4: Longest Consecutive Sequence (Hash Set)"""
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
if __name__ == "__main__":
    print("Example 4 — Longest Consecutive:", longest_consecutive([100, 4, 200, 1, 3, 2]))
