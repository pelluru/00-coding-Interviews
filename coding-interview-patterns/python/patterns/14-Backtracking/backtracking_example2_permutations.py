#!/usr/bin/env python3
"""Example 2: Permutations of Distinct Integers (Backtracking)
Backtracking idea:
- Build permutations incrementally, track used elements.
- Choose an unused element, place it, recurse; then remove it.
Time: O(n * n!), Space: O(n) + output.
"""
from typing import List

def permutations(nums: List[int]) -> List[List[int]]:
    res: List[List[int]] = []
    used = [False] * len(nums)
    path: List[int] = []

    def dfs() -> None:
        if len(path) == len(nums):
            res.append(path.copy())
            return
        for i in range(len(nums)):
            if used[i]: 
                continue
            used[i] = True       # choose
            path.append(nums[i])
            dfs()                # explore
            path.pop()           # backtrack
            used[i] = False      # un-choose

    dfs()
    return res

def run_example_2() -> None:
    nums = [1, 2, 3]
    print("Example 2 — Permutations:", permutations(nums))

if __name__ == "__main__":
    run_example_2()
