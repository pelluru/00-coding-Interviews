#!/usr/bin/env python3
"""Example 1: Subsets (Power Set) using Backtracking
Given a list of distinct integers, return all possible subsets (the power set).
Backtracking idea:
- At each index i, choose to include nums[i] or skip it, then recurse.
- Record the current path at each node as a valid subset.
Time: O(2^n * n), Space: O(n) recursion + output.
"""
from typing import List

def subsets(nums: List[int]) -> List[List[int]]:
    res: List[List[int]] = []
    path: List[int] = []

    def dfs(i: int) -> None:
        # Record current subset (copy path to avoid later mutation)
        res.append(path.copy())
        # Try extending with each of the remaining elements
        for j in range(i, len(nums)):
            path.append(nums[j])   # choose
            dfs(j + 1)             # explore
            path.pop()             # un-choose (backtrack)

    dfs(0)
    return res

def run_example_1() -> None:
    nums = [1, 2, 3]
    print("Example 1 — Subsets:", subsets(nums))

if __name__ == "__main__":
    run_example_1()
