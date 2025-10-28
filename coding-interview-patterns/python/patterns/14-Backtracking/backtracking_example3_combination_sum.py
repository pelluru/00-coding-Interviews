#!/usr/bin/env python3
"""Example 3: Combination Sum (allow reuse)
Given candidates and a target, find combinations that sum to target.
Backtracking idea:
- At index 'start', try each candidate >= start (to avoid reordering duplicates).
- Recurse with remaining target; allow reusing the same candidate by passing same index.
- Backtrack after exploring.
Time: roughly O(T * N) in practice; worst-case exponential.
"""
from typing import List

def combination_sum(candidates: List[int], target: int) -> List[List[int]]:
    res: List[List[int]] = []
    path: List[int] = []
    candidates.sort()

    def dfs(start: int, remain: int) -> None:
        if remain == 0:
            res.append(path.copy())
            return
        if remain < 0:
            return
        for i in range(start, len(candidates)):
            c = candidates[i]
            # pruning: if candidate exceeds remain after sort, we can break
            if c > remain:
                break
            path.append(c)            # choose
            dfs(i, remain - c)        # explore (i not i+1 to allow reuse)
            path.pop()                # backtrack

    dfs(0, target)
    return res

def run_example_3() -> None:
    candidates = [2, 3, 6, 7]
    target = 7
    print("Example 3 — Combination Sum:", combination_sum(candidates, target))

if __name__ == "__main__":
    run_example_3()
