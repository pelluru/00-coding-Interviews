#!/usr/bin/env python3
"""Backtracking — Five Examples in One File
Examples:
  1) Subsets (Power Set)
  2) Permutations (distinct)
  3) Combination Sum (allow reuse)
  4) N-Queens
  5) Letter Combinations of a Phone Number
Usage:
  python backtracking_all_examples.py
  python backtracking_all_examples.py 1 3 5
"""
from typing import List, Set

# -------- Example 1: Subsets --------
def subsets(nums: List[int]) -> List[List[int]]:
    res, path = [], []
    def dfs(i: int):
        res.append(path.copy())
        for j in range(i, len(nums)):
            path.append(nums[j]); dfs(j+1); path.pop()
    dfs(0); return res
def run_example_1():
    print("Example 1 — Subsets:", subsets([1,2,3]))

# -------- Example 2: Permutations --------
def permutations(nums: List[int]) -> List[List[int]]:
    res, used, path = [], [False]*len(nums), []
    def dfs():
        if len(path)==len(nums): res.append(path.copy()); return
        for i in range(len(nums)):
            if used[i]: continue
            used[i]=True; path.append(nums[i]); dfs(); path.pop(); used[i]=False
    dfs(); return res
def run_example_2():
    print("Example 2 — Permutations:", permutations([1,2,3]))

# -------- Example 3: Combination Sum --------
def combination_sum(candidates: List[int], target: int) -> List[List[int]]:
    candidates.sort(); res, path = [], []
    def dfs(start: int, remain: int):
        if remain==0: res.append(path.copy()); return
        if remain<0: return
        for i in range(start,len(candidates)):
            c=candidates[i]
            if c>remain: break
            path.append(c); dfs(i, remain-c); path.pop()
    dfs(0,target); return res
def run_example_3():
    print("Example 3 — Combination Sum:", combination_sum([2,3,6,7], 7))

# -------- Example 4: N-Queens --------
def solve_n_queens(n: int):
    res=[]; cols=set(); d1=set(); d2=set(); board=[['.' for _ in range(n)] for _ in range(n)]
    def dfs(r:int):
        if r==n: res.append([''.join(row) for row in board]); return
        for c in range(n):
            if c in cols or (r-c) in d1 or (r+c) in d2: continue
            cols.add(c); d1.add(r-c); d2.add(r+c); board[r][c]='Q'
            dfs(r+1)
            board[r][c]='.'; cols.remove(c); d1.remove(r-c); d2.remove(r+c)
    dfs(0); return res
def run_example_4():
    sols=solve_n_queens(4)
    print(f"Example 4 — N-Queens (n=4) count={len(sols)}:")
    for s in sols:
        for row in s: print(row)
        print('----')

# -------- Example 5: Letter Combinations --------
def letter_combinations(digits: str) -> List[str]:
    if not digits: return []
    phone={'2':'abc','3':'def','4':'ghi','5':'jkl','6':'mno','7':'pqrs','8':'tuv','9':'wxyz'}
    res=[]; path=[]
    def dfs(i:int):
        if i==len(digits): res.append(''.join(path)); return
        for ch in phone[digits[i]]:
            path.append(ch); dfs(i+1); path.pop()
    dfs(0); return res
def run_example_5():
    print("Example 5 — Letter combinations for '23':", letter_combinations('23'))

def _run_all(selected=None):
    mapping={1:run_example_1,2:run_example_2,3:run_example_3,4:run_example_4,5:run_example_5}
    order=selected or [1,2,3,4,5]
    for i in order: mapping[i]()

if __name__=="__main__":
    import sys
    if len(sys.argv)==1: _run_all()
    else: _run_all([int(x) for x in sys.argv[1:]])
