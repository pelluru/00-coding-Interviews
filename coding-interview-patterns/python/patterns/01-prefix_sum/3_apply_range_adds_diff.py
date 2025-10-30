"""
Prefix Sum — Apply Many Range Increments via Difference Array
-------------------------------------------------------------
Apply updates (l, r, v) on an initially zero array in O(n + q).
"""
from typing import List, Tuple

def apply_range_adds(n: int, updates: List[Tuple[int, int, int]]) -> List[int]:
    diff = [0] * (n + 1)
    for l, r, v in updates:
        diff[l] += v
        if r + 1 < len(diff):
            diff[r + 1] -= v
    cur = 0
    out = [0] * n
    for i in range(n):
        cur += diff[i]
        out[i] = cur
    return out

def main():
    n = 5
    updates = [(1, 3, 2), (2, 4, 3)]
    print("n=5, updates=", updates)
    print("Result:", apply_range_adds(n, updates))  # [0,2,5,5,3]

if __name__ == "__main__":
    main()
