#!/usr/bin/env python3
"""Example 2: Maximum Subarray (Divide & Conquer)
Returns the maximum subarray sum using a conquer step with (sum, bestpref, bestsuf, best).
Time: O(n), Space: O(log n) recursion.
"""
from typing import List, Tuple

def max_subarray_divide(a: List[int]) -> int:
    def rec(l: int, r: int) -> Tuple[int, int, int, int]:
        if l == r:
            return a[l], a[l], a[l], a[l]  # total sum, best prefix, best suffix, best subarray
        m = (l + r) // 2
        ls, lp, lsu, lb = rec(l, m)
        rs, rp, rsu, rb = rec(m + 1, r)
        s = ls + rs
        pref = max(lp, ls + rp)
        suf  = max(rsu, rs + lsu)
        best = max(lb, rb, lsu + rp)
        return s, pref, suf, best
    return rec(0, len(a) - 1)[3]

if __name__ == "__main__":
    print("Max subarray of [-2,1,-3,4,-1,2,1,-5,4]:", max_subarray_divide([-2,1,-3,4,-1,2,1,-5,4]))  # 6
