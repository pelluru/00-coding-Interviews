#!/usr/bin/env python3
"""Example 1: Count Inversions (Divide & Conquer with Merge Sort)
Counts pairs (i < j) such that a[i] > a[j].
Time: O(n log n), Space: O(n).
"""
from typing import List, Tuple

def count_inversions(a: List[int]) -> int:
    def rec(x: List[int]) -> Tuple[List[int], int]:
        if len(x) <= 1:
            return x, 0
        m = len(x) // 2
        L, c1 = rec(x[:m])
        R, c2 = rec(x[m:])
        i = j = 0
        out: List[int] = []
        inv = 0
        while i < len(L) and j < len(R):
            if L[i] <= R[j]:
                out.append(L[i]); i += 1
            else:
                out.append(R[j]); j += 1
                inv += len(L) - i   # all remaining in L are greater than R[j]
        out.extend(L[i:]); out.extend(R[j:])
        return out, inv + c1 + c2

    return rec(list(a))[1]

if __name__ == "__main__":
    print("Inversions in [2,4,1,3,5]:", count_inversions([2,4,1,3,5]))  # 3
