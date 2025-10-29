"""
https://blog.algomaster.io/p/15-leetcode-patterns

Prefix Sum Patterns — Three Classic Variants
--------------------------------------------
1. subarray_sum_k()       → count subarrays whose sum == k
2. NumMatrix              → constant-time 2D range-sum queries
3. apply_range_adds()     → batch range updates using diff array
"""
https://www.youtube.com/watch?v=yuws7YK0Yng

from collections import defaultdict
from typing import List, Tuple


def subarray_sum_k(nums: List[int], k: int) -> int:
    """
    Count how many contiguous subarrays have sum exactly k.
    """
    pref = 0
    seen = defaultdict(int)
    seen[0] = 1  # base prefix (empty array)
    ans = 0

    for x in nums:
        pref += x
        ans += seen[pref - k]  # how many prefixes yield sum k
        seen[pref] += 1
    return ans


class NumMatrix:
    """
    2D prefix sum (summed-area table) for O(1) region queries.
    """

    def __init__(self, M: List[List[int]]):
        m = len(M)
        n = len(M[0]) if m else 0
        self.P = [[0] * (n + 1) for _ in range(m + 1)]

        for i in range(1, m + 1):
            for j in range(1, n + 1):
                self.P[i][j] = (
                    M[i - 1][j - 1]
                    + self.P[i - 1][j]
                    + self.P[i][j - 1]
                    - self.P[i - 1][j - 1]
                )

    def sumRegion(self, r1: int, c1: int, r2: int, c2: int) -> int:
        P = self.P
        return P[r2 + 1][c2 + 1] - P[r1][c2 + 1] - P[r2 + 1][c1] + P[r1][c1]


def apply_range_adds(n: int, updates: List[Tuple[int, int, int]]) -> List[int]:
    """
    Apply many (l, r, v) increments efficiently.
    """
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


# ---------------------------------------------------------------------------
# Demo / Self-test
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("=== 1. subarray_sum_k ===")
    nums = [1, 1, 1]
    print(f"Array: {nums}, k=2 -> Count =", subarray_sum_k(nums, 2))  # 2

    print("\n=== 2. NumMatrix ===")
    M = [
        [3, 0, 1, 4, 2],
        [5, 6, 3, 2, 1],
        [1, 2, 0, 1, 5],
        [4, 1, 0, 1, 7],
        [1, 0, 3, 0, 5],
    ]
    num_matrix = NumMatrix(M)
    print("Sum of region (2,1)-(4,3):", num_matrix.sumRegion(2, 1, 4, 3))  # 8
    print("Sum of region (1,1)-(2,2):", num_matrix.sumRegion(1, 1, 2, 2))  # 11

    print("\n=== 3. apply_range_adds ===")
    updates = [(1, 3, 2), (2, 4, 3)]
    print("n=5, updates=", updates)
    print("Resulting array:", apply_range_adds(5, updates))  # [0,2,5,5,3]
