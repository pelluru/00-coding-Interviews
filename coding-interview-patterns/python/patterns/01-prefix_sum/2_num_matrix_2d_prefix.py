"""
Prefix Sum — 2D Summed-Area Table (NumMatrix)
---------------------------------------------
Answer O(1) region-sum queries after O(mn) preprocessing.
"""
from typing import List

class NumMatrix:
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

def main():
    M = [
        [3, 0, 1, 4, 2],
        [5, 6, 3, 2, 1],
        [1, 2, 0, 1, 5],
        [4, 1, 0, 1, 7],
        [1, 0, 3, 0, 5],
    ]
    nm = NumMatrix(M)
    print("Sum (2,1)-(4,3):", nm.sumRegion(2,1,4,3))  # 8
    print("Sum (1,1)-(2,2):", nm.sumRegion(1,1,2,2))  # 11

if __name__ == "__main__":
    main()
