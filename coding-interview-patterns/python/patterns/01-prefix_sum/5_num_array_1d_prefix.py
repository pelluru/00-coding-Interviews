"""
Prefix Sum — 1D Immutable Range Sum (NumArray)
----------------------------------------------
Build a 1D prefix array to answer sumRange(i, j) in O(1).
"""
from typing import List

class NumArray:
    def __init__(self, nums: List[int]):
        self.P = [0]
        for x in nums:
            self.P.append(self.P[-1] + x)

    def sumRange(self, i: int, j: int) -> int:
        return self.P[j + 1] - self.P[i]

def main():
    arr = NumArray([-2, 0, 3, -5, 2, -1])
    print("sumRange(0,2)=", arr.sumRange(0,2))  # 1
    print("sumRange(2,5)=", arr.sumRange(2,5))  # -1
    print("sumRange(0,5)=", arr.sumRange(0,5))  # -3

if __name__ == "__main__":
    main()
