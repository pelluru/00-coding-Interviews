#!/usr/bin/env python3
"""Example 1: Next Greater Element (Monotonic Decreasing Stack)
For each element, find the next element to the right that is greater; otherwise -1.

Algorithm:
- Maintain a stack of indices with decreasing values.
- When current value x is greater than value at stack top, pop and set result for that index to x.
- Push current index.
Time: O(n), Space: O(n).
"""
from typing import List

def next_greater(nums: List[int]) -> List[int]:
    st: List[int] = []
    res: List[int] = [-1] * len(nums)
    for i, x in enumerate(nums):
        while st and nums[st[-1]] < x:
            res[st.pop()] = x
        st.append(i)
    return res

if __name__ == "__main__":
    print("Example 1 — Next Greater:", next_greater([2,1,2,4,3]))  # [4,2,4,-1,-1]
