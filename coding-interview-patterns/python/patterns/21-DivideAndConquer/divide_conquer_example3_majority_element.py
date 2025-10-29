#!/usr/bin/env python3
"""Example 3: Majority Element (Boyer–Moore Voting)
Finds the element occurring > n/2 times (assuming it exists).
Time: O(n), Space: O(1).
Note: This is not divide-and-conquer per se, but often paired with that section.
"""
from typing import List

def majority_element(nums: List[int]) -> int:
    cand = None; cnt = 0
    for x in nums:
        if cnt == 0:
            cand = x; cnt = 1
        elif x == cand:
            cnt += 1
        else:
            cnt -= 1
    return cand

if __name__ == "__main__":
    print("Majority of [2,2,1,1,1,2,2]:", majority_element([2,2,1,1,1,2,2]))  # 2
