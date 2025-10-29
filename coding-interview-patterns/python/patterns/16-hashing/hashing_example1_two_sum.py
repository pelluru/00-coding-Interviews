#!/usr/bin/env python3
"""Example 1: Two Sum (Hash Map)"""
from typing import List, Optional, Tuple
def two_sum(nums: List[int], target: int) -> Optional[Tuple[int, int]]:
    seen = {}
    for i, x in enumerate(nums):
        y = target - x
        if y in seen:
            return (seen[y], i)
        seen[x] = i
    return None
if __name__ == "__main__":
    print("Example 1 — Two Sum:", two_sum([2, 7, 11, 15], 9))
