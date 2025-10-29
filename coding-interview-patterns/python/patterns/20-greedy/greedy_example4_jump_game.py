#!/usr/bin/env python3
from typing import List
def can_jump(nums: List[int]) -> bool:
    far=0
    for i,step in enumerate(nums):
        if i>far: return False
        far=max(far,i+step)
    return True
if __name__ == "__main__":
    print("Example 4 — Can Jump:", can_jump([2,3,1,1,4]), can_jump([3,2,1,0,4]))
