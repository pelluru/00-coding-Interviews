"""
Prefix Sum — Count Subarrays with Sum == k
------------------------------------------
Reference patterns:
- https://blog.algomaster.io/p/15-leetcode-patterns
- https://www.youtube.com/watch?v=yuws7YK0Yng
"""
from collections import defaultdict
from typing import List

def subarray_sum_k(nums: List[int], k: int) -> int:
    """
    Count how many contiguous subarrays have sum exactly k.
    Idea:
      - Maintain running prefix sum 'pref'.
      - For each position, we need earlier prefix 'pref - k'.
      - Use hashmap to count how many times each prefix has occurred.
    """
    pref = 0
    seen = defaultdict(int)
    seen[0] = 1  # empty prefix
    ans = 0
    for x in nums:
        pref += x
        ans += seen[pref - k]
        seen[pref] += 1
    return ans

def main():
    nums = [1, 1, 1]
    print("Array:", nums, "k=2 ->", subarray_sum_k(nums, 2))  # 2

if __name__ == "__main__":
    main()
