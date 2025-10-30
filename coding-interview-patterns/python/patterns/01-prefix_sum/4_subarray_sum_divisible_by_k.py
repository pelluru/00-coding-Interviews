"""
Prefix Sum — Count Subarrays with Sum % K == 0
----------------------------------------------
Observation:
  sum(i..j) % K == 0  <=>  prefix[j] % K == prefix[i-1] % K
We count equal remainders in the running prefix sum modulo K.
"""
from collections import defaultdict
from typing import List

def subarray_sum_divisible_by_k(nums: List[int], K: int) -> int:
    cnt = defaultdict(int)
    cnt[0] = 1     # empty prefix remainder
    pref = 0
    ans = 0
    for x in nums:
        pref += x
        r = pref % K
        ans += cnt[r]   # all earlier prefixes with same remainder
        cnt[r] += 1
    return ans

def main():
    nums = [4,5,0,-2,-3,1]
    K = 5
    print("Array:", nums, "K=", K, "->", subarray_sum_divisible_by_k(nums, K))  # 7

if __name__ == "__main__":
    main()
