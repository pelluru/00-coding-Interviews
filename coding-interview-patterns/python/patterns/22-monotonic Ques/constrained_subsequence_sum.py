"""
Constrained Subsequence Sum (Monotonic Deque over DP)
-----------------------------------------------------
Given nums and an integer k, find the max sum of a non-empty subsequence such
that for every adjacent pair (i_prev, i_cur) in the subsequence, we have:
    0 < i_cur - i_prev <= k.

Technique:
- Let dp[i] = nums[i] + max(0, max(dp[j]) for j in (i-k .. i-1)).
- Maintain a deque of pairs (dp value, index) with decreasing dp values.
- For each i:
    • Take mx = deque[0].value if deque not empty else 0.
    • dp[i] = nums[i] + max(0, mx) and best = max(best, dp[i]).
    • Pop from back while deque[-1].value <= dp[i], then append (dp[i], i).
    • Pop from front if deque[0].index <= i - k (out of window).

Complexity:
- O(n) time, O(k) extra space.

Run:
    python constrained_subsequence_sum.py
"""
from collections import deque
from typing import List

def constrained_subsequence_sum(nums: List[int], k: int) -> int:
    dq = deque()  # stores (dp value, index), dp values decreasing
    best = float('-inf')
    dp = [0] * len(nums)

    for i, x in enumerate(nums):
        mx = dq[0][0] if dq else 0
        dp[i] = x + max(0, mx)
        best = max(best, dp[i])

        # Maintain decreasing dp in deque
        while dq and dq[-1][0] <= dp[i]:
            dq.pop()
        dq.append((dp[i], i))

        # Remove out-of-window entries
        if dq and dq[0][1] <= i - k:
            dq.popleft()

    return best


def main():
    nums = [10,2,-10,5,20]
    k = 2
    print("Input:", nums, "k=", k)
    print("Constrained subsequence sum:", constrained_subsequence_sum(nums, k))

if __name__ == "__main__":
    main()
