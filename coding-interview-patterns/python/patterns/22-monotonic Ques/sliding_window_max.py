"""
Sliding Window Maximum (Monotonic Deque)
----------------------------------------
Given an array nums and window size k, return the maximum for each window.

Technique:
- Maintain a decreasing deque of indices (front always the max index).
- For incoming index i with value x:
    • Pop from the back while nums[deque[-1]] <= x (remove weaker candidates).
    • Append i.
    • Pop from the front if it's out of the current window (i - k).
    • Once i >= k-1, record nums[deque[0]].

Complexity:
- O(n) time: each index pushed/popped at most once.
- O(k) extra space.

Run:
    python sliding_window_max.py
"""
from collections import deque
from typing import List

def sliding_window_max(nums: List[int], k: int) -> List[int]:
    q = deque()   # will store indices, values decreasing
    out = []
    for i, x in enumerate(nums):
        # Maintain decreasing deque
        while q and nums[q[-1]] <= x:
            q.pop()
        q.append(i)

        # Remove indices that are out of the window
        if q[0] <= i - k:
            q.popleft()

        # Record result once the first full window is formed
        if i >= k - 1:
            out.append(nums[q[0]])
    return out


def main():
    nums = [1,3,-1,-3,5,3,6,7]
    k = 3
    print("Input:", nums, "k=", k)
    print("Sliding window max:", sliding_window_max(nums, k))

if __name__ == "__main__":
    main()
