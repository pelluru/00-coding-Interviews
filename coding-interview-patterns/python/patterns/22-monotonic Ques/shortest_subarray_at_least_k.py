"""
Shortest Subarray With Sum >= K (Prefix + Monotonic Deque)
----------------------------------------------------------
Return the length of the shortest, non-empty subarray of nums with sum >= K,
or -1 if no such subarray exists.

Technique:
- Build prefix sums P where P[i] = sum(nums[:i]).
- We want minimal j - i with P[j] - P[i] >= K -> P[i] <= P[j] - K.
- Maintain deque of candidate indices i with increasing P[i].
    • While deque not empty and P[j] - P[deque[0]] >= K, update answer and pop left
      (we found a valid i; older i gives shorter length due to FIFO order).
    • While deque not empty and P[deque[-1]] >= P[j], pop right (new j is better).

Complexity:
- O(n) time, O(n) space.

Run:
    python shortest_subarray_at_least_k.py
"""
from collections import deque
from math import inf
from typing import List

def shortest_subarray_at_least_k(nums: List[int], K: int) -> int:
    # Prefix sums: P[0]=0, P[i]=sum(nums[:i])
    P = [0]
    for x in nums:
        P.append(P[-1] + x)

    dq = deque()  # indices of prefix sums, increasing P
    ans = inf

    for j, cur in enumerate(P):
        # Try to satisfy P[j] - P[i] >= K with the smallest j - i
        while dq and cur - P[dq[0]] >= K:
            ans = min(ans, j - dq.popleft())
        # Maintain increasing P on dq
        while dq and P[dq[-1]] >= cur:
            dq.pop()
        dq.append(j)

    return ans if ans < inf else -1


def main():
    nums = [2, -1, 2]
    K = 3
    print("Input:", nums, "K=", K)
    print("Shortest subarray length >= K:", shortest_subarray_at_least_k(nums, K))

if __name__ == "__main__":
    main()
