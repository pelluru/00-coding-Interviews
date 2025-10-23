"""
Deque example 2: sliding window maximum
Run: python sliding_window_max.py
"""
from collections import deque

def sliding_window_max(nums, k):
    dq = deque()
    out = []
    for i, x in enumerate(nums):
        while dq and dq[0] <= i - k:
            dq.popleft()
        while dq and nums[dq[-1]] <= x:
            dq.pop()
        dq.append(i)
        if i >= k - 1:
            out.append(nums[dq[0]])
    return out

def main():
    print(sliding_window_max([1,3,-1,-3,5,3,6,7], 3))

if __name__ == "__main__":
    main()
