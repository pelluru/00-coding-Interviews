"""
Monotonic Queue / Deque Template
--------------------------------
A reusable scaffold for problems that need O(n) windowed extremes or
prefix-sum optimizations. Customize the comparison and window rules.
"""
from collections import deque
from typing import Callable, Any

class MonotonicDeque:
    """
    Keeps elements in monotonic order under a key function.
    For sliding maximum: keep decreasing keys (pop back while key(new) >= key(back)).
    For sliding minimum: keep increasing keys (pop back while key(new) <= key(back)).
    """
    def __init__(self, key: Callable[[Any], Any], decreasing: bool = True):
        self.key = key
        self.decreasing = decreasing
        self.dq = deque()

    def push(self, item: Any):
        k = self.key(item)
        if self.decreasing:
            while self.dq and self.key(self.dq[-1]) <= k:
                self.dq.pop()
        else:
            while self.dq and self.key(self.dq[-1]) >= k:
                self.dq.pop()
        self.dq.append(item)

    def pop_front_if(self, pred):
        if self.dq and pred(self.dq[0]):
            self.dq.popleft()

    def front(self):
        return self.dq[0] if self.dq else None

    def __len__(self):
        return len(self.dq)


def demo_sliding_max(nums, k):
    md = MonotonicDeque(key=lambda i: nums[i], decreasing=True)
    out = []
    for i, _ in enumerate(nums):
        md.push(i)
        md.pop_front_if(lambda j: j <= i - k)
        if i >= k - 1:
            out.append(nums[md.front()])
    return out


def main():
    nums = [1,3,-1,-3,5,3,6,7]
    k = 3
    print("Template demo (sliding max):", demo_sliding_max(nums, k))

if __name__ == "__main__":
    main()
