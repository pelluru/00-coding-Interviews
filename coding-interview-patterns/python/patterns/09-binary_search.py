"""
https://www.youtube.com/watch?v=nuN3-AkykfM

Modified Binary Search Patterns — Explained & Commented
=======================================================

Included:
1) search_range(nums, target)      — lower/upper bound (first/last position)
2) ship_within_days(weights, days) — binary search on minimal capacity
3) min_eating_speed(piles, h)      — binary search on minimal rate (Koko bananas)
"""

from typing import List


# -------------------------------------------------------------------
# 1) First/Last Position in Sorted Array (lower/upper bound pattern)
# -------------------------------------------------------------------
def search_range(nums: List[int], target: int) -> List[int]:
    """
    Problem:
      Given a sorted array `nums`, return the first and last index of `target`.
      If not found, return [-1, -1].

    Algorithm (Two Binary Searches):
      We use two "boundary" searches:
        • lower():  first index i where nums[i] >= target   (lower_bound)
        • upper():  first index i where nums[i] >  target   (upper_bound)
      Then:
        left  = lower()
        right = upper() - 1
        Validate that left is in range and nums[left] == target.

    Why this works:
      For a sorted array, the range of equal elements is contiguous:
        [first >= target, first > target) → all == target.

    Complexity:
      Time  O(log n) for each bound → overall O(log n)
      Space O(1)

    Edge cases:
      - target not present
      - all elements equal to target
      - empty array
    """
    def lower() -> int:
        lo, hi = 0, len(nums)
        # Invariant: answer in [lo, hi)
        while lo < hi:
            mid = (lo + hi) // 2
            # Move left boundary: we want first index with nums[mid] >= target
            if nums[mid] >= target:
                hi = mid
            else:
                lo = mid + 1
        return lo  # first index where nums[i] >= target

    def upper() -> int:
        lo, hi = 0, len(nums)
        # Invariant: answer in [lo, hi)
        while lo < hi:
            mid = (lo + hi) // 2
            # Move left boundary: first index with nums[mid] > target
            if nums[mid] > target:
                hi = mid
            else:
                lo = mid + 1
        return lo  # first index where nums[i] > target

    l = lower()
    r = upper() - 1
    if l < len(nums) and l <= r and nums[l] == target:
        return [l, r]
    return [-1, -1]


# -------------------------------------------------------------------
# 2) Capacity to Ship Packages Within D Days (binary search on answer)
# -------------------------------------------------------------------
def ship_within_days(weights: List[int], days: int) -> int:
    """
    Problem:
      Given weights of packages in order and an integer `days`,
      find the least ship capacity to deliver all packages within `days`.
      Each day, load packages in order until capacity would be exceeded.

    Algorithm (Binary Search on Capacity):
      1) Search space:
           lo = max(weights)   (must at least carry the heaviest package)
           hi = sum(weights)   (enough to carry all in one day)
      2) Feasibility check can(cap):
           Simulate loading packages day by day with capacity `cap`.
           Count days needed; feasible if days_used <= days.
      3) Standard binary search:
           mid = (lo + hi) // 2
           if can(mid): hi = mid       # try smaller capacity
           else:        lo = mid + 1   # need bigger capacity
      4) Return lo (smallest feasible capacity).

    Complexity:
      Let n = len(weights), W = sum(weights) - max(weights).
      Time  O(n log(sum(weights)))   — each check is O(n)
      Space O(1)

    Edge cases:
      - Single package/day
      - All equal weights
      - Large arrays (simulation is linear)
    """
    lo, hi = max(weights), sum(weights)

    def can(cap: int) -> bool:
        # Simulate loading with capacity `cap`
        d = 1       # days used so far
        cur = 0     # current day's load
        for w in weights:
            # If adding w exceeds capacity, start a new day
            if cur + w > cap:
                d += 1
                cur = 0
            cur += w
        return d <= days

    while lo < hi:
        mid = (lo + hi) // 2
        if can(mid):
            hi = mid
        else:
            lo = mid + 1
    return lo


# -------------------------------------------------------------------
# 3) Koko Eating Bananas (binary search on rate)
# -------------------------------------------------------------------
def min_eating_speed(piles: List[int], h: int) -> int:
    """
    Problem:
      Koko can eat at a speed `k` bananas/hour. For each pile p,
      hours needed = ceil(p / k). Given `h` hours total, find the minimum k
      so that the total hours across all piles ≤ h.

    Algorithm (Binary Search on Speed):
      1) Search space for speed k:
           lo = 1
           hi = max(piles)
      2) Feasibility ok(k):
           Sum over piles of ceil(p / k) ≤ h ?
           Use: ceil(p/k) = (p + k - 1) // k
      3) Binary search on k:
           If ok(mid) → hi = mid (can try slower)
           Else       → lo = mid + 1 (need faster)
      4) Return lo (smallest feasible speed).

    Complexity:
      Let n = len(piles), P = max(piles).
      Time  O(n log P) — each check is O(n), search over log P speeds
      Space O(1)

    Edge cases:
      - Single pile
      - h == n (must eat each pile in one hour → speed = max(piles))
      - Very large piles (use integer arithmetic)
    """
    lo, hi = 1, max(piles)

    def ok(v: int) -> bool:
        # Total hours if eating at speed v
        return sum((p + v - 1) // v for p in piles) <= h

    while lo < hi:
        mid = (lo + hi) // 2
        if ok(mid):
            hi = mid
        else:
            lo = mid + 1
    return lo


# -------------------------
# Simple demonstration main
# -------------------------
def main():
    print("search_range demos:")
    print(search_range([5,7,7,8,8,10], 8), "expected [3, 4]")
    print(search_range([5,7,7,8,8,10], 6), "expected [-1, -1]")
    print(search_range([], 0), "expected [-1, -1]")

    print("\nship_within_days demos:")
    print(ship_within_days([1,2,3,4,5,6,7,8,9,10], 5), "expected 15")
    print(ship_within_days([3,2,2,4,1,4], 3), "expected 6")

    print("\nmin_eating_speed demos:")
    print(min_eating_speed([3,6,7,11], 8), "expected 4")
    print(min_eating_speed([30,11,23,4,20], 5), "expected 30")
    print(min_eating_speed([30,11,23,4,20], 6), "expected 23")


if __name__ == "__main__":
    main()
