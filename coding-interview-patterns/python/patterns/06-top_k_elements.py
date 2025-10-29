"""

https://www.youtube.com/watch?v=6_v6OoxvMOE

Top-K Elements Pattern (Heap-Based)
===================================

Why heaps?
----------
A heap gives "best-so-far in O(log k)" behavior. For Top-K problems where k << n,
we can scan the data once, keep only k candidates in a small heap, and discard
the rest — achieving O(n log k) time and O(k) extra space.

Recipes:
--------
1) Keep k largest values:
   - Use a **min-heap** of size k that stores the largest k seen so far.
   - When a new number x arrives:
       * If heap size < k: push x.
       * Else if x > heap[0] (the smallest among current k): pop the smallest and push x.
   - At the end, the heap holds the k largest values (in no particular order).

2) k-th largest:
   - Same min-heap of size k; return heap[0] after the scan.

3) Top-K frequent:
   - Count with a hashmap: freq[value].
   - Keep a **min-heap** of tuples (freq, value). Size capped to k.
   - Push each (freq, value); if heap grows to k+1, pop the smallest frequency.
   - End heap contains the k most frequent values.

4) K closest points to origin:
   - We want the *smallest* distances; an easy trick is to maintain a **max-heap** of size k.
   - Python only has a min-heap, so push **negative distances** to simulate a max-heap:
       push((-dist, point)), and if size > k, pop (which removes the farthest).
"""

from typing import List, Tuple
import heapq


# ---------------------------------------------------------------------
# 1) k largest elements (min-heap of size k)
# ---------------------------------------------------------------------
def k_largest_elements(nums: List[int], k: int) -> List[int]:
    """
    Return the k largest elements from nums (order not guaranteed).

    Algorithm:
      - Use a min-heap that stores at most k elements.
      - For each x in nums:
          if heap size < k: push x
          else if x > heap[0]: pop heap[0], then push x
      - heap holds the k largest at the end.

    Complexity:
      Time:  O(n log k)
      Space: O(k)
    """
    if k <= 0:
      return []
    heap: List[int] = []
    for x in nums:
        if len(heap) < k:
            heapq.heappush(heap, x)
        elif x > heap[0]:
            heapq.heapreplace(heap, x)   # pop smallest and push x in one step
    return heap  # contains the k largest (unsorted)


# ---------------------------------------------------------------------
# 2) kth largest element (min-heap of size k)
# ---------------------------------------------------------------------
def kth_largest(nums: List[int], k: int) -> int:
    """
    Return the k-th largest element in nums.

    Same min-heap idea as k_largest_elements, but we return heap[0] at the end.

    Example:
      nums = [3,2,1,5,6,4], k = 2 -> 5
    """
    if k <= 0 or k > len(nums):
        raise ValueError("k must be between 1 and len(nums)")
    heap: List[int] = []
    for x in nums:
        if len(heap) < k:
            heapq.heappush(heap, x)
        elif x > heap[0]:
            heapq.heapreplace(heap, x)
    return heap[0]


# ---------------------------------------------------------------------
# 3) Top-K frequent elements (min-heap on (freq, value))
# ---------------------------------------------------------------------
def top_k_frequent(nums: List[int], k: int) -> List[int]:
    """
    Return any order list of the k most frequent numbers in nums.

    Steps:
      1. Count frequencies with a hashmap.
      2. Maintain a min-heap of (freq, value) with size at most k.
         - Push (cnt, val).
         - If size becomes k+1, pop the smallest frequency.
      3. Extract values from heap.

    Complexity:
      Counting:          O(n)
      Heap maintenance:  O(m log k), where m = #distinct values
      Space:             O(m + k)
    """
    if k <= 0:
        return []
    # frequency map
    from collections import Counter
    freq = Counter(nums)

    heap: List[Tuple[int, int]] = []  # (frequency, value)
    for val, cnt in freq.items():
        if len(heap) < k:
            heapq.heappush(heap, (cnt, val))
        else:
            # if this value is more frequent than the smallest in heap
            if cnt > heap[0][0]:
                heapq.heapreplace(heap, (cnt, val))

    # heap holds k most frequent; return just the values
    return [val for (cnt, val) in heap]


# ---------------------------------------------------------------------
# 4) K closest points to origin (max-heap via negative distances)
# ---------------------------------------------------------------------
def k_closest_points(points: List[Tuple[int, int]], k: int) -> List[Tuple[int, int]]:
    """
    Return k points closest to the origin (0,0) using a max-heap of size k.

    Trick:
      - Python has only a min-heap, so we push negative distances:
          heap item = (-dist_sq, (x, y))
      - When size exceeds k, pop removes the farthest (largest -dist -> smallest dist).

    Complexity:
      Time:  O(n log k)
      Space: O(k)
    """
    if k <= 0:
        return []
    heap: List[Tuple[int, Tuple[int, int]]] = []
    for x, y in points:
        dist2 = x * x + y * y
        item = (-dist2, (x, y))     # negative for max-heap behavior
        if len(heap) < k:
            heapq.heappush(heap, item)
        else:
            # if current point is closer than the farthest in heap
            if -dist2 > heap[0][0]:  # compare negatives (i.e., dist2 < farthest_dist2)
                heapq.heapreplace(heap, item)

    # Extract points (discard negative distances)
    return [pt for (_, pt) in heap]


# ---------------------------------------------------------------------
# Main: demos / quick tests
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Top-K Elements Pattern Demo ===\n")

    # 1) k largest elements
    arr = [3, 2, 3, 1, 2, 4, 5, 5, 6]
    k = 4
    largest_k = k_largest_elements(arr, k)
    print(f"k_largest_elements({arr}, k={k}) -> {sorted(largest_k)}  (sorted for display)")
    # Expected (as a set): {3,4,5,6}  (any order; above prints sorted)

    # 2) kth largest element
    arr2 = [3, 2, 1, 5, 6, 4]
    kth = kth_largest(arr2, 2)
    print(f"kth_largest({arr2}, k=2) -> {kth}")  # Expected: 5

    # 3) top-k frequent numbers
    nums = [1,1,1,2,2,3,3,3,3,4,5,5,5]
    kf = 3
    topk_freq = top_k_frequent(nums, kf)
    print(f"top_k_frequent({nums}, k={kf}) -> {topk_freq}")
    # Expected: the three most frequent numbers (order may vary). For nums above, likely [3,1,5] or similar.

    # 4) k closest points to origin
    pts = [(1, 3), (-2, 2), (5, 8), (0, 1), (2, -1)]
    kc = 3
    closest = k_closest_points(pts, kc)
    print(f"k_closest_points({pts}, k={kc}) -> {closest}")
    # Expected: three points with smallest distances; e.g., (0,1), (2,-1), (-2,2)
