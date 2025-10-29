#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Heaps & Frequency — Worked Examples with Algorithm Steps

Included:
1) kth_largest(nums, k)                  -> Min-heap of size k
2) top_k_frequent(nums, k)               -> Counter + most_common
3) merge_k_lists(lists_of_listnodes)     -> Min-heap by (value, tie-breaker, node)

Run:
  python heaps_playbook.py
"""

from typing import List, Optional, Iterable, Tuple
import heapq as hq
from collections import Counter


# =============================================================================
# 1) Kth Largest Element in an Array
# =============================================================================
def kth_largest(nums: List[int], k: int) -> int:
    """
    Return the k-th largest element in the array using a min-heap of size k.

    Algorithm (Min-heap of size k):
      1. Maintain a min-heap `pq` that stores the top-k largest elements seen so far.
      2. For each x in nums:
         - If pq has fewer than k elements, push x.
         - Else if x is larger than the smallest in pq (pq[0]), replace pq[0] with x
           (i.e., pop the smallest and push x in one operation via heapreplace).
      3. After processing all numbers, pq[0] holds the k-th largest.

    Correctness intuition:
      - The heap always keeps k largest elements seen so far.
      - The smallest among those k is exactly the k-th largest overall.

    Complexity:
      - Time: O(n log k), since we push/replace at most n times and heap size ≤ k.
      - Space: O(k) for the heap.
    """
    if k <= 0 or k > len(nums):
        raise ValueError("k must be in 1..len(nums)")

    pq: List[int] = []

    for x in nums:
        # Step 1: if heap size < k, just push
        if len(pq) < k:
            hq.heappush(pq, x)
            # Now pq holds up to k elements; the smallest is at pq[0]
        # Step 2: if x is bigger than the smallest in the heap, it deserves a spot
        elif x > pq[0]:
            # heapreplace pops the smallest then pushes x (faster than heappop + heappush)
            hq.heapreplace(pq, x)

    # The smallest element in this k-sized heap is the k-th largest overall
    return pq[0]


# =============================================================================
# 2) Top-K Frequent Elements
# =============================================================================
def top_k_frequent(nums: List[int], k: int) -> List[int]:
    """
    Return the k most frequent elements in nums.

    Algorithm (Counter + most_common):
      1. Count frequencies using Counter.
      2. Use cnt.most_common(k) to retrieve the k elements with highest counts.
      3. Return just the elements (ignore the counts in the result).

    Notes:
      - Counter.most_common(k) is implemented efficiently in CPython (uses a heap under the hood).
      - If you need strictly O(n log k) with large universes, you can manually push into
        a min-heap of size k keyed by frequency.

    Complexity:
      - Building the Counter: O(n)
      - most_common(k): O(u log u) worst (u = #unique), but optimized;
        typically fine for interview scenarios. A manual k-heap approach is O(u log k).
      - Space: O(u)
    """
    if k <= 0:
        return []

    cnt = Counter(nums)
    # most_common returns list of (element, frequency) pairs sorted by frequency desc
    return [x for x, _ in cnt.most_common(k)]


# =============================================================================
# 3) Merge K Sorted Linked Lists
# =============================================================================
class LNode:
    """Singly-linked list node used for merge_k_lists."""
    def __init__(self, val: int, next: Optional["LNode"] = None) -> None:
        self.val = val
        self.next = next

    def __repr__(self) -> str:
        return f"LNode({self.val})"


def merge_k_lists(lists: List[Optional[LNode]]) -> Optional[LNode]:
    """
    Merge k sorted linked lists and return the head of the merged sorted list.

    Algorithm (Min-heap on (node.val, tie_breaker, node)):
      1. Initialize an empty min-heap `pq`.
      2. Push the head of each non-empty list as a tuple:
         (node.val, unique_tie_breaker, node). We add a tie-breaker (increasing int)
         to avoid issues when node values are equal (tuples need a deterministic order).
      3. While pq is not empty:
         a) Pop the smallest (by node.val).
         b) Append that node to the merged result (track with a `cur` pointer).
         c) If popped node has a next, push (next.val, tie_breaker, next) into pq.
      4. Return dummy.next (the head of the merged list).

    Correctness:
      - At each step, the smallest head among remaining lists is appended next.
      - This is the standard way to merge multiple sorted streams.

    Complexity:
      - Let N be total number of nodes across all lists and k be the number of lists.
      - Time: O(N log k) because each node is pushed/popped once.
      - Space: O(k) for the heap (and O(1) auxiliary aside from the output list).
    """
    pq: List[Tuple[int, int, LNode]] = []
    tie = 0  # strictly increasing integer used as tie-breaker

    # Step 1: Seed heap with the head of each list
    for node in lists:
        if node:
            hq.heappush(pq, (node.val, tie, node))
            tie += 1

    # Step 2: Pop/push to build merged list
    dummy = LNode(0)
    cur = dummy

    while pq:
        _, _, node = hq.heappop(pq)
        cur.next = node
        cur = node
        if node.next:
            hq.heappush(pq, (node.next.val, tie, node.next))
            tie += 1

    return dummy.next


# =============================================================================
# Helpers for linked lists + Pretty Printing
# =============================================================================
def build_list(values: Iterable[int]) -> Optional[LNode]:
    """Build a linked list from an iterable of ints and return the head."""
    it = iter(values)
    try:
        head = LNode(next(it))
    except StopIteration:
        return None
    cur = head
    for v in it:
        cur.next = LNode(v)
        cur = cur.next
    return head


def list_to_python(head: Optional[LNode]) -> List[int]:
    """Convert a linked list to a Python list for easy comparison/printing."""
    out: List[int] = []
    cur = head
    while cur:
        out.append(cur.val)
        cur = cur.next
    return out


# =============================================================================
# Demo / Quick Tests
# =============================================================================
def _demo_kth_largest():
    print("\n[Demo] kth_largest")
    nums = [3, 2, 1, 5, 6, 4]
    k = 2
    print(f"nums={nums}, k={k} -> k-th largest:", kth_largest(nums, k))
    # Expected: 5 (the 2nd largest)


def _demo_top_k_frequent():
    print("\n[Demo] top_k_frequent")
    nums = [1,1,1,2,2,3,3,3,3,4]
    k = 2
    print(f"nums={nums}, k={k} -> top-k frequent:", top_k_frequent(nums, k))
    # Frequencies: 1->3, 2->2, 3->4, 4->1; top-2 likely [3, 1]


def _demo_merge_k_lists():
    print("\n[Demo] merge_k_lists")
    # Build three sorted lists
    a = build_list([1, 4, 5])
    b = build_list([1, 3, 4])
    c = build_list([2, 6])
    merged = merge_k_lists([a, b, c])
    print("Merged:", list_to_python(merged))
    # Expected: [1,1,2,3,4,4,5,6]


if __name__ == "__main__":
    # Run demos (lightweight checks). You can replace with `unittest`/`pytest` if desired.
    _demo_kth_largest()
    _demo_top_k_frequent()
    _demo_merge_k_lists()

    # Additional sanity asserts (optional)
    assert kth_largest([3,2,3,1,2,4,5,5,6], 4) == 4
    assert set(top_k_frequent([1,1,1,2,2,3], 2)) == {1, 2}
    arr_lists = [build_list([1,4,5]), build_list([1,3,4]), build_list([2,6])]
    assert list_to_python(merge_k_lists(arr_lists)) == [1,1,2,3,4,4,5,6]
    print("\nAll quick demos and asserts passed ✅")
