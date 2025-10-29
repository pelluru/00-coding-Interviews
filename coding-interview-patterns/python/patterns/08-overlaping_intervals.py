"""
https://www.youtube.com/watch?v=hG9QDwiE28w

Overlapping Intervals — Core Patterns
=====================================

Why sort by start time?
-----------------------
If we sort intervals by start, then when we scan left→right we only need to
compare the current interval with the *last merged* interval to detect overlap.
Most interval problems boil down to one of these two strategies:

A) Greedy merge after sorting:
   - Sort by start.
   - Keep a list of merged intervals.
   - If current.start > last.end → no overlap → append.
     Else overlap → extend last.end = max(last.end, current.end).

B) Greedy selection / counting with end-sorted order:
   - Sort by end.
   - Keep track of the current end and greedily pick non-overlapping intervals
     (or count how many overlaps we must remove).
   - This is used for minimizing removals or maximizing non-overlapping set size.

C) Using a min-heap of end times:
   - Sort by start.
   - Push the end of each interval into a min-heap (earliest finishing on top).
   - If the earliest end ≤ next start, we can reuse that resource (e.g., a room).
   - Size of the heap at any time = concurrent intervals (e.g., rooms needed).

All routines below run in O(n log n) due to sorting (and heap pushes/pops).
"""

from typing import List
import heapq


# ---------------------------------------------------------------------
# 1) Merge Intervals
# ---------------------------------------------------------------------
def merge_intervals(intervals: List[List[int]]) -> List[List[int]]:
    """
    Merge all overlapping intervals and return disjoint intervals.

    Algorithm (Greedy merge by start):
      1) Sort intervals by start.
      2) Scan; compare each interval with the last interval in `merged`:
         - If current.start > merged[-1].end: no overlap → append.
         - Else: overlap → merged[-1].end = max(merged[-1].end, current.end).

    Complexity: O(n log n) for sort; O(n) scan; O(1) extra (ignoring output).
    """
    if not intervals:
        return []

    intervals.sort(key=lambda x: x[0])
    merged: List[List[int]] = []

    for s, e in intervals:
        if not merged or merged[-1][1] < s:
            # No overlap: start a new merged block
            merged.append([s, e])
        else:
            # Overlap: extend the previous block
            merged[-1][1] = max(merged[-1][1], e)

    return merged


# ---------------------------------------------------------------------
# 2) Insert Interval (and merge)
# ---------------------------------------------------------------------
def insert_interval(intervals: List[List[int]], new_interval: List[int]) -> List[List[int]]:
    """
    Insert `new_interval` into non-overlapping intervals sorted by start,
    merging if necessary. If `intervals` may be unsorted, we sort first.

    Algorithm (Linear merge):
      - Sort by start (if not guaranteed).
      - Append all intervals that end before new.start.
      - Merge overlaps with new_interval while current.start <= new.end.
      - Append the merged `new_interval` and then the rest.

    Complexity: O(n log n) if unsorted; O(n) if already sorted.
    """
    if not intervals:
        return [new_interval[:]]

    intervals = sorted(intervals, key=lambda x: x[0])
    res: List[List[int]] = []
    i = 0
    n = len(intervals)
    s, e = new_interval

    # 1) Add all intervals ending before new_interval starts
    while i < n and intervals[i][1] < s:
        res.append(intervals[i])
        i += 1

    # 2) Merge all intervals that overlap with new_interval
    while i < n and intervals[i][0] <= e:
        s = min(s, intervals[i][0])
        e = max(e, intervals[i][1])
        i += 1
    res.append([s, e])

    # 3) Append the remaining intervals
    while i < n:
        res.append(intervals[i])
        i += 1

    return res


# ---------------------------------------------------------------------
# 3) Erase Overlap Intervals (minimum removals)
# ---------------------------------------------------------------------
def erase_overlap_intervals(intervals: List[List[int]]) -> int:
    """
    Return the minimum number of intervals to remove to make the rest non-overlapping.

    Algorithm (Greedy by earliest end):
      - Sort intervals by end time.
      - Greedily keep an interval when its start >= current_end.
      - Otherwise it's overlapping with the kept set -> we "remove" it.
      - Removing the one with the *largest* end among the overlappers is optimal;
        sorting by end and always keeping the earliest end achieves that.

    Complexity: O(n log n) sort + O(n) scan.
    """
    if not intervals:
        return 0

    intervals.sort(key=lambda x: x[1])  # sort by end
    keep = 0
    current_end = float("-inf")

    for s, e in intervals:
        if s >= current_end:
            keep += 1
            current_end = e
        else:
            # Overlap: removing this one is implied by not increasing `keep`
            pass

    # Removed = total - kept
    return len(intervals) - keep


# ---------------------------------------------------------------------
# 4) Minimum Meeting Rooms (count max overlap)
# ---------------------------------------------------------------------
def min_meeting_rooms(intervals: List[List[int]]) -> int:
    """
    Given meeting intervals [start, end), find the minimum number of rooms required.

    Algorithm (Min-heap of end times):
      - Sort intervals by start.
      - Maintain a min-heap of current meeting end times.
      - For each meeting:
          * If the earliest finishing meeting ends ≤ current start,
            we reuse that room: pop and push the new end.
          * Else we need a new room: push the end (heap grows).
      - The maximum heap size observed is the answer (or simply size at the end).

    Complexity: O(n log n) due to heap operations; O(n) space in worst case.
    """
    if not intervals:
        return 0

    intervals.sort(key=lambda x: x[0])
    heap: List[int] = []  # end times

    for s, e in intervals:
        # Reuse a room if the earliest finish time is ≤ this start
        if heap and heap[0] <= s:
            heapq.heapreplace(heap, e)  # pop+push in one step
        else:
            heapq.heappush(heap, e)

    return len(heap)


# ---------------------------------------------------------------------
# Main: quick demonstrations
# ---------------------------------------------------------------------
if __name__ == "__main__":
    print("=== Overlapping Intervals — Demo ===\n")

    # 1) Merge Intervals
    arr = [[1, 3], [2, 4], [6, 7], [7, 9], [10, 12], [11, 15]]
    merged = merge_intervals(arr)
    print(f"merge_intervals({arr}) -> {merged}")
    # Expected: [[1,4], [6,9], [10,15]]

    # 2) Insert Interval
    base = [[1, 2], [3, 5], [6, 7], [8, 10], [12, 16]]
    new = [4, 8]
    inserted = insert_interval(base, new)
    print(f"insert_interval({base}, {new}) -> {inserted}")
    # Expected: [[1,2],[3,10],[12,16]]

    # 3) Erase Overlap Intervals
    to_erase = [[1,2], [2,3], [3,4], [1,3]]
    removals = erase_overlap_intervals(to_erase)
    print(f"erase_overlap_intervals({to_erase}) -> {removals}")
    # Expected: 1  (remove [1,3] to keep the rest non-overlapping)

    # 4) Meeting Rooms
    meetings = [[0, 30], [5, 10], [15, 20], [17, 25]]
    rooms = min_meeting_rooms(meetings)
    print(f"min_meeting_rooms({meetings}) -> {rooms}")
    # Expected: 3
