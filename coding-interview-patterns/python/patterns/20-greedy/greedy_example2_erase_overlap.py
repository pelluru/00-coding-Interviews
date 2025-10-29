#!/usr/bin/env python3
from typing import List
def erase_overlap_intervals(intervals: List[List[int]]) -> int:
    if not intervals: return 0
    intervals.sort(key=lambda x:x[1])
    end=intervals[0][1]; keep=1
    for s,e in intervals[1:]:
        if s>=end: keep+=1; end=e
    return len(intervals)-keep
if __name__ == "__main__":
    print("Example 2 — Erase Overlap:", erase_overlap_intervals([[1,2],[2,3],[3,4],[1,3]]))
