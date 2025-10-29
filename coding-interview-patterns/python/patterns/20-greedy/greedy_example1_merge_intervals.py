#!/usr/bin/env python3
from typing import List
def merge_intervals(intervals: List[List[int]]) -> List[List[int]]:
    intervals.sort(); out=[]
    for s,e in intervals:
        if not out or out[-1][1]<s: out.append([s,e])
        else: out[-1][1]=max(out[-1][1],e)
    return out
if __name__ == "__main__":
    print("Example 1 — Merge:", merge_intervals([[1,3],[2,6],[8,10],[15,18]]))
