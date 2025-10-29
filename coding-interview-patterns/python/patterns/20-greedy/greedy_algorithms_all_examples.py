#!/usr/bin/env python3
from typing import List
def merge_intervals(intervals: List[List[int]]) -> List[List[int]]:
    intervals.sort(); out=[]
    for s,e in intervals:
        if not out or out[-1][1]<s: out.append([s,e])
        else: out[-1][1]=max(out[-1][1],e)
    return out
def erase_overlap_intervals(intervals: List[List[int]]) -> int:
    if not intervals: return 0
    intervals.sort(key=lambda x:x[1])
    end=intervals[0][1]; keep=1
    for s,e in intervals[1:]:
        if s>=end: keep+=1; end=e
    return len(intervals)-keep
def find_min_arrow_shots(points: List[List[int]]) -> int:
    if not points: return 0
    points.sort(key=lambda x:x[1])
    end=points[0][1]; arrows=1
    for s,e in points[1:]:
        if s>end: arrows+=1; end=e
    return arrows
def can_jump(nums: List[int]) -> bool:
    far=0
    for i,step in enumerate(nums):
        if i>far: return False
        far=max(far,i+step)
    return True
def partition_labels(s: str) -> List[int]:
    last={ch:i for i,ch in enumerate(s)}
    ans=[]; start=end=0
    for i,ch in enumerate(s):
        end=max(end,last[ch])
        if i==end: ans.append(end-start+1); start=i+1
    return ans
def run_all():
    print("Example 1 — Merge:", merge_intervals([[1,3],[2,6],[8,10],[15,18]]))
    print("Example 2 — Erase Overlap:", erase_overlap_intervals([[1,2],[2,3],[3,4],[1,3]]))
    print("Example 3 — Min Arrows:", find_min_arrow_shots([[10,16],[2,8],[1,6],[7,12]]))
    print("Example 4 — Can Jump:", can_jump([2,3,1,1,4]), can_jump([3,2,1,0,4]))
    print("Example 5 — Partition Labels:", partition_labels("ababcbacadefegdehijhklij"))
if __name__ == "__main__":
    run_all()
