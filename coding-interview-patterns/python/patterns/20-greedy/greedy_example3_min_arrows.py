#!/usr/bin/env python3
from typing import List
def find_min_arrow_shots(points: List[List[int]]) -> int:
    if not points: return 0
    points.sort(key=lambda x:x[1])
    end=points[0][1]; arrows=1
    for s,e in points[1:]:
        if s>end: arrows+=1; end=e
    return arrows
if __name__ == "__main__":
    print("Example 3 — Min Arrows:", find_min_arrow_shots([[10,16],[2,8],[1,6],[7,12]]))
