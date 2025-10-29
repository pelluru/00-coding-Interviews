#!/usr/bin/env python3
from typing import List
def partition_labels(s: str) -> List[int]:
    last={ch:i for i,ch in enumerate(s)}
    ans=[]; start=end=0
    for i,ch in enumerate(s):
        end=max(end,last[ch])
        if i==end: ans.append(end-start+1); start=i+1
    return ans
if __name__ == "__main__":
    print("Example 5 — Partition Labels:", partition_labels("ababcbacadefegdehijhklij"))
