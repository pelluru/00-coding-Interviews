#!/usr/bin/env python3
"""Divide & Conquer — Core (3 problems in one file)
1) count_inversions
2) max_subarray_divide
3) majority_element (Boyer–Moore)
"""
from typing import List, Tuple

def count_inversions(a: List[int]) -> int:
    def rec(x: List[int]) -> Tuple[List[int], int]:
        if len(x) <= 1: return x, 0
        m = len(x)//2; L,c1 = rec(x[:m]); R,c2 = rec(x[m:])
        i = j = 0; out = []; inv = 0
        while i < len(L) and j < len(R):
            if L[i] <= R[j]: out.append(L[i]); i += 1
            else: out.append(R[j]); j += 1; inv += len(L) - i
        out.extend(L[i:]); out.extend(R[j:])
        return out, inv + c1 + c2
    return rec(list(a))[1]

def max_subarray_divide(a: List[int]) -> int:
    def rec(l: int, r: int):
        if l == r: return a[l], a[l], a[l], a[l]
        m = (l + r)//2
        ls,lp,lsu,lb = rec(l,m)
        rs,rp,rsu,rb = rec(m+1,r)
        s = ls + rs
        pref = max(lp, ls + rp)
        suf  = max(rsu, rs + lsu)
        best = max(lb, rb, lsu + rp)
        return s,pref,suf,best
    return rec(0, len(a)-1)[3]

def majority_element(nums: List[int]) -> int:
    cand=None; cnt=0
    for x in nums:
        if cnt==0: cand=x; cnt=1
        elif x==cand: cnt+=1
        else: cnt-=1
    return cand

def run_all():
    print("Inversions [2,4,1,3,5]:", count_inversions([2,4,1,3,5]))
    print("Max subarray [-2,1,-3,4,-1,2,1,-5,4]:", max_subarray_divide([-2,1,-3,4,-1,2,1,-5,4]))
    print("Majority [2,2,1,1,1,2,2]:", majority_element([2,2,1,1,1,2,2]))

if __name__ == "__main__":
    run_all()
