#!/usr/bin/env python3
"""Stack & Monotonic Stack — Five Examples in One File
1) next_greater           : monotonic decreasing stack -> next greater to the right
2) daily_temperatures     : days to a warmer temperature
3) largest_rectangle      : histogram area via monotonic increasing stack
4) stock_span             : span using decreasing stack
5) is_valid_parentheses   : classic stack for brackets
"""
from typing import List, Tuple

def next_greater(nums: List[int]) -> List[int]:
    st=[]; res=[-1]*len(nums)
    for i,x in enumerate(nums):
        while st and nums[st[-1]]<x:
            res[st.pop()]=x
        st.append(i)
    return res

def daily_temperatures(T: List[int]) -> List[int]:
    st=[]; res=[0]*len(T)
    for i,t in enumerate(T):
        while st and T[st[-1]]<t:
            j=st.pop(); res[j]=i-j
        st.append(i)
    return res

def largest_rectangle(h: List[int]) -> int:
    st=[]; ans=0; h.append(0)
    for i,x in enumerate(h):
        while st and h[st[-1]]>x:
            H=h[st.pop()]; L=st[-1] if st else -1
            ans=max(ans, H*(i-L-1))
        st.append(i)
    h.pop(); return ans

def stock_span(prices: List[int]) -> List[int]:
    st=[]; res=[0]*len(prices)
    for i,p in enumerate(prices):
        while st and st[-1][1]<=p:
            st.pop()
        last_greater_idx=st[-1][0] if st else -1
        res[i]=i-last_greater_idx
        st.append((i,p))
    return res

def is_valid_parentheses(s: str) -> bool:
    pairs={')':'(',']':'[','}':'{'}
    st=[]
    for ch in s:
        if ch in '([{': st.append(ch)
        elif ch in pairs:
            if not st or st[-1]!=pairs[ch]: return False
            st.pop()
    return not st

def _run_all(selected=None):
    def ex1(): print("Example 1 — Next Greater:", next_greater([2,1,2,4,3]))
    def ex2(): print("Example 2 — Daily Temperatures:", daily_temperatures([73,74,75,71,69,72,76,73]))
    def ex3(): print("Example 3 — Largest Rectangle:", largest_rectangle([2,1,5,6,2,3]))
    def ex4(): print("Example 4 — Stock Span:", stock_span([100,80,60,70,60,75,85]))
    def ex5(): print("Example 5 — Valid Parentheses:", is_valid_parentheses("()[]{}"), is_valid_parentheses("(]"))
    mapping={1:ex1,2:ex2,3:ex3,4:ex4,5:ex5}
    for i in (selected or [1,2,3,4,5]): mapping[i]()

if __name__=="__main__":
    import sys
    if len(sys.argv)==1: _run_all()
    else: _run_all([int(x) for x in sys.argv[1:]])
