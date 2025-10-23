from collections import Counter
def min_window(s: str, t: str) -> str:
    if not s or not t: return ""
    need=Counter(t); have=Counter(); req=len(need); formed=0; l=0; best=(10**9,None,None)
    for r,ch in enumerate(s):
        have[ch]+=1
        if ch in need and have[ch]==need[ch]: formed+=1
        while formed==req:
            if r-l+1<best[0]: best=(r-l+1,l,r)
            c=s[l]; have[c]-=1
            if c in need and have[c]<need[c]: formed-=1
            l+=1
    return "" if best[1] is None else s[best[1]:best[2]+1]

def length_of_longest_substring(s: str) -> int:
    last={}; l=best=0
    for r,ch in enumerate(s):
        if ch in last and last[ch]>=l: l=last[ch]+1
        last[ch]=r; best=max(best,r-l+1)
    return best

from collections import deque
def max_sliding_window(nums, k):
    q=deque(); out=[]
    for i,x in enumerate(nums):
        while q and nums[q[-1]]<=x: q.pop()
        q.append(i)
        if q[0]<=i-k: q.popleft()
        if i>=k-1: out.append(nums[q[0]])
    return out
