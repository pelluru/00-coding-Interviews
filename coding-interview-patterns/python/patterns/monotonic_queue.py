from collections import deque
def sliding_window_max(nums,k):
    q=deque(); out=[]
    for i,x in enumerate(nums):
        while q and nums[q[-1]]<=x: q.pop()
        q.append(i)
        if q[0]<=i-k: q.popleft()
        if i>=k-1: out.append(nums[q[0]])
    return out

from collections import deque
def shortest_subarray_at_least_k(nums,K):
    P=[0]
    for x in nums: P.append(P[-1]+x)
    from math import inf
    dq=deque(); ans=inf
    for i,cur in enumerate(P):
        while dq and cur-P[dq[0]]>=K:
            ans=min(ans, i-dq.popleft())
        while dq and P[dq[-1]]>=cur: dq.pop()
        dq.append(i)
    return ans if ans<inf else -1

from collections import deque
def constrained_subsequence_sum(nums,k):
    dq=deque(); best=float('-inf'); dp=[0]*len(nums)
    for i,x in enumerate(nums):
        mx=dq[0][0] if dq else 0
        dp[i]=x+max(0,mx)
        best=max(best,dp[i])
        while dq and dq[-1][0]<=dp[i]: dq.pop()
        dq.append((dp[i],i))
        if dq[0][1]<=i-k: dq.popleft()
    return best
