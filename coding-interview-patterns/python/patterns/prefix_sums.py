from collections import defaultdict
def subarray_sum_k(nums,k):
    pref=0; seen=defaultdict(int); seen[0]=1; ans=0
    for x in nums:
        pref+=x; ans+=seen[pref-k]; seen[pref]+=1
    return ans

class NumMatrix:
    def __init__(self, M):
        m=len(M); n=len(M[0]) if m else 0
        self.P=[[0]*(n+1) for _ in range(m+1)]
        for i in range(1,m+1):
            for j in range(1,n+1):
                self.P[i][j]=M[i-1][j-1]+self.P[i-1][j]+self.P[i][j-1]-self.P[i-1][j-1]
    def sumRegion(self,r1,c1,r2,c2):
        P=self.P; return P[r2+1][c2+1]-P[r1][c2+1]-P[r2+1][c1]+P[r1][c1]

def apply_range_adds(n, updates):
    diff=[0]*(n+1)
    for l,r,v in updates:
        diff[l]+=v
        if r+1<len(diff): diff[r+1]-=v
    cur=0; out=[0]*n
    for i in range(n):
        cur+=diff[i]; out[i]=cur
    return out
