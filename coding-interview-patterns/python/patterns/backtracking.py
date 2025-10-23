def permutations(nums):
    n=len(nums); used=[False]*n; path=[]; res=[]
    def dfs():
        if len(path)==n: res.append(path.copy()); return
        for i in range(n):
            if used[i]: continue
            used[i]=True; path.append(nums[i]); dfs(); path.pop(); used[i]=False
    dfs(); return res

def combination_sum(cands, target):
    cands.sort(); res=[]; path=[]
    def dfs(i, rem):
        if rem==0: res.append(path.copy()); return
        if i==len(cands) or rem<0: return
        path.append(cands[i]); dfs(i, rem-cands[i]); path.pop()
        dfs(i+1, rem)
    dfs(0, target); return res

def subsets_with_dup(nums):
    nums.sort(); res=[]; path=[]
    def dfs(i):
        res.append(path.copy())
        for j in range(i, len(nums)):
            if j>i and nums[j]==nums[j-1]: continue
            path.append(nums[j]); dfs(j+1); path.pop()
    dfs(0); return res
