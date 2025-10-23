def two_sum_sorted(a, t):
    l,r=0,len(a)-1
    while l<r:
        s=a[l]+a[r]
        if s==t: return [l+1,r+1]
        if s<t: l+=1
        else: r-=1
    return []

def three_sum(nums):
    nums.sort(); n=len(nums); res=[]
    for i in range(n-2):
        if i and nums[i]==nums[i-1]: continue
        l,r=i+1,n-1
        while l<r:
            s=nums[i]+nums[l]+nums[r]
            if s==0:
                res.append([nums[i],nums[l],nums[r]]); l+=1; r-=1
                while l<r and nums[l]==nums[l-1]: l+=1
                while l<r and nums[r]==nums[r+1]: r-=1
            elif s<0: l+=1
            else: r-=1
    return res

def remove_duplicates_sorted(a):
    if not a: return 0
    w=1
    for r in range(1,len(a)):
        if a[r]!=a[w-1]: a[w]=a[r]; w+=1
    return w
