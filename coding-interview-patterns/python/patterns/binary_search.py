def search_range(nums,target):
    def lower():
        lo,hi=0,len(nums)
        while lo<hi:
            mid=(lo+hi)//2
            if nums[mid]>=target: hi=mid
            else: lo=mid+1
        return lo
    def upper():
        lo,hi=0,len(nums)
        while lo<hi:
            mid=(lo+hi)//2
            if nums[mid]>target: hi=mid
            else: lo=mid+1
        return lo
    l=lower(); r=upper()-1
    return [l,r] if l<len(nums) and l<=r and nums[l]==target else [-1,-1]

def ship_within_days(weights, days):
    lo,hi=max(weights),sum(weights)
    def can(cap):
        d=1; cur=0
        for w in weights:
            if cur+w>cap: d+=1; cur=0
            cur+=w
        return d<=days
    while lo<hi:
        mid=(lo+hi)//2
        if can(mid): hi=mid
        else: lo=mid+1
    return lo

def min_eating_speed(piles,h):
    lo,hi=1,max(piles)
    def ok(v): return sum((p+v-1)//v for p in piles)<=h
    while lo<hi:
        mid=(lo+hi)//2
        if ok(mid): hi=mid
        else: lo=mid+1
    return lo
