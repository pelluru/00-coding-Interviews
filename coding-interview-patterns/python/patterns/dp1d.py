def coin_change_min(coins, amount):
    INF=10**9; dp=[0]+[INF]*amount
    for s in range(1, amount+1):
        for c in coins:
            if s-c>=0 and dp[s-c]+1<dp[s]: dp[s]=dp[s-c]+1
    return -1 if dp[amount]>=INF else dp[amount]

def lis_length(nums):
    import bisect
    tails=[]
    for x in nums:
        i=bisect.bisect_left(tails,x)
        if i==len(tails): tails.append(x)
        else: tails[i]=x
    return len(tails)

def house_robber(nums):
    take=skip=0
    for x in nums:
        take,skip = skip+x, max(skip,take)
    return max(take,skip)
