def edit_distance(a,b):
    m,n=len(a),len(b)
    dp=[[0]*(n+1) for _ in range(m+1)]
    for i in range(m+1): dp[i][0]=i
    for j in range(n+1): dp[0][j]=j
    for i in range(1,m+1):
        for j in range(1,n+1):
            if a[i-1]==b[j-1]: dp[i][j]=dp[i-1][j-1]
            else: dp[i][j]=1+min(dp[i-1][j],dp[i][j-1],dp[i-1][j-1])
    return dp[m][n]

def lcs_length(a,b):
    m,n=len(a),len(b)
    dp=[[0]*(n+1) for _ in range(m+1)]
    for i in range(1,m+1):
        for j in range(1,n+1):
            if a[i-1]==b[j-1]: dp[i][j]=dp[i-1][j-1]+1
            else: dp[i][j]=max(dp[i-1][j], dp[i][j-1])
    return dp[m][n]

def unique_paths_with_obstacles(grid):
    m=len(grid); n=len(grid[0]) if m else 0
    if not m or not n or grid[0][0]==1: return 0
    dp=[[0]*n for _ in range(m)]; dp[0][0]=1
    for i in range(m):
        for j in range(n):
            if grid[i][j]==1: dp[i][j]=0
            else:
                if i: dp[i][j]+=dp[i-1][j]
                if j: dp[i][j]+=dp[i][j-1]
    return dp[-1][-1]
