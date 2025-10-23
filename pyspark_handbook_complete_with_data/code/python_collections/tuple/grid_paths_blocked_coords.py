"""
Tuple Interview: Count paths in grid with blocked cells using tuple keys
Run: python grid_paths_blocked_coords.py
"""
def unique_paths(m, n, blocked):
    blocked = set(blocked)
    dp = {(0,0): 0 if (0,0) in blocked else 1}
    for i in range(m):
        for j in range(n):
            if (i,j) in blocked: 
                dp[(i,j)] = 0
            else:
                if i or j:
                    dp[(i,j)] = (dp.get((i-1,j),0) + dp.get((i,j-1),0))
    return dp[(m-1,n-1)]

def main():
    print(unique_paths(3,3, blocked=[(1,1)]))

if __name__ == "__main__":
    main()
