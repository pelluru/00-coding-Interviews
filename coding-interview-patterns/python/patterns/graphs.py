from collections import deque
def num_islands(grid):
    if not grid: return 0
    m,n=len(grid),len(grid[0]); seen=[[False]*n for _ in range(m)]
    def bfs(i,j):
        q=deque([(i,j)]); seen[i][j]=True
        while q:
            x,y=q.popleft()
            for dx,dy in [(1,0),(-1,0),(0,1),(0,-1)]:
                nx,ny=x+dx,y+dy
                if 0<=nx<m and 0<=ny<n and not seen[nx][ny] and grid[nx][ny]=='1':
                    seen[nx][ny]=True; q.append((nx,ny))
    ans=0
    for i in range(m):
        for j in range(n):
            if grid[i][j]=='1' and not seen[i][j]: ans+=1; bfs(i,j)
    return ans

from collections import defaultdict, deque
def ladder_length(beginWord, endWord, wordList):
    if endWord not in wordList: return 0
    L=len(beginWord); buckets=defaultdict(list)
    for w in wordList:
        for i in range(L):
            buckets[w[:i]+'*'+w[i+1:]].append(w)
    q=deque([(beginWord,1)]); seen={beginWord}
    while q:
        w,d=q.popleft()
        if w==endWord: return d
        for i in range(L):
            key=w[:i]+'*'+w[i+1:]
            for nei in buckets.get(key, []):
                if nei not in seen:
                    seen.add(nei); q.append((nei,d+1))
            buckets[key]=[]
    return 0

class Node:
    def __init__(self,val,neighbors=None): self.val=val; self.neighbors=neighbors or []
def clone_graph(node):
    if not node: return None
    mp={}
    def dfs(u):
        if u in mp: return mp[u]
        copy=Node(u.val); mp[u]=copy
        for v in u.neighbors: copy.neighbors.append(dfs(v))
        return copy
    return dfs(node)
