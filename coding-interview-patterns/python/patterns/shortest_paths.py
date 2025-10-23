import heapq as hq
def dijkstra(graph, src):
    n=len(graph); INF=10**18
    dist=[INF]*n; dist[src]=0; pq=[(0,src)]
    while pq:
        d,u=hq.heappop(pq)
        if d!=dist[u]: continue
        for v,w in graph[u]:
            nd=d+w
            if nd<dist[v]: dist[v]=nd; hq.heappush(pq,(nd,v))
    return dist

from collections import deque
def zero_one_bfs(graph, src):
    n=len(graph); INF=10**18
    dist=[INF]*n; dist[src]=0; dq=deque([src])
    while dq:
        u=dq.popleft()
        for v,w in graph[u]:  # w is 0 or 1
            if dist[u]+w<dist[v]:
                dist[v]=dist[u]+w
                if w==0: dq.appendleft(v)
                else: dq.append(v)
    return dist

def bellman_ford_has_neg_cycle(n, edges, src=0):
    INF=10**18; dist=[INF]*n; dist[src]=0
    for _ in range(n-1):
        updated=False
        for u,v,w in edges:
            if dist[u]<INF and dist[u]+w<dist[v]:
                dist[v]=dist[u]+w; updated=True
        if not updated: break
    for u,v,w in edges:
        if dist[u]<INF and dist[u]+w<dist[v]: return True
    return False
