from collections import defaultdict, deque
def can_finish(n, edges):
    g=defaultdict(list); indeg=[0]*n
    for u,v in edges: g[u].append(v); indeg[v]+=1
    q=deque([i for i in range(n) if indeg[i]==0]); seen=0
    while q:
        u=q.popleft(); seen+=1
        for v in g[u]:
            indeg[v]-=1
            if indeg[v]==0: q.append(v)
    return seen==n

from collections import defaultdict, deque
def topo_order(n, edges):
    g=defaultdict(list); indeg=[0]*n
    for u,v in edges: g[u].append(v); indeg[v]+=1
    q=deque([i for i in range(n) if indeg[i]==0]); order=[]
    while q:
        u=q.popleft(); order.append(u)
        for v in g[u]:
            indeg[v]-=1
            if indeg[v]==0: q.append(v)
    return order if len(order)==n else []

from collections import defaultdict, deque
def min_semesters(n, edges):
    g=defaultdict(list); indeg=[0]*n
    for u,v in edges: g[u].append(v); indeg[v]+=1
    q=deque([i for i in range(n) if indeg[i]==0]); sem=0; taken=0
    while q:
        for _ in range(len(q)):
            u=q.popleft(); taken+=1
            for v in g[u]:
                indeg[v]-=1
                if indeg[v]==0: q.append(v)
        sem+=1
    return sem if taken==n else -1
