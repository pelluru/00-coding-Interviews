class DSU:
    def __init__(self,n): self.p=list(range(n)); self.r=[0]*n; self.c=n
    def find(self,x):
        if self.p[x]!=x: self.p[x]=self.find(self.p[x])
        return self.p[x]
    def union(self,a,b):
        pa,pb=self.find(a),self.find(b)
        if pa==pb: return False
        if self.r[pa]<self.r[pb]: pa,pb=pb,pa
        self.p[pb]=pa
        if self.r[pa]==self.r[pb]: self.r[pa]+=1
        self.c-=1; return True
def count_components(n, edges):
    d=DSU(n)
    for u,v in edges: d.union(u,v)
    return d.c

def redundant_connection(edges):
    n=max(max(u,v) for u,v in edges)+1
    d=DSU(n)
    for u,v in edges:
        if not d.union(u,v): return [u,v]
    return []

def find_circle_num(M):
    n=len(M); d=DSU(n)
    for i in range(n):
        for j in range(i+1,n):
            if M[i][j]==1: d.union(i,j)
    return d.c
