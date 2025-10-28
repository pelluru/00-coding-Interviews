#!/usr/bin/env python3
"""BFS — Five Examples in One File"""
from typing import List, Dict, Set, Optional, Tuple
from collections import deque

def run_example_1():
    graph={'A':['B','C'],'B':['A','D','E'],'C':['A','F'],'D':['B'],'E':['B','F'],'F':['C','E']}
    visited=set(['A']);q=deque(['A'])
    print("Example 1 — BFS Traversal starting from 'A':")
    while q:
        node=q.popleft();print(node,end=' ')
        for nbr in graph[node]:
            if nbr not in visited:
                visited.add(nbr);q.append(nbr)
    print()

class Node:
    def __init__(self,val:str):
        self.val=val;self.left=None;self.right=None
def run_example_2():
    root=Node('A');root.left=Node('B');root.right=Node('C');root.left.left=Node('D');root.left.right=Node('E');root.right.right=Node('F')
    print("Example 2 — Binary Tree Level Order BFS:")
    q=deque([root])
    while q:
        n=q.popleft();print(n.val,end=' ')
        if n.left:q.append(n.left)
        if n.right:q.append(n.right)
    print()

def run_example_3():
    graph={'A':['B','C'],'B':['D','E'],'C':['F'],'D':[],'E':['F'],'F':[]}
    q=deque([('A',['A'])]);visited=set(['A'])
    while q:
        node,path=q.popleft()
        if node=='F':
            print('Example 3 — Shortest path from A to F:', '->'.join(path));return
        for nbr in graph[node]:
            if nbr not in visited:
                visited.add(nbr);q.append((nbr,path+[nbr]))

def run_example_4():
    grid=[[1,1,0,1],[0,1,1,1],[0,0,1,0],[1,1,1,1]]
    start=(0,0);end=(3,3)
    q=deque([(start[0],start[1],0)]);visited={start}
    while q:
        r,c,d=q.popleft()
        if (r,c)==end:print(f"Example 4 — Shortest path distance in grid: {d}");return
        for dr,dc in[(1,0),(-1,0),(0,1),(0,-1)]:
            nr,nc=r+dr,c+dc
            if 0<=nr<len(grid)and 0<=nc<len(grid[0])and grid[nr][nc]==1 and(nr,nc)not in visited:
                visited.add((nr,nc));q.append((nr,nc,d+1))

def run_example_5():
    graph={1:[2],2:[1,3],3:[2],4:[5],5:[4],6:[]}
    visited=set();count=0
    for node in graph:
        if node not in visited:
            q=deque([node]);visited.add(node)
            while q:
                n=q.popleft()
                for nbr in graph[n]:
                    if nbr not in visited:
                        visited.add(nbr);q.append(nbr)
            count+=1
    print(f"Example 5 — Number of connected components: {count}")

def _run_all(selected=None):
    mapping={1:run_example_1,2:run_example_2,3:run_example_3,4:run_example_4,5:run_example_5}
    for i in (selected or [1,2,3,4,5]): mapping[i]()

if __name__=="__main__":
    import sys
    if len(sys.argv)==1:_run_all()
    else:_run_all([int(x) for x in sys.argv[1:]])
