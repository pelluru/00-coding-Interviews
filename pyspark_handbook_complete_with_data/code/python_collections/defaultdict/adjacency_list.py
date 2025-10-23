"""
defaultdict example 2: adjacency list for a graph
Run: python adjacency_list.py
"""
from collections import defaultdict

def main():
    edges = [(1,2),(2,3),(1,3)]
    g = defaultdict(list)
    for u,v in edges:
        g[u].append(v)
    print(dict(g))

if __name__ == "__main__":
    main()
