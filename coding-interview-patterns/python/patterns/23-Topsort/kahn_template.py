"""
Kahn's Algorithm Template for Topological Problems
--------------------------------------------------
Reusable scaffold to solve:
  • Detect cycle (can finish?)
  • Produce topological ordering
  • Count layered depth (min semesters / phases)

Customize:
  - Graph construction (directed edges u->v).
  - What to record while popping from the queue.
  - Whether to process in "layers" for step-counting.

Note:
  - Works only for DAGs; if cycle exists, queue empties early.
"""
from collections import defaultdict, deque
from typing import Callable, List, Tuple

def kahn_process(n: int, edges: List[Tuple[int,int]],
                 on_pop: Callable[[int], None] = lambda u: None,
                 layered: bool = False) -> tuple[bool, int]:
    g = defaultdict(list)
    indeg = [0] * n
    for u, v in edges:
        g[u].append(v)
        indeg[v] += 1

    q = deque([i for i in range(n) if indeg[i] == 0])
    seen = 0
    layers = 0

    if not layered:
        while q:
            u = q.popleft()
            on_pop(u)
            seen += 1
            for v in g[u]:
                indeg[v] -= 1
                if indeg[v] == 0:
                    q.append(v)
    else:
        while q:
            for _ in range(len(q)):
                u = q.popleft()
                on_pop(u)
                seen += 1
                for v in g[u]:
                    indeg[v] -= 1
                    if indeg[v] == 0:
                        q.append(v)
            layers += 1

    return (seen == n, layers)


def main():
    n = 4
    edges = [(0,2), (1,2), (2,3)]

    # 1) can finish?
    ok, _ = kahn_process(n, edges, on_pop=lambda u: None, layered=False)
    print("Can finish?", ok)

    # 2) topo order
    order = []
    ok2, _ = kahn_process(n, edges, on_pop=lambda u: order.append(u), layered=False)
    print("Topo order:", order if ok2 else [])

    # 3) min semesters (layers)
    ok3, layers = kahn_process(n, edges, on_pop=lambda u: None, layered=True)
    print("Min semesters:", layers if ok3 else -1)

if __name__ == "__main__":
    main()
