import heapq as hq
def kth_largest(nums,k):
    pq=[]
    for x in nums:
        if len(pq)<k: hq.heappush(pq,x)
        elif x>pq[0]: hq.heapreplace(pq,x)
    return pq[0]

from collections import Counter
def top_k_frequent(nums,k):
    cnt=Counter(nums)
    return [x for x,_ in cnt.most_common(k)]

import heapq as hq
class LNode:
    def __init__(self,val,next=None): self.val=val; self.next=next
def merge_k_lists(lists):
    pq=[]; idx=0
    for node in lists:
        if node: hq.heappush(pq,(node.val, idx, node)); idx+=1
    dummy=LNode(0); cur=dummy
    while pq:
        _,_,node=hq.heappop(pq)
        cur.next=node; cur=node
        if node.next: hq.heappush(pq,(node.next.val, idx, node.next)); idx+=1
    return dummy.next
