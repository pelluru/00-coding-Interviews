class TNode:
    def __init__(self,val,left=None,right=None):
        self.val=val; self.left=left; self.right=right
def is_valid_bst(root, low=float('-inf'), high=float('inf')):
    if not root: return True
    if not (low<root.val<high): return False
    return is_valid_bst(root.left, low, root.val) and is_valid_bst(root.right, root.val, high)

def kth_smallest(root, k):
    st=[]; cur=root
    while cur or st:
        while cur: st.append(cur); cur=cur.left
        cur=st.pop(); k-=1
        if k==0: return cur.val
        cur=cur.right

def lca(root, p, q):
    if not root or root==p or root==q: return root
    L=lca(root.left,p,q); R=lca(root.right,p,q)
    if L and R: return root
    return L or R
