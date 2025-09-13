package problems;


public class Problem037LowestCommonAncestor {
    static class Node{int v; Node l,r; Node(int v){this.v=v;}}
    public static Node lca(Node root, Node p, Node q){ if(root==null || root==p || root==q) return root; Node L=lca(root.l,p,q), R=lca(root.r,p,q); if(L!=null && R!=null) return root; return L!=null? L: R; }
}

