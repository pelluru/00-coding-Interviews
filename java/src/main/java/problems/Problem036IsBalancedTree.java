package problems;


public class Problem036IsBalancedTree {
    static class Node{int v; Node l,r; Node(int v){this.v=v;}}
    public static boolean isBalanced(Node n){ return height(n)!=-1; }
    static int height(Node n){ if(n==null) return 0; int lh=height(n.l); if(lh==-1) return -1; int rh=height(n.r); if(rh==-1) return -1; if(Math.abs(lh-rh)>1) return -1; return Math.max(lh,rh)+1; }
}

