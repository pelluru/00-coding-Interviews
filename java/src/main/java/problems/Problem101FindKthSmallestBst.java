package problems;


public class Problem101FindKthSmallestBst {
    public static class Node{int v; Node l,r; Node(int v){this.v=v;}}
    public static Integer kthSmallest(Node root, int k){
        java.util.Deque<Node> st=new java.util.ArrayDeque<>();
        Node cur=root; int count=0;
        while(cur!=null || !st.isEmpty()){
            while(cur!=null){ st.push(cur); cur=cur.l; }
            cur=st.pop();
            if(++count==k) return cur.v;
            cur=cur.r;
        }
        return null;
    }
}

