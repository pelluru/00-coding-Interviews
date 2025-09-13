package problems;


public class Problem009LinkedListCycle {
    static class Node{int v; Node next; Node(int v){this.v=v;}}
    public static boolean hasCycle(Node h){
        Node s=h,f=h; while(f!=null&&f.next!=null){ s=s.next; f=f.next.next; if(s==f) return true; }
        return false;
    }
}

