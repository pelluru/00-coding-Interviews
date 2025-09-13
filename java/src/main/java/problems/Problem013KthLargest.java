package problems;


import java.util.*;
public class Problem013KthLargest {
    public static int kthLargest(int[] a,int k) {
        PriorityQueue<Integer> pq=new PriorityQueue<>();
        for(int x:a){ pq.offer(x); if(pq.size()>k) pq.poll(); }
        return pq.peek();
    }
}

