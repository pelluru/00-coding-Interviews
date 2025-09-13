package problems;


import java.util.*;
public class Problem023MinHeapKSmallest {
    public static List<Integer> kSmallest(int[] a,int k){ PriorityQueue<Integer> pq=new PriorityQueue<>(); for(int x:a)pq.offer(x); List<Integer> res=new ArrayList<>(); for(int i=0;i<k&& !pq.isEmpty();i++) res.add(pq.poll()); return res; }
}

