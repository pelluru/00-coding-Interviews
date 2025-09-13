package problems;


import java.util.*;
public class Problem024TopKFrequentWords {
    public static List<String> topK(String[] words,int k){
        Map<String,Integer> c=new HashMap<>(); for(String w:words)c.put(w,c.getOrDefault(w,0)+1);
        PriorityQueue<String> pq=new PriorityQueue<>((a,b)->{int ca=c.get(a), cb=c.get(b); if(ca==cb) return b.compareTo(a); return ca-cb;});
        for(String w:c.keySet()){ pq.offer(w); if(pq.size()>k) pq.poll(); }
        List<String> res=new ArrayList<>(); while(!pq.isEmpty()) res.add(0,pq.poll()); return res;
    }
}

