package problems;


import java.util.*;
public class Problem039DijkstraSimple {
    public static Map<String,Integer> dijkstra(Map<String,List<String[]>> g,String src){
        Map<String,Integer> d=new HashMap<>(); d.put(src,0);
        PriorityQueue<String[]> pq=new PriorityQueue<>(Comparator.comparingInt(a->Integer.parseInt(a[0])));
        pq.offer(new String[]{"0",src});
        while(!pq.isEmpty()){ String[] cur=pq.poll(); int dist=Integer.parseInt(cur[0]); String u=cur[1]; if(dist> d.getOrDefault(u,1<<30)) continue;
            for(String[] e: g.getOrDefault(u, List.of())){ String v=e[0]; int w=Integer.parseInt(e[1]); int nd=dist+w; if(nd<d.getOrDefault(v,1<<30)){ d.put(v,nd); pq.offer(new String[]{String.valueOf(nd), v}); } }
        } return d;
    }
}

