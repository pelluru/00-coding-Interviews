package problems;


import java.util.*;
public class Problem040BfsGraph {
    public static List<Integer> bfs(Map<Integer,List<Integer>> g,int s){
        List<Integer> seen=new ArrayList<>(); ArrayDeque<Integer> q=new ArrayDeque<>(); q.add(s); seen.add(s);
        while(!q.isEmpty()){ int u=q.poll(); for(int v: g.getOrDefault(u, List.of())) if(!seen.contains(v)){ seen.add(v); q.add(v); } }
        return seen;
    }
}

