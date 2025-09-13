package problems;


import java.util.*;
public class Problem050TopologicalSort {
    public static java.util.List<Integer> topo(int n, int[][] edges){
        java.util.List<java.util.List<Integer>> g=new ArrayList<>(); for(int i=0;i<n;i++) g.add(new ArrayList<>());
        int[] indeg=new int[n]; for(int[] e: edges){ g.get(e[0]).add(e[1]); indeg[e[1]]++; }
        java.util.ArrayDeque<Integer> q=new java.util.ArrayDeque<>(); for(int i=0;i<n;i++) if(indeg[i]==0) q.add(i);
        java.util.List<Integer> res=new java.util.ArrayList<>(); while(!q.isEmpty()){ int u=q.poll(); res.add(u); for(int v: g.get(u)) if(--indeg[v]==0) q.add(v); }
        return res;
    }
}

