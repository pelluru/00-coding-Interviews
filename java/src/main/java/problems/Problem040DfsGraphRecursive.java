package problems;


import java.util.*;
public class Problem040DfsGraphRecursive {
    public static List<Integer> dfs(Map<Integer, List<Integer>> g, int start){
        List<Integer> res = new ArrayList<>();
        Set<Integer> vis = new HashSet<>();
        dfsRec(g, start, vis, res);
        return res;
    }
    static void dfsRec(Map<Integer,List<Integer>> g, int u, Set<Integer> vis, List<Integer> res){
        if(vis.contains(u)) return;
        vis.add(u); res.add(u);
        for(int v: g.getOrDefault(u, List.of())) dfsRec(g, v, vis, res);
    }
}

