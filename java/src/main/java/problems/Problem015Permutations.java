package problems;


import java.util.*;
public class Problem015Permutations {
    public static List<List<Integer>> permute(int[] nums){
        List<List<Integer>> res=new ArrayList<>(); backtrack(nums,new boolean[nums.length],new ArrayList<>(),res); return res;
    }
    static void backtrack(int[] n, boolean[] used, List<Integer> cur, List<List<Integer>> res){
        if(cur.size()==n.length){res.add(new ArrayList<>(cur)); return;}
        for(int i=0;i<n.length;i++) if(!used[i]){ used[i]=true; cur.add(n[i]); backtrack(n,used,cur,res); cur.remove(cur.size()-1); used[i]=false; }
    }
}

