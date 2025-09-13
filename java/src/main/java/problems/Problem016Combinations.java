package problems;


import java.util.*;
public class Problem016Combinations {
    public static List<List<Integer>> combine(int n,int k){
        List<List<Integer>> res=new ArrayList<>(); backtrack(1,n,k,new ArrayList<>(),res); return res;
    }
    static void backtrack(int start,int n,int k,List<Integer> cur,List<List<Integer>> res){
        if(cur.size()==k){res.add(new ArrayList<>(cur)); return;}
        for(int i=start;i<=n;i++){ cur.add(i); backtrack(i+1,n,k,cur,res); cur.remove(cur.size()-1);}
    }
}

