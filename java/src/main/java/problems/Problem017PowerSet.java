package problems;


import java.util.*;
public class Problem017PowerSet {
    public static List<List<Integer>> powerSet(int[] nums){
        List<List<Integer>> res=new ArrayList<>(); res.add(new ArrayList<>());
        for(int x: nums){
            int sz=res.size();
            for(int i=0;i<sz;i++){ List<Integer> n=new ArrayList<>(res.get(i)); n.add(x); res.add(n);}
        } return res;
    }
}

