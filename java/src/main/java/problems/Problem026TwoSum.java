package problems;


import java.util.*;
public class Problem026TwoSum {
    public static int[] twoSum(int[] a,int target){
        Map<Integer,Integer> m=new HashMap<>();
        for(int i=0;i<a.length;i++){ int need=target-a[i]; if(m.containsKey(need)) return new int[]{m.get(need), i}; m.put(a[i], i);}
        return null;
    }
}

