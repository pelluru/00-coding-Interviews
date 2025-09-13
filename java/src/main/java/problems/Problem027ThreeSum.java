package problems;


import java.util.*;
public class Problem027ThreeSum {
    public static List<List<Integer>> threeSum(int[] a){
        List<List<Integer>> res=new ArrayList<>(); java.util.Arrays.sort(a);
        for(int i=0;i<a.length;i++){ if(i>0&&a[i]==a[i-1]) continue; int l=i+1,r=a.length-1; while(l<r){ int s=a[i]+a[l]+a[r]; if(s==0){ res.add(java.util.List.of(a[i],a[l],a[r])); l++; r--; while(l<r&&a[l]==a[l-1])l++; while(l<r&&a[r]==a[r+1])r--; } else if(s<0) l++; else r--; }}
        return res;
    }
}

