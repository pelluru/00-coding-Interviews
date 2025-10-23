package com.interview.patterns;
import java.util.*;
public class Backtracking {
  public static List<List<Integer>> permutations(int[] nums){
    boolean[] used=new boolean[nums.length];
    List<Integer> cur=new ArrayList<>(); List<List<Integer>> res=new ArrayList<>();
    dfs(nums, used, cur, res); return res;
  }
  static void dfs(int[] nums, boolean[] used, List<Integer> cur, List<List<Integer>> res){
    if (cur.size()==nums.length){ res.add(new ArrayList<>(cur)); return; }
    for (int i=0;i<nums.length;i++){
      if (used[i]) continue;
      used[i]=true; cur.add(nums[i]); dfs(nums,used,cur,res); cur.remove(cur.size()-1); used[i]=false;
    }
  }
  public static List<List<Integer>> combinationSum(int[] c, int target){
    Arrays.sort(c); List<List<Integer>> res=new ArrayList<>(); List<Integer> cur=new ArrayList<>();
    comb(0, target, c, cur, res); return res;
  }
  static void comb(int i, int rem, int[] c, List<Integer> cur, List<List<Integer>> res){
    if (rem==0){ res.add(new ArrayList<>(cur)); return; }
    if (i==c.length || rem<0) return;
    cur.add(c[i]); comb(i, rem-c[i], c, cur, res); cur.remove(cur.size()-1);
    comb(i+1, rem, c, cur, res);
  }
  public static List<List<Integer>> subsetsWithDup(int[] nums){
    Arrays.sort(nums); List<List<Integer>> res=new ArrayList<>(); List<Integer> cur=new ArrayList<>();
    sub(0, nums, cur, res); return res;
  }
  static void sub(int i, int[] a, List<Integer> cur, List<List<Integer>> res){
    res.add(new ArrayList<>(cur));
    for (int j=i;j<a.length;j++){
      if (j>i && a[j]==a[j-1]) continue;
      cur.add(a[j]); sub(j+1, a, cur, res); cur.remove(cur.size()-1);
    }
  }
}
