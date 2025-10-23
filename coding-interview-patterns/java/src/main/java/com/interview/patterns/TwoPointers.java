package com.interview.patterns;
import java.util.*;
public class TwoPointers {
  public static int[] twoSumSorted(int[] a, int t){
    int l=0,r=a.length-1;
    while (l<r){
      int s=a[l]+a[r];
      if (s==t) return new int[]{l+1,r+1};
      if (s<t) l++; else r--;
    }
    return new int[]{};
  }
  public static List<List<Integer>> threeSum(int[] nums){
    Arrays.sort(nums); List<List<Integer>> res=new ArrayList<>(); int n=nums.length;
    for (int i=0;i<n-2;i++){
      if (i>0 && nums[i]==nums[i-1]) continue;
      int l=i+1, r=n-1;
      while (l<r){
        int s=nums[i]+nums[l]+nums[r];
        if (s==0){ res.add(Arrays.asList(nums[i],nums[l],nums[r])); l++; r--;
          while (l<r && nums[l]==nums[l-1]) l++;
          while (l<r && nums[r]==nums[r+1]) r--;
        } else if (s<0) l++; else r--;
      }
    }
    return res;
  }
  public static int removeDuplicatesSorted(int[] a){
    if (a.length==0) return 0;
    int w=1;
    for (int r=1;r<a.length;r++){
      if (a[r]!=a[w-1]) a[w++]=a[r];
    }
    return w;
  }
}
