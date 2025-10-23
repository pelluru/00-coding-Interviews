package com.interview.patterns;
public class BinarySearch {
  public static int[] searchRange(int[] a, int target){
    int l=lower(a,target), r=upper(a,target)-1;
    if (l<a.length && l<=r && a[l]==target) return new int[]{l,r};
    return new int[]{-1,-1};
  }
  static int lower(int[] a,int t){ int lo=0,hi=a.length; while (lo<hi){ int mid=(lo+hi)/2; if (a[mid]>=t) hi=mid; else lo=mid+1; } return lo; }
  static int upper(int[] a,int t){ int lo=0,hi=a.length; while (lo<hi){ int mid=(lo+hi)/2; if (a[mid]>t) hi=mid; else lo=mid+1; } return lo; }
  public static int shipWithinDays(int[] w, int days){
    int lo=0, hi=0; for (int x: w){ lo=Math.max(lo,x); hi+=x; }
    while (lo<hi){
      int mid=(lo+hi)/2; int d=1, cur=0;
      for (int x: w){ if (cur+x>mid){ d++; cur=0; } cur+=x; }
      if (d<=days) hi=mid; else lo=mid+1;
    }
    return lo;
  }
  public static int minEatingSpeed(int[] piles, int h){
    int lo=1, hi=0; for (int p: piles) hi=Math.max(hi,p);
    while (lo<hi){
      int mid=(lo+hi)/2; long hours=0;
      for (int p: piles){ hours += (p + mid - 1) / mid; }
      if (hours<=h) hi=mid; else lo=mid+1;
    }
    return lo;
  }
}
