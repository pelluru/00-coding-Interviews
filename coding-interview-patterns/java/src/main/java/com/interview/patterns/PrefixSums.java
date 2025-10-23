package com.interview.patterns;
import java.util.*;
public class PrefixSums {
  public static int subarraySumK(int[] nums, int k){
    Map<Integer,Integer> map=new HashMap<>(); map.put(0,1);
    int pref=0, ans=0;
    for (int x: nums){ pref+=x; ans+=map.getOrDefault(pref-k,0); map.put(pref, map.getOrDefault(pref,0)+1); }
    return ans;
  }
  public static class NumMatrix{
    int[][] P;
    public NumMatrix(int[][] M){
      int m=M.length, n=m==0?0:M[0].length;
      P=new int[m+1][n+1];
      for (int i=1;i<=m;i++) for (int j=1;j<=n;j++)
        P[i][j]=M[i-1][j-1]+P[i-1][j]+P[i][j-1]-P[i-1][j-1];
    }
    public int sumRegion(int r1,int c1,int r2,int c2){
      return P[r2+1][c2+1]-P[r1][c2+1]-P[r2+1][c1]+P[r1][c1];
    }
  }
  public static int[] applyRangeAdds(int n, int[][] updates){
    int[] diff=new int[n+1];
    for (int[] u: updates){ int l=u[0], r=u[1], v=u[2]; diff[l]+=v; if (r+1<diff.length) diff[r+1]-=v; }
    int[] out=new int[n]; int cur=0; for (int i=0;i<n;i++){ cur+=diff[i]; out[i]=cur; } return out;
  }
}
