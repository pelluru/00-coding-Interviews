package com.interview.patterns;
public class DP1D {
  public static int coinChangeMin(int[] coins, int amount){
    final int INF=1_000_000_000; int[] dp=new int[amount+1];
    for (int s=1;s<=amount;s++){
      int best=INF;
      for (int c: coins) if (s-c>=0 && dp[s-c]+1<best) best=dp[s-c]+1;
      dp[s]=best;
    }
    return dp[amount]>=INF? -1 : dp[amount];
  }
  public static int lisLength(int[] nums){
    int[] tails=new int[nums.length]; int len=0;
    for (int x: nums){
      int lo=0, hi=len;
      while (lo<hi){
        int mid=(lo+hi)/2;
        if (tails[mid]>=x) hi=mid; else lo=mid+1;
      }
      tails[lo]=x; if (lo==len) len++;
    }
    return len;
  }
  public static int houseRobber(int[] nums){
    int take=0, skip=0;
    for (int x: nums){ int ntake=skip+x; int nskip=Math.max(skip,take); take=ntake; skip=nskip; }
    return Math.max(take,skip);
  }
}
