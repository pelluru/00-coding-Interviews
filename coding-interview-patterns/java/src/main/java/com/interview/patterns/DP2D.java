package com.interview.patterns;
public class DP2D {
  public static int editDistance(String a, String b){
    int m=a.length(), n=b.length();
    int[][] dp=new int[m+1][n+1];
    for (int i=0;i<=m;i++) dp[i][0]=i;
    for (int j=0;j<=n;j++) dp[0][j]=j;
    for (int i=1;i<=m;i++){
      for (int j=1;j<=n;j++){
        if (a.charAt(i-1)==b.charAt(j-1)) dp[i][j]=dp[i-1][j-1];
        else dp[i][j]=1+Math.min(dp[i-1][j-1], Math.min(dp[i-1][j], dp[i][j-1]));
      }
    }
    return dp[m][n];
  }
  public static int lcsLength(String a, String b){
    int m=a.length(), n=b.length();
    int[][] dp=new int[m+1][n+1];
    for (int i=1;i<=m;i++) for (int j=1;j<=n;j++)
      dp[i][j] = (a.charAt(i-1)==b.charAt(j-1)) ? dp[i-1][j-1]+1 : Math.max(dp[i-1][j], dp[i][j-1]);
    return dp[m][n];
  }
  public static int uniquePathsWithObstacles(int[][] grid){
    int m=grid.length, n=m==0?0:grid[0].length;
    if (m==0 || n==0 || grid[0][0]==1) return 0;
    int[][] dp=new int[m][n]; dp[0][0]=1;
    for (int i=0;i<m;i++) for (int j=0;j<n;j++){
      if (grid[i][j]==1) dp[i][j]=0;
      else{
        if (i>0) dp[i][j]+=dp[i-1][j];
        if (j>0) dp[i][j]+=dp[i][j-1];
      }
    }
    return dp[m-1][n-1];
  }
}
