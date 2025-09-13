package problems;


public class Problem032CoinChangeMin {
    public static int coinChange(int[] coins,int amount){
        int INF=1_000_000, dp[]=new int[amount+1]; java.util.Arrays.fill(dp, INF); dp[0]=0;
        for(int a=1;a<=amount;a++) for(int c: coins) if(c<=a) dp[a]=Math.min(dp[a], dp[a-c]+1);
        return dp[amount]>=INF? -1: dp[amount];
    }
}

