package problems;


public class Problem060MaxProfitKTransactions {
    public static int maxProfit(int k, int[] prices){
        int n=prices.length; if(n==0||k==0) return 0;
        if(k>=n/2){ // unlimited
            int prof=0; for(int i=1;i<n;i++) if(prices[i]>prices[i-1]) prof+=prices[i]-prices[i-1]; return prof;
        }
        int[] buy=new int[k+1], sell=new int[k+1];
        for(int i=0;i<=k;i++){ buy[i]=Integer.MIN_VALUE/4; sell[i]=0; }
        for(int p: prices){
            for(int t=1;t<=k;t++){
                buy[t]=Math.max(buy[t], sell[t-1]-p);
                sell[t]=Math.max(sell[t], buy[t]+p);
            }
        }
        return sell[k];
    }
}

