
/* Best Time II — sum increases. O(n) time, O(1) space */
public class BestTimeToBuyAndSellStockII {
    public static int maxProfit(int[] prices){
        int ans=0; for (int i=1;i<prices.length;i++) if (prices[i]>prices[i-1]) ans += prices[i]-prices[i-1];
        return ans;
    }
    public static void main(String[] args){
        System.out.println(maxProfit(new int[]{7,1,5,3,6,4}));
        System.out.println(maxProfit(new int[]{1,2,3,4,5}));
    }
}
