
/* Best Time to Buy and Sell Stock (single) — O(n) time, O(1) space */
public class BestTimeToBuyAndSellStock {
    public static int maxProfit(int[] prices){
        int mn=Integer.MAX_VALUE, ans=0;
        for (int p: prices){ if (p<mn) mn=p; ans=Math.max(ans, p-mn); }
        return ans;
    }
    public static void main(String[] args){
        System.out.println(maxProfit(new int[]{7,1,5,3,6,4}));
        System.out.println(maxProfit(new int[]{7,6,4,3,1}));
    }
}
