package problems;


public class Problem047BestTimeToBuySellStock {
    public static int maxProfit(int[] prices){
        int min=prices[0], best=0; for(int p: prices){ if(p<min) min=p; best=Math.max(best, p-min); } return best;
    }
}

