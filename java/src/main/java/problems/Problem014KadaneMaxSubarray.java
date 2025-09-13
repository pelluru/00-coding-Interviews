package problems;


public class Problem014KadaneMaxSubarray {
    public static int maxSubarray(int[] a) {
        int best=a[0], cur=a[0];
        for(int i=1;i<a.length;i++){ cur=Math.max(a[i], cur+a[i]); best=Math.max(best,cur);}
        return best;
    }
}

