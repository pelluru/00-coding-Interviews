package problems;


public class Problem029LongestIncreasingSubsequence {
    public static int lis(int[] a){
        int[] tails=new int[a.length]; int size=0;
        for(int x: a){ int i=java.util.Arrays.binarySearch(tails,0,size,x); if(i<0)i=-(i+1); tails[i]=x; if(i==size) size++; }
        return size;
    }
}

