package problems;


public class Problem043FindMissingNumber {
    public static int missing(int[] a){
        int n=a.length; int expected=n*(n+1)/2, sum=0; for(int x:a) sum+=x; return expected-sum;
    }
}

