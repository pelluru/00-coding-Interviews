package problems;


public class Problem019NthFibonacciIterative {
    public static long fibN(int n){ if(n<2) return n; long a=0,b=1; for(int i=2;i<=n;i++){ long c=a+b; a=b; b=c; } return b; }
}

