package problems;


public class Problem055MedianTwoSortedArraysSmall {
    public static double median(int[] A, int[] B){
        int n=A.length+B.length;
        int i=0,j=0,prev=0,cur=0;
        for(int k=0;k<=n/2;k++){
            prev=cur;
            if(i<A.length && (j>=B.length || A[i]<=B[j])) cur=A[i++];
            else cur=B[j++];
        }
        if(n%2==1) return cur;
        return (prev+cur)/2.0;
    }
}

