package problems;


public class Problem051SlidingWindowMax {
    public static int[] maxSliding(int[] a,int k){
        int n=a.length; if(n==0||k==0) return new int[0];
        int[] res=new int[n-k+1]; java.util.Deque<Integer> dq=new java.util.ArrayDeque<>();
        for(int i=0;i<n;i++){ while(!dq.isEmpty() && dq.peekFirst()<=i-k) dq.pollFirst(); while(!dq.isEmpty() && a[dq.peekLast()]<=a[i]) dq.pollLast(); dq.offerLast(i); if(i>=k-1) res[i-k+1]=a[dq.peekFirst()]; }
        return res;
    }
}

