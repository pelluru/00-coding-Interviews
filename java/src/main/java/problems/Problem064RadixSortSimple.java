package problems;


public class Problem064RadixSortSimple {
    public static int[] sort(int[] a){
        int max=0; for(int x: a) if(x>max) max=x;
        int exp=1; int n=a.length;
        int[] out=new int[n];
        while(max/exp>0){
            int[] cnt=new int[10];
            for(int i=0;i<n;i++) cnt[(a[i]/exp)%10]++;
            for(int i=1;i<10;i++) cnt[i]+=cnt[i-1];
            for(int i=n-1;i>=0;i--) out[--cnt[(a[i]/exp)%10]] = a[i];
            System.arraycopy(out,0,a,0,n);
            exp*=10;
        }
        return a;
    }
}

