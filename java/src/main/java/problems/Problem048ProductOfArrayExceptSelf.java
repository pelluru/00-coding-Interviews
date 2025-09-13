package problems;


public class Problem048ProductOfArrayExceptSelf {
    public static int[] productExceptSelf(int[] a){
        int n=a.length; int[] res=new int[n]; int pref=1;
        for(int i=0;i<n;i++){ res[i]=pref; pref*=a[i]; }
        int suf=1; for(int i=n-1;i>=0;i--){ res[i]*=suf; suf*=a[i]; }
        return res;
    }
}

