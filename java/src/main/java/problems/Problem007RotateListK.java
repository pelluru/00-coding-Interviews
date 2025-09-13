package problems;


public class Problem007RotateListK {
    public static int[] rotateRight(int[] a,int k) {
        if(a==null||a.length==0) return a;
        int n=a.length; k%=n; if(k==0) return a.clone();
        int[] r=new int[n];
        for(int i=0;i<n;i++) r[(i+k)%n]=a[i];
        return r;
    }
}

