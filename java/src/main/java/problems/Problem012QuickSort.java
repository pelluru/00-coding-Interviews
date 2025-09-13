package problems;


public class Problem012QuickSort {
    public static void sort(int[] a){qs(a,0,a.length-1);}
    static void qs(int[] a,int l,int r){ if(l>=r)return; int p=a[(l+r)/2],i=l,j=r; while(i<=j){ while(a[i]<p)i++; while(a[j]>p)j--; if(i<=j){int t=a[i];a[i]=a[j];a[j]=t;i++;j--; } } qs(a,l,j); qs(a,i,r);}
}

