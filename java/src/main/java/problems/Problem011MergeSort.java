package problems;


public class Problem011MergeSort {
    public static int[] sort(int[] a){
        if(a.length<=1) return a.clone();
        int m=a.length/2; int[] L=new int[m], R=new int[a.length-m];
        System.arraycopy(a,0,L,0,m); System.arraycopy(a,m,R,0,a.length-m);
        L=sort(L); R=sort(R);
        int[] res=new int[a.length]; int i=0,j=0,k=0;
        while(i<L.length&&j<R.length) res[k++]= (L[i]<=R[j]?L[i++]:R[j++]);
        while(i<L.length) res[k++]=L[i++]; while(j<R.length) res[k++]=R[j++];
        return res;
    }
}

