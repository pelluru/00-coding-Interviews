package problems;


public class Problem063CountingSortSimple {
    public static int[] sort(int[] a, int maxVal){
        int[] count=new int[maxVal+1];
        for(int x: a) count[x]++;
        int idx=0;
        for(int v=0; v<=maxVal; v++) while(count[v]-- > 0) a[idx++]=v;
        return a;
    }
}

