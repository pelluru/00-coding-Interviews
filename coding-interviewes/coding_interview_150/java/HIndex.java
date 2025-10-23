
/* H-Index — sort ascending & scan. O(n log n) */
import java.util.Arrays;
public class HIndex {
    public static int hIndex(int[] c){
        Arrays.sort(c);
        int n=c.length;
        for (int i=0;i<n;i++){ int papers=n-i; if (c[i]>=papers) return papers; }
        return 0;
    }
    public static void main(String[] args){
        System.out.println(hIndex(new int[]{3,0,6,1,5}));
    }
}
