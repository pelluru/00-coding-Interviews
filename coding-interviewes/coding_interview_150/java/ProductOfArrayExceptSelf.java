
/* Product of Array Except Self — prefix & suffix. O(n) time */
import java.util.Arrays;
public class ProductOfArrayExceptSelf {
    public static int[] productExceptSelf(int[] nums){
        int n=nums.length; int[] out=new int[n]; int pref=1;
        for (int i=0;i<n;i++){ out[i]=pref; pref*=nums[i]; }
        int suff=1;
        for (int i=n-1;i>=0;i--){ out[i]*=suff; suff*=nums[i]; }
        return out;
    }
    public static void main(String[] args){
        System.out.println(Arrays.toString(productExceptSelf(new int[]{1,2,3,4})));
    }
}
