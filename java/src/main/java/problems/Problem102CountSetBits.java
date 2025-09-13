package problems;


public class Problem102CountSetBits {
    public static int countBits(int x){
        int c=0; while(x!=0){ x&=x-1; c++; } return c;
    }
}

