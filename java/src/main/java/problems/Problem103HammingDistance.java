package problems;


public class Problem103HammingDistance {
    public static int hamming(int a, int b){
        int x=a^b, c=0; while(x!=0){ x&=x-1; c++; } return c;
    }
}

