package problems;


public class Problem042RotateMatrix90 {
    public static void rotate(int[][] m){
        int n=m.length;
        for(int i=0;i<n;i++) for(int j=i;j<n;j++){ int t=m[i][j]; m[i][j]=m[j][i]; m[j][i]=t; }
        for(int i=0;i<n;i++){ int l=0,r=n-1; while(l<r){ int t=m[i][l]; m[i][l]=m[i][r]; m[i][r]=t; l++; r--; } }
    }
}

