package problems;


public class Problem051TransposeMatrix {
    public static int[][] transpose(int[][] M){
        int m=M.length, n=M[0].length;
        int[][] T=new int[n][m];
        for(int i=0;i<m;i++) for(int j=0;j<n;j++) T[j][i]=M[i][j];
        return T;
    }
}

