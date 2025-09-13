package problems;


import java.util.*;
public class Problem053SpiralMatrix {
    public static java.util.List<Integer> spiral(int[][] m){
        List<Integer> res=new ArrayList<>();
        if(m.length==0) return res;
        int top=0, bot=m.length-1, left=0, right=m[0].length-1;
        while(top<=bot && left<=right){
            for(int j=left;j<=right;j++) res.add(m[top][j]); top++;
            for(int i=top;i<=bot;i++) res.add(m[i][right]); right--;
            if(top<=bot) for(int j=right;j>=left;j--) res.add(m[bot][j]); bot--;
            if(left<=right) for(int i=bot;i>=top;i--) res.add(m[i][left]); left++;
        }
        return res;
    }
}

