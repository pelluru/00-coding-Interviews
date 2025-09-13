package problems;


import java.util.*;
public class Problem095MaxRectangleHistogram {
    public static int largestRectangleArea(int[] heights){
        int n=heights.length, max=0;
        java.util.Deque<Integer> st=new java.util.ArrayDeque<>();
        for(int i=0;i<=n;i++){
            int h = (i==n)? 0: heights[i];
            while(!st.isEmpty() && h < heights[st.peek()]){
                int height=heights[st.pop()];
                int left = st.isEmpty()? -1: st.peek();
                int width = i - left - 1;
                max = Math.max(max, height*width);
            }
            st.push(i);
        }
        return max;
    }
}

