package com.interview.patterns;
import java.util.*;
public class Stacks {
  public static int[] nextGreater(int[] nums){
    int n=nums.length; int[] res=new int[n]; Arrays.fill(res,-1);
    Deque<Integer> st=new ArrayDeque<>();
    for (int i=0;i<n;i++){
      while (!st.isEmpty() && nums[st.peek()]<nums[i]) res[st.pop()] = nums[i];
      st.push(i);
    }
    return res;
  }
  public static int[] dailyTemperatures(int[] T){
    int n=T.length; int[] res=new int[n];
    Deque<Integer> st=new ArrayDeque<>();
    for (int i=0;i<n;i++){
      while (!st.isEmpty() && T[st.peek()]<T[i]){ int j=st.pop(); res[j]=i-j; }
      st.push(i);
    }
    return res;
  }
  public static int largestRectangle(int[] h){
    int n=h.length; int[] a=Arrays.copyOf(h, n+1);
    Deque<Integer> st=new ArrayDeque<>(); int ans=0;
    for (int i=0;i<a.length;i++){
      while (!st.isEmpty() && a[st.peek()] > a[i]){
        int H=a[st.pop()]; int L=st.isEmpty()? -1 : st.peek();
        ans=Math.max(ans, H * (i - L - 1));
      }
      st.push(i);
    }
    return ans;
  }
}
