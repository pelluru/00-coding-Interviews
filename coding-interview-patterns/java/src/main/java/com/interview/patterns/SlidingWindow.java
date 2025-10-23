package com.interview.patterns;
import java.util.*;
public class SlidingWindow {
  public static String minWindow(String s, String t){
    if (s.isEmpty() || t.isEmpty()) return "";
    Map<Character,Integer> need=new HashMap<>(), have=new HashMap<>();
    for (char c: t.toCharArray()) need.put(c, need.getOrDefault(c,0)+1);
    int req=need.size(), formed=0, l=0; int bestLen=Integer.MAX_VALUE, bl=0, br=0;
    for (int r=0;r<s.length();r++){
      char ch=s.charAt(r); have.put(ch, have.getOrDefault(ch,0)+1);
      if (need.containsKey(ch) && have.get(ch).intValue()==need.get(ch).intValue()) formed++;
      while (formed==req){
        if (r-l+1<bestLen){ bestLen=r-l+1; bl=l; br=r; }
        char c=s.charAt(l++);
        have.put(c, have.get(c)-1);
        if (need.containsKey(c) && have.get(c)<need.get(c)) formed--;
      }
    }
    return bestLen==Integer.MAX_VALUE? "" : s.substring(bl, br+1);
  }
  public static int longestSubstringNoRepeat(String s){
    Map<Character,Integer> last=new HashMap<>(); int l=0,best=0;
    for (int r=0;r<s.length();r++){
      char ch=s.charAt(r);
      if (last.containsKey(ch) && last.get(ch)>=l) l=last.get(ch)+1;
      last.put(ch,r); best=Math.max(best, r-l+1);
    }
    return best;
  }
  public static int[] maxSlidingWindow(int[] nums, int k){
    Deque<Integer> q=new ArrayDeque<>(); int n=nums.length; int[] out=new int[Math.max(0,n-k+1)]; int oi=0;
    for (int i=0;i<n;i++){
      while (!q.isEmpty() && nums[q.peekLast()]<=nums[i]) q.pollLast();
      q.addLast(i);
      if (q.peekFirst()<=i-k) q.pollFirst();
      if (i>=k-1) out[oi++]=nums[q.peekFirst()];
    }
    return out;
  }
}
