package com.interview.patterns;
import java.util.*;
public class Greedy {
  public static int[][] mergeIntervals(int[][] intervals){
    Arrays.sort(intervals, java.util.Comparator.comparingInt(a->a[0]));
    List<int[]> out=new ArrayList<>();
    for (int[] iv: intervals){
      if (out.isEmpty() || out.get(out.size()-1)[1] < iv[0]) out.add(new int[]{iv[0], iv[1]});
      else out.get(out.size()-1)[1] = Math.max(out.get(out.size()-1)[1], iv[1]);
    }
    return out.toArray(new int[0][]);
  }
  public static int eraseOverlapIntervals(int[][] intervals){
    if (intervals.length==0) return 0;
    Arrays.sort(intervals, java.util.Comparator.comparingInt(a->a[1]));
    int end=intervals[0][1], keep=1;
    for (int i=1;i<intervals.length;i++) if (intervals[i][0]>=end){ keep++; end=intervals[i][1]; }
    return intervals.length - keep;
  }
  public static int minArrows(int[][] points){
    if (points.length==0) return 0;
    Arrays.sort(points, java.util.Comparator.comparingInt(a->a[1]));
    int end=points[0][1], arrows=1;
    for (int i=1;i<points.length;i++) if (points[i][0]>end){ arrows++; end=points[i][1]; }
    return arrows;
  }
}
