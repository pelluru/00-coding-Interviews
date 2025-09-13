package problems;


import java.util.*;
public class Problem006RemoveDuplicates {
    public static int[] dedup(int[] a) {
        if(a==null) return null;
        LinkedHashSet<Integer> s=new LinkedHashSet<>(); for(int x:a) s.add(x);
        return s.stream().mapToInt(Integer::intValue).toArray();
    }
}

