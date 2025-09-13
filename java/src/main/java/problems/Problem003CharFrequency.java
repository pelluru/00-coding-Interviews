package problems;


import java.util.*;
public class Problem003CharFrequency {
    public static Map<Character,Integer> charFrequency(String s) {
        Map<Character,Integer> m=new HashMap<>();
        if(s==null) return m;
        for(char c: s.toCharArray()) m.put(c, m.getOrDefault(c,0)+1);
        return m;
    }
}

