package problems;


import java.util.*;
public class Problem025AnagramGroups {
    public static List<List<String>> group(String[] words){
        Map<String,List<String>> m=new HashMap<>();
        for(String w: words){ char[] cs=w.toCharArray(); java.util.Arrays.sort(cs); String k=new String(cs); m.computeIfAbsent(k, z->new ArrayList<>()).add(w);}
        return new ArrayList<>(m.values());
    }
}

