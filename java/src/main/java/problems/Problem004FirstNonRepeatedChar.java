package problems;


import java.util.*;
public class Problem004FirstNonRepeatedChar {
    public static Character firstNonRepeated(String s) {
        if(s==null) return null;
        Map<Character,Integer> c=new LinkedHashMap<>();
        for(char ch: s.toCharArray()) c.put(ch, c.getOrDefault(ch,0)+1);
        for(var e: c.entrySet()) if(e.getValue()==1) return e.getKey();
        return null;
    }
}

