package problems;


public class Problem028LongestCommonPrefix {
    public static String lcp(String[] s){
        if(s==null||s.length==0) return "";
        String p=s[0];
        for(int i=1;i<s.length;i++){ while(!s[i].startsWith(p)){ p=p.substring(0, p.length()-1); if(p.isEmpty()) return ""; } }
        return p;
    }
}

