package problems;


public class Problem034LongestPalindromicSubstring {
    public static String lps(String s){
        if(s==null||s.isEmpty()) return "";
        int start=0,end=0;
        for(int i=0;i<s.length();i++){ int[] a=expand(s,i,i), b=expand(s,i,i+1); int[] best = (a[1]-a[0] > b[1]-b[0])? a: b; if(best[1]-best[0] > end-start){ start=best[0]; end=best[1]; } }
        return s.substring(start, end+1);
    }
    static int[] expand(String s,int l,int r){ while(l>=0&&r<s.length()&&s.charAt(l)==s.charAt(r)){ l--; r++; } return new int[]{l+1,r-1}; }
}

