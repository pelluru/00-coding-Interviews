package problems;


public class Problem094MinWindowSubstring {
    public static String minWindow(String s, String t){
        if(t.length()==0) return "";
        int[] need=new int[128]; int required=0;
        for(char c: t.toCharArray()){ if(need[c]==0) required++; need[c]++; }
        int[] have=new int[128]; int formed=0;
        int l=0, bestLen=Integer.MAX_VALUE, bestL=0;
        for(int r=0;r<s.length();r++){
            char c=s.charAt(r); have[c]++;
            if(have[c]==need[c] && need[c]>0) formed++;
            while(formed==required){
                if(r-l+1<bestLen){bestLen=r-l+1; bestL=l;}
                char lc=s.charAt(l); have[lc]--; if(have[lc]<need[lc] && need[lc]>0) formed--; l++;
            }
        }
        return bestLen==Integer.MAX_VALUE? "": s.substring(bestL,bestL+bestLen);
    }
}

