package problems;


public class Problem002IsPalindrome {
    public static boolean isPalindrome(String s) {
        if (s == null) return false;
        int i=0,j=s.length()-1;
        while(i<j){ if(s.charAt(i++)!=s.charAt(j--)) return false; }
        return true;
    }
}

