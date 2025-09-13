package problems;


public class Problem001ReverseString {
    public static String reverseString(String s) {
        if (s == null) return null;
        return new StringBuilder(s).reverse().toString();
    }
}

