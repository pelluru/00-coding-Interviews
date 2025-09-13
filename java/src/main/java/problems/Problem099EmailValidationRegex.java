package problems;


import java.util.regex.*;
public class Problem099EmailValidationRegex {
    private static final Pattern P = Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$");
    public static boolean isValid(String email){ return email!=null && P.matcher(email).matches(); }
}

