package problems;


import java.util.regex.*;
public class Problem098UrlValidation {
    private static final Pattern P = Pattern.compile("^(https?://)?([\w.-]+)(:[0-9]+)?(/.*)?$");
    public static boolean isValid(String url){ return url!=null && P.matcher(url).matches(); }
}

