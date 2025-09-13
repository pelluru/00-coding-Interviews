package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem034LongestPalindromicSubstring;


public class TestProblem034LongestPalindromicSubstring {
    @Test void t() { String r=Problem034LongestPalindromicSubstring.lps("babad"); assertTrue(r.equals("bab")||r.equals("aba")); }
}

