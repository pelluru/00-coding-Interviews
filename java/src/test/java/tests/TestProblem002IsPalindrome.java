package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem002IsPalindrome;


public class TestProblem002IsPalindrome {
    @Test void t() {
        assertTrue(Problem002IsPalindrome.isPalindrome("racecar"));
        assertFalse(Problem002IsPalindrome.isPalindrome("hello"));
    }
}

