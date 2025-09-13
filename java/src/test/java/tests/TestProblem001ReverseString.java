package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem001ReverseString;


public class TestProblem001ReverseString {
    @Test void t() {
        assertEquals("cba", Problem001ReverseString.reverseString("abc"));
        assertEquals("", Problem001ReverseString.reverseString(""));
        assertNull(Problem001ReverseString.reverseString(null));
    }
}

