package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem094MinWindowSubstring;


public class TestProblem094MinWindowSubstring {
    @Test void t(){ assertEquals("BANC", Problem094MinWindowSubstring.minWindow("ADOBECODEBANC","ABC")); }
}

