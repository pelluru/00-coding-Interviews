package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem028LongestCommonPrefix;


public class TestProblem028LongestCommonPrefix {
    @Test void t() { assertEquals("fl", Problem028LongestCommonPrefix.lcp(new String[]{"flower","flow","flight"})); }
}

