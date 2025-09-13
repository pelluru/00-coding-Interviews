package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem033WordBreak;


import java.util.*;
public class TestProblem033WordBreak {
    @Test void t() { assertTrue(Problem033WordBreak.wordBreak("leetcode", new java.util.HashSet<>(java.util.List.of("leet","code")))); }
}

