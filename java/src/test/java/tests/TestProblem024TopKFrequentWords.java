package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem024TopKFrequentWords;


import java.util.*;
public class TestProblem024TopKFrequentWords {
    @Test void t() { assertEquals(java.util.List.of("a"), Problem024TopKFrequentWords.topK(new String[]{"a","b","a"},1)); }
}

