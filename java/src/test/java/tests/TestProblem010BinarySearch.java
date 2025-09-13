package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem010BinarySearch;


public class TestProblem010BinarySearch {
    @Test void t() { assertEquals(2, Problem010BinarySearch.search(new int[]{1,2,3,4},3)); }
}

