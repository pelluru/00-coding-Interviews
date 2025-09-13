package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem029LongestIncreasingSubsequence;


public class TestProblem029LongestIncreasingSubsequence {
    @Test void t() { assertEquals(4, Problem029LongestIncreasingSubsequence.lis(new int[]{10,9,2,5,3,7,101,18})); }
}

