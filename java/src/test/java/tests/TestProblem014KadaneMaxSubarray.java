package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem014KadaneMaxSubarray;


public class TestProblem014KadaneMaxSubarray {
    @Test void t() { assertEquals(6, Problem014KadaneMaxSubarray.maxSubarray(new int[]{-2,1,-3,4,-1,2,1,-5,4})); }
}

