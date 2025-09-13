package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem055MedianTwoSortedArraysSmall;


public class TestProblem055MedianTwoSortedArraysSmall {
    @Test void t(){
        assertEquals(2.0, Problem055MedianTwoSortedArraysSmall.median(new int[]{1,3}, new int[]{2}), 1e-9);
        assertEquals(2.5, Problem055MedianTwoSortedArraysSmall.median(new int[]{1,2}, new int[]{3,4}), 1e-9);
    }
}

