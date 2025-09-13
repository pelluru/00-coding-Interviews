package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem095MaxRectangleHistogram;


public class TestProblem095MaxRectangleHistogram {
    @Test void t(){ assertEquals(10, Problem095MaxRectangleHistogram.largestRectangleArea(new int[]{2,1,5,6,2,3})); }
}

