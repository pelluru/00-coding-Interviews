package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem027ThreeSum;


public class TestProblem027ThreeSum {
    @Test void t() { assertFalse(Problem027ThreeSum.threeSum(new int[]{-1,0,1,2,-1,-4}).isEmpty()); }
}

