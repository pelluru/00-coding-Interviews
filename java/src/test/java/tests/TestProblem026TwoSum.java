package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem026TwoSum;


public class TestProblem026TwoSum {
    @Test void t() { assertArrayEquals(new int[]{0,1}, Problem026TwoSum.twoSum(new int[]{2,7,11,15},9)); }
}

