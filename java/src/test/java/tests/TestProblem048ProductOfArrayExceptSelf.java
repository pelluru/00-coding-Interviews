package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem048ProductOfArrayExceptSelf;


public class TestProblem048ProductOfArrayExceptSelf {
    @Test void t(){ assertArrayEquals(new int[]{24,12,8,6}, Problem048ProductOfArrayExceptSelf.productExceptSelf(new int[]{1,2,3,4})); }
}

