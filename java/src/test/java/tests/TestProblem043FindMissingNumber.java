package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem043FindMissingNumber;


public class TestProblem043FindMissingNumber {
    @Test void t(){ assertEquals(2, Problem043FindMissingNumber.missing(new int[]{0,1,3})); }
}

