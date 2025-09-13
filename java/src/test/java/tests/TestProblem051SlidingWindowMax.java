package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem051SlidingWindowMax;


public class TestProblem051SlidingWindowMax {
    @Test void t(){ assertArrayEquals(new int[]{3,3,5,5,6,7}, Problem051SlidingWindowMax.maxSliding(new int[]{1,3,-1,-3,5,3,6,7},3)); }
}

