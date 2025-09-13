package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem046IntervalMerge;


public class TestProblem046IntervalMerge {
    @Test void t(){ int[][] r=Problem046IntervalMerge.merge(new int[][]{ {1,3}, {2,6}, {8,10}, {15,18} }); assertEquals(3, r.length); }
}

