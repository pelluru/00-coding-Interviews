package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem050TopologicalSort;


public class TestProblem050TopologicalSort {
    @Test void t(){ assertEquals(4, Problem050TopologicalSort.topo(4, new int[][]{ {0,1}, {0,2}, {1,3}, {2,3} }).size()); }
}

