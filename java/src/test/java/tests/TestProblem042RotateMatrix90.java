package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem042RotateMatrix90;


public class TestProblem042RotateMatrix90 {
    @Test void t(){ int[][] m={ {1,2}, {3,4} }; Problem042RotateMatrix90.rotate(m); assertArrayEquals(new int[][]{ {3,1}, {4,2} }, m); }
}

