package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem053SpiralMatrix;


import java.util.*;
public class TestProblem053SpiralMatrix {
    @Test void t(){
        int[][] m={{1,2,3},{4,5,6},{7,8,9}};
        assertEquals(List.of(1,2,3,6,9,8,7,4,5), Problem053SpiralMatrix.spiral(m));
    }
}

