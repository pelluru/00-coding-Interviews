package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem050MatrixMultiply;


public class TestProblem050MatrixMultiply {
    @Test void t(){
        int[][] A={{1,2},{3,4}}, B={{5,6},{7,8}};
        int[][] R = Problem050MatrixMultiply.multiply(A,B);
        assertArrayEquals(new int[]{19,22}, R[0]);
        assertArrayEquals(new int[]{43,50}, R[1]);
    }
}

