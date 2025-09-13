package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem051TransposeMatrix;


public class TestProblem051TransposeMatrix {
    @Test void t(){
        int[][] M={{1,2,3},{4,5,6}};
        int[][] T=Problem051TransposeMatrix.transpose(M);
        assertArrayEquals(new int[]{1,4}, T[0]);
        assertArrayEquals(new int[]{2,5}, T[1]);
        assertArrayEquals(new int[]{3,6}, T[2]);
    }
}

