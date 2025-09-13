package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem061HeapSort;


public class TestProblem061HeapSort {
    @Test void t(){
        int[] a={4,1,3,2}; Problem061HeapSort.sort(a);
        assertArrayEquals(new int[]{1,2,3,4}, a);
    }
}

