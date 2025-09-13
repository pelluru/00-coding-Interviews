package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem012QuickSort;


public class TestProblem012QuickSort {
    @Test void t() { int[] a={3,1,2}; Problem012QuickSort.sort(a); assertArrayEquals(new int[]{1,2,3}, a); }
}

