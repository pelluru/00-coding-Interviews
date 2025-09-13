package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem011MergeSort;


public class TestProblem011MergeSort {
    @Test void t() { assertArrayEquals(new int[]{1,2,3}, Problem011MergeSort.sort(new int[]{3,1,2})); }
}

