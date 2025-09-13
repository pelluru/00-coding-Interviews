package tests;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import problems.Problem023MinHeapKSmallest;


import java.util.*;
public class TestProblem023MinHeapKSmallest {
    @Test void t() { assertEquals(java.util.Arrays.asList(1,2), Problem023MinHeapKSmallest.kSmallest(new int[]{3,1,2,4},2)); }
}

